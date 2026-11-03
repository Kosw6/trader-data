"""Bounded opt-in post-raw recovery. No provider fetch, pause reset or budget reset."""

import argparse
import json
import os
import time
from contextlib import closing
from itertools import pairwise
from pathlib import Path

import psycopg2
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash

from workers.coordinated_consumer import load_registry
from workers.coordinated_recovery import advance

EDGES = {
    ("FETCH_RAW", "RAW_READY"),
    ("RAW_READY", "ETL_READY"),
    ("ETL_READY", "CANDIDATE"),
    ("CANDIDATE", "VERIFIED"),
    ("VERIFIED", "PUBLISHED"),
    ("PUBLISHED", "COMPLETE"),
}


def audit(db, registry):
    if (
        db.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE
        or not registry
        or len(registry) > 1000
    ):
        raise CoordinationError("invalid_contract")
    counts = dict.fromkeys(
        [
            "verifiedSuccess",
            "verifiedNoChange",
            "heldReview",
            "failedFinal",
            "unfinished",
            "commitUnresolved",
        ],
        0,
    )
    results = {}
    try:
        with db.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            for key, plan in registry.items():
                if plan.get("workKey") != key:
                    raise CoordinationError("contract_conflict")
                cur.execute(
                    """SELECT w.phase,w.state,w.owner,w.lease_until,w.deadline,clock_timestamp(),w.claims,w.policy,
                  c.content_hash,c.plan_hash,c.published_result,
                  EXISTS(SELECT 1 FROM pipeline_recovery_request r WHERE r.work_key=w.work_key AND r.outcome='PENDING')
                  FROM pipeline_recovery_work w LEFT JOIN pipeline_recovery_candidate c USING(work_key) WHERE w.work_key=%s""",
                    (key,),
                )
                row = cur.fetchone()
                if not row:
                    raise CoordinationError("contract_conflict")
                (
                    phase,
                    state,
                    owner,
                    lease_until,
                    deadline,
                    now,
                    claims,
                    policy,
                    digest,
                    plan_hash,
                    result,
                    pending,
                ) = row
                if plan_hash and plan_hash != canonical_hash(plan):
                    raise CoordinationError("contract_conflict")
                cur.execute(
                    "SELECT from_phase,to_phase,content_hash FROM pipeline_recovery_receipt WHERE work_key=%s",
                    (key,),
                )
                receipts = cur.fetchall()
                chain = [
                    "FETCH_RAW",
                    "RAW_READY",
                    "ETL_READY",
                    "CANDIDATE",
                    "VERIFIED",
                    "PUBLISHED",
                    "COMPLETE",
                ]
                index = chain.index(phase)
                expected_edges = set(pairwise(chain[: index + 1]))
                prefix_ok = (
                    len(receipts) == index
                    and {(a, b) for a, b, _ in receipts} == expected_edges
                    and all(
                        h == digest
                        for a, _, h in receipts
                        if a not in {"FETCH_RAW", "RAW_READY"}
                    )
                    and len(
                        {h for a, _, h in receipts if a in {"FETCH_RAW", "RAW_READY"}}
                    )
                    <= 1
                )
                eligible = (
                    state in {"READY", "LEASED"}
                    and phase
                    in {"RAW_READY", "ETL_READY", "CANDIDATE", "VERIFIED", "PUBLISHED"}
                    and not pending
                    and (lease_until is None or lease_until <= now)
                    and deadline > now
                    and claims < policy["maxClaims"]
                )
                eligible = eligible and prefix_ok
                if phase == "COMPLETE" or state == "COMPLETE":
                    cur.execute(
                        "SELECT from_phase,to_phase,content_hash FROM pipeline_recovery_receipt WHERE work_key=%s",
                        (key,),
                    )
                    receipts = cur.fetchall()
                    edges = {(a, b) for a, b, _ in receipts}
                    valid = (
                        prefix_ok
                        and phase == state == "COMPLETE"
                        and not owner
                        and lease_until is None
                        and not pending
                        and result is not None
                        and edges == EDGES
                        and len(receipts) == len(EDGES)
                        and all(
                            h == digest
                            for a, _, h in receipts
                            if a not in {"FETCH_RAW", "RAW_READY"}
                        )
                    )
                    if not valid:
                        bucket = "commitUnresolved"
                    elif result.get("needsReview"):
                        bucket = "heldReview"
                    elif result.get("inserted") == 0:
                        bucket = "verifiedNoChange"
                    else:
                        bucket = "verifiedSuccess"
                elif pending or not prefix_ok:
                    bucket = "commitUnresolved"
                elif state == "REVIEW":
                    bucket = "heldReview"
                else:
                    bucket = "unfinished"
                counts[bucket] += 1
                results[key] = {
                    "phase": phase,
                    "state": state,
                    "bucket": bucket,
                    "eligible": eligible,
                }
    finally:
        db.rollback()
    unfinished = counts["unfinished"] + counts["commitUnresolved"]
    return {
        "planned": len(registry),
        **counts,
        "unresolvedTotal": unfinished,
        "reviewComplete": unfinished == 0,
        "work": results,
    }


def run_cycle(db, api, registry, raw_root, stop_file):
    if Path(stop_file).exists():
        return {"state": "PAUSED_USER", "advanced": []}
    before = audit(db, registry)
    advanced = []
    for key, status in before["work"].items():
        if Path(stop_file).exists():
            break  # current work is the atomic bounded unit
        if status["eligible"]:
            advance(db, api, registry[key], raw_root, enabled=True, max_steps=5)
            advanced.append(key)
    return {
        "state": "PAUSED_USER" if Path(stop_file).exists() else "CHECKED",
        "advanced": advanced,
        "audit": audit(db, registry),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", required=True)
    p.add_argument("--registry-sha256", required=True)
    p.add_argument("--raw-root", required=True)
    p.add_argument("--stop-file", required=True)
    p.add_argument("--cycles", type=int, default=1)
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--audit-only", action="store_true")
    args = p.parse_args()
    if not args.audit_only and os.getenv("COORDINATED_STAGES_ENABLED") != "1":
        raise CoordinationError("invalid_contract")
    if not 1 <= args.cycles <= 1000 or not 30 <= args.interval <= 3600:
        raise CoordinationError("invalid_contract")
    registry = load_registry(args.registry, args.registry_sha256)
    with (
        closing(psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)) as db,
        closing(
            Controller(os.environ["CONTROLLER_URL"], os.environ["ADMIN_API_KEY"])
        ) as api,
    ):
        for cycle in range(args.cycles):
            result = (
                audit(db, registry)
                if args.audit_only
                else run_cycle(db, api, registry, args.raw_root, args.stop_file)
            )
            print(json.dumps(result))
            if (
                args.audit_only
                or result["state"] == "PAUSED_USER"
                or result["audit"]["reviewComplete"]
            ):
                break
            if cycle + 1 < args.cycles:
                for _ in range(args.interval):
                    if Path(args.stop_file).exists():
                        break
                    time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001 -- never log DB/auth details
        print('{"state":"HELD","automaticRetry":false}')
        raise SystemExit(1) from None
