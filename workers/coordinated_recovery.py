"""Explicit opt-in, bounded stage runner and read-only operations snapshot.

Never starts a server, fetches a provider, retries an uncertain write, or discovers
jobs outside the operator's pinned plan. Invoke with python -m workers.coordinated_recovery.
"""

import argparse
import hashlib
import html
import json
import os
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from pipeline.coordinated_financial import (
    financial_publication_verifier,
    financial_validator,
    publish_financial,
)
from pipeline.coordinated_price import (
    price_content,
    price_validator,
    publish_price,
    verify_published_price,
)
from pipeline.db_connections import independent_connection
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash
from pipeline.financial_revision import (
    publish_revisions,
    revision_publication_verifier,
    revision_validator,
)
from pipeline.stage_transaction import (
    confirm_publication,
    stage_candidate,
    verify_candidate,
)


def pinned_plan(path, expected_hash):
    raw = Path(path).read_bytes()
    if len(raw) > 1_000_000 or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise CoordinationError("invalid_contract")
    plan = json.loads(raw)
    if (
        plan.get("kind") not in {"PRICE", "FINANCIAL", "FINANCIAL_REVISION", "NEWS"}
        or len(plan.get("workKey", "")) != 64
    ):
        raise CoordinationError("invalid_contract")
    return plan


def advance(db, api, plan, allowed_root, *, enabled=False, max_steps=1):
    if not enabled or max_steps not in range(1, 6):
        raise CoordinationError("invalid_contract")
    key = plan["workKey"]
    result = []
    for _ in range(max_steps):
        work = api.call("GET", f"/{key}")
        if work["state"] in {"PAUSED", "REVIEW"}:
            raise CoordinationError("review_required")
        if work["phase"] == "COMPLETE":
            return result
        if work["phase"] not in {
            "RAW_READY",
            "ETL_READY",
            "CANDIDATE",
            "VERIFIED",
            "PUBLISHED",
        }:
            raise CoordinationError("review_required")
        # Ensure HTTP controller and SQL adapter are on the same work/identity.
        with db.cursor() as cur:
            cur.execute(
                "SELECT identity,policy,phase,fence FROM pipeline_recovery_work WHERE work_key=%s",
                (key,),
            )
            same = cur.fetchone()
        db.rollback()
        if same != (work["identity"], work["policy"], work["phase"], work["fence"]):
            raise CoordinationError("contract_conflict")
        claimed = api.call(
            "POST", f"/{key}/claim", {"owner": "pipeline-" + uuid.uuid4().hex}
        )
        lease = {"workKey": key, "owner": claimed["owner"], "fence": claimed["fence"]}
        phase = claimed["phase"]
        try:
            if phase == "RAW_READY":
                value = api.call("POST", f"/{key}/dispatch-raw", lease)
            else:
                financial = plan["kind"] == "FINANCIAL"
                revision = plan["kind"] == "FINANCIAL_REVISION"
                news = plan["kind"] == "NEWS"
                validate = (
                    financial_validator(allowed_root)
                    if financial
                    else price_validator(allowed_root)
                )
                verify = (
                    financial_publication_verifier(allowed_root)
                    if financial
                    else verify_published_price
                )
                if revision:
                    validate, verify = (
                        revision_validator(allowed_root),
                        revision_publication_verifier(allowed_root),
                    )
                if news:
                    from pipeline.coordinated_news import (
                        news_publication_verifier,
                        news_validator,
                    )

                    validate, verify = (
                        news_validator(allowed_root),
                        news_publication_verifier(allowed_root),
                    )
                if phase == "ETL_READY":
                    with db.cursor() as cur:
                        if news:
                            from pipeline.coordinated_news import news_content

                            content = news_content(cur, plan, allowed_root)
                        else:
                            content = (
                                plan["revisions"]
                                if revision
                                else plan["approvedMappings"]
                                if financial or revision
                                else price_content(cur, plan, allowed_root)
                            )
                    db.rollback()
                    value = stage_candidate(db, lease, plan, content, validate)
                elif phase == "CANDIDATE":
                    value = verify_candidate(db, lease, canonical_hash(plan), validate)
                elif phase == "VERIFIED":
                    from pipeline.coordinated_news import publish_news

                    value = (
                        publish_news
                        if news
                        else publish_revisions
                        if revision
                        else publish_financial
                        if financial
                        else publish_price
                    )(db, lease, canonical_hash(plan), allowed_root)
                elif phase == "PUBLISHED":
                    with closing(independent_connection(db)) as read:
                        value = confirm_publication(
                            db, read, lease, canonical_hash(plan), verify
                        )
                else:
                    raise CoordinationError("review_required")
            result.append({"from": phase, "result": value})
        except Exception as exc:  # noqa: BLE001 -- redact SQL/provider details at the boundary
            db.rollback()
            # Data/contract failures do not become an infinite reclaim loop.
            # Unknown commits/API failures stay untouched for independent reconciliation.
            if isinstance(exc, (ValueError, KeyError)) or (
                isinstance(exc, CoordinationError)
                and str(exc)
                in {"review_required", "contract_conflict", "invalid_contract"}
            ):
                try:
                    api.call("POST", f"/{key}/hold-owned", lease)
                except CoordinationError:
                    raise CoordinationError("coordination_unavailable") from None
            raise CoordinationError(
                str(exc) if isinstance(exc, CoordinationError) else "review_required"
            ) from None
    return result


def render_status(rows):
    columns = (
        "provider",
        "target",
        "phase",
        "state",
        "requests",
        "remainingRequests",
        "retries",
        "pendingRequests",
        "outboxStatus",
        "heartbeatAt",
        "progressAt",
    )
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(str(row.get(c, '')))}</td>" for c in columns)
        + "</tr>"
        for row in rows
    )
    return (
        "<!doctype html><meta charset='utf-8'><title>Pipeline recovery snapshot</title><style>body{font:15px system-ui;background:#101722;color:#e9eef5;padding:32px}table{border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #344154;text-align:left}</style><h1>Pipeline recovery</h1><p>Read-only snapshot. RAW_READY / PUBLISHED are not verified completion. Heartbeat is not proof of progress.</p><table><thead><tr>"
        + "".join(f"<th>{c}</th>" for c in columns)
        + "</tr></thead><tbody>"
        + body
        + "</tbody></table>"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "advance"))
    parser.add_argument("--plan")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--raw-root")
    parser.add_argument("--steps", type=int, default=1)
    parser.add_argument("--html", type=Path)
    parser.add_argument("--after", default="")
    args = parser.parse_args()
    try:
        with closing(
            Controller(os.environ["CONTROLLER_URL"], os.environ["ADMIN_API_KEY"])
        ) as api:
            if args.action == "status":
                rows = api.call("GET", "?after=" + args.after if args.after else "")[
                    "rows"
                ]
                if args.html:
                    # New snapshot only; never overwrite an existing file.
                    with args.html.open("x", encoding="utf-8") as out:
                        out.write(render_status(rows))
                print(
                    json.dumps(
                        {
                            "checkedAt": datetime.now(timezone.utc).isoformat(),
                            "rows": rows,
                            "pageLimit": 100,
                            "nextAfter": rows[-1]["key"] if len(rows) == 100 else None,
                        }
                    )
                )
            else:
                if os.getenv("COORDINATED_STAGES_ENABLED") != "1" or not args.raw_root:
                    raise CoordinationError("invalid_contract")
                plan = pinned_plan(args.plan, args.plan_sha256)
                with closing(
                    psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)
                ) as db:
                    print(
                        json.dumps(
                            advance(
                                db,
                                api,
                                plan,
                                args.raw_root,
                                enabled=True,
                                max_steps=args.steps,
                            )
                        )
                    )
    except Exception:  # noqa: BLE001 -- never print connection details or secrets
        print(
            json.dumps(
                {
                    "status": "HELD",
                    "message": "Stage not completed; inspect durable receipts before retry. No provider recollection performed.",
                }
            )
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
