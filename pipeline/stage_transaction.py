"""Opt-in phase transactions. No retry, provider HTTP or implicit lease takeover."""

import json

import psycopg2

from pipeline.durable_requests import CoordinationError, canonical_hash

TRANSITIONS = {
    "ETL_READY": "CANDIDATE",
    "CANDIDATE": "VERIFIED",
    "VERIFIED": "PUBLISHED",
    "PUBLISHED": "COMPLETE",
}


def apply_phase(db, lease, phase, digest, write):
    if (
        db.autocommit
        or db.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE
    ):
        raise CoordinationError("invalid_contract")
    if phase not in TRANSITIONS or len(digest) != 64:
        raise CoordinationError("invalid_contract")
    receipt_id = phase.lower() + "-" + digest
    try:
        with db.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='2s'")
            cur.execute("SET LOCAL statement_timeout='30s'")
            cur.execute(
                "SELECT identity,policy FROM pipeline_recovery_work WHERE work_key=%s FOR UPDATE",
                (lease["workKey"],),
            )
            if not cur.fetchone():
                raise CoordinationError("invalid_contract")
            require_lease(cur, lease, phase)
            cur.execute(
                "SELECT job_id,item_id,outbox_id FROM pipeline_recovery_raw_intent WHERE work_key=%s",
                (lease["workKey"],),
            )
            intent = cur.fetchone()
            if not intent or intent[2] is None:
                raise CoordinationError("review_required")
            cur.execute(
                "SELECT status FROM pipeline_job WHERE id=%s FOR UPDATE", (intent[0],)
            )
            if cur.fetchone()[0] not in {"RUNNING", "PARTIAL_SUCCESS", "SUCCESS"}:
                raise CoordinationError("review_required")
            cur.execute(
                "SELECT status,checkpoint FROM pipeline_job_item WHERE id=%s AND job_id=%s FOR UPDATE",
                (intent[1], intent[0]),
            )
            item = cur.fetchone()
            if (
                not item
                or item[0] != "COLLECTED"
                or item[1].get("workKey") != lease["workKey"]
            ):
                raise CoordinationError("review_required")
            result = write(cur)
            require_lease(cur, lease, phase)
            to = TRANSITIONS[phase]
            cur.execute(
                """INSERT INTO pipeline_recovery_receipt(work_key,receipt_id,from_phase,to_phase,content_hash,fence)
                VALUES(%s,%s,%s,%s,%s,%s)""",
                (lease["workKey"], receipt_id, phase, to, digest, lease["fence"]),
            )
            cur.execute(
                """UPDATE pipeline_recovery_work SET phase=%s,state=%s,owner='',lease_until=NULL,progress_at=clock_timestamp()
                WHERE work_key=%s AND owner=%s AND fence=%s AND lease_until>clock_timestamp() AND deadline>clock_timestamp()""",
                (
                    to,
                    "COMPLETE" if to == "COMPLETE" else "READY",
                    lease["workKey"],
                    lease["owner"],
                    lease["fence"],
                ),
            )
            if cur.rowcount != 1:
                raise CoordinationError("lease_lost")
        try:
            db.commit()
        except psycopg2.Error:
            raise CoordinationError("commit_uncertain") from None
        return {"receiptId": receipt_id, "hash": digest, "result": result}
    except BaseException:
        db.rollback()
        raise


def require_lease(cur, lease, phase):
    cur.execute(
        """SELECT 1 FROM pipeline_recovery_work WHERE work_key=%s AND owner=%s AND fence=%s
      AND phase=%s AND state='LEASED' AND lease_until>clock_timestamp() AND deadline>clock_timestamp()
      AND NOT EXISTS(SELECT 1 FROM pipeline_recovery_request WHERE work_key=%s AND outcome='PENDING')""",
        (lease["workKey"], lease["owner"], lease["fence"], phase, lease["workKey"]),
    )
    if cur.fetchone() is None:
        raise CoordinationError("lease_lost")


def stage_candidate(db, lease, plan, content, validate):
    if plan.get("workKey") != lease["workKey"]:
        raise CoordinationError("contract_conflict")
    digest = canonical_hash(content)
    plan_hash = canonical_hash(plan)

    def write(cur):
        validate(cur, plan, content)
        cur.execute(
            """INSERT INTO pipeline_recovery_candidate(work_key,kind,plan,plan_hash,content,content_hash)
          VALUES(%s,%s,%s::jsonb,%s,%s::jsonb,%s)""",
            (
                lease["workKey"],
                plan["kind"],
                json.dumps(plan),
                plan_hash,
                json.dumps(content),
                digest,
            ),
        )
        return {"rows": len(content)}

    return apply_phase(db, lease, "ETL_READY", digest, write)


def verify_candidate(db, lease, expected_plan_hash, validate):
    def read(cur):
        cur.execute(
            "SELECT plan,plan_hash,content,content_hash FROM pipeline_recovery_candidate WHERE work_key=%s FOR UPDATE",
            (lease["workKey"],),
        )
        row = cur.fetchone()
        if (
            not row
            or row[0].get("workKey") != lease["workKey"]
            or row[1] != expected_plan_hash
            or canonical_hash(row[0]) != row[1]
            or canonical_hash(row[2]) != row[3]
        ):
            raise CoordinationError("contract_conflict")
        return row

    # One read for digest, re-read locked inside transaction before any writes.
    with db.cursor() as cur:
        row = read(cur)
    db.rollback()

    def write(cur):
        current = read(cur)
        if current[3] != row[3]:
            raise CoordinationError("contract_conflict")
        evidence = validate(cur, current[0], current[2])
        cur.execute(
            """INSERT INTO pipeline_recovery_verification(work_key,plan_hash,content_hash,evidence)
          VALUES(%s,%s,%s,%s::jsonb)""",
            (lease["workKey"], current[1], current[3], json.dumps(evidence)),
        )
        return evidence

    return apply_phase(db, lease, "CANDIDATE", row[3], write)


def confirm_publication(db, independent_read, lease, plan_hash, verify):
    fields = ("host", "port", "dbname", "user", "options")
    if db is independent_read or any(
        db.get_dsn_parameters().get(k) != independent_read.get_dsn_parameters().get(k)
        for k in fields
    ):
        raise CoordinationError("invalid_contract")
    independent_read.set_session(readonly=True, isolation_level="REPEATABLE READ")
    with independent_read.cursor() as cur:
        evidence = verify(cur, lease["workKey"], plan_hash)
    independent_read.rollback()

    def write(cur):
        if verify(cur, lease["workKey"], plan_hash) != evidence:
            raise CoordinationError("contract_conflict")
        return evidence

    return apply_phase(db, lease, "PUBLISHED", evidence["hash"], write)
