"""Explicit exact-scope revision promotion; no automatic latest/legacy rewrite."""

from datetime import date, datetime, timezone

from psycopg2.extras import Json

from pipeline.coordinated_price import load_raw
from pipeline.durable_requests import CoordinationError, canonical_hash
from pipeline.financial_revision import revision_publication_verifier


def promote(db, approval, raw_root):
    required = {
        "workKey",
        "planHash",
        "revisionHash",
        "scopeHash",
        "expectedPreviousRevisionId",
    }
    if set(approval) != required or db.get_transaction_status() != 0:
        raise CoordinationError("invalid_contract")
    digest = canonical_hash(approval)
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT phase,state FROM pipeline_recovery_work WHERE work_key=%s FOR UPDATE",
                (approval["workKey"],),
            )
            if cur.fetchone() != ("COMPLETE", "COMPLETE"):
                raise CoordinationError("review_required")
            revision_publication_verifier(raw_root)(
                cur, approval["workKey"], approval["planHash"]
            )
            cur.execute(
                "SELECT id,company_id,revision FROM pipeline_financial_revision WHERE revision_hash=%s",
                (approval["revisionHash"],),
            )
            row = cur.fetchone()
            if not row:
                raise CoordinationError("contract_conflict")
            rid, company, revision = row
            scope = revision["scope"]
            if canonical_hash(scope) != approval["scopeHash"]:
                raise CoordinationError("contract_conflict")
            cur.execute("SELECT id FROM company WHERE id=%s FOR UPDATE", (company,))
            cur.execute(
                "SELECT 1 FROM pipeline_financial_revision_origin WHERE revision_id=%s AND work_key=%s",
                (rid, approval["workKey"]),
            )
            if cur.fetchone() is None:
                raise CoordinationError("contract_conflict")
            cur.execute(
                "SELECT revision_id,approval_id FROM pipeline_financial_revision_active WHERE scope_hash=%s FOR UPDATE",
                (approval["scopeHash"],),
            )
            previous = cur.fetchone()
            cur.execute(
                "SELECT id,revision_id FROM pipeline_financial_revision_approval WHERE approval_hash=%s",
                (digest,),
            )
            replay = cur.fetchone()
            if replay:
                if previous != (rid, replay[0]) or replay[1] != rid:
                    raise CoordinationError("contract_conflict")
                db.rollback()
                return {"approvalId": replay[0], "revisionId": rid, "noChange": True}
            if (previous[0] if previous else None) != approval[
                "expectedPreviousRevisionId"
            ]:
                raise CoordinationError("contract_conflict")
            _, sources = load_raw(cur, approval["workKey"], raw_root)
            recent = next(
                body["filings"]["recent"]
                for _, provider, kind, body in sources
                if provider == "SEC" and kind == "SUBMISSIONS"
            )
            index = recent["accessionNumber"].index(revision["accession"])
            filed = date.fromisoformat(recent["filingDate"][index])
            if (
                not date.fromisoformat(scope["end"])
                <= filed
                <= datetime.now(timezone.utc).date()
            ):
                raise CoordinationError("review_required")
            cur.execute(
                "SELECT plan FROM pipeline_recovery_candidate WHERE work_key=%s",
                (approval["workKey"],),
            )
            plan = cur.fetchone()[0]
            entries = [
                e
                for e in plan["revisions"]
                if e["tag"] == scope["tag"]
                and e["start"] == scope["start"]
                and e["end"] == scope["end"]
                and e["accession"] == revision["accession"]
                and e["form"] == revision["form"]
            ]
            if len(entries) != 1:
                raise CoordinationError("contract_conflict")
            fact = {
                "metric_code": scope["metricCode"],
                "value": revision["value"],
                "unit": scope["unit"],
                "period_start": scope["start"],
                "period_end": scope["end"],
                "taxonomy": scope["taxonomy"],
                "source_tag": scope["tag"],
                "dimensions": scope["dimensions"],
                "accession_number": revision["accession"],
                "form_type": revision["form"],
                "filing_date": str(filed),
                "filing_report_date": scope["end"],
                "fiscal_period": entries[0]["fiscalPeriod"],
                "source_url": entries[0]["official"]["url"],
            }
            cur.execute(
                "INSERT INTO pipeline_financial_revision_approval(scope_hash,revision_id,work_key,previous_revision_id,approval_hash,public_fact,public_fact_hash) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (
                    approval["scopeHash"],
                    rid,
                    approval["workKey"],
                    approval["expectedPreviousRevisionId"],
                    digest,
                    Json(fact),
                    canonical_hash(fact),
                ),
            )
            aid = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO pipeline_financial_revision_active(scope_hash,company_id,metric_code,period_start,period_end,revision_id,approval_id) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(scope_hash) DO UPDATE SET revision_id=EXCLUDED.revision_id,approval_id=EXCLUDED.approval_id",
                (
                    approval["scopeHash"],
                    company,
                    scope["metricCode"],
                    scope["start"],
                    scope["end"],
                    rid,
                    aid,
                ),
            )
        db.commit()  # Commit exceptions are not retried or called success.
        return {"approvalId": aid, "revisionId": rid, "noChange": False}
    except Exception:
        db.rollback()
        raise
