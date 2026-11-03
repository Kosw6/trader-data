"""Explicit one-company live recovery proof. Never polls or adopts existing jobs."""

import hashlib
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from collectors.common import LocalPipelineStorage, transformed_record_hash
from collectors.policy_clients import PolicySECClient
from pipeline.coordinated_financial import official_values
from pipeline.durable_raw import commit_raw
from pipeline.durable_requests import (
    Controller,
    DurableLease,
    DurableSession,
    canonical_hash,
)
from psycopg2.extras import Json
from transformers.sec_transformer import transform_filings, transform_financial_facts
from workers.coordinated_recovery import advance
from workers.coordinated_scheduler import audit
from workers.job_repository import claim_queued_item_by_id

ROOT = Path("/recovery-proof")
CIK = "0000006201"
GENERATION = "approved-one-company-recovery-20261102"
AGENT = "Revest Lab contact@revestlab.com"


class PublicController(Controller):
    def __init__(self):
        super().__init__("https://revestlab.com", os.environ["ADMIN_API_KEY"])
        # Same existing first-party HTTPS proxy used by the admin browser.
        self.url = (
            "https://revestlab.com/api/controller/api/v1/admin/pipeline/recovery/work"
        )


def save(name, value):
    with (ROOT / name).open("x", encoding="utf8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, default=str)


def read(name):
    return json.loads((ROOT / name).read_text(encoding="utf8"))


def connect():
    db = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=5)
    with db.cursor() as cur:
        cur.execute("SELECT current_schema()")
        assert cur.fetchone()[0] == "control649", "WRONG_DATABASE_NAMESPACE"
    db.rollback()
    return db


def fingerprint(db, boundaries=None):
    result = {}
    tables = [
        "company",
        "asset",
        "filing",
        "financial_fact",
        "financial_metric",
        "financial_metric_mapping",
        "price_bar",
        "source_object",
        "record_lineage",
        "pipeline_job",
        "pipeline_job_item",
        "pipeline_outbox",
    ]
    with db.cursor() as cur:
        for table in tables:
            if boundaries is None:
                cur.execute("SELECT coalesce(max(id),0) FROM control649." + table)
                maximum = cur.fetchone()[0]
            else:
                maximum = boundaries[table]["maximumId"]
            cur.execute(
                "SELECT count(*),md5(coalesce(string_agg(row_to_json(t)::text,E'\\n' ORDER BY id),'')) FROM control649."
                + table
                + " t WHERE id<=%s",
                (maximum,),
            )
            n, h = cur.fetchone()
            result[table] = {"maximumId": maximum, "count": n, "hash": h}
    db.rollback()
    return result


def collect():
    assert not (ROOT / "intent.json").exists(), "COLLECTION_INTENT_EXISTS_DO_NOT_REPLAY"
    db = connect()
    lease = client = None
    try:
        baseline = fingerprint(db)
        with db.cursor() as cur:
            cur.execute(
                "SELECT id FROM company WHERE cik::text IN (%s,%s)",
                (CIK, str(int(CIK))),
            )
            rows = cur.fetchall()
            assert len(rows) == 1, "COMPANY_NOT_UNIQUE"
            company = rows[0][0]
            cur.execute(
                "SELECT count(*) FROM pipeline_job WHERE status IN ('QUEUED','RUNNING','SCHEDULED')"
            )
            assert cur.fetchone()[0] == 0, "OTHER_COLLECTION_ACTIVE"
            cur.execute(
                "INSERT INTO pipeline_job(job_type,source,status,priority,requested_by,idempotency_key,parameters) VALUES('SEC_FINANCIAL_BACKFILL','SEC','QUEUED',0,'approved-recovery-verification',%s,%s) RETURNING id",
                (
                    GENERATION,
                    Json(
                        {
                            "pipelineMode": "COLLECT_ONLY",
                            "coordinatedProof": True,
                            "cik": CIK,
                        }
                    ),
                ),
            )
            job = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO pipeline_job_item(job_id,item_key,status) VALUES(%s,%s,'QUEUED') RETURNING id",
                (job, CIK),
            )
            item_id = cur.fetchone()[0]
        # Claim inside the creating transaction, before another worker can see it.
        item = claim_queued_item_by_id(
            db, item_id, source="SEC", job_type="SEC_FINANCIAL_BACKFILL"
        )
        assert item and item.job_id == job
        contract = {
            "target": f"company:{company}",
            "cik": CIK,
            "includeHistory": True,
            "historyScope": "one-approved-filing-page",
            "allFacts": False,
            "requestIntervalMs": 1000,
        }
        identity = {
            "provider": "SEC",
            "quotaScope": "sec-public-revestlab-production",
            "target": contract["target"],
            "dataset": "sec-all-history",
            "start": "0001-01-01",
            "end": "9999-12-31",
            "revision": canonical_hash(
                {"generation": GENERATION, "contract": contract}
            ),
        }
        policy = {
            "version": "coord-v1",
            "maxClaims": 12,
            "maxRequests": 3,
            "maxRetries": 0,
            "maxElapsedMs": 3600000,
            "leaseMs": 120000,
            "intervalMs": 1000,
            "baseDelayMs": 1000,
            "maxDelayMs": 10000,
            "rateLimitMs": 30000,
        }
        save(
            "intent.json",
            {
                "baseline": baseline,
                "jobId": job,
                "itemId": item_id,
                "companyId": company,
                "identity": identity,
                "policy": policy,
            },
        )
        lease = DurableLease(PublicController(), identity, policy)
        storage = LocalPipelineStorage(ROOT / "collected", item_id)
        assert storage.storage_backend == "LOCAL" and not storage.storage_base_uri
        client = PolicySECClient(AGENT)
        client.session.close()
        client.session = DurableSession("SEC", lease, AGENT)
        submissions = client.get_submissions(CIK)
        facts = client.get_company_facts(CIK)
        assert str(submissions["cik"]).zfill(10) == str(facts["cik"]).zfill(10) == CIK
        now = datetime.now(timezone.utc)
        sources = [
            storage.write_raw_json("SEC", kind, CIK, data, now)
            for kind, data in [("SUBMISSIONS", submissions), ("COMPANY_FACTS", facts)]
        ]
        official = read("official-meta.json")
        compact = official["url"].split("/")[-2]
        accession = compact[:10] + "-" + compact[10:12] + "-" + compact[12:]
        pages = [submissions.get("filings", {}).get("recent", {})]
        if accession not in pages[0].get("accessionNumber", []):
            year = "20" + compact[10:12]
            options = [
                x
                for x in submissions.get("filings", {}).get("files", [])
                if x.get("filingFrom", "")[:4] <= year <= x.get("filingTo", "")[:4]
            ]
            assert len(options) == 1, "HISTORICAL_PAGE_NOT_UNIQUE"
            historical = client.get_historical_submission_page(options[0]["name"])
            sources.append(
                storage.write_raw_json(
                    "SEC", "SUBMISSIONS_HISTORY", options[0]["name"], historical, now
                )
            )
            pages.append(historical)
        filings = transform_filings(submissions, pages, sources[0]["object_uri"], now)
        selected = [
            f
            for f in filings
            if f["form_type"] == "10-K"
            and f["accession_number"] == accession
            and f["document_url"] == official["url"]
        ]
        assert len(selected) == 1, "APPROVED_FILING_NOT_PRESENT"
        filing = selected[0]
        assert filing["report_date"] == "2019-12-31"
        candidates = [
            f
            for f in transform_financial_facts(
                facts, sources[1]["object_uri"], now, core_metrics_only=True
            )
            if f["taxonomy"] == "us-gaap"
            and f["source_tag"] == "Assets"
            and f["unit"] == "USD"
            and f["period_end"] == filing["report_date"]
            and f["accession_number"] == filing["accession_number"]
            and not f.get("period_start")
            and not f.get("dimensions")
        ]
        assert candidates and len({str(f["value"]) for f in candidates}) == 1, (
            "ASSET_FACT_NOT_UNIQUE"
        )
        fact = candidates[0]
        document = ROOT / "official.html"
        raw = document.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == official["sha256"], (
            "OFFICIAL_ARCHIVE_CHANGED"
        )
        requests_used = lease.api.call("GET", f"/{lease.key}")["requests"]
        values = official_values(raw, CIK, "Assets", "USD", None, fact["period_end"])
        assert values == {Decimal(str(fact["value"]))}, "OFFICIAL_FACT_VALUE_MISMATCH"
        save(
            "normalized-input.json",
            {
                "filing": filing,
                "fact": fact,
                "official": {
                    "path": str(document),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "url": official["url"],
                },
            },
        )
        committed = commit_raw(db, lease, sources, ROOT, item, contract)
        key = lease.key
        api = PublicController()
        stored = api.call("GET", f"/{key}")
        assert stored["phase"] == "RAW_READY" and stored["requests"] == requests_used
        api.call("POST", f"/{key}/pause", {})
        paused = api.call("GET", f"/{key}")
        assert paused["state"] == "PAUSED" and paused["phase"] == "RAW_READY"
        save(
            "checkpoint.json",
            {
                "workKey": key,
                "receipt": committed,
                "work": paused,
                "sourceHashes": [s["content_hash"] for s in sources],
                "stoppedAt": "RAW_READY",
                "publicMarketWrites": 0,
            },
        )
        api.close()
        print(
            json.dumps(
                {
                    "status": "PAUSED_AT_VERIFIED_RAW_CHECKPOINT",
                    "workKey": key,
                    "jobId": job,
                    "sourceObjects": len(sources),
                    "requests": requests_used,
                    "processWillExit": True,
                }
            )
        )
    finally:
        if client:
            client.session.close()
        if lease:
            lease.close()
        db.close()


def resume():
    checkpoint = read("checkpoint.json")
    intent = read("intent.json")
    inputs = read("normalized-input.json")
    assert not (ROOT / "completed.json").exists(), "COMPLETION_EXISTS_READ_FIRST"
    db = connect()
    api = PublicController()
    key = checkpoint["workKey"]
    try:
        assert fingerprint(db, intent["baseline"]) == intent["baseline"], (
            "EXISTING_DATA_CHANGED"
        )
        work = api.call("GET", f"/{key}")
        assert (
            work["state"] == "PAUSED"
            and work["phase"] == "RAW_READY"
            and work["requests"] == checkpoint["work"]["requests"]
        )
        api.call("POST", f"/{key}/resume", {})
        # An unregistered placeholder plan is used only to dispatch the durable raw intent.
        advance(
            db,
            api,
            {"workKey": key, "kind": "FINANCIAL"},
            ROOT,
            enabled=True,
            max_steps=1,
        )
        from backfills.sec_financial.backfill import (
            upsert_filings,
            upsert_financial_facts,
        )

        company = intent["companyId"]
        f = inputs["fact"]
        stats = Counter()
        skipped = []
        with db.cursor() as cur:
            cur.execute("SELECT count(*) FROM financial_fact")
            assert cur.fetchone()[0] == 0, "STAGING_FACTS_NOT_EMPTY"
            cur.execute(
                "SELECT count(*) FROM filing WHERE accession_number=%s",
                (inputs["filing"]["accession_number"],),
            )
            assert cur.fetchone()[0] == 0, "FILING_ALREADY_PRESENT"
            cur.execute("SELECT id FROM financial_metric WHERE metric_code='ASSETS'")
            metric = cur.fetchone()
            if metric:
                metric_id = metric[0]
            else:
                cur.execute(
                    "INSERT INTO financial_metric(metric_code,name,statement_type,value_type,default_unit) VALUES('ASSETS','Total assets','BALANCE_SHEET','INSTANT','USD') RETURNING id"
                )
                metric_id = cur.fetchone()[0]
            filings = upsert_filings(cur, company, [inputs["filing"]])
            facts = upsert_financial_facts(
                cur, company, [f], filings, {"ASSETS": metric_id}, stats, skipped
            )
            assert not skipped and len(facts) == 1
            fact_id = facts[f["source_fact_hash"]]
            source_id = checkpoint["receipt"]["sourceIds"][1]
            output_hash = transformed_record_hash(f)
            cur.execute(
                "INSERT INTO record_lineage(source_object_id,target_table,target_record_id,target_record_key,pipeline_job_id,transformer_name,transformer_version,output_hash,processed_at) VALUES(%s,'financial_fact',%s,%s,%s,'sec-transformer','2.0.0',%s,clock_timestamp())",
                (
                    source_id,
                    fact_id,
                    f["source_fact_hash"],
                    intent["jobId"],
                    output_hash,
                ),
            )
        db.commit()
        entry = {
            "factId": fact_id,
            "metricCode": "ASSETS",
            "tag": "Assets",
            "unit": "USD",
            "start": None,
            "end": f["period_end"],
            "value": str(f["value"]),
            "form": f["form_type"],
            "accession": f["accession_number"],
            "factHash": f["source_fact_hash"],
            "lineageHash": canonical_hash(
                [(source_id, output_hash, checkpoint["sourceHashes"][1])]
            ),
            "official": inputs["official"],
        }
        plan = {
            "kind": "FINANCIAL",
            "policyVersion": "exact-us-gaap-v1",
            "workKey": key,
            "companyId": company,
            "approvedMappings": [entry],
        }
        save("plan.json", plan)
        steps = advance(db, api, plan, ROOT, enabled=True, max_steps=4)
        final = api.call("GET", f"/{key}")
        assert (
            final["phase"] == final["state"] == "COMPLETE"
            and final["requests"] == checkpoint["work"]["requests"]
            and final["retries"] == 0
        )
        assert advance(db, api, plan, ROOT, enabled=True, max_steps=5) == []
        verified = audit(db, {key: plan})
        assert verified["verifiedSuccess"] == 1 and verified["unresolvedTotal"] == 0
        assert fingerprint(db, intent["baseline"]) == intent["baseline"], (
            "EXISTING_DATA_CHANGED"
        )
        result = {
            "status": "VERIFIED_AFTER_NEW_PROCESS_RESUME",
            "workKey": key,
            "work": final,
            "audit": verified,
            "newFactId": fact_id,
            "steps": steps,
            "providerRequestsBefore": checkpoint["work"]["requests"],
            "providerRequestsAfter": final["requests"],
            "sourceObjectsRecollected": 0,
            "duplicatePublicationReplay": "NO_OP",
            "existingRowsFingerprintUnchanged": True,
            "publicationScope": "control649 staging only; public market data unchanged",
            "kafkaTransport": "durable outbox recorded; Kafka transmission not exercised",
        }
        save("completed.json", result)
        print(
            json.dumps(
                {
                    k: result[k]
                    for k in [
                        "status",
                        "workKey",
                        "providerRequestsAfter",
                        "sourceObjectsRecollected",
                        "duplicatePublicationReplay",
                        "existingRowsFingerprintUnchanged",
                        "publicationScope",
                        "kafkaTransport",
                    ]
                }
            )
        )
    finally:
        api.close()
        db.close()


if __name__ == "__main__":
    assert os.environ.get("APPROVED_RECOVERY_PROOF") == GENERATION, (
        "EXPLICIT_PROOF_OPT_IN_REQUIRED"
    )
    try:
        {"collect": collect, "resume": resume}[sys.argv[1]]()
    except Exception as e:  # noqa: BLE001 -- redact all provider/SQL details at the outer boundary
        print(
            json.dumps(
                {
                    "status": "HELD_NO_AUTOMATIC_RETRY",
                    "errorType": type(e).__name__,
                    "safeCode": str(e)
                    if re.fullmatch(r"[A-Z_]{1,80}", str(e))
                    else None,
                }
            )
        )
        raise SystemExit(1) from None
