"""Revision preservation uses isolated DB, actual migration and synthetic XBRL."""

# ruff: noqa: F811
import copy
import hashlib
import os
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pytest
from collectors.common import LocalPipelineStorage
from pipeline.durable_raw import commit_raw
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash
from workers.coordinated_recovery import advance

from tests.test_coordinated_financial import financial_fixture
from tests.test_coordinated_stages import stages  # noqa: F401
from tests.test_durable_integration import db, lease_for, make_item  # noqa: F401


def test_revision_decimal_keeps_all_digits():
    from decimal import Decimal, localcontext

    from pipeline.financial_revision import exact_decimal

    with localcontext() as context:
        context.prec = 6
        value = "12345678901234567890123456789012345678.1234567890"
        assert exact_decimal(Decimal(value)) == value[:-1]
        assert exact_decimal(Decimal("1.00")) == "1"
        assert exact_decimal(Decimal("-0.00")) == "0"


def revision_plan(db, base, root, value):
    root.mkdir()
    with db.cursor() as cur:
        cur.execute("SELECT to_regclass('pipeline_financial_revision')")
        if cur.fetchone()[0] is None:
            cur.execute(
                (
                    Path(__file__).resolve().parents[2]
                    / "trader-controller/db/migration/V046__financial_revision_evidence.sql"
                ).read_text()
            )
        cur.execute("SELECT cik FROM company WHERE id=%s", (base["companyId"],))
        cik = cur.fetchone()[0]
    db.commit()
    item = make_item(db)
    identity = {
        "provider": "SEC",
        "quotaScope": uuid.uuid4().hex,
        "target": f"company:{base['companyId']}",
        "dataset": "sec-all-history",
        "start": "0001-01-01",
        "end": "9999-12-31",
        "revision": canonical_hash(uuid.uuid4().hex),
    }
    lease = lease_for(identity=identity)
    entry = copy.deepcopy(base["approvedMappings"][0])
    original = Path(entry["official"]["path"]).read_bytes()
    document = original.replace(
        b">123</ix:nonFraction>", f">{value}</ix:nonFraction>".encode()
    )
    path = root / "official.xml"
    path.write_bytes(document)
    entry["value"], entry["fiscalPeriod"] = str(value), "FY"
    entry["official"].update(
        path=str(path), sha256=hashlib.sha256(document).hexdigest()
    )
    fact = {
        "start": entry["start"],
        "end": entry["end"],
        "accn": entry["accession"],
        "form": entry["form"],
        "val": value,
    }
    store = LocalPipelineStorage(root, item.item_id)
    store.storage_backend, store.storage_base_uri = "LOCAL", ""
    now = datetime.now(timezone.utc)
    sources = [
        store.write_raw_json("SEC", kind, cik, raw, now)
        for kind, raw in [
            (
                "SUBMISSIONS",
                {
                    "cik": cik,
                    "filings": {
                        "recent": {
                            "accessionNumber": [entry["accession"]],
                            "form": [entry["form"]],
                            "reportDate": [entry["end"]],
                            "primaryDocument": ["annual.htm"],
                            "filingDate": ["2026-02-01"],
                        }
                    },
                },
            ),
            (
                "COMPANY_FACTS",
                {
                    "cik": cik,
                    "facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [fact]}}}},
                },
            ),
        ]
    ]
    commit_raw(
        db, lease, sources, root, item, {"target": identity["target"], "cik": cik}
    )
    lease.close()
    return {
        "kind": "FINANCIAL_REVISION",
        "policyVersion": "sec-revision-v1",
        "workKey": lease.key,
        "companyId": base["companyId"],
        "revisions": [entry],
    }


def test_revision_correction_keeps_old_fact_and_no_duplicate(stages, tmp_path):
    base = financial_fixture(stages, tmp_path / "original")
    plans = [
        revision_plan(stages, base, tmp_path / str(i), value)
        for i, value in enumerate([123, 124, 124])
    ]
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        results = [
            advance(stages, api, p, tmp_path, enabled=True, max_steps=5)[-1]["result"][
                "result"
            ]
            for p in plans
        ]
        assert results[0]["result"]["inserted"] == 1
        assert results[1]["result"]["needsReview"] is True
        assert results[2]["result"]["noChange"] == 1
        assert advance(stages, api, plans[0], tmp_path, enabled=True, max_steps=5) == []
    with stages.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pipeline_financial_revision WHERE company_id=%s",
            (base["companyId"],),
        )
        assert cur.fetchone()[0] == 2
        cur.execute(
            "SELECT value FROM financial_fact WHERE id=%s",
            (base["approvedMappings"][0]["factId"],),
        )
        assert cur.fetchone()[0] == 123
    stages.rollback()


def test_revision_official_mismatch_cannot_publish(stages, tmp_path):
    base = financial_fixture(stages, tmp_path / "original")
    plan = revision_plan(stages, base, tmp_path / "candidate", 124)
    plan["revisions"][0]["official"] = base["approvedMappings"][0]["official"]
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        with pytest.raises(CoordinationError):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{plan['workKey']}")["state"] == "REVIEW"


def test_revision_promotion_and_real_finance_reader(stages, tmp_path):
    import sys
    from unittest.mock import patch

    import psycopg
    from pipeline.revision_serving import promote

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "trader-ai/src"))
    from trader_ai.finance import FinanceReader, FinancialRequest

    base = financial_fixture(stages, tmp_path / "original")
    plan = revision_plan(stages, base, tmp_path / "revision", 124)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
    with stages.cursor() as cur:
        cur.execute("SELECT to_regclass('pipeline_financial_revision_active')")
        if cur.fetchone()[0] is None:
            cur.execute(
                (
                    Path(__file__).resolve().parents[2]
                    / "trader-controller/db/migration/V047__financial_revision_serving.sql"
                ).read_text()
            )
        cur.execute(
            "ALTER TABLE financial_fact ADD COLUMN IF NOT EXISTS metric_id bigint; ALTER TABLE financial_fact ADD COLUMN IF NOT EXISTS fiscal_year integer; ALTER TABLE financial_fact ADD COLUMN IF NOT EXISTS frame text; ALTER TABLE filing ADD COLUMN IF NOT EXISTS filing_date date"
        )
        cur.execute(
            "UPDATE financial_fact SET metric_id=(SELECT id FROM financial_metric WHERE metric_code='NET_INCOME'),fiscal_year=2025 WHERE company_id=%s",
            (base["companyId"],),
        )
        cur.execute(
            "UPDATE filing SET filing_date='2026-02-01' WHERE company_id=%s",
            (base["companyId"],),
        )
        cur.execute(
            "INSERT INTO financial_metric_mapping(company_id,taxonomy,source_tag,metric_id,valid_from,valid_to) SELECT %s,'us-gaap','NetIncomeLoss',id,'2025-12-31','2025-12-31' FROM financial_metric WHERE metric_code='NET_INCOME'",
            (base["companyId"],),
        )
        cur.execute(
            "SELECT revision_hash,scope FROM pipeline_financial_revision WHERE company_id=%s",
            (base["companyId"],),
        )
        revision_hash, scope = cur.fetchone()
    stages.commit()
    approval = {
        "workKey": plan["workKey"],
        "planHash": canonical_hash(plan),
        "revisionHash": revision_hash,
        "scopeHash": canonical_hash(scope),
        "expectedPreviousRevisionId": None,
    }
    result = promote(stages, approval, tmp_path)
    assert promote(stages, approval, tmp_path)["noChange"] is True
    with pytest.raises(CoordinationError):
        promote(stages, {**approval, "expectedPreviousRevisionId": 999}, tmp_path)
    with pytest.raises(CoordinationError):
        promote(stages, {**approval, "scopeHash": "0" * 64}, tmp_path)
    real_connect = psycopg.connect

    def isolated_connect(dsn, **kwargs):
        kwargs["options"] += " -c search_path=" + os.environ["RECOVERY_TEST_SCHEMA"]
        return real_connect(dsn, **kwargs)

    request = FinancialRequest(
        company_id=base["companyId"],
        metric="NET_INCOME",
        start="2025-01-01",
        end="2025-12-31",
        period_type="ANNUAL",
    )
    with patch("trader_ai.finance.psycopg.connect", side_effect=isolated_connect):
        legacy = FinanceReader(stages.dsn, enabled_metrics=["NET_INCOME"])
        enabled = FinanceReader(
            stages.dsn, enabled_metrics=["NET_INCOME"], revision_enabled=True
        )
        assert legacy.financial(request)["data"][0]["value"] == "123"
        answer = enabled.financial(request)
        assert answer["status"] == "SUCCESS" and answer["data"][0]["value"] == "124"
        assert (
            answer["data"][0]["sources"][0]["source_url"]
            == plan["revisions"][0]["official"]["url"]
        )
        assert (
            enabled.financial(
                request.model_copy(update={"company_id": base["companyId"] + 1})
            )["reasonCode"]
            == "METRIC_MAPPING_MISSING"
        )
        assert (
            enabled.financial(request.model_copy(update={"metric": "ASSETS"}))[
                "reasonCode"
            ]
            == "METRIC_NOT_VALIDATED"
        )
        assert (
            enabled.financial(request.model_copy(update={"mode": "LATEST"}))[
                "reasonCode"
            ]
            == "REVISION_EXACT_SCOPE_ONLY"
        )
        assert not enabled.financial(
            request.model_copy(update={"start": "2024-01-01", "end": "2024-12-31"})
        ).get("data")
        assert not enabled.financial(
            request.model_copy(update={"period_type": "QUARTER"})
        ).get("data")
        with stages.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_financial_revision_approval SET public_fact=jsonb_set(public_fact,'{value}','\"999\"') WHERE id=%s",
                (result["approvalId"],),
            )
        stages.commit()
        assert enabled.financial(request)["reasonCode"] == "REVISION_EVIDENCE_CONFLICT"
