"""Exact official evidence, mapping scope and revision conflict regression."""
# ruff: noqa: F811 -- pytest imported fixture injection

import hashlib
import os
import uuid
from contextlib import closing
from datetime import datetime, timezone

import psycopg2
import pytest
from collectors.common import LocalPipelineStorage
from pipeline.coordinated_financial import (
    financial_publication_verifier,
    financial_validator,
    publish_financial,
)
from pipeline.durable_raw import commit_raw
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash
from pipeline.stage_transaction import (
    confirm_publication,
    stage_candidate,
    verify_candidate,
)

from tests.test_coordinated_stages import claim_stage, stages  # noqa: F401
from tests.test_durable_integration import (  # noqa: F401
    db,
    dispatch,
    lease_for,
    make_item,
)


def financial_fixture(db, root):
    with db.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS company(id bigint PRIMARY KEY,cik text);
          CREATE TABLE IF NOT EXISTS filing(id bigserial PRIMARY KEY,company_id bigint,form_type text,accession_number text,report_date date,document_url text);
          CREATE TABLE IF NOT EXISTS financial_metric(id bigserial PRIMARY KEY,metric_code text UNIQUE);
          CREATE TABLE IF NOT EXISTS financial_metric_mapping(id bigserial PRIMARY KEY,company_id bigint,taxonomy text,source_tag text,metric_id bigint,valid_from date,valid_to date);
          CREATE TABLE IF NOT EXISTS financial_fact(id bigserial PRIMARY KEY,company_id bigint,filing_id bigint,taxonomy text,source_tag text,unit text,period_start date,
            period_end date,value numeric,dimensions jsonb,source_fact_hash text,fiscal_period text);
          INSERT INTO financial_metric(metric_code) VALUES('NET_INCOME') ON CONFLICT DO NOTHING;""")
    db.commit()
    company = int(uuid.uuid4().hex[:8], 16)
    cik = str(company).zfill(10)
    accession = f"{cik}-25-000001"
    url = f"https://www.sec.gov/Archives/edgar/data/{company}/{accession.replace('-', '')}/annual.htm"
    item = make_item(db)
    identity = {
        "provider": "SEC",
        "quotaScope": uuid.uuid4().hex,
        "target": f"company:{company}",
        "dataset": "sec-all-history",
        "start": "0001-01-01",
        "end": "9999-12-31",
        "revision": canonical_hash(uuid.uuid4().hex),
    }
    lease = lease_for(identity=identity)
    now = datetime.now(timezone.utc)
    store = LocalPipelineStorage(root, item.item_id)
    store.storage_backend, store.storage_base_uri = "LOCAL", ""
    fact = {
        "start": "2025-01-01",
        "end": "2025-12-31",
        "val": 123,
        "accn": accession,
        "form": "20-F",
    }
    sources = [
        store.write_raw_json("SEC", kind, str(company), raw, now)
        for kind, raw in (
            ("SUBMISSIONS", {"cik": cik}),
            (
                "COMPANY_FACTS",
                {
                    "cik": cik,
                    "facts": {"us-gaap": {"NetIncomeLoss": {"units": {"USD": [fact]}}}},
                },
            ),
        )
    ]
    result = commit_raw(
        db, lease, sources, root, item, {"target": identity["target"], "cik": cik}
    )
    lease.close()
    fact_hash = canonical_hash(fact)
    with db.cursor() as cur:
        cur.execute("INSERT INTO company(id,cik) VALUES(%s,%s)", (company, cik))
        cur.execute(
            "INSERT INTO filing(company_id,form_type,accession_number,report_date,document_url) VALUES(%s,'20-F',%s,'2025-12-31',%s) RETURNING id",
            (company, accession, url),
        )
        filing = cur.fetchone()[0]
        cur.execute(
            """INSERT INTO financial_fact(company_id,filing_id,taxonomy,source_tag,unit,period_start,period_end,value,dimensions,source_fact_hash,fiscal_period)
            VALUES(%s,%s,'us-gaap','NetIncomeLoss','USD','2025-01-01','2025-12-31',123,'{}',%s,'FY') RETURNING id""",
            (company, filing, fact_hash),
        )
        fid = cur.fetchone()[0]
        source_id = result["sourceIds"][1]
        cur.execute(
            """INSERT INTO record_lineage(source_object_id,target_table,target_record_id,target_record_key,transformer_name,transformer_version,output_hash,processed_at)
            VALUES(%s,'financial_fact',%s,%s,'fixture','1',%s,clock_timestamp())""",
            (source_id, fid, str(fid), fact_hash),
        )
    db.commit()
    xml = f"""<html xmlns:x="http://www.xbrl.org/2003/instance" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" xmlns:us-gaap="http://fasb.org/us-gaap/2025" xmlns:iso="http://www.xbrl.org/2003/iso4217">
      <x:context id="c"><x:entity><x:identifier scheme="http://www.sec.gov/CIK">{cik}</x:identifier></x:entity><x:period><x:startDate>2025-01-01</x:startDate><x:endDate>2025-12-31</x:endDate></x:period></x:context>
      <x:unit id="u"><x:measure>iso:USD</x:measure></x:unit><ix:nonFraction name="us-gaap:NetIncomeLoss" contextRef="c" unitRef="u">123</ix:nonFraction></html>""".encode()
    path = root / "official.xml"
    path.write_bytes(xml)  # generated test fixture, not a repository edit
    entry = {
        "factId": fid,
        "metricCode": "NET_INCOME",
        "tag": "NetIncomeLoss",
        "unit": "USD",
        "start": fact["start"],
        "end": fact["end"],
        "value": "123",
        "form": "20-F",
        "accession": accession,
        "factHash": fact_hash,
        "lineageHash": canonical_hash(
            [(source_id, fact_hash, sources[1]["content_hash"])]
        ),
        "official": {
            "path": str(path),
            "sha256": hashlib.sha256(xml).hexdigest(),
            "url": url,
        },
    }
    return {
        "kind": "FINANCIAL",
        "policyVersion": "exact-us-gaap-v1",
        "workKey": lease.key,
        "companyId": company,
        "approvedMappings": [entry],
    }


def test_foreign_annual_exact_mapping_and_independent_read(stages, tmp_path):
    plan = financial_fixture(stages, tmp_path)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        dispatch(api, plan["workKey"])
        stage_candidate(
            stages,
            claim_stage(api, plan["workKey"]),
            plan,
            plan["approvedMappings"],
            financial_validator(tmp_path),
        )
        verify_candidate(
            stages,
            claim_stage(api, plan["workKey"]),
            canonical_hash(plan),
            financial_validator(tmp_path),
        )
        published = publish_financial(
            stages, claim_stage(api, plan["workKey"]), canonical_hash(plan), tmp_path
        )
        assert published["result"]["inserted"] == 1
        with closing(psycopg2.connect(stages.dsn)) as read:
            confirm_publication(
                stages,
                read,
                claim_stage(api, plan["workKey"]),
                canonical_hash(plan),
                financial_publication_verifier(tmp_path),
            )
        assert api.call("GET", f"/{plan['workKey']}")["state"] == "COMPLETE"
    with stages.cursor() as cur:
        cur.execute(
            "SELECT valid_from,valid_to FROM financial_metric_mapping WHERE company_id=%s",
            (plan["companyId"],),
        )
        row = cur.fetchone()
        assert str(row[0]) == str(row[1]) == "2025-12-31"
        cur.execute(
            "SELECT value FROM financial_fact WHERE id=%s",
            (plan["approvedMappings"][0]["factId"],),
        )
        assert cur.fetchone() == (123,)
    stages.rollback()


@pytest.mark.parametrize(
    "failure",
    ["official_value", "company", "unit", "period", "revision", "meaning", "lineage"],
)
def test_financial_conflicts_never_insert_mapping(stages, tmp_path, failure):
    plan = financial_fixture(stages, tmp_path)
    entry = plan["approvedMappings"][0]
    if failure == "official_value":
        path = tmp_path / "official.xml"
        raw = path.read_bytes().replace(b">123<", b">124<")
        path.write_bytes(raw)
        entry["official"]["sha256"] = hashlib.sha256(raw).hexdigest()
    elif failure == "company":
        plan["companyId"] += 1
    elif failure == "unit":
        entry["unit"] = "EUR"
    elif failure == "period":
        entry["start"] = "2025-10-01"
    elif failure == "meaning":
        entry["metricCode"] = "REVENUE"
    elif failure == "lineage":
        entry["lineageHash"] = "0" * 64
    else:
        with stages.cursor() as cur:
            cur.execute(
                """INSERT INTO financial_fact(company_id,filing_id,taxonomy,source_tag,unit,period_start,period_end,value,source_fact_hash)
                SELECT company_id,filing_id,taxonomy,source_tag,unit,period_start,period_end,124,'conflicting-revision' FROM financial_fact WHERE id=%s""",
                (entry["factId"],),
            )
        stages.commit()
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        dispatch(api, plan["workKey"])
        with pytest.raises(CoordinationError):
            stage_candidate(
                stages,
                claim_stage(api, plan["workKey"]),
                plan,
                plan["approvedMappings"],
                financial_validator(tmp_path),
            )
    with stages.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM financial_metric_mapping WHERE company_id=%s",
            (plan["companyId"],),
        )
        assert cur.fetchone() == (0,)
    stages.rollback()
