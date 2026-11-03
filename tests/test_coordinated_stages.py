"""Stage gates use the isolated two-controller PostgreSQL harness only."""
# ruff: noqa: F811 -- imported pytest fixture is intentionally injected by name

import os
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pytest
from collectors.common import LocalPipelineStorage
from pipeline.coordinated_price import (
    price_content,
    price_validator,
    publish_price,
    verify_published_price,
)
from pipeline.durable_raw import commit_raw
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash
from pipeline.stage_transaction import (
    confirm_publication,
    stage_candidate,
    verify_candidate,
)
from workers.coordinated_recovery import advance

from tests.test_durable_integration import (  # noqa: F401
    db,
    dispatch,
    lease_for,
    make_item,
)


@pytest.fixture
def stages(db):
    migrations = Path(__file__).resolve().parents[2] / "trader-controller/db/migration"
    with db.cursor() as cur:
        cur.execute("SELECT to_regclass('pipeline_recovery_candidate')")
        if cur.fetchone()[0] is None:
            cur.execute(
                (migrations / "V045__coordinated_stage_evidence.sql").read_text()
            )
            cur.execute("""CREATE TABLE asset(id bigint PRIMARY KEY);
                CREATE TABLE price_bar(asset_id bigint,source text,"interval" text,bar_time timestamptz,display_policy text);
                CREATE TABLE record_lineage(id bigserial PRIMARY KEY,source_object_id bigint,target_table text,target_record_id bigint,
                target_record_key text,transformer_name text,transformer_version text,output_hash text,processed_at timestamptz,
                UNIQUE(source_object_id,target_table,target_record_key,transformer_name,transformer_version));""")
            for file in (
                "V035__add_price_adjustment_candidate.sql",
                "V038__adjusted_chart_snapshot.sql",
                "V039__default_adjusted_chart_snapshot.sql",
            ):
                cur.execute((migrations / file).read_text(encoding="utf-8"))
    db.commit()
    return db


def claim_stage(api, key):
    work = api.call("POST", f"/{key}/claim", {"owner": "stage-" + uuid.uuid4().hex})
    return {"workKey": key, "owner": work["owner"], "fence": work["fence"]}


def price_fixture(db, root, *, asset=None, previous=None, bad=False):
    asset = asset or int(uuid.uuid4().hex[:10], 16)
    with db.cursor() as cur:
        cur.execute("INSERT INTO asset(id) VALUES(%s) ON CONFLICT DO NOTHING", (asset,))
    db.commit()
    item = make_item(db)
    identity = {
        "provider": "KIS",
        "quotaScope": uuid.uuid4().hex,
        "target": f"asset:{asset}",
        "dataset": "price-1D",
        "start": "2026-01-01",
        "end": "2026-01-02",
        "revision": canonical_hash(uuid.uuid4().hex),
    }
    lease = lease_for(identity=identity)
    now = datetime.now(timezone.utc)
    contract = {
        "target": identity["target"],
        "symbol": "TEST",
        "exchange": "NAS",
        "start": identity["start"],
        "end": identity["end"],
        "adjusted": True,
        "interval": "1D",
        "source": "KIS",
        "currency": "USD",
    }
    payload = {
        "provider": "KIS",
        "asset_id": asset,
        "provider_symbol": "TEST",
        "provider_exchange_code": "NAS",
        "start_date": identity["start"],
        "end_date": identity["end"],
        "adjusted": True,
        "collected_at": now.isoformat(),
        "pages": [
            {
                "payload": {
                    "rt_cd": "0",
                    "output2": [
                        {
                            "xymd": day,
                            "open": "1",
                            "high": "0.5" if bad else "2",
                            "low": "1",
                            "clos": "2",
                            "tvol": "10",
                        }
                        for day in ("20260101", "20260102")
                    ],
                }
            }
        ],
    }
    storage = LocalPipelineStorage(root, item.item_id)
    storage.storage_backend, storage.storage_base_uri = "LOCAL", ""
    raw = storage.write_raw_json("KIS", "PRICE_BAR", str(asset), payload, now)
    commit_raw(db, lease, [raw], root, item, contract)
    lease.close()
    plan = {
        "kind": "PRICE",
        "policyVersion": "adjusted-full-v1",
        "workKey": lease.key,
        "expectedDates": [identity["start"], identity["end"]],
        "closeOnlyDates": [],
        "expectedSnapshotId": previous[0] if previous else None,
        "expectedSnapshotHash": previous[1] if previous else None,
        "coverageEvidence": "synthetic fixture approved dates",
        "maxRawAgeSeconds": 600,
    }
    return asset, plan


def prepare_price(db, api, plan, root):
    dispatch(api, plan["workKey"])
    with db.cursor() as cur:
        content = price_content(cur, plan, root)
    db.rollback()
    stage_candidate(
        db, claim_stage(api, plan["workKey"]), plan, content, price_validator(root)
    )
    verify_candidate(
        db,
        claim_stage(api, plan["workKey"]),
        canonical_hash(plan),
        price_validator(root),
    )


def test_price_publish_exact_snapshot_and_lineage(stages, tmp_path):
    asset, plan = price_fixture(stages, tmp_path)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        prepare_price(stages, api, plan, tmp_path)
        result = publish_price(
            stages, claim_stage(api, plan["workKey"]), canonical_hash(plan), tmp_path
        )
        assert api.call("GET", f"/{plan['workKey']}")["phase"] == "PUBLISHED"
        assert result["result"]["rows"] == 2
        with closing(psycopg2.connect(stages.dsn)) as independent:
            confirm_publication(
                stages,
                independent,
                claim_stage(api, plan["workKey"]),
                canonical_hash(plan),
                verify_published_price,
            )
        assert api.call("GET", f"/{plan['workKey']}")["phase"] == "COMPLETE"
    with stages.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM price_chart_snapshot WHERE asset_id=%s AND default_for_chart",
            (asset,),
        )
        assert cur.fetchone() == (1,)
        cur.execute(
            "SELECT count(*) FROM record_lineage WHERE target_record_key LIKE %s",
            (f"{asset}:%",),
        )
        assert cur.fetchone() == (2,)
    stages.rollback()


def test_invalid_ohlc_cannot_create_candidate_or_change_active(stages, tmp_path):
    asset, plan = price_fixture(stages, tmp_path, bad=True)
    with (
        closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api,
        pytest.raises((ValueError, CoordinationError)),
    ):
        prepare_price(stages, api, plan, tmp_path)
    stages.rollback()
    with stages.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM price_chart_snapshot WHERE asset_id=%s", (asset,)
        )
        assert cur.fetchone() == (0,)
    stages.rollback()


def test_old_publication_plan_cannot_replace_new_active(stages, tmp_path):
    asset, first = price_fixture(stages, tmp_path / "first")
    _, second = price_fixture(stages, tmp_path / "second", asset=asset)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        prepare_price(stages, api, first, tmp_path)
        prepare_price(stages, api, second, tmp_path)
        published = publish_price(
            stages, claim_stage(api, first["workKey"]), canonical_hash(first), tmp_path
        )
        with pytest.raises(CoordinationError, match="contract_conflict"):
            publish_price(
                stages,
                claim_stage(api, second["workKey"]),
                canonical_hash(second),
                tmp_path,
            )
    with stages.cursor() as cur:
        cur.execute(
            "SELECT id FROM price_chart_snapshot WHERE asset_id=%s AND default_for_chart",
            (asset,),
        )
        assert cur.fetchone() == (published["result"]["snapshotId"],)
    stages.rollback()


def test_bounded_runner_completes_all_five_steps_and_replay_is_noop(stages, tmp_path):
    _asset, plan = price_fixture(stages, tmp_path)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        result = advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
        assert [r["from"] for r in result] == [
            "RAW_READY",
            "ETL_READY",
            "CANDIDATE",
            "VERIFIED",
            "PUBLISHED",
        ]
        assert advance(stages, api, plan, tmp_path, enabled=True, max_steps=5) == []
        rows = api.call("GET")["rows"]
        status = next(r for r in rows if r["key"] == plan["workKey"])
        assert status["phase"] == "COMPLETE" and status["pendingRequests"] == 0
        assert (
            status["outboxStatus"] == "PENDING"
        )  # broker delivery != processing completion


def test_runner_holds_bad_candidate_and_never_auto_resumes(stages, tmp_path):
    _asset, plan = price_fixture(stages, tmp_path, bad=True)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        with pytest.raises(CoordinationError):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{plan['workKey']}")["state"] == "REVIEW"
        with pytest.raises(CoordinationError, match="review_required"):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)


def test_runner_preserves_user_pause(stages, tmp_path):
    _asset, plan = price_fixture(stages, tmp_path)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        api.call("POST", f"/{plan['workKey']}/pause", {})
        with pytest.raises(CoordinationError, match="review_required"):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{plan['workKey']}")["state"] == "PAUSED"


@pytest.mark.parametrize(
    "tamper", ["lineage", "snapshot_currency", "candidate_currency"]
)
def test_independent_read_detects_post_commit_tampering(stages, tmp_path, tamper):
    _asset, plan = price_fixture(stages, tmp_path)
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        advance(stages, api, plan, tmp_path, enabled=True, max_steps=4)
        with stages.cursor() as cur:
            if tamper == "lineage":
                cur.execute(
                    """UPDATE record_lineage SET output_hash=%s WHERE source_object_id IN
                (SELECT (s->>'id')::bigint FROM pipeline_recovery_raw_intent i,jsonb_array_elements(i.sources) s WHERE work_key=%s)""",
                    ("0" * 64, plan["workKey"]),
                )
            elif tamper == "snapshot_currency":
                cur.execute(
                    "UPDATE price_chart_snapshot SET currency='EUR' WHERE asset_id=%s",
                    (_asset,),
                )
            else:
                cur.execute(
                    "UPDATE price_adjustment_candidate SET currency='EUR' WHERE asset_id=%s",
                    (_asset,),
                )
        stages.commit()
        with pytest.raises(CoordinationError):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=1)
        assert api.call("GET", f"/{plan['workKey']}")["phase"] == "PUBLISHED"


def test_disabled_runner_never_calls_database_or_controller():
    with pytest.raises(CoordinationError, match="invalid_contract"):
        advance(None, None, {}, None)
