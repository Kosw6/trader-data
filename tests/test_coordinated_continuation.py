"""Continuation gates on isolated DB and optional isolated Kafka only."""

# ruff: noqa: F811
import json
import os
import uuid
from contextlib import closing

import pytest
from pipeline.durable_requests import Controller, CoordinationError, canonical_hash
from workers.coordinated_consumer import TOPIC, consume_once, process_event
from workers.coordinated_recovery import advance

from tests.test_coordinated_stages import price_fixture, stages  # noqa: F401
from tests.test_durable_integration import db, dispatch  # noqa: F401


def event_fixture(stages, tmp_path, api):
    _, plan = price_fixture(stages, tmp_path)
    dispatch(api, plan["workKey"])
    with stages.cursor() as cur:
        cur.execute(
            "SELECT o.payload FROM pipeline_outbox o JOIN pipeline_recovery_raw_intent i ON i.outbox_id=o.id WHERE i.work_key=%s",
            (plan["workKey"],),
        )
        payload = cur.fetchone()[0]
    stages.rollback()
    return plan, payload


def test_consumer_duplicate_is_noop_and_forged_event_refused(stages, tmp_path):
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        plan, payload = event_fixture(stages, tmp_path, api)
        registry = {plan["workKey"]: plan}
        assert process_event(stages, api, payload, registry, tmp_path) == "VERIFIED"
        assert process_event(stages, api, payload, registry, tmp_path) == "VERIFIED"
        with pytest.raises(CoordinationError):
            process_event(stages, api, {**payload, "jobId": -1}, registry, tmp_path)
        with stages.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline_recovery_receipt WHERE work_key=%s AND to_phase='COMPLETE'",
                (plan["workKey"],),
            )
            assert cur.fetchone()[0] == 1
        stages.rollback()


def test_calendar_plan_checks_actual_candidate_coverage(stages, tmp_path):
    from tests.test_collection_plan import calendar

    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        _, plan = price_fixture(stages, tmp_path)
        plan["policyVersion"] = "adjusted-calendar-v2"
        c = calendar()
        plan["calendarEvidence"] = {
            "calendar": c,
            "sha256": canonical_hash(c),
            "asOf": "2026-01-06T00:00:00+00:00",
        }
        # Fixture contains Jan 1, while approved calendar excludes it. Fail closed.
        with pytest.raises(CoordinationError):
            advance(stages, api, plan, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{plan['workKey']}")["state"] == "REVIEW"


def test_scheduler_audits_receipts_and_preserves_pause(stages, tmp_path):
    from workers.coordinated_scheduler import audit, run_cycle

    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        _, first = price_fixture(stages, tmp_path / "one")
        _, paused = price_fixture(stages, tmp_path / "two")
        registry = {p["workKey"]: p for p in [first, paused]}
        api.call("POST", f"/{paused['workKey']}/pause", {})
        result = run_cycle(stages, api, registry, tmp_path, tmp_path / "STOP")
        assert result["advanced"] == [first["workKey"]]
        assert (
            result["audit"]["verifiedSuccess"] == 1
            and result["audit"]["unfinished"] == 1
        )
        assert api.call("GET", f"/{paused['workKey']}")["state"] == "PAUSED"
        with stages.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline_recovery_receipt WHERE work_key=%s AND to_phase='COMPLETE'",
                (first["workKey"],),
            )
        stages.commit()
        assert audit(stages, registry)["commitUnresolved"] == 1
        assert not run_cycle(stages, api, registry, tmp_path, tmp_path / "STOP")[
            "advanced"
        ]
        stop = tmp_path / "STOP"
        stop.touch()
        assert run_cycle(None, None, registry, tmp_path, stop)["state"] == "PAUSED_USER"


@pytest.mark.parametrize("corruption", ["raw_hash", "candidate_hash"])
def test_complete_receipt_corruption_is_not_success(stages, tmp_path, corruption):
    import requests
    from workers.coordinated_scheduler import audit

    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        plan, payload = event_fixture(stages, tmp_path, api)
        registry = {plan["workKey"]: plan}
        process_event(stages, api, payload, registry, tmp_path)
        url = (
            os.environ["RECOVERY_TEST_URL"] + "/api/v1/admin/pipeline/recovery/overview"
        )

        def summary():
            response = requests.get(
                url, headers={"X-Admin-API-Key": "test-only"}, timeout=5
            )
            response.raise_for_status()
            return response.json()

        before = summary()
        phase = "FETCH_RAW" if corruption == "raw_hash" else "CANDIDATE"
        with stages.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_recovery_receipt SET content_hash=%s WHERE work_key=%s AND from_phase=%s",
                ("f" * 64, plan["workKey"], phase),
            )
        stages.commit()
        assert audit(stages, registry)["commitUnresolved"] == 1
        after = summary()
        with pytest.raises(CoordinationError):
            process_event(stages, api, payload, registry, tmp_path)
        assert after["commitUnresolved"] == before["commitUnresolved"] + 1
        assert after["verifiedSuccess"] == before["verifiedSuccess"] - 1


def test_calendar_publication_and_changed_history_hold(stages, tmp_path):
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        asset, first = price_fixture(stages, tmp_path / "first")
        # Synthetic session evidence only; not an actual holiday assertion.
        calendar = {
            "version": "sessions-v1",
            "sourceUrl": "https://www.nyse.com/fixture",
            "start": "2026-01-01",
            "end": "2026-01-02",
            "days": {
                day: {
                    "state": "SESSION",
                    "closeAt": day + "T20:00:00+00:00",
                    "availableAt": day + "T21:00:00+00:00",
                }
                for day in first["expectedDates"]
            },
        }
        evidence = {
            "calendar": calendar,
            "sha256": canonical_hash(calendar),
            "asOf": "2026-01-03T00:00:00+00:00",
        }
        first.update(policyVersion="adjusted-calendar-v2", calendarEvidence=evidence)
        advance(stages, api, first, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{first['workKey']}")["phase"] == "COMPLETE"
        with stages.cursor() as cur:
            cur.execute(
                "SELECT id,content_hash FROM price_chart_snapshot WHERE asset_id=%s AND default_for_chart",
                (asset,),
            )
            previous = cur.fetchone()
            # Simulate a different historical generation in this isolated fixture.
            cur.execute(
                "UPDATE price_chart_snapshot_bar SET volume=11 WHERE snapshot_id=%s",
                (previous[0],),
            )
        stages.commit()
        _, next_plan = price_fixture(
            stages, tmp_path / "next", asset=asset, previous=previous
        )
        next_plan.update(
            policyVersion="adjusted-calendar-v2", calendarEvidence=evidence
        )
        with pytest.raises(CoordinationError):
            advance(stages, api, next_plan, tmp_path, enabled=True, max_steps=5)
        assert api.call("GET", f"/{next_plan['workKey']}")["state"] == "REVIEW"
        with stages.cursor() as cur:
            cur.execute(
                "SELECT id FROM price_chart_snapshot WHERE asset_id=%s AND default_for_chart",
                (asset,),
            )
            assert cur.fetchone() == (previous[0],)
        stages.rollback()


def test_admin_summary_has_authenticated_read_only_counts(stages, tmp_path):
    import requests

    base = os.environ["RECOVERY_TEST_URL"]
    url = base + "/api/v1/admin/pipeline/recovery/overview"
    response = requests.get(url, timeout=5)
    assert response.status_code == 401
    response = requests.get(url, headers={"X-Admin-API-Key": "test-only"}, timeout=5)
    assert response.status_code == 200
    body = response.json()
    assert body["registered"] == sum(
        body[k]
        for k in (
            "verifiedSuccess",
            "verifiedNoChange",
            "heldReview",
            "unfinished",
            "commitUnresolved",
        )
    )


def test_real_broker_commit_loss_redelivery_to_second_consumer(stages, tmp_path):
    brokers = os.getenv("RECOVERY_TEST_KAFKA")
    if not brokers:
        pytest.skip("separate broker not configured")
    assert brokers == "127.0.0.1:59092"
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition

    group = "coordinated-test-" + uuid.uuid4().hex
    with closing(Controller(os.environ["RECOVERY_TEST_URL"], "test-only")) as api:
        plan, payload = event_fixture(stages, tmp_path, api)
        with closing(
            KafkaProducer(bootstrap_servers=brokers, acks="all", retries=0)
        ) as producer:
            meta = producer.send(
                TOPIC, key=plan["workKey"].encode(), value=json.dumps(payload).encode()
            ).get(timeout=15)
        partition = TopicPartition(TOPIC, meta.partition)

        def consumer():
            c = KafkaConsumer(
                bootstrap_servers=brokers,
                group_id=group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
            )
            c.assign([partition])
            return c

        # First worker commits DB then disappears before Kafka acknowledgement.
        with closing(consumer()) as first:
            first.seek(partition, meta.offset)
            records = first.poll(timeout_ms=10000, max_records=1)
            message = records[partition][0]
            process_event(
                stages,
                api,
                json.loads(message.value),
                {plan["workKey"]: plan},
                tmp_path,
            )
            assert first.committed(partition) is None
        with (
            closing(
                Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")
            ) as api_b,
            closing(consumer()) as second,
        ):
            second.seek(partition, meta.offset)
            assert consume_once(
                second, stages, api_b, {plan["workKey"]: plan}, tmp_path
            )
            assert second.committed(partition) == meta.offset + 1
        with stages.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pipeline_recovery_receipt WHERE work_key=%s AND to_phase='COMPLETE'",
                (plan["workKey"],),
            )
            assert cur.fetchone()[0] == 1
        stages.rollback()
