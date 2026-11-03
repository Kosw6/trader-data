"""Explicitly approved, finite provider smoke; never enabled by default.

Only the Go throwaway-DB bridge can run this. Real public raw evidence is retained;
credentials stay in memory. No provider retries, live DB or jobs. Only the separate
approved-kis-455 opt-in permits bounded token renewal within its request budget.
"""
# ruff: noqa: F811 -- imported pytest fixtures

import hashlib
import json
import os
import uuid
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from collectors.common import LocalPipelineStorage
from collectors.policy_clients import PolicyKISClient, PolicySECClient
from dotenv import dotenv_values
from pipeline.durable_raw import commit_raw
from pipeline.durable_requests import Controller, DurableSession, canonical_hash
from workers.coordinated_recovery import advance
from workers.kis_price_worker import write_raw_pages

from tests.test_coordinated_stages import stages  # noqa: F401
from tests.test_durable_integration import (  # noqa: F401
    db,
    dispatch,
    lease_for,
    make_item,
    policy,
)


class ExistingTokenOnlyKISClient(PolicyKISClient):
    def get_access_token(self):
        if not self._token_is_usable():
            raise ValueError("existing token unavailable; issuance disabled")
        return self.access_token


@pytest.mark.parametrize("provider", ["KIS", "SEC"])
def test_approved_small_real_provider(stages, provider):
    approval = os.getenv("RECOVERY_REAL_SMOKE")
    if approval not in {"approved-small-453", "approved-kis-455"}:
        pytest.skip("requires explicit one-run approval")
    if approval == "approved-kis-455" and provider != "KIS":
        pytest.skip("455 is KIS-only; no duplicate SEC collection")
    root = Path(os.environ["RECOVERY_REAL_OUTPUT"]).resolve() / provider
    assert root.parent.parent == Path(__file__).resolve().parents[2] / "output"
    root.mkdir(parents=True, exist_ok=False)
    # Read only provider settings, never print or persist credentials.
    settings = dotenv_values(Path(__file__).resolve().parents[1] / ".env")
    report = {
        "provider": provider,
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "database": "isolated-loopback-55451",
        "providerRetries": 0,
    }
    lease = client = None
    try:
        item = make_item(stages)
        identity = {
            "provider": provider,
            "quotaScope": "approved-smoke-453-" + provider,
            "target": "asset:45301" if provider == "KIS" else "company:45302",
            "dataset": "price-1D" if provider == "KIS" else "sec-all-history",
            "start": "2026-10-19" if provider == "KIS" else "0001-01-01",
            "end": "2026-10-20" if provider == "KIS" else "9999-12-31",
            "revision": canonical_hash(uuid.uuid4().hex),
        }
        lease = lease_for(
            identity=identity,
            p=policy(
                maxRequests=2,
                maxRetries=0,
                intervalMs=5000,
                leaseMs=120000,
                maxElapsedMs=600000,
            ),
        )
        store = LocalPipelineStorage(root, item.item_id)
        store.storage_backend, store.storage_base_uri = "LOCAL", ""
        now = datetime.now(timezone.utc)
        if provider == "KIS":
            client_type = (
                PolicyKISClient
                if approval == "approved-kis-455"
                else ExistingTokenOnlyKISClient
            )
            client = client_type(
                app_key=settings.get("APPKEY"),
                app_secret=settings.get("APPSECRET"),
                access_token=settings.get("ACCESS_TOKEN"),
                access_token_expires_at=settings.get("ACCESS_TOKEN_TOKEN_EXPIRED"),
                timeout=20,
            )
            report["tokenRenewalAllowed"] = approval == "approved-kis-455"
            if not client._token_is_usable() and approval != "approved-kis-455":
                raise ValueError(
                    "existing token unavailable; issuance deliberately disabled"
                )
            client.session.close()
            client.session = DurableSession(provider, lease, "TraderDataBot-KR/1.0")
            # No automatic token renewal or retry: use only the existing usable token.
            result = client.get_daily_prices(
                exchange_code="NAS",
                symbol="NVDA",
                start_date=date(2026, 10, 19),
                end_date=date(2026, 10, 20),
                adjusted=True,
                max_pages=1,
            )
            asset = {
                "asset_id": 45301,
                "ticker": "NVDA",
                "exchange": "NASDAQ",
                "provider_exchange_code": "NAS",
                "provider_symbol": "NVDA",
            }
            options = {
                "start_date": date(2026, 10, 19),
                "end_date": date(2026, 10, 20),
                "adjusted": True,
            }
            sources = [write_raw_pages(store, asset, options, result, now)]
            contract = {
                "target": identity["target"],
                "symbol": "NVDA",
                "exchange": "NAS",
                "start": identity["start"],
                "end": identity["end"],
                "adjusted": True,
                "interval": "1D",
                "source": "KIS",
                "currency": "USD",
            }
            with stages.cursor() as cur:
                cur.execute("INSERT INTO asset(id) VALUES(45301)")
            stages.commit()
            report["returnedBars"] = len(result["bars"])
        else:
            agent = settings.get("SEC_USER_AGENT") or os.getenv("SEC_USER_AGENT")
            if not agent:
                raise ValueError("SEC identity missing")
            client = PolicySECClient(agent)
            client.session.close()
            client.session = DurableSession(provider, lease, agent)
            submissions = client.get_submissions("0000320193")
            facts = client.get_company_facts("0000320193")
            if int(submissions["cik"]) != 320193 or int(facts["cik"]) != 320193:
                raise ValueError("company conflict")
            sources = [
                store.write_raw_json("SEC", kind, "0000320193", raw, now)
                for kind, raw in [
                    ("SUBMISSIONS", submissions),
                    ("COMPANY_FACTS", facts),
                ]
            ]
            contract = {
                "target": identity["target"],
                "cik": "0000320193",
                "includeHistory": False,
                "allFacts": False,
                "requestIntervalMs": 5000,
            }
            report["company"] = "AAPL/0000320193"
        receipt = commit_raw(stages, lease, sources, root, item, contract)
        report["receipt"] = receipt
        report["workKey"] = lease.key
        report["requests"] = lease.api.call("GET", f"/{lease.key}")["requests"]
        lease.close()
        with closing(Controller(os.environ["RECOVERY_TEST_URL_B"], "test-only")) as api:
            if provider == "KIS":
                plan = {
                    "kind": "PRICE",
                    "policyVersion": "adjusted-full-v1",
                    "workKey": lease.key,
                    "expectedDates": ["2026-10-19", "2026-10-20"],
                    "closeOnlyDates": [],
                    "expectedSnapshotId": None,
                    "expectedSnapshotHash": None,
                    "coverageEvidence": "Explicit two-day smoke only; not full NVDA history/calendar proof",
                    "maxRawAgeSeconds": 600,
                }
                report["steps"] = advance(
                    stages, api, plan, root, enabled=True, max_steps=5
                )
                report["noOpReplay"] = (
                    advance(stages, api, plan, root, enabled=True, max_steps=5) == []
                )
            else:
                dispatch(api, lease.key)
                report["mapping"] = (
                    "NOT_RUN: independent filing evidence not part of this raw smoke"
                )
            report["phase"] = api.call("GET", f"/{lease.key}")["phase"]
        report["status"] = "PASS"
    except Exception as error:  # noqa: BLE001 -- no response/header/credential disclosure
        stages.rollback()
        report["status"] = "NOT_VERIFIED"
        report["errorType"] = type(error).__name__
    finally:
        if client:
            client.session.close()
        if lease:
            lease.close()
        report["finishedAt"] = datetime.now(timezone.utc).isoformat()
        report["rawFiles"] = [
            {
                "path": str(p.relative_to(root)),
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            }
            for p in root.rglob("*.json")
        ]
        with (root / "report.json").open("x", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2, default=str)
    assert report["status"] == "PASS", (
        f"{provider}: not verified; sanitized report retained"
    )
