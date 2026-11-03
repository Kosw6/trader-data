"""Explicit, manifest-pinned raw collector path. Legacy/backfills never import it."""

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from collectors.common import LocalPipelineStorage
from collectors.policy_clients import PolicyKISClient, PolicySECClient
from pipeline.durable_raw import JobGuard, commit_raw
from pipeline.durable_requests import (
    Controller,
    CoordinationError,
    DurableLease,
    DurableSession,
    canonical_hash,
)


def load_manifest(path, expected_hash, provider, contract):
    try:
        raw = Path(path).read_bytes()
        if len(raw) > 1_000_000 or hashlib.sha256(raw).hexdigest() != expected_hash:
            raise ValueError()
        document = json.loads(raw)
        identity, policy = document["identity"], document["policy"]
        if (
            document["version"] != "coordinated-v1"
            or document["contract"] != contract
            or identity["provider"] != provider
            or identity["target"] != contract["target"]
            or identity["revision"]
            != canonical_hash(
                {"generation": document["generation"], "contract": contract}
            )
            or identity["quotaScope"]
            != os.environ.get(f"RECOVERY_{provider}_QUOTA_SCOPE")
            or not 30000 <= policy["leaseMs"] <= 300000
            or policy["intervalMs"] < max(1000, contract["requestIntervalMs"])
        ):
            raise ValueError()
        if provider == "KIS" and (
            identity["start"] != contract["start"]
            or identity["end"] != contract["end"]
            or identity["dataset"] != "price-1D"
        ):
            raise ValueError()
        if provider == "SEC" and (
            identity["dataset"] != "sec-all-history"
            or identity["start"] != "0001-01-01"
            or identity["end"] != "9999-12-31"
        ):
            raise ValueError()  # Companyfacts endpoint isn't a date-filtered request.
        return identity, policy
    except (ValueError, KeyError, TypeError, OSError):
        raise CoordinationError("invalid_contract") from None


def resolve(connection, item, args, provider):
    if provider == "KIS":
        from workers.kis_price_worker import (
            load_asset,
            parse_asset_id,
            read_price_options,
        )

        target = load_asset(connection, parse_asset_id(item.item_key))
        options = read_price_options(item.parameters or {}, args)
        # Dynamic incremental ranges can silently manufacture new identities on retry.
        if (
            not target
            or options["collection_mode"] != "BACKFILL"
            or options["interval"] != "1D"
        ):
            raise CoordinationError("invalid_contract")
        contract = {
            "target": f"asset:{target['asset_id']}",
            "exchange": target["provider_exchange_code"],
            "symbol": target["provider_symbol"],
            "start": options["start_date"].isoformat(),
            "end": options["end_date"].isoformat(),
            "adjusted": options["adjusted"],
            "maxPages": options["max_pages"],
            "interval": options["interval"],
            "currency": options["currency"],
            "source": options["source"],
            "requestIntervalMs": int(options["request_interval"] * 1000),
        }
    else:
        from workers.sec_financial_worker import build_backfill_args, load_single_target

        target, _ = load_single_target(connection, item.item_key)
        options = build_backfill_args(item, args)
        if not target or not target.get("company_id"):
            raise CoordinationError("invalid_contract")
        contract = {
            "target": f"company:{target['company_id']}",
            "cik": target["cik"],
            "includeHistory": options.include_history,
            "allFacts": options.all_facts,
            "requestIntervalMs": int(options.request_interval * 1000),
        }
    return target, options, contract


def claim(connection, args, provider):
    from workers.job_repository import claim_queued_item_by_id

    try:
        raw = Path(args.recovery_manifest).read_bytes()
        if (
            len(raw) > 1_000_000
            or hashlib.sha256(raw).hexdigest() != args.recovery_manifest_sha256
            or json.loads(raw)["version"] != "coordinated-v1"
            or args.recovery_item_id <= 0
        ):
            raise ValueError()
    except (ValueError, TypeError, KeyError, OSError, AttributeError):
        raise CoordinationError("invalid_contract") from None
    # No broad queue polling: this mode must be explicitly assigned a new item.
    return claim_queued_item_by_id(
        connection,
        args.recovery_item_id,
        source=provider,
        job_type=args.job_type.upper(),
    )


def process(connection, item, args, provider):
    lease = api = guard = client = None
    try:
        target, options, contract = resolve(connection, item, args, provider)
        connection.rollback()  # Resolver SELECT transaction only; claim was already committed.
        identity, policy = load_manifest(
            args.recovery_manifest, args.recovery_manifest_sha256, provider, contract
        )
        guard = JobGuard(connection, item)
        if guard.stopped():
            raise CoordinationError("lease_lost")
        api = Controller(args.controller_url, args.admin_api_key)
        lease = DurableLease(api, identity, policy, stopped=guard.stopped)
        attempt_root = (
            Path(args.output_dir)
            / "coordinated"
            / lease.key
            / f"{lease.fence}-{uuid.uuid4().hex}"
        )
        storage = LocalPipelineStorage(attempt_root, item.item_id)
        # This release only supports a verified local raw store, never fictional S3 URIs.
        if storage.storage_base_uri or storage.storage_backend != "LOCAL":
            raise CoordinationError("invalid_contract")
        collected_at = datetime.now(timezone.utc)
        if provider == "KIS":
            from workers.kis_price_worker import write_raw_pages

            client = PolicyKISClient.from_env(
                min_request_interval=policy["intervalMs"] / 1000
            )
            client.session.close()
            client.session = DurableSession(provider, lease, "TraderDataBot-KR/1.0")
            result = client.get_daily_prices(
                exchange_code=contract["exchange"],
                symbol=contract["symbol"],
                start_date=options["start_date"],
                end_date=options["end_date"],
                adjusted=options["adjusted"],
                max_pages=options["max_pages"],
            )
            lease.check()
            sources = [write_raw_pages(storage, target, options, result, collected_at)]
            if not result["bars"]:
                raise CoordinationError("review_required")
        else:
            from backfills.sec_financial.backfill import (
                collect_sec_financial_payloads,
                require_user_agent,
            )

            user_agent = require_user_agent()
            client = PolicySECClient(user_agent)
            client.session.close()
            client.session = DurableSession(provider, lease, user_agent)
            records = collect_sec_financial_payloads(
                client, storage, target, options, collected_at
            )
            lease.check()
            sources = records["source_object"]
        result = commit_raw(connection, lease, sources, attempt_root, item, contract)
        # Post-commit verification never retries the write, even if this read fails.
        receipt = api.call("GET", f"/{lease.key}/receipts/{result['receiptId']}")
        if receipt.get("hash") != result["hash"] or receipt.get("fence") != lease.fence:
            raise CoordinationError("review_required")
        guard.verify_sources(result["sourceIds"], sources)
        return result
    except CoordinationError:
        raise
    except Exception:  # noqa: BLE001 -- legacy collectors may include secrets in errors
        # Legacy run_once must not write a FAILED result after ownership was lost.
        raise CoordinationError("review_required") from None
    finally:
        if client:
            client.session.close()
        if lease:
            lease.close()
        elif api:
            api.close()
        if guard:
            guard.close()
