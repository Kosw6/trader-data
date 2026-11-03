"""Explicit new NEWS jobs only. Immutable raw checkpoint precedes Kafka handoff."""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

from collectors.common import LocalPipelineStorage
from collectors.news_http import NewsFetchError, NewsHTTP
from pipeline.coordinated_news import VERSION, collection_contract, envelope
from pipeline.durable_raw import JobGuard, commit_raw
from pipeline.durable_requests import (
    Controller,
    CoordinationError,
    DurableLease,
    canonical_hash,
)


def controller():
    return Controller(
        os.getenv("RECOVERY_CONTROLLER_URL", "https://revestlab.com"),
        os.environ["ADMIN_API_KEY"],
        proxy_prefix=os.getenv("RECOVERY_CONTROLLER_PROXY_PREFIX", "/api/controller"),
    )


def durable_storage(root, item_id):
    # Only this adapter uses the mounted durable local store. Do not change the
    # shared worker's S3 environment or legacy collectors' storage configuration.
    storage = LocalPipelineStorage(root, item_id)
    storage.storage_backend = "LOCAL"
    storage.storage_base_uri = ""
    return storage


class DurableNewsHTTP(NewsHTTP):
    def __init__(self, lease, session=None, *, connection=None):
        super().__init__(session)
        self.lease = lease
        self.connection = connection

    def wait_shared_slot(self, provider):
        # The same pacing table as legacy NEWS collectors prevents the two paths
        # from sending concurrently during gradual cutover. Do not reset its slot.
        if self.connection is None:
            return
        with self.connection.cursor() as cur:
            cur.execute(
                "INSERT INTO news_collection_pacing(provider,next_request_at) VALUES(%s,clock_timestamp()) ON CONFLICT DO NOTHING",
                (provider,),
            )
            cur.execute(
                "UPDATE news_collection_pacing SET next_request_at=GREATEST(next_request_at,clock_timestamp())+interval '5.1 seconds' WHERE provider=%s RETURNING GREATEST(0,EXTRACT(EPOCH FROM(next_request_at-interval '5.1 seconds'-clock_timestamp())))",
                (provider,),
            )
            seconds = float(cur.fetchone()[0])
        self.connection.commit()
        until = time.monotonic() + seconds
        while True:
            self.lease.check()
            left = until - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(1, left))

    def fetch(self, provider, unit, query=None):
        operation = canonical_hash({"provider": provider, "unit": unit, "query": query})
        while True:
            request = self.lease.reserve(operation)
            self.wait_shared_slot(provider)
            try:
                raw, receipt = super().fetch(provider, unit, query)
            except NewsFetchError as e:
                outcome = (
                    "RATE_LIMIT"
                    if e.code == "NEWS_HTTP_429"
                    else "TRANSIENT_TRANSPORT"
                    if e.retryable
                    else "SOURCE_NOT_FOUND"
                    if e.code == "NEWS_HTTP_404"
                    else "AUTH_BLOCKED"
                    if e.code in {"NEWS_HTTP_401", "NEWS_HTTP_403"}
                    else "QUALITY_OR_SEMANTIC_CONFLICT"
                )
                self.lease.report(request, outcome, e.retry_after * 1000)
                if e.retryable:
                    continue  # Persistent budgets/quota decide, never local reset.
                raise CoordinationError("review_required") from None
            self.lease.report(request, "HTTP_OK")
            self.lease.check()
            return raw, receipt


def collect_raw(
    db, item, root, *, api=None, client_factory=DurableNewsHTTP, policy=None
):
    contract = collection_contract(item)
    unit = contract["unit"]
    day = (
        unit
        if contract["provider"] == "RSS"
        else unit[:4] + "-" + unit[4:6] + "-" + unit[6:8]
    )
    identity = {
        "provider": "NEWS",
        "quotaScope": "news-" + contract["provider"].lower() + "-production",
        "target": contract["target"],
        "dataset": "news-" + contract["provider"].lower(),
        "start": day,
        "end": day,
        "revision": canonical_hash({"generation": VERSION, "contract": contract}),
    }
    policy = policy or {
        "version": "coord-v1",
        "maxClaims": 30,
        "maxRequests": 3,
        "maxRetries": 2,
        "maxElapsedMs": 86400000,
        "leaseMs": 300000,
        "intervalMs": 5100,
        "baseDelayMs": 2000,
        "maxDelayMs": 60000,
        "rateLimitMs": 60000,
    }
    guard = JobGuard(db, item)
    lease = client = None
    try:
        lease = DurableLease(
            api or controller(), identity, policy, stopped=guard.stopped
        )
        attempt = Path(root) / lease.key / str(lease.fence)
        storage = durable_storage(attempt, item.item_id)
        client = client_factory(lease, connection=db)
        raw, receipt = client.fetch(contract["provider"], unit, contract["query"])
        source = storage.write_raw_json(
            "NEWS",
            "NEWS_ENVELOPE",
            item.item_key,
            envelope(raw, contract, receipt),
            datetime.now(timezone.utc),
        )
        result = commit_raw(db, lease, [source], root, item, contract)
        guard.verify_sources(result["sourceIds"], [source])
        return {"workKey": lease.key, **result}
    finally:
        if client:
            client.close()
        if lease:
            lease.close()
        guard.close()


def process_item(db, item):
    if os.getenv("NEWS_RECOVERY_ENABLED") != "true":
        raise CoordinationError("invalid_contract")
    # A single recovery consumer owns the post-raw dispatcher. The command
    # collector returns only after the atomic checkpoint; it must not race that
    # consumer for RAW_READY or accidentally begin ETL when dispatch won first.
    return collect_raw(db, item, os.environ["NEWS_RECOVERY_RAW_ROOT"])
