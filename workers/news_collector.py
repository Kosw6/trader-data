"""NEWS jobs via the existing Kafka consumer; no standalone service or automatic launch."""
import os
import csv
import zipfile
from pathlib import Path
from datetime import datetime, timezone

from collectors.common import LocalPipelineStorage
from collectors.news_http import NewsHTTP, NewsFetchError
from pipeline.news_collection import digest, normalize, parse_rss, parse_gkg, validate_unit
from pipeline.news_targets import select_targets
from workers.news_repository import NewsRepository, NewsCancelled, read_verified_raw, wait_cancelable


def collect(item, repository, storage, client, wait=wait_cancelable, title_client=None):
    provider, unit, query = validate_unit(item)
    from pipeline.news_enrichment import POLICY, enrich_observations
    policy = item.parameters.get("enrichmentPolicy")
    if policy not in (None, POLICY):
        raise ValueError("NEWS_ENRICHMENT_POLICY_INVALID")
    identity = digest([item.item_key, item.parameters])
    checkpoint = repository.load()
    if checkpoint:
        if checkpoint.get("contractHash") != identity:
            raise RuntimeError("NEWS_CHECKPOINT_CONTRACT_CONFLICT")
    else:
        checkpoint = {"worker": "news_collector", "contractHash": identity, "attempts": 0,
                      "provider": provider, "unit": unit, "stage": "READY", "ragEligible": False}
    try:
        repository.check_cancel()
        if checkpoint.get("rawSource"):
            raw = read_verified_raw(checkpoint["rawSource"])
        else:
            while checkpoint["attempts"] < 3:
                # Also honor persisted backoff after process recovery.
                not_before = checkpoint.get("retryAt")
                if not_before:
                    wait(max(0, (datetime.fromisoformat(not_before)-datetime.now(timezone.utc)).total_seconds()), repository)
                wait(repository.reserve(checkpoint, provider), repository)
                repository.check_cancel()
                try:
                    raw, receipt = client.fetch(provider, unit, query)
                    break
                except NewsFetchError as error:
                    from datetime import timedelta
                    checkpoint.setdefault("requestHistory", []).append({"attempt": checkpoint["attempts"], "error": error.code})
                    checkpoint.update(stage="FETCH_FAILED", error=error.code,
                        retryAt=(datetime.now(timezone.utc)+timedelta(seconds=max(2**checkpoint["attempts"], error.retry_after))).isoformat())
                    repository.save(checkpoint)
                    if not error.retryable or checkpoint["attempts"] >= 3:
                        return repository.finish("FAILED", checkpoint, error=error.code)
            else:
                return repository.finish("FAILED", checkpoint, error="NEWS_ATTEMPT_BUDGET_EXHAUSTED")
            # Stop before storing if a user cancelled during the bounded request.
            repository.check_cancel()
            source = storage.write_raw_bytes("NEWS", "RSS_FEED" if provider == "RSS" else "GKG_SNAPSHOT",
                item.item_key, raw, "xml" if provider == "RSS" else "zip", datetime.now(timezone.utc))
            checkpoint.pop("error", None)
            checkpoint.pop("retryAt", None)
            checkpoint.setdefault("requestHistory", []).append({"attempt": checkpoint["attempts"], "http": 200, "bytes": len(raw)})
            checkpoint.update(stage="RAW_STORED", receipt=receipt)
            repository.register_raw(source, checkpoint)
            # Re-read immutable bytes, not merely trust a path/PUT receipt.
            raw = read_verified_raw(source)
        try:
            rows, stats = parse_rss(raw, unit) if provider == "RSS" else parse_gkg(raw, unit)
            rows,target_stats=select_targets(rows,item.parameters,query)
            stats.update(target_stats)
            candidates, observations = normalize(rows, provider)
        except (ValueError, UnicodeError, csv.Error, zipfile.BadZipFile):
            checkpoint.update(stage="PARSE_FAILED", error="NEWS_SCHEMA_REVIEW_REQUIRED")
            return repository.finish("FAILED", checkpoint, error="NEWS_SCHEMA_REVIEW_REQUIRED")
        for row in observations:
            row["metadata"].update(collectionUnit=item.item_key, query=query, collectionContractHash=identity)
            # The observation hash includes query provenance, without merging away source evidence.
            row["observationHash"] = digest(row["metadata"])
        if policy == POLICY:
            observations = enrich_observations(observations, checkpoint, repository, title_client)
        checkpoint.update(stage="CANDIDATES_STORED", stats=stats, candidates=len(candidates), observations=len(observations))
        repository.check_cancel()
        if stats["invalid"]:
            return repository.finish("FAILED", checkpoint, candidates, observations, "NEWS_PARTIAL_SCHEMA_REVIEW_REQUIRED")
        return repository.finish("COLLECTED", checkpoint, candidates, observations)
    except NewsCancelled:
        checkpoint["stage"] = "CANCELLED"
        return repository.finish("CANCELLED", checkpoint, error="USER_CANCELLED")


def process_item(connection, item):
    if os.getenv("NEWS_COLLECTION_ENABLED", "false").lower() != "true":
        raise RuntimeError("NEWS_COLLECTION_DISABLED")
    if item.parameters.get("recoveryVersion"):
        from workers.coordinated_news_collector import process_item as coordinated_process
        return coordinated_process(connection,item)
    # No query- or user-controlled filesystem path.
    storage = LocalPipelineStorage(Path(os.getenv("NEWS_OUTPUT_ROOT", "pipeline_output/news")) /
                                   f"job-{int(item.job_id)}" / f"item-{int(item.item_id)}", item.item_id)
    client = NewsHTTP()
    from collectors.article_title import ArticleTitleClient
    try:
        return collect(item, NewsRepository(connection, item), storage, client, title_client=ArticleTitleClient())
    finally:
        client.close()
