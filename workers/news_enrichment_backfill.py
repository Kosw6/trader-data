"""Explicit bounded backfill for a stopped collection job. Never starts servers or schedules."""
import argparse
import json
import os
from pathlib import Path

from collectors.article_title import ArticleTitleClient
from pipeline.news_collection import digest
from pipeline.news_enrichment import POLICY, enrich_observations


class ReceiptRepository:
    def __init__(self, directory):
        self.path = Path(directory) / "checkpoint.json"

    def check_cancel(self):
        if (self.path.parent / "STOP").exists():
            from workers.news_repository import NewsCancelled
            raise NewsCancelled()

    def save(self, checkpoint):
        self.check_cancel()
        temporary = self.path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, ensure_ascii=False, indent=2); file.flush(); os.fsync(file.fileno())
        os.replace(temporary, self.path)


def select_existing(connection, job_id, limit):
    if type(job_id) is not int or job_id <= 0 or not 1 <= limit <= 100:
        raise ValueError("NEWS_BACKFILL_BOUNDS")
    with connection.cursor() as cursor:
        cursor.execute("""SELECT j.status, EXISTS(SELECT 1 FROM pipeline_job_item i WHERE i.job_id=j.id
            AND i.status IN ('QUEUED','RUNNING','SCHEDULED','RETRY_WAIT')) FROM pipeline_job j
            WHERE j.id=%s AND j.source='NEWS'""", (job_id,))
        row = cursor.fetchone()
        if not row or row[0] not in {"SUCCESS", "FAILED", "PARTIAL_SUCCESS"} or row[1]:
            raise RuntimeError("NEWS_BACKFILL_REQUIRES_FINISHED_JOB")
        cursor.execute("""SELECT DISTINCT o.url_hash,o.source_object_id,o.observation_hash,o.metadata
            FROM news_collection_observation o JOIN pipeline_job_item i
            ON o.source_object_id=CASE WHEN i.checkpoint->>'sourceId' ~ '^[0-9]+$'
                THEN (i.checkpoint->>'sourceId')::bigint END
            JOIN pipeline_job j ON j.id=i.job_id
            WHERE j.id=%s AND i.checkpoint->>'stage'='CANDIDATES_STORED'
            AND (o.metadata->>'collectionContractHash'=i.checkpoint->>'contractHash' OR
                 (j.parameters->>'contract'='news-v1' AND o.metadata->>'collectionContractHash' IS NULL))
            AND jsonb_array_length(COALESCE(o.metadata->'requestedAssets','[]'::jsonb))>0
            AND o.metadata->>'enrichmentPolicy' IS NULL
            AND NOT EXISTS(SELECT 1 FROM news_collection_observation e WHERE e.url_hash=o.url_hash
                AND e.source_object_id=o.source_object_id AND e.metadata->>'originalObservationHash'=o.observation_hash
                AND e.metadata->>'enrichmentPolicy'=%s)
            ORDER BY o.source_object_id,o.url_hash,o.observation_hash LIMIT %s""", (job_id, POLICY, limit))
        rows = [dict(urlHash=r[0].strip(), sourceId=r[1], observationHash=r[2].strip(), metadata=r[3]) for r in cursor.fetchall()]
    connection.commit()
    return rows


def append_revisions(connection, originals, enriched):
    """One transaction; keep all original observations and source IDs unchanged."""
    from psycopg2.extras import Json
    if len(originals) != len(enriched): raise ValueError("NEWS_BACKFILL_COUNT_CONFLICT")
    inserted = 0
    try:
        with connection.cursor() as cursor:
            for original, row in zip(originals, enriched):
                if (row["urlHash"] != original["urlHash"] or digest(original["metadata"]) != original["observationHash"] or
                        row["metadata"].get("originalObservationHash") != original["observationHash"] or
                        row["metadata"].get("enrichmentPolicy") != POLICY or digest(row["metadata"]) != row["observationHash"]):
                    raise ValueError("NEWS_BACKFILL_EVIDENCE_CONFLICT")
                cursor.execute("SELECT metadata FROM news_collection_observation WHERE url_hash=%s AND source_object_id=%s AND observation_hash=%s",
                               (original["urlHash"], original["sourceId"], original["observationHash"]))
                current = cursor.fetchone()
                if not current or current[0] != original["metadata"]: raise RuntimeError("NEWS_BACKFILL_SOURCE_CONFLICT")
                cursor.execute("""INSERT INTO news_collection_observation(url_hash,source_object_id,observation_hash,metadata)
                    VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (original["urlHash"], original["sourceId"], row["observationHash"], Json(row["metadata"])))
                inserted += cursor.rowcount
        connection.commit()
        # Separate post-commit read; commit failure/unknown outcome never becomes a success receipt.
        with connection.cursor() as cursor:
            for original, row in zip(originals, enriched):
                cursor.execute("SELECT metadata FROM news_collection_observation WHERE url_hash=%s AND source_object_id=%s AND observation_hash=%s",
                               (original["urlHash"], original["sourceId"], row["observationHash"]))
                if cursor.fetchone() != (row["metadata"],): raise RuntimeError("NEWS_BACKFILL_READBACK_CONFLICT")
        connection.commit()
        return inserted
    except BaseException:
        connection.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", type=int, required=True)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="fetch missing titles and append DB observations")
    parser.add_argument("--resume", action="store_true", help="reuse this directory's admitted URLs and outcomes")
    args = parser.parse_args()
    from workers.db import connect_database
    connection = connect_database()
    try:
        if args.resume:
            manifest = json.loads((args.output / "selection.json").read_text(encoding="utf-8"))
            if manifest["jobId"] != args.job_id or manifest["policy"] != POLICY: raise ValueError("NEWS_BACKFILL_SCOPE_CONFLICT")
            originals = manifest["observations"]
        else:
            originals = select_existing(connection, args.job_id, args.limit)
            args.output.mkdir(parents=True, exist_ok=False)
            with (args.output / "selection.json").open("x", encoding="utf-8") as file:
                json.dump(dict(jobId=args.job_id, policy=POLICY, observations=originals), file, ensure_ascii=False, indent=2)
        if not args.apply:
            print(json.dumps(dict(status="PREVIEW_ONLY", selected=len(originals), publisherRequests=0, dbWrites=0)))
            return
        repo = ReceiptRepository(args.output)
        checkpoint = json.loads(repo.path.read_text(encoding="utf-8")) if repo.path.exists() else {}
        selection_hash = digest(originals)
        if checkpoint and checkpoint.get("selectionHash") != selection_hash: raise ValueError("NEWS_BACKFILL_SCOPE_CONFLICT")
        checkpoint["selectionHash"] = selection_hash
        enriched = enrich_observations(originals, checkpoint, repo, ArticleTitleClient())
        repo.check_cancel()
        inserted = append_revisions(connection, originals, enriched)
        checkpoint.update(status="APPENDED_READBACK_VERIFIED", inserted=inserted)
        repo.save(checkpoint)
        result_path = args.output / "enriched.json"
        if result_path.exists():
            if json.loads(result_path.read_text(encoding="utf-8")) != enriched:
                raise RuntimeError("NEWS_BACKFILL_RECEIPT_CONFLICT")
        else:
            with result_path.open("x", encoding="utf-8") as file:
                json.dump(enriched, file, ensure_ascii=False, indent=2)
        print(json.dumps(dict(status=checkpoint["status"], inserted=inserted)))
    finally:
        connection.close()


if __name__ == "__main__": main()
