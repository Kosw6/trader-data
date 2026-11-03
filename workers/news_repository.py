"""Durable per-unit budgets, immutable source registration and append-only observations."""
import hashlib
import time
from datetime import datetime, timezone

from collectors.s3_raw import RawStorageError


class NewsCancelled(RuntimeError):
    pass


class NewsRepository:
    def __init__(self, connection, item):
        self.connection, self.item = connection, item

    def load(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT checkpoint FROM pipeline_job_item WHERE id=%s", (self.item.item_id,))
            value = cursor.fetchone()[0] or {}
        self.connection.commit()
        return value

    def check_cancel(self):
        with self.connection.cursor() as cursor:
            cursor.execute("""SELECT j.status,i.status,i.attempt_count FROM pipeline_job j
                              JOIN pipeline_job_item i ON i.job_id=j.id WHERE i.id=%s""", (self.item.item_id,))
            row = cursor.fetchone()
        self.connection.commit()
        if not row or row[1] != "RUNNING" or row[2] != self.item.attempt_count:
            raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
        if row[0] in {"CANCEL_REQUESTED", "CANCELLED"}:
            raise NewsCancelled()

    def save(self, checkpoint):
        from psycopg2.extras import Json
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE pipeline_job_item SET checkpoint=%s WHERE id=%s AND status='RUNNING' AND attempt_count=%s",
                           (Json(checkpoint), self.item.item_id, self.item.attempt_count))
            if cursor.rowcount != 1:
                raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
        self.connection.commit()

    def reserve(self, checkpoint, provider):
        # Serialize slot reservations across all processes; never hold a transaction while waiting/networking.
        from psycopg2.extras import Json
        with self.connection.cursor() as cursor:
            cursor.execute("""INSERT INTO news_collection_pacing(provider,next_request_at)
                              VALUES (%s,CURRENT_TIMESTAMP) ON CONFLICT DO NOTHING""", (provider,))
            cursor.execute("""UPDATE news_collection_pacing SET
                              next_request_at=GREATEST(next_request_at,CURRENT_TIMESTAMP)+INTERVAL '5.1 seconds'
                              WHERE provider=%s RETURNING next_request_at-INTERVAL '5.1 seconds'""", (provider,))
            slot = cursor.fetchone()[0]
            checkpoint["attempts"] += 1
            checkpoint["requestSlot"] = slot.isoformat()
            cursor.execute("UPDATE pipeline_job_item SET checkpoint=%s WHERE id=%s AND status='RUNNING' AND attempt_count=%s",
                           (Json(checkpoint), self.item.item_id, self.item.attempt_count))
            if cursor.rowcount != 1:
                raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
        self.connection.commit()  # Reserve the request before sending, including across crashes.
        return max(0, (slot-datetime.now(timezone.utc)).total_seconds())

    def finish(self, state, checkpoint, candidates=(), observations=(), error=None):
        from psycopg2.extras import Json, execute_values
        # All candidate rows, observations and terminal checkpoint commit together.
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT status FROM pipeline_job WHERE id=%s FOR UPDATE", (self.item.job_id,))
            if cursor.fetchone()[0] in {"CANCEL_REQUESTED", "CANCELLED"}:
                state, error, candidates, observations = "CANCELLED", "USER_CANCELLED", (), ()
                checkpoint["stage"] = "CANCELLED"
            cursor.execute("SELECT status,attempt_count FROM pipeline_job_item WHERE id=%s FOR UPDATE", (self.item.item_id,))
            owner = cursor.fetchone()
            if not owner or owner != ("RUNNING", self.item.attempt_count):
                raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
            if candidates:
                execute_values(cursor, """INSERT INTO news_collection_candidate(url_hash,url) VALUES %s
                                          ON CONFLICT (url_hash) DO NOTHING""",
                               [(r["urlHash"], r["url"]) for r in candidates])
            if observations:
                execute_values(cursor, """INSERT INTO news_collection_observation
                    (url_hash,source_object_id,observation_hash,metadata) VALUES %s ON CONFLICT DO NOTHING""",
                    [(r["urlHash"], checkpoint["sourceId"], r["observationHash"], Json(r["metadata"])) for r in observations])
            cursor.execute("""UPDATE pipeline_job_item SET status=%s,checkpoint=%s,error_code=%s,
                error_message=%s,completed_at=CURRENT_TIMESTAMP WHERE id=%s AND status='RUNNING' AND attempt_count=%s""",
                (state, Json(checkpoint), error, error, self.item.item_id, self.item.attempt_count))
            if cursor.rowcount != 1:
                raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
            cursor.execute("""WITH counts AS (
                SELECT COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status IN ('COLLECTED','SUCCESS','SKIPPED')) AS successful,
                    COUNT(*) FILTER (WHERE status IN ('QUEUED','SCHEDULED','RETRY_WAIT','RUNNING')) AS pending
                FROM pipeline_job_item WHERE job_id=%s
            ), resolved AS (
                SELECT CASE
                    WHEN j.status IN ('CANCEL_REQUESTED','CANCELLED') AND c.pending=0 THEN 'CANCELLED'
                    WHEN j.status IN ('CANCEL_REQUESTED','CANCELLED') THEN j.status
                    WHEN c.successful=c.total THEN 'SUCCESS'
                    WHEN c.pending>0 THEN 'RUNNING'
                    WHEN c.successful=0 THEN 'FAILED' ELSE 'PARTIAL_SUCCESS' END AS status
                FROM counts c JOIN pipeline_job j ON j.id=%s
            ) UPDATE pipeline_job j SET status=r.status,
                completed_at=CASE WHEN r.status IN ('SUCCESS','FAILED','PARTIAL_SUCCESS','CANCELLED')
                    THEN CURRENT_TIMESTAMP ELSE NULL END,updated_at=CURRENT_TIMESTAMP
                FROM resolved r WHERE j.id=%s RETURNING j.status""",
                (self.item.job_id, self.item.job_id, self.item.job_id))
            result = cursor.fetchone()[0]
        self.connection.commit()
        return result

    def register_raw(self, source, checkpoint):
        # Retain the first source's original job link, unlike mutable generic upserts.
        from psycopg2.extras import Json
        with self.connection.cursor() as cursor:
            cursor.execute("""INSERT INTO source_object(provider,data_type,external_id,pipeline_job_item_id,
                storage_backend,storage_key,object_uri,object_size,content_hash,dedupe_key,collected_at,processing_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'COLLECTED')
                ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key=source_object.dedupe_key RETURNING id""",
                tuple(source[k] for k in ("provider","data_type","external_id","pipeline_job_item_id",
                    "storage_backend","storage_key","object_uri","object_size","content_hash","dedupe_key","collected_at")))
            checkpoint["sourceId"] = cursor.fetchone()[0]
            checkpoint["rawSource"] = source
            cursor.execute("UPDATE pipeline_job_item SET checkpoint=%s WHERE id=%s AND status='RUNNING' AND attempt_count=%s",
                           (Json(checkpoint), self.item.item_id, self.item.attempt_count))
            if cursor.rowcount != 1:
                raise RuntimeError("NEWS_ITEM_OWNERSHIP_LOST")
        self.connection.commit()


def read_verified_raw(source):
    if source["storage_backend"] == "S3":
        from collectors.s3_raw import read
        raw = read(source)
    elif source["storage_backend"] == "LOCAL":
        from workers.raw_repository import local_path_from_source_object
        path = local_path_from_source_object(source)
        if path.stat().st_size != source["object_size"]:
            raise RawStorageError("news_raw_size_conflict")
        raw = path.read_bytes()
    else:
        raise RawStorageError("news_raw_backend_invalid")
    if len(raw) != source["object_size"] or hashlib.sha256(raw).hexdigest() != source["content_hash"]:
        raise RawStorageError("news_raw_hash_conflict")
    return raw


def wait_cancelable(seconds, repository, sleep=time.sleep):
    end = time.monotonic()+seconds
    while True:
        repository.check_cancel()
        remaining = end-time.monotonic()
        if remaining <= 0:
            return
        sleep(min(1, remaining))
