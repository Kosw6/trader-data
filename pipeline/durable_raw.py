"""Same-database raw commit adapter for V043. No HTTP or automatic SQL retries."""

import json
import threading
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

import psycopg2

from collectors.common import content_hash
from pipeline.db_connections import independent_connection
from pipeline.durable_requests import CoordinationError, canonical_hash


class JobGuard:
    """Independent read-only connection; never shares the worker transaction."""

    def __init__(self, connection, item):
        self.item = item
        self.lock = threading.Lock()
        self.db = independent_connection(connection)
        self.db.set_session(readonly=True, autocommit=True)
        with self.db.cursor() as cursor:
            cursor.execute("SET statement_timeout='2s'")

    def stopped(self):
        with self.lock:
            try:
                with self.db.cursor() as cursor:
                    cursor.execute(
                        """SELECT i.status,j.status,i.attempt_count FROM pipeline_job_item i
                        JOIN pipeline_job j ON j.id=i.job_id WHERE i.id=%s AND j.id=%s""",
                        (self.item.item_id, self.item.job_id),
                    )
                    return cursor.fetchone() != (
                        "RUNNING",
                        "RUNNING",
                        self.item.attempt_count,
                    )
            except psycopg2.Error:
                return True  # Missing/unknown cancellation state is not permission.

    def close(self):
        with self.lock:
            self.db.close()

    def verify_sources(self, source_ids, sources):
        with self.lock, self.db.cursor() as cursor:
            cursor.execute(
                "SELECT id,content_hash,dedupe_key FROM source_object WHERE id=ANY(%s)",
                (source_ids,),
            )
            actual = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
            expected = {
                sid: (s["content_hash"], s["dedupe_key"])
                for sid, s in zip(source_ids, sources, strict=True)
            }
            if actual != expected:
                raise CoordinationError("review_required")


def verify_files(sources, root):
    if not sources:
        raise CoordinationError("review_required")
    root = Path(root).resolve()
    for source in sources:
        uri = urlsplit(source["object_uri"])
        path = Path(url2pathname(unquote(uri.path))).resolve()
        if (
            uri.scheme != "file"
            or uri.netloc
            or source["storage_backend"] != "LOCAL"
            or not path.is_relative_to(root)
        ):
            raise CoordinationError("invalid_contract")
        encoded = path.read_bytes()
        if (
            len(encoded) != source["object_size"]
            or content_hash(json.loads(encoded)) != source["content_hash"]
        ):
            raise CoordinationError("review_required")


def insert_raw(cursor, source):
    fields = (
        "provider",
        "data_type",
        "external_id",
        "pipeline_job_item_id",
        "storage_backend",
        "storage_key",
        "object_uri",
        "object_size",
        "content_hash",
        "dedupe_key",
        "collected_at",
    )
    cursor.execute(
        """INSERT INTO source_object(provider,data_type,external_id,pipeline_job_item_id,
        storage_backend,storage_key,object_uri,object_size,content_hash,dedupe_key,collected_at,processing_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'COLLECTED') ON CONFLICT(dedupe_key) DO NOTHING""",
        tuple(source.get(field) for field in fields),
    )
    cursor.execute(
        """SELECT id,provider,data_type,external_id,content_hash,object_size
        FROM source_object WHERE dedupe_key=%s FOR SHARE""",
        (source["dedupe_key"],),
    )
    row = cursor.fetchone()
    expected = tuple(
        source.get(f)
        for f in ("provider", "data_type", "external_id", "content_hash", "object_size")
    )
    if row is None or row[1:] != expected:
        raise CoordinationError("review_required")
    return row[0]  # Existing URI/status/ownership are NEVER overwritten.


class TransactionView:
    """Trusted complete_item helper joins outer commit; not a SQL sandbox."""

    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        pass


def commit_raw(connection, lease, sources, root, item=None, contract=None):
    from workers.job_repository import complete_item

    lease.renew()
    verify_files(sources, root)
    if connection.autocommit:
        raise CoordinationError("invalid_contract")
    if (
        connection.get_transaction_status()
        != psycopg2.extensions.TRANSACTION_STATUS_IDLE
    ):
        raise CoordinationError("invalid_contract")
    manifest = sorted(
        [
            {k: s[k] for k in ("dedupe_key", "content_hash", "object_size")}
            for s in sources
        ],
        key=lambda s: s["dedupe_key"],
    )
    digest = canonical_hash(manifest)
    receipt_id = "raw-" + digest
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout='2s'")
            cursor.execute("SET LOCAL statement_timeout='10s'")
            cursor.execute(
                """SELECT identity,policy FROM pipeline_recovery_work
                WHERE work_key=%s FOR UPDATE""",
                (lease.key,),
            )
            stored_contract = cursor.fetchone()
            if stored_contract != (lease.identity, lease.policy):
                raise CoordinationError("contract_conflict")
            cursor.execute(
                """SELECT 1 FROM pipeline_recovery_work WHERE work_key=%s
                AND owner=%s AND fence=%s AND state='LEASED' AND phase='FETCH_RAW'
                AND lease_until>clock_timestamp() AND deadline>clock_timestamp()""",
                (lease.key, lease.owner, lease.fence),
            )
            if cursor.fetchone() is None:
                raise CoordinationError("lease_lost")
            cursor.execute(
                """SELECT 1 FROM pipeline_recovery_request
                WHERE work_key=%s AND outcome='PENDING' LIMIT 1""",
                (lease.key,),
            )
            if cursor.fetchone():
                raise CoordinationError("review_required")
            if item:
                cursor.execute(
                    "SELECT status FROM pipeline_job WHERE id=%s FOR UPDATE",
                    (item.job_id,),
                )
                if cursor.fetchone() != ("RUNNING",):
                    raise CoordinationError("lease_lost")
                cursor.execute(
                    """SELECT status,attempt_count FROM pipeline_job_item
                    WHERE id=%s AND job_id=%s FOR UPDATE""",
                    (item.item_id, item.job_id),
                )
                if cursor.fetchone() != ("RUNNING", item.attempt_count):
                    raise CoordinationError("lease_lost")
            source_ids = [insert_raw(cursor, source) for source in sources]
            if item:
                complete_item(
                    TransactionView(connection),
                    item.item_id,
                    "COLLECTED",
                    checkpoint={
                        "worker": "coordinated-v1",
                        "workKey": lease.key,
                        "fence": lease.fence,
                        "sourceObjectIds": source_ids,
                        "rawReceiptId": receipt_id,
                        "rawReadyNotificationPending": True,
                        "phase": "RAW_READY",
                    },
                )
            cursor.execute(
                """INSERT INTO pipeline_recovery_receipt
                (work_key,receipt_id,from_phase,to_phase,content_hash,fence)
                VALUES (%s,%s,'FETCH_RAW','RAW_READY',%s,%s)""",
                (lease.key, receipt_id, digest, lease.fence),
            )
            if item:
                if (
                    not isinstance(contract, dict)
                    or contract.get("target") != lease.identity["target"]
                ):
                    raise CoordinationError("invalid_contract")
                cursor.execute(
                    """INSERT INTO pipeline_recovery_raw_intent
                    (work_key,receipt_id,item_id,job_id,sources,contract)
                    VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                    (
                        lease.key,
                        receipt_id,
                        item.item_id,
                        item.job_id,
                        json.dumps(
                            [
                                {
                                    "id": sid,
                                    "hash": s["content_hash"],
                                    "dedupeKey": s["dedupe_key"],
                                    "size": s["object_size"],
                                    "dataType": s["data_type"],
                                }
                                for sid, s in zip(source_ids, sources, strict=True)
                            ]
                        ),
                        json.dumps(contract),
                    ),
                )
            cursor.execute(
                """UPDATE pipeline_recovery_work SET phase='RAW_READY',state='READY',
                owner='',lease_until=NULL,progress_at=clock_timestamp()
                WHERE work_key=%s AND owner=%s AND fence=%s AND state='LEASED'
                AND lease_until>clock_timestamp() AND deadline>clock_timestamp()""",
                (lease.key, lease.owner, lease.fence),
            )
            if cursor.rowcount != 1:
                raise CoordinationError("lease_lost")
        # Stop renewals only after all fenced checks; commit owns this locked transition.
        lease.done.set()
        try:
            connection.commit()
        except psycopg2.Error:
            raise CoordinationError("commit_uncertain") from None
        return {
            "receiptId": receipt_id,
            "hash": digest,
            "sourceIds": source_ids,
            "rawReadyNotificationPending": True,
        }
    except CoordinationError:
        connection.rollback()
        raise
    except Exception:  # noqa: BLE001 -- rollback and redact all callback/DB details
        connection.rollback()
        raise CoordinationError("review_required") from None
    except BaseException:
        connection.rollback()
        raise
