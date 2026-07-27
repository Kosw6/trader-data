import json
import os
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests


def upsert_collected_source_object(cursor, source):
    """수집 완료 raw 메타데이터를 source_object에 COLLECTED 상태로 저장하고 id를 반환한다."""
    cursor.execute(
        """
        INSERT INTO source_object (
            provider, data_type, external_id, pipeline_job_item_id,
            storage_backend, storage_key, object_uri, object_size,
            content_hash, dedupe_key, collected_at, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'COLLECTED')
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            pipeline_job_item_id = COALESCE(EXCLUDED.pipeline_job_item_id, source_object.pipeline_job_item_id),
            storage_backend = EXCLUDED.storage_backend,
            storage_key = EXCLUDED.storage_key,
            object_uri = EXCLUDED.object_uri,
            object_size = EXCLUDED.object_size,
            processing_status = CASE
                WHEN source_object.processing_status = 'PROCESSED' THEN source_object.processing_status
                ELSE 'COLLECTED'
            END
        RETURNING id
        """,
        (
            source["provider"],
            source["data_type"],
            source["external_id"],
            source.get("pipeline_job_item_id"),
            source.get("storage_backend", "LOCAL"),
            source["storage_key"],
            source["object_uri"],
            source["object_size"],
            source["content_hash"],
            source["dedupe_key"],
            source["collected_at"],
        ),
    )
    return cursor.fetchone()[0]


def report_raw_ready(controller_url, admin_api_key, source_object_id, timeout=10):
    """Go controller에 raw 저장 완료를 보고해 RAW_OBJECT_READY outbox 생성을 요청한다."""
    if not admin_api_key:
        raise RuntimeError("ADMIN_API_KEY is required to call trader-controller")

    url = f"{controller_url.rstrip('/')}/api/v1/admin/source-objects/{source_object_id}/raw-ready"
    response = requests.post(
        url,
        headers={"X-Admin-API-Key": admin_api_key},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def load_source_object(connection, source_object_id):
    """source_object와 연결된 job/item 정보를 함께 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                so.id,
                so.provider,
                so.data_type,
                so.external_id,
                so.pipeline_job_item_id,
                so.storage_backend,
                so.storage_key,
                so.object_uri,
                so.object_size,
                so.content_hash,
                so.dedupe_key,
                so.processing_status,
                so.collected_at,
                i.item_key,
                j.id,
                j.job_type,
                j.source,
                j.parameters
            FROM source_object so
            LEFT JOIN pipeline_job_item i ON i.id = so.pipeline_job_item_id
            LEFT JOIN pipeline_job j ON j.id = i.job_id
            WHERE so.id = %s
            """,
            (source_object_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise LookupError(f"source_object id={source_object_id} not found")
    return source_object_from_row(row)


def load_source_objects_for_item(connection, pipeline_job_item_id, provider=None):
    """하나의 job item이 만든 raw 객체 전체를 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                so.id,
                so.provider,
                so.data_type,
                so.external_id,
                so.pipeline_job_item_id,
                so.storage_backend,
                so.storage_key,
                so.object_uri,
                so.object_size,
                so.content_hash,
                so.dedupe_key,
                so.processing_status,
                so.collected_at,
                i.item_key,
                j.id,
                j.job_type,
                j.source,
                j.parameters
            FROM source_object so
            LEFT JOIN pipeline_job_item i ON i.id = so.pipeline_job_item_id
            LEFT JOIN pipeline_job j ON j.id = i.job_id
            WHERE so.pipeline_job_item_id = %s
              AND (%s IS NULL OR so.provider = %s)
            ORDER BY so.id
            """,
            (pipeline_job_item_id, provider, provider),
        )
        return [source_object_from_row(row) for row in cursor.fetchall()]


def source_object_from_row(row):
    """source_object SELECT row를 dict로 바꿔 ETL 코드가 필드명으로 다루게 한다."""
    return {
        "id": row[0],
        "provider": row[1],
        "data_type": row[2],
        "external_id": row[3],
        "pipeline_job_item_id": row[4],
        "storage_backend": row[5],
        "storage_key": row[6],
        "object_uri": row[7],
        "object_size": row[8],
        "content_hash": row[9],
        "dedupe_key": row[10],
        "processing_status": row[11],
        "collected_at": row[12],
        "item_key": row[13],
        "job_id": row[14],
        "job_type": row[15],
        "job_source": row[16],
        "parameters": row[17] or {},
    }


def mark_source_object_status(connection, source_object_id, status, error_message=None):
    """source_object의 ETL 처리 상태와 처리 시각을 함께 갱신한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE source_object
            SET
                processing_status = %s,
                processing_started_at = CASE
                    WHEN %s = 'PROCESSING' THEN COALESCE(processing_started_at, CURRENT_TIMESTAMP)
                    ELSE processing_started_at
                END,
                processing_finished_at = CASE
                    WHEN %s IN ('PROCESSED', 'FAILED') THEN CURRENT_TIMESTAMP
                    WHEN %s = 'PROCESSING' THEN NULL
                    ELSE processing_finished_at
                END,
                processing_error_message = CASE
                    WHEN %s = 'FAILED' THEN %s
                    WHEN %s IN ('PROCESSING', 'PROCESSED') THEN NULL
                    ELSE processing_error_message
                END,
                processing_attempt_count = CASE
                    WHEN %s = 'PROCESSING' THEN processing_attempt_count + 1
                    ELSE processing_attempt_count
                END
            WHERE id = %s
            """,
            (
                status,
                status,
                status,
                status,
                status,
                error_message,
                status,
                status,
                source_object_id,
            ),
        )


def mark_source_objects_status(connection, source_object_ids, status, error_message=None):
    """여러 source_object 상태와 처리 시각을 한 번에 갱신한다."""
    if not source_object_ids:
        return
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE source_object
            SET
                processing_status = %s,
                processing_started_at = CASE
                    WHEN %s = 'PROCESSING' THEN COALESCE(processing_started_at, CURRENT_TIMESTAMP)
                    ELSE processing_started_at
                END,
                processing_finished_at = CASE
                    WHEN %s IN ('PROCESSED', 'FAILED') THEN CURRENT_TIMESTAMP
                    WHEN %s = 'PROCESSING' THEN NULL
                    ELSE processing_finished_at
                END,
                processing_error_message = CASE
                    WHEN %s = 'FAILED' THEN %s
                    WHEN %s IN ('PROCESSING', 'PROCESSED') THEN NULL
                    ELSE processing_error_message
                END,
                processing_attempt_count = CASE
                    WHEN %s = 'PROCESSING' THEN processing_attempt_count + 1
                    ELSE processing_attempt_count
                END
            WHERE id = ANY(%s)
            """,
            (
                status,
                status,
                status,
                status,
                status,
                error_message,
                status,
                status,
                list(source_object_ids),
            ),
        )


def source_object_completed_with_lineage(connection, source_object_id):
    """source_object가 이미 정규화 적재와 lineage 생성까지 끝났는지 확인한다.

    Kafka offset commit 전에 worker가 죽으면 같은 RAW_OBJECT_READY 메시지가 다시 들어올 수 있다.
    이때 source_object가 PROCESSED이고 record_lineage가 있으면 ETL 재실행 없이
    processed_event만 보강해도 된다.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                so.processing_status = 'PROCESSED'
                AND EXISTS (
                    SELECT 1
                    FROM record_lineage rl
                    WHERE rl.source_object_id = so.id
                    LIMIT 1
                ) AS completed
            FROM source_object so
            WHERE so.id = %s
            """,
            (source_object_id,),
        )
        row = cursor.fetchone()
    return bool(row and row[0])


def local_path_from_source_object(source_object):
    """LOCAL source_object의 object_uri 또는 storage_key를 실제 파일 경로로 변환한다."""
    if source_object["storage_backend"] != "LOCAL":
        raise NotImplementedError("S3 raw read is not implemented in this local PoC")

    candidates = []

    uri = source_object.get("object_uri") or ""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        raw_path = unquote(parsed.path)
        path = Path(raw_path.lstrip("/")) if os.name == "nt" else Path(raw_path)
        candidates.append(path)

        # 컨테이너가 file:///app/... 형태로 기록한 raw 경로는
        # 호스트 PowerShell 실행 시 trader-data 작업 디렉터리 기준으로 다시 매핑한다.
        parts = path.parts
        if parts and parts[0].lower() == "app":
            candidates.append(Path.cwd() / Path(*parts[1:]))
    if uri:
        candidates.append(Path(uri))

    base_dir = os.getenv("RAW_STORAGE_LOCAL_ROOT", "pipeline_output")
    storage_key = source_object.get("storage_key")
    if storage_key:
        candidates.append(Path(base_dir) / storage_key)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    if candidates:
        return candidates[0]
    raise FileNotFoundError(f"source_object has no readable raw path: {source_object}")


def read_raw_payload(source_object):
    """source_object가 가리키는 raw JSON 파일을 읽어 dict로 반환한다."""
    path = local_path_from_source_object(source_object)
    if not path.exists():
        raise FileNotFoundError(f"raw file does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))
