from dataclasses import dataclass
from datetime import datetime


TERMINAL_ITEM_STATUSES = {"SUCCESS", "COLLECTED", "SKIPPED", "FAILED", "CANCELLED"}
SUCCESS_ITEM_STATUSES = {"SUCCESS", "COLLECTED", "SKIPPED"}
FAILURE_ITEM_STATUSES = {"FAILED", "CANCELLED"}


@dataclass(frozen=True)
class ClaimedJobItem:
    """worker가 처리권을 선점한 pipeline_job_item 한 건을 표현한다."""

    item_id: int
    job_id: int
    item_key: str
    job_type: str
    source: str
    priority: int
    parameters: dict
    attempt_count: int
    claimed_at: datetime


def claim_next_queued_item(connection, source=None, job_type=None):
    """QUEUED 상태의 item 하나를 잠그고 RUNNING으로 바꿔 다른 worker와 중복 처리를 막는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH candidate AS (
                SELECT i.id
                FROM pipeline_job_item i
                JOIN pipeline_job j ON j.id = i.job_id
                WHERE i.status = 'QUEUED'
                  AND j.status IN ('QUEUED', 'RUNNING')
                  AND (%s IS NULL OR j.source = %s)
                  AND (%s IS NULL OR j.job_type = %s)
                ORDER BY j.priority DESC, j.scheduled_at NULLS FIRST, i.id
                LIMIT 1
                FOR UPDATE OF i SKIP LOCKED
            ), updated_item AS (
                UPDATE pipeline_job_item i
                SET status = 'RUNNING',
                    attempt_count = attempt_count + 1,
                    started_at = CURRENT_TIMESTAMP,
                    completed_at = NULL,
                    error_code = NULL,
                    error_message = NULL
                FROM candidate
                WHERE i.id = candidate.id
                RETURNING
                    i.id,
                    i.job_id,
                    i.item_key,
                    i.attempt_count,
                    i.started_at
            )
            SELECT
                ui.id,
                ui.job_id,
                ui.item_key,
                ui.attempt_count,
                ui.started_at,
                j.job_type,
                j.source,
                j.priority,
                j.parameters
            FROM updated_item ui
            JOIN pipeline_job j ON j.id = ui.job_id
            """,
            (source, source, job_type, job_type),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        item = ClaimedJobItem(
            item_id=row[0],
            job_id=row[1],
            item_key=row[2],
            attempt_count=row[3],
            claimed_at=row[4],
            job_type=row[5],
            source=row[6],
            priority=row[7],
            parameters=row[8] or {},
        )
        _mark_job_running(cursor, item.job_id)
    connection.commit()
    return item


def claim_queued_item_by_id(connection, item_id, source=None, job_type=None):
    """Kafka 메시지에 들어 있는 item_id 하나만 RUNNING으로 선점한다.

    일반 polling worker는 다음 QUEUED item을 가져가지만, Kafka consumer는
    "이 메시지가 가리키는 item"을 처리해야 한다. 그래서 item_id를 조건에
    넣고, 상태가 아직 QUEUED일 때만 RUNNING으로 바꾼다.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH candidate AS (
                SELECT i.id
                FROM pipeline_job_item i
                JOIN pipeline_job j ON j.id = i.job_id
                WHERE i.id = %s
                  AND i.status = 'QUEUED'
                  AND j.status IN ('QUEUED', 'RUNNING')
                  AND (%s IS NULL OR j.source = %s)
                  AND (%s IS NULL OR j.job_type = %s)
                LIMIT 1
                FOR UPDATE OF i SKIP LOCKED
            ), updated_item AS (
                UPDATE pipeline_job_item i
                SET status = 'RUNNING',
                    attempt_count = attempt_count + 1,
                    started_at = CURRENT_TIMESTAMP,
                    completed_at = NULL,
                    error_code = NULL,
                    error_message = NULL
                FROM candidate
                WHERE i.id = candidate.id
                RETURNING
                    i.id,
                    i.job_id,
                    i.item_key,
                    i.attempt_count,
                    i.started_at
            )
            SELECT
                ui.id,
                ui.job_id,
                ui.item_key,
                ui.attempt_count,
                ui.started_at,
                j.job_type,
                j.source,
                j.priority,
                j.parameters
            FROM updated_item ui
            JOIN pipeline_job j ON j.id = ui.job_id
            """,
            (item_id, source, source, job_type, job_type),
        )
        row = cursor.fetchone()
        if row is None:
            connection.rollback()
            return None

        item = ClaimedJobItem(
            item_id=row[0],
            job_id=row[1],
            item_key=row[2],
            attempt_count=row[3],
            claimed_at=row[4],
            job_type=row[5],
            source=row[6],
            priority=row[7],
            parameters=row[8] or {},
        )
        _mark_job_running(cursor, item.job_id)
    connection.commit()
    return item


def get_item_status(connection, item_id):
    """consumer가 중복 메시지를 받았을 때 item의 현재 상태를 확인한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                i.status,
                i.job_id,
                i.item_key,
                j.source,
                j.job_type
            FROM pipeline_job_item i
            JOIN pipeline_job j ON j.id = i.job_id
            WHERE i.id = %s
            """,
            (item_id,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "job_id": row[1],
        "item_key": row[2],
        "source": row[3],
        "job_type": row[4],
    }


def peek_next_queued_item(connection, source=None, job_type=None):
    """DB 상태를 바꾸지 않고 다음에 선점될 QUEUED item을 미리 확인한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                i.id,
                i.job_id,
                i.item_key,
                i.attempt_count,
                i.started_at,
                j.job_type,
                j.source,
                j.priority,
                j.parameters
            FROM pipeline_job_item i
            JOIN pipeline_job j ON j.id = i.job_id
            WHERE i.status = 'QUEUED'
              AND j.status IN ('QUEUED', 'RUNNING')
              AND (%s IS NULL OR j.source = %s)
              AND (%s IS NULL OR j.job_type = %s)
            ORDER BY j.priority DESC, j.scheduled_at NULLS FIRST, i.id
            LIMIT 1
            """,
            (source, source, job_type, job_type),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return ClaimedJobItem(
            item_id=row[0],
            job_id=row[1],
            item_key=row[2],
            attempt_count=row[3],
            claimed_at=row[4],
            job_type=row[5],
            source=row[6],
            priority=row[7],
            parameters=row[8] or {},
        )


def complete_item(
    connection,
    item_id,
    status,
    checkpoint=None,
    error_code=None,
    error_message=None,
):
    """worker 처리 결과를 item에 기록하고 부모 job 상태를 item 집계 기준으로 갱신한다."""
    from psycopg2.extras import Json

    status = status.upper()
    if status not in TERMINAL_ITEM_STATUSES:
        raise ValueError(f"지원하지 않는 item 종료 상태입니다: {status}")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE pipeline_job_item
            SET status = %s,
                checkpoint = COALESCE(%s, checkpoint),
                error_code = %s,
                error_message = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING job_id
            """,
            (
                status,
                Json(checkpoint) if checkpoint is not None else None,
                error_code,
                error_message,
                item_id,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise LookupError(f"pipeline_job_item id={item_id}를 찾을 수 없습니다")
        job_id = row[0]
        next_job_status = refresh_job_status(cursor, job_id)
    connection.commit()
    return next_job_status


def fail_item(connection, item_id, error_code, error_message, checkpoint=None):
    """예외를 잡은 worker가 item을 FAILED로 종료할 때 사용하는 편의 함수다."""
    return complete_item(
        connection,
        item_id,
        "FAILED",
        checkpoint=checkpoint,
        error_code=error_code,
        error_message=error_message,
    )


def refresh_job_status(cursor, job_id):
    """하위 item 상태를 집계해 pipeline_job의 RUNNING/SUCCESS/FAILED 상태를 맞춘다."""
    cursor.execute(
        """
        WITH item_counts AS (
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE status = 'RUNNING') AS running_count,
                COUNT(*) FILTER (WHERE status IN ('QUEUED', 'SCHEDULED', 'RETRY_WAIT')) AS pending_count,
                COUNT(*) FILTER (WHERE status IN ('SUCCESS', 'COLLECTED', 'SKIPPED')) AS success_count,
                COUNT(*) FILTER (WHERE status IN ('FAILED', 'CANCELLED')) AS failure_count
            FROM pipeline_job_item
            WHERE job_id = %s
        ), resolved AS (
            SELECT
                CASE
                    WHEN total_count > 0 AND success_count = total_count THEN 'SUCCESS'
                    WHEN total_count > 0 AND failure_count = total_count THEN 'FAILED'
                    WHEN running_count > 0 THEN 'RUNNING'
                    WHEN pending_count > 0 AND (success_count > 0 OR failure_count > 0) THEN 'RUNNING'
                    WHEN pending_count = 0 AND running_count = 0 AND failure_count > 0 THEN 'PARTIAL_SUCCESS'
                    ELSE 'QUEUED'
                END AS next_status
            FROM item_counts
        )
        UPDATE pipeline_job j
        SET status = r.next_status,
            started_at = CASE
                WHEN r.next_status = 'RUNNING' THEN COALESCE(j.started_at, CURRENT_TIMESTAMP)
                ELSE j.started_at
            END,
            completed_at = CASE
                WHEN r.next_status IN ('SUCCESS', 'FAILED', 'PARTIAL_SUCCESS', 'CANCELLED')
                    THEN COALESCE(j.completed_at, CURRENT_TIMESTAMP)
                ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP
        FROM resolved r
        WHERE j.id = %s
        RETURNING j.status
        """,
        (job_id, job_id),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _mark_job_running(cursor, job_id):
    """item 선점 직후 부모 job을 RUNNING으로 전환해 관리자 페이지에 처리 중임을 표시한다."""
    cursor.execute(
        """
        UPDATE pipeline_job
        SET status = 'RUNNING',
            started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
            completed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
          AND status IN ('QUEUED', 'RUNNING')
        """,
        (job_id,),
    )
