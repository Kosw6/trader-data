"""Kafka consumer가 사용하는 processed_event 저장소 함수들.

processed_event는 "Kafka 메시지를 이미 처리했는지"를 기록하는 작은 원장이다.
Kafka는 at-least-once 특성상 같은 메시지가 다시 올 수 있으므로, consumer는
idempotency_key를 기준으로 중복 처리를 막아야 한다.
"""


def find_processed_event(connection, consumer_name, idempotency_key):
    """consumer_name과 idempotency_key로 이미 처리된 이벤트가 있는지 조회한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT status, error_message, processed_at, topic, partition_no, offset_no
            FROM processed_event
            WHERE consumer_name = %s
              AND idempotency_key = %s
            """,
            (consumer_name, idempotency_key),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "error_message": row[1],
        "processed_at": row[2],
        "topic": row[3],
        "partition": row[4],
        "offset": row[5],
    }


def record_processed_event(
    connection,
    consumer_name,
    idempotency_key,
    topic,
    partition,
    offset,
    status,
    error_message=None,
):
    """이벤트 처리 결과를 processed_event에 기록한다.

    같은 idempotency_key가 이미 있으면 최신 Kafka 위치와 처리 상태로 갱신한다.
    이 테이블의 PK가 (consumer_name, idempotency_key)이므로 consumer별 중복 방지가 가능하다.
    """
    status = status.upper()
    if status not in {"SUCCESS", "SKIPPED", "FAILED"}:
        raise ValueError(f"processed_event status는 SUCCESS/SKIPPED/FAILED만 가능합니다: {status}")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO processed_event (
                consumer_name,
                idempotency_key,
                topic,
                partition_no,
                offset_no,
                status,
                error_message,
                processed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
            )
            ON CONFLICT (consumer_name, idempotency_key)
            DO UPDATE SET
                topic = EXCLUDED.topic,
                partition_no = EXCLUDED.partition_no,
                offset_no = EXCLUDED.offset_no,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                processed_at = CURRENT_TIMESTAMP
            """,
            (
                consumer_name,
                idempotency_key,
                topic,
                partition,
                offset,
                status,
                error_message,
            ),
        )
    connection.commit()
