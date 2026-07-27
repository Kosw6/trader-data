import argparse
import json
import logging

from workers.db import connect_database
from workers.job_repository import (
    claim_next_queued_item,
    complete_item,
    peek_next_queued_item,
)


LOGGER = logging.getLogger("dry-run-worker")


def parse_args():
    """worker MVP 검증에 필요한 필터와 종료 상태를 명령행 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="QUEUED 상태의 pipeline_job_item 하나를 선점하고 선택적으로 완료 처리한다."
    )
    parser.add_argument(
        "--source",
        help="부모 job의 source가 일치하는 item만 선점한다. 예: BLS, SEC",
    )
    parser.add_argument(
        "--job-type",
        help="부모 job의 job_type이 일치하는 item만 선점한다.",
    )
    parser.add_argument(
        "--complete-status",
        default="SUCCESS",
        choices=["SUCCESS", "COLLECTED", "SKIPPED", "FAILED", "CANCELLED"],
        help="item을 선점한 뒤 기록할 종료 상태다.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="상태 전이를 실제 DB에 반영한다. 이 옵션이 없으면 다음 처리 대상만 미리 본다.",
    )
    parser.add_argument(
        "--error-code",
        default="DRY_RUN_FAILURE",
        help="--complete-status FAILED를 선택했을 때 기록할 오류 코드다.",
    )
    parser.add_argument(
        "--error-message",
        default="dry-run worker가 이 item을 실패로 표시했습니다.",
        help="--complete-status FAILED를 선택했을 때 기록할 오류 메시지다.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def item_to_dict(item):
    """출력용으로 dataclass를 JSON 직렬화 가능한 dict로 변환한다."""
    return {
        "item_id": item.item_id,
        "job_id": item.job_id,
        "item_key": item.item_key,
        "job_type": item.job_type,
        "source": item.source,
        "priority": item.priority,
        "attempt_count": item.attempt_count,
        "claimed_at": item.claimed_at.isoformat() if item.claimed_at else None,
        "parameters": item.parameters,
    }


def main():
    """QUEUED item 하나를 선점하고 worker 상태 전이 흐름을 검증한다."""
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    connection = connect_database()
    try:
        source = args.source.upper() if args.source else None
        job_type = args.job_type.upper() if args.job_type else None
        if not args.apply:
            item = peek_next_queued_item(connection, source=source, job_type=job_type)
            if item is None:
                print("claimed=false")
                print("message=조건에 맞는 QUEUED pipeline_job_item이 없습니다")
                return
            print("claimed=false")
            print("applied=false")
            print("message=미리보기만 수행했습니다. RUNNING 전환과 완료 처리는 --apply로 다시 실행하세요")
            print(json.dumps(item_to_dict(item), ensure_ascii=False, indent=2))
            return

        item = claim_next_queued_item(
            connection,
            source=source,
            job_type=job_type,
        )
        if item is None:
            print("claimed=false")
            print("message=조건에 맞는 QUEUED pipeline_job_item이 없습니다")
            return

        checkpoint = {
            "worker": "dry_run_worker",
            "mode": "apply",
            "complete_status": args.complete_status,
        }
        error_code = args.error_code if args.complete_status == "FAILED" else None
        error_message = args.error_message if args.complete_status == "FAILED" else None

        next_job_status = complete_item(
            connection,
            item.item_id,
            args.complete_status,
            checkpoint=checkpoint,
            error_code=error_code,
            error_message=error_message,
        )

        print("claimed=true")
        print("applied=true")
        print(f"next_job_status={next_job_status}")
        print(json.dumps(item_to_dict(item), ensure_ascii=False, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
