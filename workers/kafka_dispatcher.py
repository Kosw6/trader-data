"""Kafka JOB_ITEM_QUEUED 이벤트를 실제 Python worker 함수로 위임한다.

관리자 페이지와 scheduler는 동일하게 pipeline_job_item을 만들고 Kafka에는
작은 실행 명령만 보낸다. 이 dispatcher는 item parameters의 pipelineMode를 보고
기존 통합 worker 또는 raw-only collector 중 어느 경로를 탈지 결정한다.
"""

import os
from types import SimpleNamespace

from workers.job_repository import fail_item, get_item_status


COLLECT_ONLY = "COLLECT_ONLY"
COMBINED = "COMBINED"


def dispatch_item(connection, item):
    """source와 pipelineMode에 따라 적절한 worker의 process_item을 호출한다.

    pipelineMode:
    - COMBINED: 수집, raw 저장, transform, DB load를 한 worker에서 처리한다.
    - COLLECT_ONLY: raw 저장과 source_object 등록까지만 처리하고
      RAW_OBJECT_READY 이벤트를 controller에 보고한다.
    """
    source = item.source.upper()
    pipeline_mode = read_pipeline_mode(item.parameters)

    if source == "BLS":
        return _dispatch_bls(connection, item, pipeline_mode)
    if source == "SEC":
        return _dispatch_sec(connection, item, pipeline_mode)
    if source == "KIS":
        return _dispatch_kis(connection, item, pipeline_mode)
    if source == "NEWS":
        if pipeline_mode != COLLECT_ONLY:
            raise ValueError("NEWS requires COLLECT_ONLY")
        from workers.news_collector import process_item
        return process_item(connection, item)
    raise ValueError(f"지원하지 않는 Kafka worker source입니다: {item.source}")


def read_pipeline_mode(parameters):
    """job parameters에서 실행 형태를 읽는다.

    기존 mode/collectionMode는 BACKFILL/INCREMENTAL 의미로 이미 쓰고 있으므로
    raw/ETL 분리 여부는 pipelineMode 또는 executionMode만 사용한다.
    """
    parameters = parameters or {}
    value = (
        parameters.get("pipelineMode")
        or parameters.get("executionMode")
        or parameters.get("pipeline_mode")
        or COMBINED
    )
    value = str(value).strip().upper()
    if value in {"COLLECTOR_ONLY", "RAW_ONLY"}:
        return COLLECT_ONLY
    if value not in {COMBINED, COLLECT_ONLY}:
        raise ValueError(f"지원하지 않는 pipelineMode입니다: {value}")
    return value


def item_status_to_event_status(item_status):
    """pipeline_job_item 상태를 processed_event 상태로 축약한다."""
    if item_status in {"SUCCESS", "COLLECTED"}:
        return "SUCCESS"
    if item_status == "SKIPPED":
        return "SKIPPED"
    return "FAILED"


def fail_claimed_item(connection, item, error):
    """dispatch 중 예외가 난 item을 FAILED로 종료한다."""
    return fail_item(
        connection,
        item.item_id,
        "KAFKA_CONSUMER_PROCESSING_FAILED",
        str(error),
        checkpoint={
            "worker": "kafka_pipeline_consumer",
            "item_id": item.item_id,
            "item_key": item.item_key,
            "source": item.source,
            "job_type": item.job_type,
            "pipeline_mode": read_pipeline_mode(item.parameters),
        },
    )


def current_event_status(connection, item_id):
    """처리 직후 DB에 저장된 item 상태를 processed_event 상태로 변환한다."""
    status = get_item_status(connection, item_id)
    if status is None:
        return "FAILED", "pipeline_job_item을 찾을 수 없습니다"
    return item_status_to_event_status(status["status"]), None


def _dispatch_bls(connection, item, pipeline_mode):
    """BLS item을 통합 worker 또는 collector-only worker로 위임한다."""
    if pipeline_mode == COLLECT_ONLY:
        from workers import bls_macro_collector

        args = SimpleNamespace(
            job_type=item.job_type,
            output_dir="pipeline_output/bls_macro_collector",
            include_calendar=False,
            loop=False,
            poll_interval_seconds=10.0,
            controller_url=controller_url(),
            admin_api_key=admin_api_key(),
            log_level="INFO",
        )
        return bls_macro_collector.process_item(connection, item, args)

    from workers import bls_macro_worker

    args = SimpleNamespace(
        job_type=item.job_type,
        output_dir="pipeline_output/bls_macro_worker",
        include_calendar=False,
        loop=False,
        poll_interval_seconds=10.0,
        log_level="INFO",
    )
    return bls_macro_worker.process_item(connection, item, args)


def _dispatch_sec(connection, item, pipeline_mode):
    """SEC item을 통합 worker 또는 collector-only worker로 위임한다."""
    if pipeline_mode == COLLECT_ONLY:
        from workers import sec_financial_collector

        args = SimpleNamespace(
            job_type=item.job_type,
            output_dir="pipeline_output/sec_financial_collector",
            include_history=False,
            all_facts=False,
            request_interval=1.0,
            loop=False,
            poll_interval_seconds=10.0,
            controller_url=controller_url(),
            admin_api_key=admin_api_key(),
            log_level="INFO",
        )
        return sec_financial_collector.process_item(connection, item, args)

    from workers import sec_financial_worker

    args = SimpleNamespace(
        job_type=item.job_type,
        output_dir="pipeline_output/sec_financial_worker",
        include_history=False,
        all_facts=False,
        request_interval=1.0,
        loop=False,
        poll_interval_seconds=10.0,
        log_level="INFO",
    )
    return sec_financial_worker.process_item(connection, item, args)


def _dispatch_kis(connection, item, pipeline_mode):
    """KIS item을 통합 worker 또는 collector-only worker로 위임한다."""
    if pipeline_mode == COLLECT_ONLY:
        from workers import kis_price_collector

        args = SimpleNamespace(
            job_type=item.job_type,
            output_dir="pipeline_output/kis_price_collector",
            source="KIS",
            interval="1D",
            currency="USD",
            max_pages=100,
            request_interval=0.2,
            adjusted=False,
            loop=False,
            poll_interval_seconds=10.0,
            controller_url=controller_url(),
            admin_api_key=admin_api_key(),
            log_level="INFO",
        )
        return kis_price_collector.process_item(connection, item, args)

    from workers import kis_price_worker

    args = SimpleNamespace(
        job_type=item.job_type,
        output_dir="pipeline_output/kis_price_worker",
        source="KIS",
        interval="1D",
        currency="USD",
        max_pages=100,
        request_interval=0.2,
        adjusted=False,
        loop=False,
        poll_interval_seconds=10.0,
        log_level="INFO",
    )
    return kis_price_worker.process_item(connection, item, args)


def controller_url():
    """collector가 RAW_OBJECT_READY를 보고할 controller URL을 반환한다."""
    return os.getenv("TRADER_CONTROLLER_URL") or os.getenv("CONTROLLER_URL") or "http://localhost:8090"


def admin_api_key():
    """collector가 controller API를 호출할 때 사용할 관리자 API key를 반환한다."""
    return os.getenv("ADMIN_API_KEY", "")
