import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from collectors.bls_client import BLSClient
from collectors.common import (
    LocalPipelineStorage,
    build_lineage_record,
    build_target_record_key,
    utc_now,
)
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        """python-dotenv가 없는 환경에서는 이미 설정된 환경 변수만 사용한다."""
        return False

from transformers.bls_transformer import (
    TRANSFORMER_NAME,
    TRANSFORMER_VERSION,
    transform_calendar_event,
    transform_macro_series,
    transform_observation_statuses,
    transform_observation_vintages,
    transform_observations,
)


BLS_SERIES = {
    "CUSR0000SA0": {
        "name": "Consumer Price Index - All Urban Consumers",
        "category": "CPI",
        "frequency": "MONTHLY",
        "unit": "INDEX",
        "seasonal_adjustment": "SEASONALLY_ADJUSTED",
    },
    "CUSR0000SA0L1E": {
        "name": "Core Consumer Price Index",
        "category": "CPI",
        "frequency": "MONTHLY",
        "unit": "INDEX",
        "seasonal_adjustment": "SEASONALLY_ADJUSTED",
    },
    "WPUFD4": {
        "name": "Producer Price Index - Final Demand",
        "category": "PPI",
        "frequency": "MONTHLY",
        "unit": "INDEX",
        "seasonal_adjustment": "UNADJUSTED",
    },
    "LNS14000000": {
        "name": "Unemployment Rate",
        "category": "EMPLOYMENT",
        "frequency": "MONTHLY",
        "unit": "PERCENT",
        "seasonal_adjustment": "SEASONALLY_ADJUSTED",
    },
    "CES0000000001": {
        "name": "Total Nonfarm Payroll Employment",
        "category": "EMPLOYMENT",
        "frequency": "MONTHLY",
        "unit": "THOUSANDS_OF_PERSONS",
        "seasonal_adjustment": "SEASONALLY_ADJUSTED",
    },
    "CES0500000003": {
        "name": "Average Hourly Earnings - Total Private",
        "category": "EMPLOYMENT",
        "frequency": "MONTHLY",
        "unit": "USD_PER_HOUR",
        "seasonal_adjustment": "SEASONALLY_ADJUSTED",
    },
}


def build_bls_lineage(
    records,
    source_object,
    target_table,
    identity_builder,
    processed_at,
):
    """BLS 정규화 레코드에 원본 객체와 안정적인 target key를 연결한다."""
    return [
        build_lineage_record(
            source_object,
            target_table,
            build_target_record_key(target_table, identity_builder(record)),
            record,
            TRANSFORMER_NAME,
            TRANSFORMER_VERSION,
            processed_at,
        )
        for record in records
    ]


def collect_bls(
    client,
    storage,
    start_year,
    end_year,
    include_calendar=True,
    series_ids=None,
    series_metadata=None,
):
    """BLS 캘린더와 시계열 원본을 수집해 매크로 스키마 레코드로 변환한다."""
    collected_at = utc_now()
    source_objects = []
    releases = []
    calendar_source = None
    series_catalog = {**BLS_SERIES, **(series_metadata or {})}
    selected_series_ids = [series_id.upper() for series_id in series_ids] if series_ids else list(BLS_SERIES.keys())
    unknown_series_ids = sorted(set(selected_series_ids) - set(series_catalog.keys()))
    if unknown_series_ids:
        raise ValueError(f"지원하지 않는 BLS series id입니다: {', '.join(unknown_series_ids)}")
    selected_series = {
        series_id: series_catalog[series_id]
        for series_id in selected_series_ids
    }

    if include_calendar:
        try:
            from icalendar import Calendar
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "icalendar is required for BLS release calendar collection. "
                "Install it or run with --skip-calendar."
            ) from error

        calendar_bytes = client.get_calendar()
        calendar_source = storage.write_raw_bytes(
            "BLS",
            "RELEASE_CALENDAR",
            f"calendar-{collected_at.date().isoformat()}",
            calendar_bytes,
            "ics",
            collected_at,
        )
        source_objects.append(calendar_source)
        calendar = Calendar.from_ical(calendar_bytes)
        for event in calendar.walk("VEVENT"):
            release = transform_calendar_event(
                event,
                collected_at,
                calendar_source["object_uri"],
            )
            if release["release_type"] != "OTHER":
                releases.append(release)

    api_response = client.get_timeseries(
        selected_series.keys(),
        start_year,
        end_year,
        api_key=os.getenv("BLS_API_KEY"),
    )
    timeseries_external_id = (
        f"{start_year}-{end_year}"
        if series_ids is None
        else f"{start_year}-{end_year}-{'_'.join(selected_series.keys())}"
    )
    timeseries_source = storage.write_raw_json(
        "BLS",
        "TIMESERIES",
        timeseries_external_id,
        api_response,
        collected_at,
    )
    source_objects.append(timeseries_source)

    series_records = transform_macro_series(selected_series, collected_at)
    observation_records = transform_observations(
        api_response,
        collected_at,
        timeseries_source["object_uri"],
    )
    observation_status_records = transform_observation_statuses(
        api_response,
        collected_at,
        timeseries_source["object_uri"],
    )
    vintage_records = transform_observation_vintages(
        observation_records,
        collected_at,
    )
    lineage_records = []
    lineage_records.extend(
        build_bls_lineage(
            series_records,
            timeseries_source,
            "macro_series",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
            },
            collected_at,
        )
    )
    if calendar_source is not None:
        lineage_records.extend(
            build_bls_lineage(
                releases,
                calendar_source,
                "macro_release",
                lambda record: {
                    "provider": record["provider"],
                    "external_id": record["external_id"],
                },
                collected_at,
            )
        )
    lineage_records.extend(
        build_bls_lineage(
            observation_records,
            timeseries_source,
            "macro_observation",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_bls_lineage(
            observation_status_records,
            timeseries_source,
            "macro_observation_status",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
                "status": record["status"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_bls_lineage(
            vintage_records,
            timeseries_source,
            "macro_observation_vintage",
            lambda record: {
                "provider": record["provider"],
                "external_series_id": record["external_series_id"],
                "reference_period": record["reference_period"],
                "content_hash": record["content_hash"],
            },
            collected_at,
        )
    )

    return {
        "macro_series": series_records,
        "macro_release": releases,
        "macro_observation": observation_records,
        "macro_observation_status": observation_status_records,
        "macro_observation_vintage": vintage_records,
        "source_object": source_objects,
        "record_lineage": lineage_records,
    }


def build_parser():
    """수집 연도, 캘린더 포함 여부, 출력 경로를 지정하는 CLI parser를 만든다."""
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(
        description="Collect BLS data and write schema-shaped JSONL previews."
    )
    parser.add_argument("--start-year", type=int, default=current_year - 2)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="Skip BLS release calendar collection.",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).parents[1] / "pipeline_output" / "bls"),
    )
    return parser


def main():
    """BLS 수집 CLI를 실행하고 raw 파일과 테이블별 JSONL을 출력한다."""
    load_dotenv()
    args = build_parser().parse_args()
    if args.start_year > args.end_year:
        raise ValueError("start-year must be less than or equal to end-year")

    storage = LocalPipelineStorage(args.output)
    client = BLSClient(os.getenv("BLS_USER_AGENT", "TraderDataBot-KR/1.0"))
    result = collect_bls(
        client,
        storage,
        args.start_year,
        args.end_year,
        include_calendar=not args.skip_calendar,
    )

    for table_name, records in result.items():
        path, count = storage.write_table(table_name, records)
        print(f"{table_name}: {count} rows -> {path}")


if __name__ == "__main__":
    main()
