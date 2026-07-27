import argparse
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        """python-dotenv가 없는 환경에서는 이미 설정된 환경 변수만 사용한다."""
        return False

from collectors.bls_client import BLSClient
from collectors.bls_collector import collect_bls
from collectors.common import LocalPipelineStorage, build_target_record_key


LOGGER = logging.getLogger("bls-macro-backfill")


def parse_args():
    """BLS 수집 기간과 DB 반영 여부를 명령행 인자로 받는다."""
    parser = argparse.ArgumentParser(
        description="Backfill BLS macro series, releases, observations, and vintages."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to PostgreSQL. Without this flag, only collect and report.",
    )
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="Skip BLS release calendar collection.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipeline_output/bls_macro_backfill",
        help="Raw JSON/ICS and report output directory.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_database_url():
    """PostgreSQL 접속 문자열을 환경 변수에서 읽고 없으면 안전하게 중단한다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError(
            "DATABASE_URL is required, for example "
            "postgresql://postgres:<password>@localhost:55432/trader"
        )
    return value


def connect_database_if_needed(args):
    """DB 반영 모드일 때만 PostgreSQL 연결을 만든다."""
    if not args.apply:
        return None
    import psycopg2

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    return connection


def validate_args(args):
    """연도 범위처럼 실행 전에 잡을 수 있는 입력 오류를 검증한다."""
    if args.start_year > args.end_year:
        raise ValueError("start-year must be less than or equal to end-year")


def parse_decimal_value(value):
    """BLS 숫자 문자열을 PostgreSQL NUMERIC에 넣을 Decimal로 변환한다."""
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", ".", "N/A", "NA"}:
        raise ValueError(f"BLS value is not numeric: {value!r}")
    try:
        return Decimal(text)
    except InvalidOperation as error:
        raise ValueError(f"BLS value is not numeric: {value!r}") from error


def upsert_source_object(cursor, source):
    """로컬에 보존한 BLS 원본 메타데이터를 source_object에 멱등 저장한다."""
    cursor.execute(
        """
        INSERT INTO source_object (
            provider, data_type, external_id, pipeline_job_item_id,
            storage_backend, storage_key, object_uri, object_size,
            content_hash, dedupe_key, collected_at, processing_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PROCESSED')
        ON CONFLICT (dedupe_key)
        DO UPDATE SET
            pipeline_job_item_id = COALESCE(EXCLUDED.pipeline_job_item_id, source_object.pipeline_job_item_id),
            storage_backend = EXCLUDED.storage_backend,
            storage_key = EXCLUDED.storage_key,
            object_uri = EXCLUDED.object_uri,
            object_size = EXCLUDED.object_size,
            processing_status = 'PROCESSED'
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


def upsert_macro_series(cursor, records):
    """BLS Series ID 사전을 macro_series에 저장하고 Series ID에서 DB id로 가는 맵을 만든다."""
    series_id_by_external_id = {}
    target_id_by_key = {}
    for record in records:
        cursor.execute(
            """
            INSERT INTO macro_series (
                provider, external_series_id, name, category, frequency,
                unit, seasonal_adjustment, country_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (provider, external_series_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                frequency = EXCLUDED.frequency,
                unit = EXCLUDED.unit,
                seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                country_code = EXCLUDED.country_code
            RETURNING id
            """,
            (
                record["provider"],
                record["external_series_id"],
                record["name"],
                record["category"],
                record["frequency"],
                record["unit"],
                record.get("seasonal_adjustment"),
                record["country_code"],
            ),
        )
        series_id = cursor.fetchone()[0]
        series_id_by_external_id[record["external_series_id"]] = series_id
        target_id_by_key[
            build_target_record_key(
                "macro_series",
                {
                    "provider": record["provider"],
                    "external_series_id": record["external_series_id"],
                },
            )
        ] = series_id
    return series_id_by_external_id, target_id_by_key


def upsert_macro_releases(cursor, records):
    """BLS 발표 캘린더 이벤트를 macro_release에 저장하고 외부 ID에서 DB id로 가는 맵을 만든다."""
    release_id_by_external_id = {}
    target_id_by_key = {}
    for record in records:
        cursor.execute(
            """
            INSERT INTO macro_release (
                provider, external_id, release_type, title, reference_period,
                scheduled_at, actual_released_at, first_available_at,
                sequence_number, status, source_url, collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (provider, external_id)
            DO UPDATE SET
                release_type = EXCLUDED.release_type,
                title = EXCLUDED.title,
                reference_period = EXCLUDED.reference_period,
                scheduled_at = EXCLUDED.scheduled_at,
                actual_released_at = EXCLUDED.actual_released_at,
                first_available_at = EXCLUDED.first_available_at,
                sequence_number = EXCLUDED.sequence_number,
                status = EXCLUDED.status,
                source_url = EXCLUDED.source_url,
                collected_at = EXCLUDED.collected_at
            RETURNING id
            """,
            (
                record["provider"],
                record["external_id"],
                record["release_type"],
                record["title"],
                record.get("reference_period"),
                record["scheduled_at"],
                record.get("actual_released_at"),
                record.get("first_available_at"),
                record.get("sequence_number"),
                record["status"],
                record.get("source_url"),
                record["collected_at"],
            ),
        )
        release_id = cursor.fetchone()[0]
        release_id_by_external_id[record["external_id"]] = release_id
        target_id_by_key[
            build_target_record_key(
                "macro_release",
                {
                    "provider": record["provider"],
                    "external_id": record["external_id"],
                },
            )
        ] = release_id
    return release_id_by_external_id, target_id_by_key


def upsert_macro_observations(
    cursor,
    records,
    series_id_by_external_id,
    release_id_by_external_id,
):
    """월별 관측값을 macro_observation에 저장하고 Vintage FK 연결용 id 맵을 만든다."""
    from psycopg2.extras import Json

    observation_id_by_identity = {}
    target_id_by_key = {}
    for record in records:
        series_id = series_id_by_external_id[record["external_series_id"]]
        release_id = release_id_by_external_id.get(record.get("release_external_id"))
        cursor.execute(
            """
            INSERT INTO macro_observation (
                series_id, release_id, reference_period, value, period_code,
                footnotes, content_hash, collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (series_id, reference_period)
            DO UPDATE SET
                release_id = EXCLUDED.release_id,
                value = EXCLUDED.value,
                period_code = EXCLUDED.period_code,
                footnotes = EXCLUDED.footnotes,
                content_hash = EXCLUDED.content_hash,
                collected_at = EXCLUDED.collected_at
            RETURNING id
            """,
            (
                series_id,
                release_id,
                record["reference_period"],
                parse_decimal_value(record["value"]),
                record["period_code"],
                Json(record["footnotes"]) if record.get("footnotes") else None,
                record["content_hash"],
                record["collected_at"],
            ),
        )
        observation_id = cursor.fetchone()[0]
        identity = (record["external_series_id"], record["reference_period"])
        observation_id_by_identity[identity] = observation_id
        target_id_by_key[
            build_target_record_key(
                "macro_observation",
                {
                    "provider": record["provider"],
                    "external_series_id": record["external_series_id"],
                    "reference_period": record["reference_period"],
                },
            )
        ] = observation_id
    return observation_id_by_identity, target_id_by_key


def upsert_macro_observation_vintages(
    cursor,
    records,
    observation_id_by_identity,
    release_id_by_external_id,
):
    """관측값 버전을 macro_observation_vintage에 append/upsert 방식으로 저장한다."""
    target_id_by_key = {}
    for record in records:
        identity = (record["external_series_id"], record["reference_period"])
        observation_id = observation_id_by_identity[identity]
        release_id = release_id_by_external_id.get(record.get("release_external_id"))
        cursor.execute(
            """
            INSERT INTO macro_observation_vintage (
                observation_id, release_id, value, available_at,
                availability_source, collected_at, content_hash, is_initial
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id, content_hash)
            DO UPDATE SET
                release_id = EXCLUDED.release_id,
                value = EXCLUDED.value,
                available_at = EXCLUDED.available_at,
                availability_source = EXCLUDED.availability_source,
                collected_at = EXCLUDED.collected_at,
                is_initial = EXCLUDED.is_initial
            RETURNING id
            """,
            (
                observation_id,
                release_id,
                parse_decimal_value(record["value"]),
                record["available_at"],
                record["availability_source"],
                record["collected_at"],
                record["content_hash"],
                record["is_initial"],
            ),
        )
        vintage_id = cursor.fetchone()[0]
        target_id_by_key[
            build_target_record_key(
                "macro_observation_vintage",
                {
                    "provider": record["provider"],
                    "external_series_id": record["external_series_id"],
                    "reference_period": record["reference_period"],
                    "content_hash": record["content_hash"],
                },
            )
        ] = vintage_id
    return target_id_by_key


def upsert_macro_observation_statuses(cursor, records, series_id_by_external_id):
    """BLS가 숫자 값을 제공하지 못한 월별 항목을 macro_observation_status에 저장한다."""
    from psycopg2.extras import Json

    target_id_by_key = {}
    for record in records:
        series_id = series_id_by_external_id[record["external_series_id"]]
        cursor.execute(
            """
            INSERT INTO macro_observation_status (
                series_id, reference_period, period_code, period_name, status,
                reason_code, reason_message, raw_value, footnotes,
                raw_object_uri, content_hash, collected_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (series_id, reference_period, status)
            DO UPDATE SET
                period_code = EXCLUDED.period_code,
                period_name = EXCLUDED.period_name,
                reason_code = EXCLUDED.reason_code,
                reason_message = EXCLUDED.reason_message,
                raw_value = EXCLUDED.raw_value,
                footnotes = EXCLUDED.footnotes,
                raw_object_uri = EXCLUDED.raw_object_uri,
                content_hash = EXCLUDED.content_hash,
                collected_at = EXCLUDED.collected_at
            RETURNING id
            """,
            (
                series_id,
                record["reference_period"],
                record["period_code"],
                record.get("period_name"),
                record["status"],
                record.get("reason_code"),
                record.get("reason_message"),
                record.get("raw_value"),
                Json(record["footnotes"]) if record.get("footnotes") else None,
                record.get("raw_object_uri"),
                record["content_hash"],
                record["collected_at"],
            ),
        )
        status_id = cursor.fetchone()[0]
        target_id_by_key[
            build_target_record_key(
                "macro_observation_status",
                {
                    "provider": record["provider"],
                    "external_series_id": record["external_series_id"],
                    "reference_period": record["reference_period"],
                    "status": record["status"],
                },
            )
        ] = status_id
    return target_id_by_key


def upsert_lineage(cursor, lineage, source_id_by_dedupe_key, target_id_by_table_key):
    """collector가 만든 lineage preview를 실제 DB id와 연결해 record_lineage에 저장한다."""
    source_object_id = source_id_by_dedupe_key.get(lineage["source_object_dedupe_key"])
    target_record_id = target_id_by_table_key.get(
        (lineage["target_table"], lineage["target_record_key"])
    )
    if source_object_id is None or target_record_id is None:
        return False

    cursor.execute(
        """
        INSERT INTO record_lineage (
            source_object_id, target_table, target_record_id, target_record_key,
            transformer_name, transformer_version, output_hash, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (
            source_object_id, target_table, target_record_key,
            transformer_name, transformer_version
        ) DO UPDATE SET
            target_record_id = EXCLUDED.target_record_id,
            output_hash = EXCLUDED.output_hash,
            processed_at = EXCLUDED.processed_at
        """,
        (
            source_object_id,
            lineage["target_table"],
            target_record_id,
            lineage["target_record_key"],
            lineage["transformer_name"],
            lineage["transformer_version"],
            lineage.get("output_hash"),
            lineage["processed_at"],
        ),
    )
    return True


def apply_bls_macro_records(connection, records):
    """BLS 수집 결과 전체를 원본, 정규화 테이블, lineage 순서로 한 트랜잭션에 저장한다."""
    stats = {
        "source_objects_upserted": 0,
        "macro_series_upserted": 0,
        "macro_releases_upserted": 0,
        "macro_observations_upserted": 0,
        "macro_observation_statuses_upserted": 0,
        "macro_vintages_upserted": 0,
        "lineage_upserted": 0,
        "lineage_skipped": 0,
    }
    with connection.cursor() as cursor:
        source_id_by_dedupe_key = {}
        for source in records["source_object"]:
            source_id_by_dedupe_key[source["dedupe_key"]] = upsert_source_object(
                cursor,
                source,
            )
            stats["source_objects_upserted"] += 1

        series_id_by_external_id, series_target_ids = upsert_macro_series(
            cursor,
            records["macro_series"],
        )
        release_id_by_external_id, release_target_ids = upsert_macro_releases(
            cursor,
            records["macro_release"],
        )
        observation_id_by_identity, observation_target_ids = upsert_macro_observations(
            cursor,
            records["macro_observation"],
            series_id_by_external_id,
            release_id_by_external_id,
        )
        vintage_target_ids = upsert_macro_observation_vintages(
            cursor,
            records.get("macro_observation_vintage", []),
            observation_id_by_identity,
            release_id_by_external_id,
        )
        status_target_ids = upsert_macro_observation_statuses(
            cursor,
            records.get("macro_observation_status", []),
            series_id_by_external_id,
        )

        target_id_by_table_key = {}
        for table_name, target_ids in {
            "macro_series": series_target_ids,
            "macro_release": release_target_ids,
            "macro_observation": observation_target_ids,
            "macro_observation_status": status_target_ids,
            "macro_observation_vintage": vintage_target_ids,
        }.items():
            for target_key, target_id in target_ids.items():
                target_id_by_table_key[(table_name, target_key)] = target_id

        for lineage in records["record_lineage"]:
            if upsert_lineage(cursor, lineage, source_id_by_dedupe_key, target_id_by_table_key):
                stats["lineage_upserted"] += 1
            else:
                stats["lineage_skipped"] += 1

    connection.commit()
    stats["macro_series_upserted"] = len(records["macro_series"])
    stats["macro_releases_upserted"] = len(records["macro_release"])
    stats["macro_observations_upserted"] = len(records["macro_observation"])
    stats["macro_observation_statuses_upserted"] = len(records.get("macro_observation_status", []))
    stats["macro_vintages_upserted"] = len(records.get("macro_observation_vintage", []))
    return stats


def write_reports(output_dir, stats, records):
    """백필 실행 요약을 JSON 파일로 저장한다."""
    report_dir = Path(output_dir) / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_path = report_dir / f"summary_{stamp}.json"
    summary = {
        **stats,
        "source_objects": len(records["source_object"]),
        "macro_series": len(records["macro_series"]),
        "macro_releases": len(records["macro_release"]),
        "macro_observations": len(records["macro_observation"]),
        "macro_observation_statuses": len(records.get("macro_observation_status", [])),
        "macro_vintages": len(records.get("macro_observation_vintage", [])),
        "record_lineage": len(records["record_lineage"]),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary_path


def main():
    """BLS 매크로 데이터를 수집하고 preview 또는 DB 백필을 실행한다."""
    load_dotenv()
    args = parse_args()
    validate_args(args)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    storage = LocalPipelineStorage(Path(args.output_dir))
    client = BLSClient(os.getenv("BLS_USER_AGENT", "TraderDataBot-KR/1.0"))
    records = collect_bls(
        client,
        storage,
        args.start_year,
        args.end_year,
        include_calendar=not args.skip_calendar,
    )

    for table_name, table_records in records.items():
        path, count = storage.write_table(table_name, table_records)
        LOGGER.info("%s rows=%d path=%s", table_name, count, path)

    connection = connect_database_if_needed(args)
    try:
        if args.apply:
            stats = apply_bls_macro_records(connection, records)
        else:
            stats = {"dry_run": True}
    finally:
        if connection is not None:
            connection.close()

    summary_path = write_reports(args.output_dir, stats, records)
    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"mode={mode}")
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    print(f"summary_report={summary_path}")


if __name__ == "__main__":
    main()
