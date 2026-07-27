import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

from collectors.common import content_hash, to_iso


TRANSFORMER_NAME = "bls-transformer"
TRANSFORMER_VERSION = "2.0.0"


RELEASE_TYPE_BY_TITLE = {
    "consumer price index": "CPI",
    "producer price index": "PPI",
    "employment situation": "EMPLOYMENT",
    "job openings and labor turnover survey": "JOLTS",
    "employment cost index": "ECI",
    "u.s. import and export price indexes": "IMPORT_EXPORT_PRICE",
}


def normalized_title(title):
    """BLS 일정 제목의 공백과 주기 접미사를 제거해 비교 가능한 문자열로 만든다."""
    value = re.sub(r"\s*\((monthly|quarterly|annual)\)\s*$", "", title, flags=re.I)
    return " ".join(value.lower().split())


def release_type_for_title(title):
    """정확한 BLS 일정 제목을 CPI·PPI·고용 등 내부 release_type으로 매핑한다."""
    return RELEASE_TYPE_BY_TITLE.get(normalized_title(title), "OTHER")


def _utc_datetime(value):
    """timezone을 가진 datetime인지 검증하고 UTC로 변환한다."""
    if not isinstance(value, datetime):
        raise ValueError(f"Expected datetime, got {value!r}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def transform_calendar_event(event, collected_at, raw_object_uri):
    """ICS VEVENT 한 건을 macro_release 스키마 레코드로 변환한다."""
    title = str(event.get("summary", ""))
    uid = event.get("uid")
    if not uid:
        raise ValueError(f"BLS calendar event has no UID: {title}")

    return {
        "provider": "BLS",
        "external_id": str(uid),
        "release_type": release_type_for_title(title),
        "title": title,
        "reference_period": None,
        "scheduled_at": to_iso(_utc_datetime(event.decoded("dtstart"))),
        "actual_released_at": None,
        "first_available_at": None,
        "sequence_number": int(event.get("sequence", 0)),
        "status": "SCHEDULED",
        "source_url": str(event.get("url")) if event.get("url") else None,
        "raw_object_uri": raw_object_uri,
        "collected_at": to_iso(collected_at),
    }


def transform_macro_series(series_registry, collected_at):
    """설정에 정의된 BLS Series 목록을 macro_series 레코드로 변환한다."""
    return [
        {
            "provider": "BLS",
            "external_series_id": series_id,
            "name": metadata["name"],
            "category": metadata["category"],
            "frequency": metadata["frequency"],
            "unit": metadata["unit"],
            "seasonal_adjustment": metadata.get("seasonal_adjustment"),
            "country_code": metadata.get("country_code", "US"),
        }
        for series_id, metadata in series_registry.items()
    ]


def normalize_bls_numeric_value(value):
    """BLS 값 문자열을 DB NUMERIC에 적재 가능한 형태로 정규화하고 결측값은 None으로 처리한다."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", ".", "N/A", "NA"}:
        return None
    try:
        return str(Decimal(text))
    except InvalidOperation:
        return None


def unavailable_status_for_item(item):
    """BLS가 숫자 대신 결측 표시를 내려준 경우 운영 화면에 남길 상태값을 만든다."""
    raw_value = item.get("value")
    text = "" if raw_value is None else str(raw_value).strip()
    footnotes = item.get("footnotes") or []
    first_note = next((note for note in footnotes if note), {})
    note_code = first_note.get("code")
    note_text = first_note.get("text")
    status = "PROVIDER_UNAVAILABLE" if text in {"-", ".", "N/A", "NA"} else "NON_NUMERIC"
    return {
        "status": status,
        "reason_code": f"BLS_{note_code}" if note_code else "BLS_VALUE_UNAVAILABLE",
        "reason_message": note_text or f"BLS value is not numeric: {raw_value!r}",
    }


def transform_observations(api_response, collected_at, raw_object_uri):
    """BLS Timeseries 응답을 월별 macro_observation 레코드로 변환한다."""
    records = []
    for series in api_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        for item in series.get("data", []):
            period_code = item.get("period")
            if not period_code or not period_code.startswith("M") or period_code == "M13":
                continue
            normalized_value = normalize_bls_numeric_value(item.get("value"))
            if normalized_value is None:
                continue

            reference_period = f"{item['year']}-{period_code[1:]}"
            identity = {
                "provider": "BLS",
                "series_id": series_id,
                "reference_period": reference_period,
                "value": normalized_value,
                "footnotes": item.get("footnotes") or [],
            }
            records.append(
                {
                    "provider": "BLS",
                    "external_series_id": series_id,
                    "release_external_id": None,
                    "reference_period": reference_period,
                    "value": normalized_value,
                    "period_code": period_code,
                    "period_name": item.get("periodName"),
                    "footnotes": item.get("footnotes") or None,
                    "content_hash": content_hash(identity),
                    "raw_object_uri": raw_object_uri,
                    "collected_at": to_iso(collected_at),
                }
            )
    return records


def transform_observation_statuses(api_response, collected_at, raw_object_uri):
    """숫자 관측값으로 저장하지 못한 BLS 월별 항목을 별도 상태 레코드로 보존한다."""
    records = []
    for series in api_response.get("Results", {}).get("series", []):
        series_id = series.get("seriesID")
        for item in series.get("data", []):
            period_code = item.get("period")
            if not period_code or not period_code.startswith("M") or period_code == "M13":
                continue
            if normalize_bls_numeric_value(item.get("value")) is not None:
                continue

            reference_period = f"{item['year']}-{period_code[1:]}"
            status_info = unavailable_status_for_item(item)
            footnotes = item.get("footnotes") or []
            identity = {
                "provider": "BLS",
                "series_id": series_id,
                "reference_period": reference_period,
                "status": status_info["status"],
                "raw_value": item.get("value"),
                "footnotes": footnotes,
            }
            records.append(
                {
                    "provider": "BLS",
                    "external_series_id": series_id,
                    "reference_period": reference_period,
                    "period_code": period_code,
                    "period_name": item.get("periodName"),
                    "status": status_info["status"],
                    "reason_code": status_info["reason_code"],
                    "reason_message": status_info["reason_message"],
                    "raw_value": None if item.get("value") is None else str(item.get("value")),
                    "footnotes": footnotes or None,
                    "content_hash": content_hash(identity),
                    "raw_object_uri": raw_object_uri,
                    "collected_at": to_iso(collected_at),
                }
            )
    return records


def transform_observation_vintages(observations, collected_at):
    """관측값을 당시 내부에서 처음 확인한 시각 기준의 append-only Vintage로 만든다."""
    available_at = to_iso(collected_at)
    return [
        {
            "provider": observation["provider"],
            "external_series_id": observation["external_series_id"],
            "reference_period": observation["reference_period"],
            "release_external_id": observation.get("release_external_id"),
            "value": observation["value"],
            "available_at": available_at,
            "availability_source": "COLLECTED_FALLBACK",
            "collected_at": available_at,
            "content_hash": observation["content_hash"],
            "is_initial": False,
        }
        for observation in observations
    ]
