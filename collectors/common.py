import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote


def utc_now():
    """수집 시각 기록에 사용할 timezone-aware UTC 현재 시각을 반환한다."""
    return datetime.now(timezone.utc)


def to_iso(value):
    """날짜 또는 시각 값을 JSON과 DB 적재에 적합한 ISO 8601 문자열로 변환한다."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def json_default(value):
    """json.dumps가 날짜·시각 객체를 직렬화할 때 사용하는 변환 함수다."""
    if isinstance(value, (datetime, date)):
        return to_iso(value)
    raise TypeError(f"Unsupported JSON type: {type(value)!r}")


def canonical_json_bytes(value):
    """필드 순서와 공백에 영향받지 않는 해시 계산용 JSON 바이트를 생성한다.(순서에 무관한 중복처리 방지용)"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=json_default,
    ).encode("utf-8")


def content_hash(value):
    """원본 변경 및 중복 판별에 사용할 SHA-256 해시를 계산한다."""
    payload = value if isinstance(value, bytes) else canonical_json_bytes(value)
    return hashlib.sha256(payload).hexdigest()


def build_dedupe_key(*parts):
    """nullable 값과 구분자를 안전하게 처리하는 안정적인 SHA-256 멱등 키를 만든다."""
    return content_hash(list(parts))


def build_target_record_key(table_name, identity):
    """DB PK와 무관하게 동일 정규화 레코드를 다시 식별할 target key를 만든다."""
    return build_dedupe_key(table_name, identity)


def transformed_record_hash(record):
    """수집시각과 저장 위치를 제외한 정규화 결과의 내용 hash를 계산한다."""
    volatile_fields = {"collected_at", "updated_at", "raw_object_uri"}
    stable_record = {
        key: value for key, value in record.items() if key not in volatile_fields
    }
    return content_hash(stable_record)


def build_lineage_record(
    source_object,
    target_table,
    target_record_key,
    transformed_record,
    transformer_name,
    transformer_version,
    processed_at,
):
    """DB Loader가 원본·결과 PK를 해석하기 전 단계의 Lineage 레코드를 만든다."""
    return {
        "source_object_dedupe_key": source_object["dedupe_key"],
        "target_table": target_table,
        "target_record_key": target_record_key,
        "pipeline_job_id": None,
        "transformer_name": transformer_name,
        "transformer_version": transformer_version,
        "output_hash": transformed_record_hash(transformed_record),
        "processed_at": to_iso(processed_at),
    }


def build_http_session(user_agent, total_retries=4):
    """429·5xx와 일시 네트워크 오류를 재시도하는 공통 HTTP 세션을 생성한다."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=1,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
    )
    return session


def safe_filename(value):
    """외부 식별자를 로컬 파일명으로 안전하게 사용할 수 있도록 정규화한다."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._") or "unknown"


def normalize_storage_key(path):
    """Build an environment-independent raw object key from a local path."""
    normalized = Path(path).as_posix()
    marker = "pipeline_output/"
    if marker in normalized:
        return normalized.split(marker, 1)[1].lstrip("/")

    raw_marker = "raw/"
    if raw_marker in normalized:
        return normalized.split(raw_marker, 1)[1].lstrip("/")

    return normalized.lstrip("/")


def infer_storage_backend(base_uri):
    """Infer LOCAL or S3 from the configured raw storage base URI."""
    if base_uri and base_uri.lower().startswith("s3://"):
        return "S3"
    return "LOCAL"


def join_storage_uri(base_uri, storage_key, local_path):
    """Resolve a storage key to the URI stored in source_object."""
    if not base_uri:
        return Path(local_path).resolve().as_uri()

    encoded_key = "/".join(quote(part) for part in storage_key.split("/"))
    return f"{base_uri.rstrip('/')}/{encoded_key}"


class LocalPipelineStorage:
    """Raw 원본과 스키마형 JSONL을 저장하는 로컬 미리보기 저장소다."""

    def __init__(self, root, pipeline_job_item_id=None):
        """출력 루트 아래에 raw와 tables 저장 경로를 설정한다."""
        self.root = Path(root)
        self.raw_root = self.root / "raw"
        self.table_root = self.root / "tables"
        self.pipeline_job_item_id = pipeline_job_item_id
        self.storage_base_uri = os.getenv("RAW_STORAGE_BASE_URI", "").strip()
        self.storage_backend = os.getenv(
            "RAW_STORAGE_BACKEND",
            infer_storage_backend(self.storage_base_uri),
        ).strip().upper()

    def write_raw_json(self, provider, data_type, external_id, payload, collected_at):
        """JSON 원본을 보존하고 해당 파일을 설명하는 source_object를 반환한다."""
        digest = content_hash(payload)
        path = (
            self.raw_root
            / provider.lower()
            / data_type.lower()
            / f"{safe_filename(external_id)}-{digest[:16]}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        ).encode("utf-8")
        path.write_bytes(encoded)
        return self._source_object(
            provider,
            data_type,
            external_id,
            path,
            digest,
            len(encoded),
            collected_at,
        )

    def write_raw_bytes(
        self,
        provider,
        data_type,
        external_id,
        payload,
        suffix,
        collected_at,
    ):
        """ICS 등 JSON이 아닌 원본 바이트를 저장하고 source_object를 반환한다."""
        digest = content_hash(payload)
        path = (
            self.raw_root
            / provider.lower()
            / data_type.lower()
            / (
                f"{safe_filename(external_id)}-{digest[:16]}."
                f"{suffix.lstrip('.')}"
            )
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return self._source_object(
            provider,
            data_type,
            external_id,
            path,
            digest,
            len(payload),
            collected_at,
        )

    def _source_object(
        self,
        provider,
        data_type,
        external_id,
        path,
        digest,
        object_size,
        collected_at,
    ):
        """저장한 원본의 URI·크기·해시를 source_object 스키마로 구성한다."""
        external_id = str(external_id) if external_id is not None else None
        storage_key = normalize_storage_key(path)
        return {
            "provider": provider,
            "data_type": data_type,
            "external_id": external_id,
            "storage_backend": self.storage_backend,
            "storage_key": storage_key,
            "object_uri": join_storage_uri(
                self.storage_base_uri,
                storage_key,
                path,
            ),
            "object_size": object_size,
            "content_hash": digest,
            "dedupe_key": build_dedupe_key(
                provider,
                data_type,
                external_id,
                digest,
            ),
            "collected_at": to_iso(collected_at),
            "processing_status": "COLLECTED",
            "pipeline_job_item_id": self.pipeline_job_item_id,
        }

    def write_table(self, table_name, records):
        """DB 테이블 적재 전 검증을 위해 레코드를 테이블별 JSONL로 출력한다."""
        records = list(records)
        path = self.table_root / f"{safe_filename(table_name)}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as output:
            for record in records:
                output.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        default=json_default,
                    )
                )
                output.write("\n")
        return path, len(records)
