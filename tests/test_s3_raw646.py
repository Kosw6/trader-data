import hashlib
import io
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from collectors import s3_raw
from collectors.common import LocalPipelineStorage, content_hash
from workers.raw_repository import read_raw_payload


class S3:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.fail_put = False
        self.break_read = False

    def put_object(self, **args):
        assert args["IfNoneMatch"] == "*" and args["ServerSideEncryption"] == "AES256"
        if self.fail_put:
            raise OSError("do not expose original response")
        key = args["Bucket"], args["Key"]
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.puts += 1
        self.objects[key] = args

    def get_object(self, Bucket, Key):
        obj = self.objects[Bucket, Key]
        body = b"corrupt" if self.break_read else obj["Body"]
        return {"ContentLength": len(obj["Body"]), "Body": io.BytesIO(body), "Metadata": obj["Metadata"]}


@pytest.fixture
def store(tmp_path, monkeypatch):
    fake = S3()
    monkeypatch.setenv("RAW_STORAGE_BACKEND", "S3")
    monkeypatch.setenv("RAW_STORAGE_BASE_URI", "s3://fixture-bucket/shared646/raw")
    monkeypatch.setattr(s3_raw, "_CLIENT", fake)
    return LocalPipelineStorage(tmp_path / "pipeline_output"), fake


def write(storage, payload=None):
    return storage.write_raw_json("BLS", "TIMESERIES", "fixture", payload or {"value": 42}, datetime.now(timezone.utc))


def test_upload_verified_before_metadata_and_local_cleanup(store):
    storage, fake = store
    source = write(storage)
    assert source["storage_backend"] == "S3" and source["object_uri"].startswith("s3://fixture-bucket/shared646/raw/")
    assert not list(storage.raw_root.rglob("*.json"))
    assert read_raw_payload(source) == {"value": 42}
    assert fake.puts == 1


def test_existing_equivalent_json_is_immutable_and_reusable(store):
    storage, fake = store
    first = write(storage, {"a": 1, "b": 2})
    second = write(storage, {"b": 2, "a": 1})
    assert first["object_uri"] == second["object_uri"] and fake.puts == 1


@pytest.mark.parametrize("failure", ["upload", "read"])
def test_failure_never_returns_false_source_metadata_and_keeps_local(store, failure):
    storage, fake = store
    fake.fail_put = failure == "upload"
    fake.break_read = failure == "read"
    with pytest.raises(s3_raw.RawStorageError):
        write(storage)
    assert list(storage.raw_root.rglob("*.json"))


def test_restart_reads_without_original_local_directory(store, tmp_path):
    storage, _ = store
    source = write(storage)
    assert read_raw_payload(dict(source)) == {"value": 42}


def test_changed_remote_bytes_rejected(store):
    storage, fake = store
    source = write(storage)
    obj = next(iter(fake.objects.values()))
    obj["Body"] = obj["Body"].replace(b"42", b"43")
    with pytest.raises(s3_raw.RawStorageError, match="hash_mismatch"):
        read_raw_payload(source)


def test_remote_metadata_cannot_replace_database_canonical_hash(store):
    storage, fake = store
    source = write(storage)
    obj = next(iter(fake.objects.values()))
    obj["Body"] = obj["Body"].replace(b"42", b"43")
    obj["Metadata"]["raw-sha256"] = hashlib.sha256(obj["Body"]).hexdigest()
    with pytest.raises(s3_raw.RawStorageError, match="canonical_content_hash"):
        read_raw_payload(source)


@pytest.mark.parametrize("uri", ["s3://other-bucket/shared646/raw/x", "s3://fixture-bucket/other/x", "https://example.com/x"])
def test_read_cannot_escape_configured_bucket_or_prefix(store, uri):
    storage, _ = store
    source = write(storage)
    source["object_uri"] = uri
    with pytest.raises(s3_raw.RawStorageError, match="outside"):
        read_raw_payload(source)


def test_large_object_rejected_without_s3_or_local_file(store, monkeypatch):
    storage, fake = store
    monkeypatch.setenv("RAW_MAX_OBJECT_BYTES", "10")
    with pytest.raises(s3_raw.RawStorageError, match="size_limit"):
        write(storage)
    assert not fake.objects and not list(storage.raw_root.rglob("*.json"))


def test_local_storage_backwards_compatible(tmp_path, monkeypatch):
    monkeypatch.setenv("RAW_STORAGE_BACKEND", "LOCAL")
    monkeypatch.setenv("RAW_STORAGE_BASE_URI", "")
    storage = LocalPipelineStorage(tmp_path)
    source = write(storage)
    assert source["content_hash"] == content_hash({"value": 42})
    assert read_raw_payload(source) == {"value": 42}
    assert list(storage.raw_root.rglob("*.json"))


def test_cleanup_removes_derived_previews_but_keeps_uncertain_raw(store, tmp_path, monkeypatch):
    from collectors.common import cleanup_verified_s3_previews
    _, fake = store
    base=tmp_path / "pipeline_output"
    monkeypatch.setenv("RAW_STORAGE_LOCAL_ROOT", str(base))
    storage=LocalPipelineStorage(base / "bls" / "job-8" / "item-9", pipeline_job_item_id=9)
    write(storage)
    storage.write_table("macro_series", [{"fixture":True}])
    uncertain=storage.raw_root / "unconfirmed.json"
    uncertain.write_bytes(b"not-yet-durable")
    cleanup_verified_s3_previews(9,8)
    assert uncertain.read_bytes()==b"not-yet-durable"
    assert not list(storage.table_root.glob("*.jsonl"))
