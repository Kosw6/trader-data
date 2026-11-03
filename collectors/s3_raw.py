"""Immutable, bounded S3 raw objects, verified before source metadata is returned."""
import base64
import hashlib
import os
from urllib.parse import quote, unquote, urlsplit

_CLIENT = None


class RawStorageError(RuntimeError):
    pass


def max_bytes():
    value = int(os.getenv("RAW_MAX_OBJECT_BYTES", str(32 * 1024 * 1024)))
    if not 1 <= value <= 128 * 1024 * 1024:
        raise RawStorageError("invalid_raw_object_limit")
    return value


def client():
    global _CLIENT
    if _CLIENT is None:
        import boto3
        from botocore.config import Config
        _CLIENT = boto3.client("s3", config=Config(connect_timeout=5, read_timeout=30,
                                                 retries={"total_max_attempts": 1}))
    return _CLIENT


def location(base_uri, key):
    parsed = urlsplit(base_uri)
    prefix = unquote(parsed.path).strip("/")
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment or "@" in parsed.netloc:
        raise RawStorageError("invalid_s3_base_uri")
    if any(p in (".", "..") for p in (prefix + "/" + key).split("/")) or key.startswith("/"):
        raise RawStorageError("invalid_raw_key")
    return parsed.netloc, "/".join(p for p in (prefix, key) if p)


def _fetch(bucket, key, expected_size, expected_content_hash):
    if not 0 <= expected_size <= max_bytes():
        raise RawStorageError("raw_object_size_limit")
    response = client().get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        if response["ContentLength"] != expected_size:
            raise RawStorageError("raw_object_size_mismatch")
        encoded = body.read(expected_size + 1)
    finally:
        body.close()
    metadata = response.get("Metadata", {})
    if (len(encoded) != expected_size
            or hashlib.sha256(encoded).hexdigest() != metadata.get("raw-sha256")
            or metadata.get("content-hash") != expected_content_hash):
        raise RawStorageError("raw_object_hash_mismatch")
    return encoded


def publish(path, base_uri, provider, data_type, digest, expected_size):
    if expected_size > max_bytes():
        raise RawStorageError("raw_object_size_limit")
    encoded = path.read_bytes()
    if len(encoded) != expected_size:
        raise RawStorageError("raw_local_size_changed")
    relative = f"{provider.lower()}/{data_type.lower()}/{digest}{path.suffix}"
    bucket, key = location(base_uri, relative)
    byte_hash = hashlib.sha256(encoded).hexdigest()
    try:
        client().put_object(Bucket=bucket, Key=key, Body=encoded, IfNoneMatch="*",
                            ServerSideEncryption="AES256", ContentType="application/octet-stream",
                            ChecksumSHA256=base64.b64encode(hashlib.sha256(encoded).digest()).decode(),
                            Metadata={"raw-sha256": byte_hash, "content-hash": digest})
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") not in ("PreconditionFailed", "412"):
            raise RawStorageError("raw_upload_unconfirmed") from None
    # Also covers an existing immutable object. Never trust ETag or PUT alone.
    if _fetch(bucket, key, len(encoded), digest) != encoded:
        raise RawStorageError("raw_object_content_conflict")
    uri = "s3://" + bucket + "/" + quote(key, safe="/")
    # The remote object is already fully verified. DB publication comes later;
    # a crash here leaves an immutable raw object rather than a false local URI.
    path.unlink()
    return relative, uri


def read(source):
    parsed = urlsplit(source["object_uri"])
    key = unquote(parsed.path).lstrip("/")
    bucket, expected_key = location(os.environ.get("RAW_STORAGE_BASE_URI", ""), source["storage_key"])
    if parsed.scheme != "s3" or parsed.netloc != bucket or key != expected_key or parsed.query or parsed.fragment:
        raise RawStorageError("raw_object_outside_configured_prefix")
    return _fetch(bucket, key, int(source["object_size"]), source["content_hash"])


def read_json(source):
    import json
    from collectors.common import content_hash
    payload = json.loads(read(source))
    if content_hash(payload) != source["content_hash"]:
        raise RawStorageError("raw_canonical_content_hash_mismatch")
    return payload
