"""Append-only news discovery stages bound to exact raw bytes and collection contract."""

import base64
import hashlib
from types import SimpleNamespace

from psycopg2.extras import Json

from pipeline.coordinated_price import load_raw
from pipeline.durable_requests import CoordinationError, canonical_hash
from pipeline.news_collection import (
    digest,
    normalize,
    parse_gkg,
    parse_rss,
    validate_unit,
)
from pipeline.news_enrichment import POLICY, relevance
from pipeline.news_targets import select_targets
from pipeline.stage_transaction import apply_phase

VERSION = "news-coordinated-v1"


def collection_contract(item):
    provider, unit, query = validate_unit(item)
    if item.parameters.get("recoveryVersion") != VERSION:
        raise CoordinationError("invalid_contract")
    if item.parameters.get("enrichmentPolicy") not in (None, POLICY):
        raise CoordinationError("invalid_contract")
    return {
        "version": VERSION,
        "itemKey": item.item_key,
        "provider": provider,
        "unit": unit,
        "query": query,
        "parameters": item.parameters,
        "target": "news:" + canonical_hash([item.item_key, item.parameters]),
    }


def envelope(raw, contract, receipt):
    limit = 1024**2 if contract["provider"] == "RSS" else 32 * 1024**2
    if not isinstance(raw, bytes) or len(raw) > limit:
        raise CoordinationError("invalid_contract")
    return {
        "version": VERSION,
        "contract": contract,
        "rawBase64": base64.b64encode(raw).decode(),
        "rawSha256": hashlib.sha256(raw).hexdigest(),
        "rawBytes": len(raw),
        "transport": receipt,
    }


def decode(payload, contract):
    if payload.get("version") != VERSION or payload.get("contract") != contract:
        raise CoordinationError("contract_conflict")
    try:
        raw = base64.b64decode(payload["rawBase64"], validate=True)
    except (ValueError, TypeError):
        raise CoordinationError("invalid_contract") from None
    if (
        len(raw) != payload["rawBytes"]
        or hashlib.sha256(raw).hexdigest() != payload["rawSha256"]
    ):
        raise CoordinationError("contract_conflict")
    if len(raw) > (1024**2 if contract["provider"] == "RSS" else 32 * 1024**2):
        raise CoordinationError("invalid_contract")
    return raw


def normalized(payload, contract):
    item = SimpleNamespace(
        source="NEWS",
        job_type="NEWS_" + contract["provider"] + "_COLLECT",
        item_key=contract["itemKey"],
        parameters=contract["parameters"],
    )
    if collection_contract(item) != contract:
        raise CoordinationError("contract_conflict")
    raw = decode(payload, contract)
    provider, unit = contract["provider"], contract["unit"]
    rows, stats = parse_rss(raw, unit) if provider == "RSS" else parse_gkg(raw, unit)
    rows, target_stats = select_targets(rows, item.parameters, contract["query"])
    stats.update(target_stats)
    candidates, observations = normalize(rows, provider)
    contract_hash = digest([item.item_key, item.parameters])
    for row in observations:
        m = row["metadata"]
        m.update(
            collectionUnit=item.item_key,
            query=contract["query"],
            collectionContractHash=contract_hash,
        )
        row["observationHash"] = digest(m)
        if item.parameters.get("enrichmentPolicy") == POLICY:
            m["originalObservationHash"] = row["observationHash"]
            m["providerTitle"] = m.get("title")
            # Network title enrichment remains a separately budgeted append-only task.
            m["titleEnrichment"] = (
                {"status": "PROVIDER_TITLE_RETAINED", "method": "PROVIDER_METADATA"}
                if m.get("title")
                else {
                    "status": "REVIEW_REQUIRED",
                    "reason": "DURABLE_TITLE_FETCH_SEPARATE_REVIEW",
                }
            )
            m["assetRelevance"] = relevance(m)
            m["enrichmentPolicy"] = POLICY
            row["observationHash"] = digest(m)
    return [
        {
            "candidates": sorted(candidates, key=lambda r: r["urlHash"]),
            "observations": sorted(
                observations, key=lambda r: (r["urlHash"], r["observationHash"])
            ),
            "stats": stats,
        }
    ]


def plan_for(key):
    return {"kind": "NEWS", "policyVersion": VERSION, "workKey": key}


def news_content(cur, plan, root, *, locking=True):
    if plan != plan_for(plan["workKey"]):
        raise CoordinationError("invalid_contract")
    contract, sources = load_raw(cur, plan["workKey"], root, locking=locking)
    if len(sources) != 1 or sources[0][1:3] != ("NEWS", "NEWS_ENVELOPE"):
        raise CoordinationError("contract_conflict")
    cur.execute(
        "SELECT identity FROM pipeline_recovery_work WHERE work_key=%s",
        (plan["workKey"],),
    )
    identity = cur.fetchone()[0]
    if (
        identity["provider"] != "NEWS"
        or identity["target"] != contract["target"]
        or identity["dataset"] != "news-" + contract["provider"].lower()
    ):
        raise CoordinationError("contract_conflict")
    return normalized(sources[0][3], contract)


def news_validator(root, *, locking=True):
    def validate(cur, plan, content):
        if content != news_content(cur, plan, root, locking=locking):
            raise CoordinationError("contract_conflict")
        return {
            "rawReplayMatch": True,
            "observations": len(content[0]["observations"]),
            "invalid": content[0]["stats"]["invalid"],
            "discoveryOnly": True,
        }

    return validate


def verify_publication(cur, key, plan_hash, root):
    cur.execute(
        "SELECT plan,plan_hash,content,content_hash,published_result FROM pipeline_recovery_candidate WHERE work_key=%s",
        (key,),
    )
    row = cur.fetchone()
    if (
        not row
        or row[1] != plan_hash
        or canonical_hash(row[0]) != row[1]
        or canonical_hash(row[2]) != row[3]
        or row[4] is None
    ):
        raise CoordinationError("contract_conflict")
    news_validator(root, locking=False)(cur, row[0], row[2])
    cur.execute(
        "SELECT sources,item_id FROM pipeline_recovery_raw_intent WHERE work_key=%s",
        (key,),
    )
    sources, item = cur.fetchone()
    source_id = sources[0]["id"]
    for r in row[2][0]["candidates"]:
        cur.execute(
            "SELECT url FROM news_collection_candidate WHERE url_hash=%s",
            (r["urlHash"],),
        )
        if cur.fetchone() != (r["url"],):
            raise CoordinationError("contract_conflict")
    for r in row[2][0]["observations"]:
        cur.execute(
            "SELECT metadata FROM news_collection_observation WHERE url_hash=%s AND source_object_id=%s AND observation_hash=%s",
            (r["urlHash"], source_id, r["observationHash"]),
        )
        if cur.fetchone() != (r["metadata"],):
            raise CoordinationError("contract_conflict")
    cur.execute("SELECT checkpoint FROM pipeline_job_item WHERE id=%s", (item,))
    checkpoint = cur.fetchone()[0]
    if (
        checkpoint.get("stage") != "CANDIDATES_STORED"
        or checkpoint.get("sourceId") != source_id
    ):
        raise CoordinationError("contract_conflict")
    return {"hash": row[3], "rows": len(row[2][0]["observations"]), "result": row[4]}


def news_publication_verifier(root):
    return lambda cur, key, plan_hash: verify_publication(cur, key, plan_hash, root)


def publish_news(db, lease, plan_hash, root):
    with db.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM pipeline_recovery_candidate WHERE work_key=%s",
            (lease["workKey"],),
        )
        row = cur.fetchone()
    db.rollback()
    if not row:
        raise CoordinationError("review_required")
    digest_hash = row[0]

    def write(cur):
        cur.execute(
            "SELECT c.plan,c.plan_hash,c.content,c.content_hash,v.plan_hash,v.content_hash FROM pipeline_recovery_candidate c JOIN pipeline_recovery_verification v USING(work_key) WHERE work_key=%s FOR UPDATE OF c,v",
            (lease["workKey"],),
        )
        r = cur.fetchone()
        if (
            not r
            or r[1] != plan_hash
            or r[4] != plan_hash
            or canonical_hash(r[0]) != plan_hash
            or canonical_hash(r[2]) != digest_hash
            or r[3] != r[5]
        ):
            raise CoordinationError("contract_conflict")
        news_validator(root)(cur, r[0], r[2])
        data = r[2][0]
        cur.execute(
            "SELECT sources,item_id,contract FROM pipeline_recovery_raw_intent WHERE work_key=%s",
            (lease["workKey"],),
        )
        sources, item, contract = cur.fetchone()
        sid = sources[0]["id"]
        inserted = 0
        for c in data["candidates"]:
            cur.execute(
                "INSERT INTO news_collection_candidate(url_hash,url) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (c["urlHash"], c["url"]),
            )
            cur.execute(
                "SELECT url FROM news_collection_candidate WHERE url_hash=%s",
                (c["urlHash"],),
            )
            if cur.fetchone() != (c["url"],):
                raise CoordinationError("contract_conflict")
        for o in data["observations"]:
            cur.execute(
                "INSERT INTO news_collection_observation(url_hash,source_object_id,observation_hash,metadata) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (o["urlHash"], sid, o["observationHash"], Json(o["metadata"])),
            )
            inserted += cur.rowcount
        cur.execute("SELECT checkpoint FROM pipeline_job_item WHERE id=%s", (item,))
        cp = cur.fetchone()[0]
        cp.update(
            stage="CANDIDATES_STORED",
            sourceId=sid,
            contractHash=digest([contract["itemKey"], contract["parameters"]]),
            stats=data["stats"],
            candidates=len(data["candidates"]),
            observations=len(data["observations"]),
        )
        cur.execute(
            "UPDATE pipeline_job_item SET checkpoint=%s WHERE id=%s", (Json(cp), item)
        )
        result = {
            "inserted": inserted,
            "observations": len(data["observations"]),
            "needsReview": bool(data["stats"]["invalid"]),
            "discoveryOnly": True,
        }
        cur.execute(
            "UPDATE pipeline_recovery_candidate SET published_result=%s WHERE work_key=%s",
            (Json(result), lease["workKey"]),
        )
        return result

    return apply_phase(db, lease, "VERIFIED", digest_hash, write)
