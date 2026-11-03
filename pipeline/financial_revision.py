"""Validated immutable financial revisions; no implicit serving/mapping switch."""

import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from pipeline.coordinated_financial import TAGS, official_values
from pipeline.coordinated_price import load_raw
from pipeline.durable_requests import CoordinationError, canonical_hash
from pipeline.stage_transaction import apply_phase


def exact_decimal(value):
    """Canonical spelling without Decimal.normalize's context-dependent rounding."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if value == 0 else text


def revision_validator(root, *, locking=True):
    def validate(cur, plan, content):
        if (
            plan["kind"] != "FINANCIAL_REVISION"
            or plan["policyVersion"] != "sec-revision-v1"
            or content != plan["revisions"]
            or not 1 <= len(content) <= 10
        ):
            raise CoordinationError("invalid_contract")
        contract, raw = load_raw(cur, plan["workKey"], root, locking=locking)
        company = int(contract["target"].removeprefix("company:"))
        if company != plan["companyId"]:
            raise CoordinationError("contract_conflict")
        cur.execute(
            "SELECT cik FROM company WHERE id=%s" + (" FOR UPDATE" if locking else ""),
            (company,),
        )
        row = cur.fetchone()
        if not row or str(row[0]).zfill(10) != contract["cik"]:
            raise CoordinationError("contract_conflict")
        facts = [
            (s, body)
            for s, provider, kind, body in raw
            if provider == "SEC" and kind == "COMPANY_FACTS"
        ]
        submissions = [
            body
            for _, provider, kind, body in raw
            if provider == "SEC" and kind == "SUBMISSIONS"
        ]
        if (
            len(facts) != 1
            or len(submissions) != 1
            or any(
                str(body.get("cik", "")).zfill(10) != contract["cik"]
                for _, _, _, body in raw
            )
        ):
            raise CoordinationError("contract_conflict")
        source, facts = facts[0]
        recent = submissions[0].get("filings", {}).get("recent", {})
        normalized, seen = [], set()
        for e in content:
            value = Decimal(e["value"])
            base_form = e["form"].removesuffix("/A")
            end = date.fromisoformat(e["end"])
            start = date.fromisoformat(e["start"]) if e["start"] else None
            if (
                TAGS.get(e["tag"]) != e["metricCode"]
                or e["unit"]
                != ("USD/shares" if e["tag"] == "EarningsPerShareDiluted" else "USD")
                or not value.is_finite()
                or end > datetime.now(timezone.utc).date()
                or base_form not in {"10-Q", "10-K", "20-F", "40-F"}
            ):
                raise CoordinationError("review_required")
            if start:
                duration = (end - start).days
                if not (
                    (base_form == "10-Q" and 70 <= duration <= 105)
                    or (
                        base_form != "10-Q"
                        and 335 <= duration <= 395
                        and e["fiscalPeriod"] == "FY"
                    )
                ):
                    raise CoordinationError("review_required")
            elif e["tag"] not in {"Assets", "Liabilities"}:
                raise CoordinationError("review_required")
            matches = [
                v
                for v in facts.get("facts", {})
                .get("us-gaap", {})
                .get(e["tag"], {})
                .get("units", {})
                .get(e["unit"], [])
                if (v.get("start"), v.get("end"), v.get("accn"), v.get("form"))
                == (e["start"], e["end"], e["accession"], e["form"])
            ]
            if not matches or {Decimal(str(v["val"])) for v in matches} != {value}:
                raise CoordinationError("contract_conflict")
            accession_indices = [
                i
                for i, a in enumerate(recent.get("accessionNumber", []))
                if a == e["accession"]
            ]
            if len(accession_indices) != 1:
                raise CoordinationError("review_required")
            i = accession_indices[0]
            primary = recent["primaryDocument"][i]
            if (
                (recent["form"][i], recent["reportDate"][i]) != (e["form"], e["end"])
                or not re.fullmatch(r"[A-Za-z0-9_.-]+", primary)
                or not re.fullmatch(r"\d{10}-\d{2}-\d{6}", e["accession"])
            ):
                raise CoordinationError("contract_conflict")
            expected_url = f"https://www.sec.gov/Archives/edgar/data/{int(contract['cik'])}/{e['accession'].replace('-', '')}/{primary}"
            evidence = e["official"]
            if (
                evidence["url"] != expected_url
                or urlsplit(expected_url).hostname != "www.sec.gov"
            ):
                raise CoordinationError("invalid_contract")
            path = Path(evidence["path"]).resolve()
            if not path.is_relative_to(Path(root).resolve()):
                raise CoordinationError("invalid_contract")
            document = path.read_bytes()
            if hashlib.sha256(document).hexdigest() != evidence[
                "sha256"
            ] or official_values(
                document, contract["cik"], e["tag"], e["unit"], e["start"], e["end"]
            ) != {value}:
                raise CoordinationError("contract_conflict")
            scope = {
                "companyId": company,
                "cik": contract["cik"],
                "taxonomy": "us-gaap",
                "tag": e["tag"],
                "metricCode": e["metricCode"],
                "unit": e["unit"],
                "start": e["start"],
                "end": e["end"],
                "dimensions": {},
            }
            revision = {
                "scope": scope,
                "accession": e["accession"],
                "form": e["form"],
                "value": exact_decimal(value),
            }
            digest = canonical_hash(revision)
            if digest in seen:
                raise CoordinationError("contract_conflict")
            seen.add(digest)
            normalized.append(
                {
                    "hash": digest,
                    "revision": revision,
                    "officialHash": evidence["sha256"],
                }
            )
        return {
            "companyId": company,
            "sourceId": source["id"],
            "sourceHash": source["hash"],
            "normalized": normalized,
        }

    return validate


def publish_revisions(db, lease, plan_hash, root):
    with db.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM pipeline_recovery_candidate WHERE work_key=%s",
            (lease["workKey"],),
        )
        digest = cur.fetchone()[0]
    db.rollback()

    def write(cur):
        cur.execute(
            """SELECT c.plan,c.content,c.plan_hash,c.content_hash,v.plan_hash,v.content_hash FROM pipeline_recovery_candidate c
          JOIN pipeline_recovery_verification v USING(work_key) WHERE work_key=%s FOR UPDATE OF c,v""",
            (lease["workKey"],),
        )
        row = cur.fetchone()
        if (
            not row
            or row[2:] != (plan_hash, digest, plan_hash, digest)
            or canonical_hash(row[0]) != plan_hash
            or canonical_hash(row[1]) != digest
        ):
            raise CoordinationError("contract_conflict")
        evidence = revision_validator(root)(cur, row[0], row[1])
        ids, inserted, changes = [], 0, []
        for n in evidence["normalized"]:
            revision, scope = n["revision"], n["revision"]["scope"]
            cur.execute(
                "SELECT id,revision FROM pipeline_financial_revision WHERE company_id=%s AND scope->>'tag'=%s AND scope->>'end'=%s",
                (evidence["companyId"], scope["tag"], scope["end"]),
            )
            old = cur.fetchall()
            if any(
                v["scope"] != scope or Decimal(v["value"]) != Decimal(revision["value"])
                for _, v in old
            ):
                changes.append(n["hash"])
            cur.execute(
                """INSERT INTO pipeline_financial_revision(company_id,revision_hash,scope,revision) VALUES(%s,%s,%s::jsonb,%s::jsonb)
              ON CONFLICT(revision_hash) DO NOTHING RETURNING id""",
                (
                    evidence["companyId"],
                    n["hash"],
                    json.dumps(scope),
                    json.dumps(revision),
                ),
            )
            added = cur.fetchone()
            inserted += bool(added)
            cur.execute(
                "SELECT id,revision FROM pipeline_financial_revision WHERE revision_hash=%s",
                (n["hash"],),
            )
            rid, stored = cur.fetchone()
            if stored != revision:
                raise CoordinationError("contract_conflict")
            ids.append(rid)
            cur.execute(
                "INSERT INTO pipeline_financial_revision_origin(work_key,revision_id,source_object_id,source_hash,official_hash) VALUES(%s,%s,%s,%s,%s)",
                (
                    lease["workKey"],
                    rid,
                    evidence["sourceId"],
                    evidence["sourceHash"],
                    n["officialHash"],
                ),
            )
            cur.execute(
                """INSERT INTO record_lineage(source_object_id,target_table,target_record_id,target_record_key,transformer_name,transformer_version,output_hash,processed_at)
              VALUES(%s,'pipeline_financial_revision',%s,%s,'coordinated-revision','1',%s,clock_timestamp()) ON CONFLICT DO NOTHING""",
                (evidence["sourceId"], rid, n["hash"], n["hash"]),
            )
        result = {
            "revisionIds": ids,
            "inserted": inserted,
            "noChange": len(ids) - inserted,
            "changedRevisions": changes,
            "needsReview": bool(changes),
            "servingChanged": False,
        }
        cur.execute(
            "UPDATE pipeline_recovery_candidate SET published_result=%s::jsonb WHERE work_key=%s",
            (json.dumps(result), lease["workKey"]),
        )
        return result

    return apply_phase(db, lease, "VERIFIED", digest, write)


def revision_publication_verifier(root):
    def verify(cur, key, plan_hash):
        cur.execute(
            "SELECT plan,content,content_hash,published_result FROM pipeline_recovery_candidate WHERE work_key=%s AND plan_hash=%s",
            (key, plan_hash),
        )
        row = cur.fetchone()
        if (
            not row
            or canonical_hash(row[0]) != plan_hash
            or canonical_hash(row[1]) != row[2]
            or not row[3]
        ):
            raise CoordinationError("contract_conflict")
        evidence = revision_validator(root, locking=False)(cur, row[0], row[1])
        ids = []
        for n in evidence["normalized"]:
            cur.execute(
                """SELECT r.id,r.revision,o.source_hash,o.official_hash,l.output_hash FROM pipeline_financial_revision r
              JOIN pipeline_financial_revision_origin o ON o.revision_id=r.id AND o.work_key=%s
              JOIN record_lineage l ON l.target_record_id=r.id AND l.target_table='pipeline_financial_revision' AND l.source_object_id=o.source_object_id
              AND l.transformer_name='coordinated-revision' AND l.transformer_version='1'
              WHERE r.revision_hash=%s AND o.source_object_id=%s""",
                (key, n["hash"], evidence["sourceId"]),
            )
            saved = cur.fetchall()
            if len(saved) != 1 or saved[0][1:] != (
                n["revision"],
                evidence["sourceHash"],
                n["officialHash"],
                n["hash"],
            ):
                raise CoordinationError("contract_conflict")
            ids.append(saved[0][0])
        if ids != row[3]["revisionIds"]:
            raise CoordinationError("contract_conflict")
        return {"hash": row[2], "rows": len(ids), "result": row[3]}

    return verify
