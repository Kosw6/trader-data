"""Exact-scope, insert-only mapping of existing facts; never invent accounting values."""

import hashlib
import io
import json
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from pipeline.coordinated_price import load_raw
from pipeline.durable_requests import CoordinationError, canonical_hash
from pipeline.stage_transaction import apply_phase

X = "{http://www.xbrl.org/2003/instance}"
IX = "{http://www.xbrl.org/2013/inlineXBRL}"
# Deliberately narrow, unambiguous accounting meanings. Banks/component revenue and
# aliases require a separately reviewed extension, not a global synonym mapping.
TAGS = {
    "Assets": "ASSETS",
    "Liabilities": "LIABILITIES",
    "NetIncomeLoss": "NET_INCOME",
    "OperatingIncomeLoss": "OPERATING_INCOME",
    "EarningsPerShareDiluted": "DILUTED_EPS",
    "NetCashProvidedByUsedInOperatingActivities": "OPERATING_CASH_FLOW",
}


def gaap_namespace(uri):
    match = re.fullmatch(
        r"https?://fasb\.org/us-gaap/(\d{4})(?:-(\d{2})-(\d{2}))?", uri or ""
    )
    if not match:
        return False
    try:
        date(int(match[1]), int(match[2] or 1), int(match[3] or 1))
    except ValueError:
        return False
    return True


def official_values(raw, cik, tag, unit, start, end):
    if (
        len(raw) > 25_000_000
        or b"<!ENTITY" in raw.upper()
        or b"<!DOCTYPE" in raw.upper()
    ):
        raise CoordinationError("review_required")
    namespaces = {}
    for _, (prefix, uri) in ET.iterparse(io.BytesIO(raw), events=("start-ns",)):
        if prefix in namespaces and namespaces[prefix] != uri:
            raise CoordinationError("review_required")
        namespaces[prefix] = uri

    def qname(name):
        parts = (name or "").split(":")
        return (namespaces.get(parts[0]), parts[1]) if len(parts) == 2 else (None, "")

    root = ET.fromstring(raw)
    seen_ids = set()
    for node in root.iter():
        identity = node.get("id")
        if identity is not None:
            if not identity or identity in seen_ids:
                raise CoordinationError("review_required")
            seen_ids.add(identity)
    contexts, units = set(), set()
    for node in root.iter(X + "context"):
        identifier = node.find(".//" + X + "identifier")
        period = node.find(X + "period")
        if (
            identifier is None
            or period is None
            or identifier.get("scheme") != "http://www.sec.gov/CIK"
        ):
            continue
        if any(
            n.tag.rsplit("}", 1)[-1] in {"segment", "scenario"} for n in node.iter()
        ):
            continue
        if (
            (identifier.text or "").strip().zfill(10),
            period.findtext(X + "startDate"),
            period.findtext(X + "endDate") or period.findtext(X + "instant"),
        ) == (cik, start, end):
            contexts.add(node.get("id"))
    for node in root.iter(X + "unit"):
        if unit == "USD":
            measures = node.findall(X + "measure")
            ok = (
                len(node) == 1
                and len(measures) == 1
                and qname(measures[0].text)
                == (
                    "http://www.xbrl.org/2003/iso4217",
                    "USD",
                )
            )
        else:
            ok = (
                unit == "USD/shares"
                and len(node) == 1
                and len(node.findall(X + "divide")) == 1
                and len(node.find(X + "divide")) == 2
                and len(node.findall(".//" + X + "measure")) == 2
                and qname(
                    node.findtext(X + "divide/" + X + "unitNumerator/" + X + "measure")
                )
                == ("http://www.xbrl.org/2003/iso4217", "USD")
                and qname(
                    node.findtext(
                        X + "divide/" + X + "unitDenominator/" + X + "measure"
                    )
                )
                == ("http://www.xbrl.org/2003/instance", "shares")
            )
        if ok:
            units.add(node.get("id"))
    values = set()
    for node in root.iter(IX + "nonFraction"):
        uri, name = qname(node.get("name"))
        if (
            name != tag
            or not gaap_namespace(uri)
            or node.get("contextRef") not in contexts
            or node.get("unitRef") not in units
        ):
            continue
        if node.get("{http://www.w3.org/2001/XMLSchema-instance}nil") in {"true", "1"}:
            continue
        if node.get("format"):
            format_uri, format_name = qname(node.get("format"))
            if format_name not in {
                "num-dot-decimal",
                "numdotdecimal",
            } or not re.fullmatch(
                r"http://www.xbrl.org/inlineXBRL/transformation/(2010-04-20|2011-07-31|2015-02-26|2020-02-12|2022-02-16)",
                format_uri or "",
            ):
                raise CoordinationError("review_required")
        text = "".join(node.itertext()).strip().replace(",", "")
        if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text):
            continue
        if node.get("sign") not in {None, "-"} or (
            node.get("sign") and text.startswith(("+", "-"))
        ):
            raise CoordinationError("review_required")
        scale = int(node.get("scale", "0"))
        if not -12 <= scale <= 12:
            continue
        value = Decimal(text) * (Decimal(10) ** scale)
        if node.get("sign") == "-":
            value = -value
        values.add(value)
    return values


def financial_validator(allowed_root, *, locking=True):
    def validate(cur, plan, content):
        if (
            plan["kind"] != "FINANCIAL"
            or plan["policyVersion"] != "exact-us-gaap-v1"
            or content != plan["approvedMappings"]
            or not 1 <= len(content) <= 10
        ):
            raise CoordinationError("invalid_contract")
        contract, sources = load_raw(
            cur, plan["workKey"], allowed_root, locking=locking
        )
        company = int(contract["target"].removeprefix("company:"))
        if plan.get("companyId") != company:
            raise CoordinationError("contract_conflict")
        cur.execute(
            "SELECT cik FROM company WHERE id=%s" + (" FOR UPDATE" if locking else ""),
            (company,),
        )
        row = cur.fetchone()
        if not row or str(row[0]).zfill(10) != contract["cik"]:
            raise CoordinationError("contract_conflict")
        cik = contract["cik"]
        facts_sources = [
            raw
            for _, provider, kind, raw in sources
            if provider == "SEC" and kind == "COMPANY_FACTS"
        ]
        submissions = [
            raw
            for _, provider, kind, raw in sources
            if provider == "SEC" and kind == "SUBMISSIONS"
        ]
        if (
            len(facts_sources) != 1
            or not submissions
            or any(
                str(raw.get("cik", "")).zfill(10) != cik
                for raw in facts_sources + submissions
            )
        ):
            raise CoordinationError("review_required")
        seen = set()
        for entry in content:
            scope = (entry["metricCode"], entry["end"])
            if scope in seen or TAGS.get(entry["tag"]) != entry["metricCode"]:
                raise CoordinationError("review_required")
            seen.add(scope)
            cur.execute(
                """SELECT f.company_id,f.taxonomy,f.source_tag,f.unit,f.period_start,f.period_end,f.value,
                f.dimensions,f.source_fact_hash,g.form_type,g.accession_number,g.report_date,g.document_url,f.fiscal_period
                FROM financial_fact f JOIN filing g ON g.id=f.filing_id AND g.company_id=f.company_id WHERE f.id=%s"""
                + (" FOR SHARE OF f,g" if locking else ""),
                (entry["factId"],),
            )
            fact = cur.fetchone()
            if not fact or (
                fact[0],
                fact[1],
                fact[2],
                fact[3],
                str(fact[4]) if fact[4] else None,
                str(fact[5]),
                fact[6],
                fact[8],
                fact[9],
                fact[10],
            ) != (
                company,
                "us-gaap",
                entry["tag"],
                entry["unit"],
                entry["start"],
                entry["end"],
                Decimal(entry["value"]),
                entry["factHash"],
                entry["form"],
                entry["accession"],
            ):
                raise CoordinationError("contract_conflict")
            cur.execute(
                """SELECT l.source_object_id,l.output_hash,s.content_hash FROM record_lineage l JOIN source_object s ON s.id=l.source_object_id
                WHERE l.target_table='financial_fact' AND l.target_record_id=%s ORDER BY l.source_object_id,l.output_hash""",
                (entry["factId"],),
            )
            lineage = cur.fetchall()
            if (
                not lineage
                or any(not a or not b for _, a, b in lineage)
                or canonical_hash(lineage) != entry["lineageHash"]
            ):
                raise CoordinationError("contract_conflict")
            if (
                fact[7] not in (None, {})
                or not fact[6].is_finite()
                or fact[3]
                != ("USD/shares" if entry["metricCode"] == "DILUTED_EPS" else "USD")
            ):
                raise CoordinationError("review_required")
            form = fact[9].removesuffix("/A")
            if (
                form not in {"10-Q", "10-K", "20-F", "40-F"}
                or fact[11] != fact[5]
                or fact[5] > datetime.now(timezone.utc).date()
            ):
                raise CoordinationError("review_required")
            if fact[4]:
                days = (fact[5] - fact[4]).days
                if not (
                    (form == "10-Q" and 70 <= days <= 105)
                    or (form != "10-Q" and 335 <= days <= 395 and fact[13] == "FY")
                ):
                    raise CoordinationError("review_required")
            elif entry["metricCode"] not in {"ASSETS", "LIABILITIES"}:
                raise CoordinationError("review_required")
            # No direct metric mutation and no deriving any missing fact.
            candidates = (
                facts_sources[0]
                .get("facts", {})
                .get("us-gaap", {})
                .get(entry["tag"], {})
                .get("units", {})
                .get(entry["unit"], [])
            )
            values = {
                Decimal(str(v["val"]))
                for v in candidates
                if (v.get("start"), v.get("end"), v.get("accn"), v.get("form"))
                == (entry["start"], entry["end"], entry["accession"], entry["form"])
            }
            if values != {fact[6]}:
                raise CoordinationError("contract_conflict")
            # Existing date-scoped mapping cannot distinguish conflicting revisions.
            cur.execute(
                """SELECT DISTINCT value,unit,period_start FROM financial_fact WHERE company_id=%s
                AND taxonomy='us-gaap' AND source_tag=%s AND period_end=%s""",
                (company, entry["tag"], entry["end"]),
            )
            if cur.fetchall() != [(fact[6], fact[3], fact[4])]:
                raise CoordinationError("review_required")
            evidence = entry["official"]
            uri = urlsplit(evidence["url"])
            prefix = f"/Archives/edgar/data/{int(cik)}/{entry['accession'].replace('-', '')}/"
            if (
                uri.scheme != "https"
                or uri.netloc != "www.sec.gov"
                or not uri.path.startswith(prefix)
                or not re.fullmatch(r"[A-Za-z0-9/_.-]+", uri.path)
                or ".." in uri.path
                or uri.query
                or uri.fragment
                or (fact[12] and evidence["url"] != fact[12])
            ):
                raise CoordinationError("invalid_contract")
            path = Path(evidence["path"]).resolve()
            if not path.is_relative_to(Path(allowed_root).resolve()):
                raise CoordinationError("invalid_contract")
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != evidence["sha256"] or official_values(
                raw, cik, entry["tag"], entry["unit"], entry["start"], entry["end"]
            ) != {fact[6]}:
                raise CoordinationError("contract_conflict")
            cur.execute(
                "SELECT id FROM financial_metric WHERE metric_code=%s",
                (entry["metricCode"],),
            )
            metric = cur.fetchone()
            if not metric:
                raise CoordinationError("review_required")
            cur.execute(
                """SELECT id,source_tag FROM financial_metric_mapping WHERE (company_id=%s OR company_id IS NULL)
                AND taxonomy='us-gaap' AND metric_id=%s AND (valid_from IS NULL OR valid_from<=%s)
                AND (valid_to IS NULL OR valid_to>=%s)""",
                (company, metric[0], entry["end"], entry["end"]),
            )
            existing = cur.fetchall()
            if len(existing) > 1 or any(tag != entry["tag"] for _, tag in existing):
                raise CoordinationError("review_required")
        return {
            "companyId": company,
            "verifiedScopes": len(content),
            "officialAndFactsMatch": True,
        }

    return validate


def publish_financial(db, lease, plan_hash, allowed_root):
    with db.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM pipeline_recovery_candidate WHERE work_key=%s",
            (lease["workKey"],),
        )
        row = cur.fetchone()
    db.rollback()
    if not row:
        raise CoordinationError("review_required")
    digest = row[0]

    def write(cur):
        cur.execute(
            """SELECT c.plan,c.plan_hash,c.content,c.content_hash,v.content_hash,v.plan_hash
            FROM pipeline_recovery_candidate c JOIN pipeline_recovery_verification v USING(work_key)
            WHERE work_key=%s FOR UPDATE OF c,v""",
            (lease["workKey"],),
        )
        row = cur.fetchone()
        if (
            not row
            or row[1] != plan_hash
            or row[5] != plan_hash
            or canonical_hash(row[0]) != plan_hash
            or canonical_hash(row[2]) != digest
            or row[3] != digest
            or row[4] != digest
            or row[0]["workKey"] != lease["workKey"]
        ):
            raise CoordinationError("contract_conflict")
        evidence = financial_validator(allowed_root)(cur, row[0], row[2])
        results = []
        for entry in row[2]:
            cur.execute(
                "SELECT id FROM financial_metric WHERE metric_code=%s",
                (entry["metricCode"],),
            )
            metric = cur.fetchone()[0]
            cur.execute(
                """SELECT id FROM financial_metric_mapping WHERE (company_id=%s OR company_id IS NULL) AND taxonomy='us-gaap'
                AND metric_id=%s AND source_tag=%s AND (valid_from IS NULL OR valid_from<=%s) AND (valid_to IS NULL OR valid_to>=%s)""",
                (
                    evidence["companyId"],
                    metric,
                    entry["tag"],
                    entry["end"],
                    entry["end"],
                ),
            )
            found = cur.fetchone()
            if found:
                mapping = found[0]
            else:
                cur.execute(
                    """INSERT INTO financial_metric_mapping(company_id,taxonomy,source_tag,metric_id,valid_from,valid_to)
                    VALUES(%s,'us-gaap',%s,%s,%s,%s) RETURNING id""",
                    (
                        evidence["companyId"],
                        entry["tag"],
                        metric,
                        entry["end"],
                        entry["end"],
                    ),
                )
                mapping = cur.fetchone()[0]
            results.append(
                {
                    "mappingId": mapping,
                    "factId": entry["factId"],
                    "outcome": "NO_CHANGE_VERIFIED" if found else "INSERTED",
                }
            )
        result = {
            "scopes": results,
            "inserted": sum(r["outcome"] == "INSERTED" for r in results),
        }
        cur.execute(
            "UPDATE pipeline_recovery_candidate SET published_result=%s::jsonb WHERE work_key=%s",
            (json.dumps(result), lease["workKey"]),
        )
        return result

    return apply_phase(db, lease, "VERIFIED", digest, write)


def financial_publication_verifier(allowed_root):
    def verify(cur, key, plan_hash):
        cur.execute(
            "SELECT plan,plan_hash,content,content_hash,published_result FROM pipeline_recovery_candidate WHERE work_key=%s",
            (key,),
        )
        row = cur.fetchone()
        if (
            not row
            or row[1] != plan_hash
            or canonical_hash(row[0]) != plan_hash
            or canonical_hash(row[2]) != row[3]
            or not row[4]
        ):
            raise CoordinationError("contract_conflict")
        financial_validator(allowed_root, locking=False)(cur, row[0], row[2])
        for entry, result in zip(row[2], row[4]["scopes"], strict=True):
            cur.execute(
                """SELECT m.source_tag,f.metric_code,m.company_id,m.valid_from,m.valid_to
                FROM financial_metric_mapping m JOIN financial_metric f ON f.id=m.metric_id WHERE m.id=%s""",
                (result["mappingId"],),
            )
            mapping = cur.fetchone()
            company = int(row[0]["companyId"])
            if (
                not mapping
                or mapping[:2] != (entry["tag"], entry["metricCode"])
                or mapping[2] not in (None, company)
                or (mapping[3] and str(mapping[3]) > entry["end"])
                or (mapping[4] and str(mapping[4]) < entry["end"])
            ):
                raise CoordinationError("contract_conflict")
        return {"hash": row[3], "rows": len(row[2]), "result": row[4]}

    return verify
