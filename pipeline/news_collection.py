"""Metadata discovery only: dates and name matches never become verified financial evidence."""
import csv
import hashlib
import html
import io
import json
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

from collectors.google_news_rss import parse_feed
from collectors.marketaux_client import safe_url
from pipeline.news_targets import validate_targets


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def validate_unit(item, now=None):
    p = item.parameters
    now = now or datetime.now(timezone.utc)
    provider = p.get("provider")
    if (item.source != "NEWS" or provider not in {"RSS", "GDELT"} or
            item.job_type != f"NEWS_{provider}_COLLECT" or p.get("contract") not in {"news-v1","news-v2"} or
            p.get("pipelineMode") != "COLLECT_ONLY"):
        raise ValueError("NEWS_CONTRACT_INVALID")
    validate_targets(p)
    parts = item.item_key.split(":")
    if provider == "RSS":
        if len(parts) != 3 or parts[0] != "RSS":
            raise ValueError("RSS_UNIT_INVALID")
        queries = p.get("queries")
        if not isinstance(queries, dict) or not 1 <= len(queries) <= 10:
            raise ValueError("RSS_QUERIES_INVALID")
        query = queries.get(parts[1])
        if (not isinstance(query, str) or not 2 <= len(query.encode()) <= 300 or query != query.strip() or
                any(x in query.lower() for x in ("after:", "before:", "\r", "\n", "\x00")) or
                hashlib.sha256(query.encode()).hexdigest()[:16] != parts[1]):
            raise ValueError("RSS_QUERY_INVALID")
        day = date.fromisoformat(parts[2])
        if day.isoformat() != parts[2] or day < date(2015, 2, 19) or day >= now.date():
            raise ValueError("RSS_DATE_INVALID")
        return provider, parts[2], query
    if len(parts) != 2 or parts[0] != "GKG" or not re.fullmatch(r"\d{14}", parts[1]):
        raise ValueError("GKG_UNIT_INVALID")
    stamp = datetime.strptime(parts[1], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    if stamp.minute % 15 or stamp.second or stamp.date() < date(2015, 2, 19) or stamp > now-timedelta(minutes=30):
        raise ValueError("GKG_DATE_INVALID")
    return provider, parts[1], None


def canonical_url(value):
    value = safe_url(value)
    if not value or len(value) > 8192:
        raise ValueError("NEWS_URL_INVALID")
    parts = urlsplit(value)
    # Keep path/query, including Google's opaque IDs. Never merge by title or decode redirects.
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path, parts.query, ""))


def parse_rss(raw, day):
    parsed = parse_feed(raw, year=None, sample_limit=None)
    rows, excluded = [], parsed["outOfYearItems"]
    for article in parsed["samples"]:
        when = datetime.fromisoformat(article["aggregatorReportedPublishedAt"])
        if when.astimezone(timezone.utc).date().isoformat() != day:
            excluded += 1
            continue
        rows.append({"url": article["aggregatorUrl"], "title": article["title"],
                     "publisher": article["publisher"], "providerReportedPublishedAt": when.isoformat(),
                     "dateMeaning": "AGGREGATOR_REPORTED_PUBLICATION", "association": "QUERY_MATCH_ONLY"})
    return rows, {"returned": parsed["returnedItems"], "invalid": parsed["invalidItems"],
                  "outsideWindow": excluded, "coverage": "SEARCH_RESULTS_NOT_EXHAUSTIVE",
                  "possiblyTruncated": parsed["returnedItems"] >= 100}


def parse_gkg(raw, stamp):
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise ValueError("GKG_ZIP_INVALID") from None
    rows, invalid, total = [], 0, 0
    with archive:
        files = archive.infolist()
        if (len(files) != 1 or files[0].filename != stamp+".gkg.csv" or
                files[0].file_size > 128*1024**2 or files[0].flag_bits & 1):
            raise ValueError("GKG_ZIP_MEMBER_OR_SIZE_INVALID")
        with archive.open(files[0]) as file:
            stream = io.TextIOWrapper(file, encoding="utf-8", errors="strict", newline="")
            for fields in csv.reader(stream, delimiter="\t", quoting=csv.QUOTE_NONE):
                total += 1
                if total > 100000:
                    raise ValueError("GKG_RECORD_LIMIT")
                if len(fields) != 27:
                    invalid += 1
                    continue
                try:
                    canonical_url(fields[4])
                    if not re.fullmatch(r"\d{14}", fields[1]):
                        raise ValueError()
                    datetime.strptime(fields[1], "%Y%m%d%H%M%S")
                    if fields[0].split("-")[0] != stamp:
                        raise ValueError()
                except ValueError:
                    invalid += 1
                    continue
                title = re.search(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", fields[26], re.S)
                rows.append({"url": fields[4], "recordId": fields[0], "title": html.unescape(title[1])[:2000] if title else None,
                             "publisher": fields[3], "providerDocumentDateRaw": fields[1], "gdeltBatchAt": stamp,
                             "organizations": fields[13].split(";") if fields[13] else [],
                             "themes": fields[7].split(";") if fields[7] else [],
                             "dateMeaning": "GDELT_DOCUMENT_METADATA_NOT_VERIFIED_EVENT_DATE",
                             "association": "ORGANIZATION_OR_THEME_MENTION_ONLY"})
    return rows, {"returned": total, "invalid": invalid, "coverage": "ONE_GKG_SNAPSHOT_NOT_ALL_NEWS"}


def normalize(rows, provider):
    """URL identity deduplicates candidates, while every distinct observation is retained."""
    candidates, observations = {}, {}
    for row in rows:
        url = canonical_url(row["url"])
        key = hashlib.sha256(url.encode()).hexdigest()
        candidates.setdefault(key, {"urlHash": key, "url": url})
        payload = dict(row, provider=provider, originalArticleVerified=False,
                       tickerIdentityVerified=False, eventDate=None, body=None, ragEligible=False)
        observations.setdefault((key, digest(payload)), {"urlHash": key, "observationHash": digest(payload), "metadata": payload})
    return list(candidates.values()), list(observations.values())
