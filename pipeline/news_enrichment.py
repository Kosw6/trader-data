"""Versioned title/relevance evidence. Classification is per asset, never a RAG admission decision."""
import copy
import re
import time

from pipeline.news_collection import digest
from pipeline.news_targets import core_name, phrase_in

POLICY = "news-title-relevance-v1"
LABELS = {"DIRECT": "직접 관련", "INDIRECT": "간접 관련", "MENTION": "단순 언급", "REVIEW": "판단 보류"}


def relevance(metadata):
    title = metadata.get("title") or ""
    # RSS often appends a publisher. It must not act as company-subject evidence.
    subject = title
    publisher = metadata.get("publisher") or ""
    if publisher:
        subject = re.sub(r"\s[-|–]\s" + re.escape(publisher) + r"\s*$", "", subject, flags=re.I)
    normalized = core_name(subject)
    results = []
    matches = {m["assetId"]: m for m in metadata.get("targetMatches", [])}
    for target in metadata.get("requestedAssets", []):
        name = core_name(target.get("companyName") or target["name"])
        ticker = target["ticker"]
        basis = matches.get(target["assetId"], {}).get("basis", [])
        named = phrase_in(name, subject)
        # Bare tickers in prose (A, ON, ALL, etc.) are not company identity evidence.
        ticker_context = bool(re.search(r"(?:\$|NASDAQ\s*:\s*|NYSE\s*:\s*)" + re.escape(ticker) + r"\b|\b" + re.escape(ticker) + r"\s+(?:stock|shares)\b", subject, re.I))
        level, reason = "REVIEW", "INSUFFICIENT_TITLE_IDENTITY_OR_SUBJECT"
        evidence = []
        if not title:
            reason = "MISSING_TITLE_OR_FETCH_UNAVAILABLE"
        elif re.search(r"pre.?market.*(?:most active|movers)|market roundup|stocks to watch|most active (?:stocks|for)|market movers", title, re.I) and (named or ticker_context or basis):
            level, reason = "MENTION", "MULTI_COMPANY_MARKET_LIST"
        elif (named or ticker_context) and re.search(r"\breview\b|\brival\b|\bversus\b|\bcompared?\b|\bvs\b", subject, re.I):
            # Comparison/review is indirect unless the selected company is the main subject.
            if not normalized.startswith(name + " "):
                level, reason = "INDIRECT", "COMPETITOR_OR_PRODUCT_COMPARISON"
        elif named or ticker_context:
            # Ambiguous dictionary/company names require corroborating organization/financial context.
            ambiguous = name in {"apple", "meta", "target", "gap", "shell", "block", "on"}
            corroborated = "ORGANIZATION_NAME_MENTION" in basis or ticker_context or bool(re.search(
                r"\b(?:iphone|ipad|facetime|macbook|earnings|revenue|shares|stock|ceo|nasdaq|lawsuit|sue|sues)\b", subject, re.I))
            event = re.search(r"\b(?:announc\w*|report\w*|earnings|revenue|financial results|acquir\w*|launch\w*|releas\w*|sue|sues|sued|lawsuit|recall\w*|gpu\w*|chip\w*|powered|driver\w*)\b", subject, re.I)
            # Avoid describing an industry-wide multi-company article as dedicated company news.
            industry = bool(re.search(r"khronos|industry|multiple companies|amd[/\s]+nvidia", subject, re.I))
            if ambiguous and not corroborated:
                reason = "AMBIGUOUS_COMPANY_NAME"
            elif industry:
                level, reason = "INDIRECT", "INDUSTRY_OR_MULTIPLE_COMPANY_COVERAGE"
            elif event and (normalized.startswith(name + " ") or re.search(r"\b(?:sue|sues|sued|lawsuit)\b", subject, re.I)):
                level, reason = "DIRECT", "NAMED_COMPANY_EVENT_IN_HEADLINE"
            else:
                # A title mention alone is still not enough to assert direct relevance.
                level, reason = "MENTION", "TITLE_MENTION_WITHOUT_DIRECT_SUBJECT_EVIDENCE"
            evidence = [subject[:1000]]
        elif basis:
            level, reason = "MENTION", "ORGANIZATION_OR_QUERY_MENTION_ONLY"
        results.append(dict(assetId=target["assetId"], ticker=ticker, category=level,
                            label=LABELS[level], reason=reason, evidence=evidence,
                            basis=basis, policy=POLICY, verified=False))
    return results


def enrich_observations(observations, checkpoint, repository, client=None, clock=time.monotonic):
    """10 missing URLs / 60s per unit. Reserve before fetch, save before the next URL; no silent retries."""
    state = checkpoint.setdefault("titleEnrichment", {"policy": POLICY, "urls": {}})
    if state.get("policy") != POLICY: raise RuntimeError("NEWS_ENRICHMENT_POLICY_CONFLICT")
    saved = state["urls"]
    deadline = clock() + 60
    output = []
    for observation in observations:
        repository.check_cancel()
        row = copy.deepcopy(observation); metadata = row["metadata"]
        metadata["originalObservationHash"] = observation["observationHash"]
        metadata["providerTitle"] = metadata.get("title")
        if metadata.get("title"):
            receipt = {"status": "PROVIDER_TITLE_RETAINED", "method": "PROVIDER_METADATA"}
        elif row["urlHash"] in saved:
            receipt = saved[row["urlHash"]]
            if receipt.get("status") == "RESERVED":
                receipt = {"status": "REVIEW_REQUIRED", "reason": "PRIOR_FETCH_OUTCOME_UNKNOWN_NO_AUTOMATIC_RETRY"}
                saved[row["urlHash"]] = receipt; repository.save(checkpoint)
        elif client is None or len(saved) >= 10 or clock() >= deadline:
            receipt = {"status": "REVIEW_REQUIRED", "reason": "TITLE_FETCH_DISABLED_OR_UNIT_BUDGET"}
        else:
            saved[row["urlHash"]] = {"status": "RESERVED"}
            repository.save(checkpoint)  # A crash cannot reset URL admission or repeat unknown requests.
            receipt = client.fetch(metadata["url"], check=repository.check_cancel, deadline=deadline)
            saved[row["urlHash"]] = receipt
            repository.save(checkpoint)
        metadata["titleEnrichment"] = receipt
        if receipt.get("status") == "RECOVERED": metadata["title"] = receipt["title"]
        metadata["assetRelevance"] = relevance(metadata)
        metadata["enrichmentPolicy"] = POLICY
        # Preserve fail-closed discovery boundaries even if a publisher title was recovered.
        metadata.update(originalArticleVerified=False, tickerIdentityVerified=False, ragEligible=False, body=None)
        row["observationHash"] = digest(metadata)
        output.append(row)
    state["summary"] = {"missingUrlsAdmitted": len(saved), "observations": len(output),
                        "titleRecovered": sum(r["metadata"]["titleEnrichment"]["status"] == "RECOVERED" for r in output),
                        "classificationCounts": {level: sum(a["category"] == level for r in output for a in r["metadata"]["assetRelevance"]) for level in LABELS}}
    repository.save(checkpoint)
    return output
