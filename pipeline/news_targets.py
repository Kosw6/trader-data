"""Catalog-selected news discovery. Mentions are never verified security/article identity."""
import hashlib
import re


def core_name(name):
    normalized="".join(c.lower() if c.isalnum() or c==" " else " " for c in name)
    words=normalized.split()
    suffix={"inc","incorporated","corp","corporation","ltd","limited","plc","co","company"}
    while len(words)>1 and words[-1] in suffix:
        words.pop()
    return " ".join(words)


def validate_targets(parameters):
    targets=parameters.get("targets",[])
    if parameters.get("contract")=="news-v1":
        if targets:raise ValueError("NEWS_TARGETS_REQUIRE_V2")
        return []
    if parameters.get("contract")!="news-v2" or not isinstance(targets,list) or not 1<=len(targets)<=10:
        raise ValueError("NEWS_TARGET_CONTRACT_INVALID")
    ids=set();queries=parameters.get("queries",{})
    if not isinstance(queries,dict):raise ValueError("NEWS_TARGET_QUERIES_INVALID")
    for t in targets:
        if not isinstance(t,dict):raise ValueError("NEWS_TARGET_INVALID")
        asset_id=t.get("assetId");ticker=t.get("ticker");name=t.get("name");exchange=t.get("exchange")
        if (type(asset_id) is not int or not 0<asset_id<2**63 or asset_id in ids or
            not isinstance(ticker,str) or not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,19}",ticker) or
            not isinstance(name,str) or not isinstance(exchange,str) or not exchange.strip()):
            raise ValueError("NEWS_TARGET_IDENTITY_INVALID")
        company_name=t.get("companyName","")
        if not isinstance(company_name,str):raise ValueError("NEWS_TARGET_COMPANY_NAME_INVALID")
        core=core_name(company_name or name)
        if not 2<=len(core.encode())<=160 or t.get("organizationNames")!=[core]:
            raise ValueError("NEWS_TARGET_NAME_INVALID")
        ids.add(asset_id)
        if parameters["provider"]=="RSS":
            query=f'("{core}" OR "{ticker} stock")'
            key=hashlib.sha256(query.encode()).hexdigest()[:16]
            if t.get("queryKey")!=key or queries.get(key)!=query:
                raise ValueError("NEWS_TARGET_QUERY_MISMATCH")
    if parameters["provider"]=="RSS" and len(queries)!=len(targets):
        raise ValueError("NEWS_TARGET_QUERY_COUNT_INVALID")
    if parameters["provider"]=="GDELT" and queries:
        raise ValueError("GKG_TARGET_QUERY_INVALID")
    return targets


def phrase_in(phrase,text):
    return re.search(r"(?<!\w)"+re.escape(phrase)+r"(?!\w)",core_name(text)) is not None


def select_targets(rows,parameters,query):
    targets=validate_targets(parameters)
    if not targets:return rows,{}
    if parameters["provider"]=="RSS":
        key=hashlib.sha256(query.encode()).hexdigest()[:16]
        targets=[t for t in targets if t["queryKey"]==key]
    selected=[];weak=0;excluded=0
    for row in rows:
        matches=[]
        for target in targets:
            name=target["organizationNames"][0];basis=[]
            if any(phrase_in(name,org) for org in row.get("organizations",[])):
                basis.append("ORGANIZATION_NAME_MENTION")
            if len(target["ticker"])>=3 and any(core_name(org)==target["ticker"].lower() for org in row.get("organizations",[])):
                basis.append("ORGANIZATION_TICKER_MENTION")
            title=row.get("title") or ""
            if phrase_in(name,title):basis.append("TITLE_COMPANY_NAME_MENTION")
            ticker=re.escape(target["ticker"])
            if re.search(r"(?<![A-Za-z0-9])(?:\$"+ticker+r"\b|(?:NASDAQ|NYSE|AMEX)\s*:\s*"+ticker+r"\b|"+ticker+r"\s+(?:stock|shares)\b)",title,re.I):
                basis.append("TITLE_TICKER_CONTEXT_MENTION")
            if not basis and parameters["provider"]=="RSS":
                basis=["QUERY_RESULT_ONLY_REVIEW_REQUIRED"]
                weak+=1
            if basis:matches.append(dict(assetId=target["assetId"],ticker=target["ticker"],exchange=target["exchange"],basis=basis))
        if not matches:
            excluded+=1
            continue
        matched_ids={m["assetId"] for m in matches}
        selected.append(dict(row,requestedAssets=[dict(assetId=t["assetId"],ticker=t["ticker"],name=t["name"],companyName=t.get("companyName"),exchange=t["exchange"]) for t in targets if t["assetId"] in matched_ids],
            targetMatches=matches,targetIdentityBasis="ACTIVE_CATALOG_SNAPSHOT_NOT_HISTORICAL_IDENTITY_VERIFICATION"))
    return selected,dict(targets=len(targets),excludedByTargetMetadata=excluded,queryOnlyReviewCandidates=weak,
                         targetMatching="MENTION_CANDIDATES_NOT_VERIFIED_DIRECT_RELEVANCE")
