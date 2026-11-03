import copy
import hashlib
from types import SimpleNamespace

import pytest

from pipeline.news_targets import select_targets,validate_targets
from pipeline.news_collection import validate_unit,normalize


def parameters(provider="GDELT"):
    targets=[dict(assetId=42,ticker="NVDA",name="NVIDIA CORP",exchange="NASDAQ",organizationNames=["nvidia"]),
             dict(assetId=43,ticker="IBM",name="International Business Machines Corp.",exchange="NYSE",organizationNames=["international business machines"])]
    queries={}
    if provider=="RSS":
        for t in targets:
            q=f'("{t["organizationNames"][0]}" OR "{t["ticker"]} stock")'
            key=hashlib.sha256(q.encode()).hexdigest()[:16];t["queryKey"]=key;queries[key]=q
    return dict(contract="news-v2",provider=provider,pipelineMode="COLLECT_ONLY",targets=targets,queries=queries)


def test_gkg_filters_requested_companies_and_keeps_multi_stock_mentions():
    p=parameters();rows=[dict(url="https://example.org/a",organizations=["nvidia corporation"],title=None),
        dict(url="https://example.org/b",organizations=["apple inc"],title=None),
        dict(url="https://example.org/c",organizations=["IBM","nvidia"],title=None)]
    selected,stats=select_targets(rows,p,None)
    assert len(selected)==2 and stats["excludedByTargetMetadata"]==1
    assert {m["assetId"] for m in selected[1]["targetMatches"]}=={42,43}
    c,o=normalize(selected,"GDELT")
    assert len(c)==2 and all(not x["metadata"]["tickerIdentityVerified"] and not x["metadata"]["ragEligible"] for x in o)
    assert o[0]["metadata"]["eventDate"] is None


def test_korean_display_names_bind_to_linked_english_company_names():
    p=parameters();p["targets"][0].update(name="엔비디아",companyName="NVIDIA CORP")
    rows=[dict(title="NVIDIA reports earnings",organizations=["nvidia corporation"])]
    selected,stats=select_targets(rows,p,None)
    assert len(selected)==1 and selected[0]["requestedAssets"][0]["name"]=="엔비디아"
    assert selected[0]["requestedAssets"][0]["companyName"]=="NVIDIA CORP"
    p=parameters("RSS");p["targets"][0].update(name="엔비디아",companyName="NVIDIA CORP")
    assert len(validate_targets(p))==2


def test_rss_binds_each_query_to_its_target_and_marks_weak_results():
    p=parameters("RSS");q=p["queries"][p["targets"][0]["queryKey"]]
    selected,stats=select_targets([dict(title="NVIDIA announces earnings"),dict(title="Markets today")],p,q)
    assert len(selected)==2 and stats["queryOnlyReviewCandidates"]==1
    assert selected[0]["targetMatches"][0]["assetId"]==42
    assert selected[1]["targetMatches"][0]["basis"]==["QUERY_RESULT_ONLY_REVIEW_REQUIRED"]
    assert len(selected[0]["requestedAssets"])==1


def test_title_ticker_context_supports_arbitrary_stocks_not_hardcoded():
    p=parameters()
    selected,stats=select_targets([dict(title="IBM stock rises after results",organizations=[])],p,None)
    assert len(selected)==1 and selected[0]["targetMatches"][0]["ticker"]=="IBM"
    assert "TITLE_TICKER_CONTEXT_MENTION" in selected[0]["targetMatches"][0]["basis"]
    assert stats["excludedByTargetMetadata"]==0


def test_empty_matches_does_not_claim_no_real_articles_exist():
    selected,stats=select_targets([dict(title=None,organizations=[])],parameters(),None)
    assert not selected and stats["excludedByTargetMetadata"]==1
    assert stats["targetMatching"]=="MENTION_CANDIDATES_NOT_VERIFIED_DIRECT_RELEVANCE"


def test_name_matching_has_word_boundaries():
    p=parameters();p["targets"]=[dict(assetId=1,ticker="AAPL",name="Apple Inc.",exchange="NASDAQ",organizationNames=["apple"])]
    rows=[dict(title="Pineapple growers",organizations=[]),dict(title="Apple reports results",organizations=[])]
    selected,stats=select_targets(rows,p,None)
    assert len(selected)==1 and stats["excludedByTargetMetadata"]==1


@pytest.mark.parametrize("mutate",[
    lambda p:p.update(contract="news-v1"),
    lambda p:p["targets"][0].update(organizationNames=["tesla"]),
    lambda p:p["targets"][0].update(assetId=-1),
    lambda p:p["targets"][0].update(ticker="NVDA\" OR everything"),
    lambda p:p["targets"][1].update(assetId=42),
    lambda p:p["targets"][0].update(queryKey="forged"),
    lambda p:p.update(queries=[]),
])
def test_forged_and_inconsistent_snapshots_rejected(mutate):
    p=parameters("RSS");mutate(p)
    with pytest.raises(ValueError):validate_targets(p)


def test_v1_unselected_jobs_keep_previous_behavior():
    rows=[dict(title="Anything")]
    assert select_targets(rows,dict(contract="news-v1"),None)==(rows,{})


def test_targeted_unit_validates_same_cross_language_rss_contract():
    p=parameters("RSS");key=p["targets"][0]["queryKey"]
    i=SimpleNamespace(source="NEWS",job_type="NEWS_RSS_COLLECT",parameters=p,item_key=f"RSS:{key}:2016-12-30")
    assert validate_unit(i)==("RSS","2016-12-30",p["queries"][key])
    i.parameters=copy.deepcopy(p);i.parameters["targets"][0]["queryKey"]="wrong"
    with pytest.raises(ValueError):validate_unit(i)
