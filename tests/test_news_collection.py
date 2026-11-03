import copy
import hashlib
import io
import json
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from collectors.news_http import NewsHTTP, NewsFetchError
from pipeline.news_collection import parse_rss, parse_gkg, normalize, validate_unit
from workers.news_collector import collect
from workers.news_repository import NewsCancelled


def item(provider="RSS"):
    query="NVIDIA earnings"
    key=hashlib.sha256(query.encode()).hexdigest()[:16]
    return SimpleNamespace(source="NEWS",job_type=f"NEWS_{provider}_COLLECT",job_id=1,item_id=2,attempt_count=1,
        item_key=f"RSS:{key}:2016-12-30" if provider=="RSS" else "GKG:20161230141500",
        parameters=dict(contract="news-v1",provider=provider,pipelineMode="COLLECT_ONLY",queries={key:query} if provider=="RSS" else {}))


def rss(count=1, date="Fri, 30 Dec 2016 13:05:00 GMT"):
    return ("<rss><channel>"+"".join(f"<item><title>NVIDIA {i}</title><link>https://news.google.com/rss/articles/{i}</link>"
        f"<pubDate>{date}</pubDate><source>Example</source></item>" for i in range(count))+"</channel></rss>").encode()


def gkg(stamp="20161230141500", lines=1, bad=False):
    fields=[""]*27
    fields[0]=stamp+"-1";fields[1]=stamp;fields[3]="example.org";fields[4]="https://example.org/article"
    fields[13]="nvidia;apple";fields[7]="ECON_INTEREST_RATE;SANCTIONS"
    output=io.BytesIO()
    with zipfile.ZipFile(output,"w",zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(stamp+".gkg.csv",("\t".join(fields)+"\n")*lines+("bad\n" if bad else ""))
    return output.getvalue()


class Repository:
    def __init__(self):
        self.checkpoint={};self.finishes=[];self.reservations=0;self.cancelled=False
    def load(self):return copy.deepcopy(self.checkpoint)
    def save(self,c):self.checkpoint=copy.deepcopy(c)
    def check_cancel(self):
        if self.cancelled:raise NewsCancelled()
    def reserve(self,c,p):
        c["attempts"]+=1;self.reservations+=1;self.save(c);return 0
    def register_raw(self,s,c):
        c.update(rawSource=s,sourceId=7);self.save(c)
    def finish(self,state,c,candidates=(),observations=(),error=None):
        self.save(c);self.finishes.append((state,candidates,observations,error));return state


@pytest.fixture
def storage(tmp_path,monkeypatch):
    from collectors.common import LocalPipelineStorage
    monkeypatch.setenv("RAW_STORAGE_BACKEND","LOCAL")
    monkeypatch.delenv("RAW_STORAGE_BASE_URI",raising=False)
    return LocalPipelineStorage(tmp_path,2)


def client(raw=None,errors=()):
    result=Mock();result.fetch.side_effect=list(errors)+[(rss() if raw is None else raw,{"http":200})];return result


def test_rss_collects_all_items_not_experiment_first_twenty():
    rows,stats=parse_rss(rss(50),"2016-12-30")
    assert len(rows)==50 and stats["invalid"]==0


def test_rss_utc_window_and_saturation_are_not_full_history_claims():
    rows,stats=parse_rss(rss(100,"Sat, 31 Dec 2016 00:30:00 +0900"),"2016-12-30")
    assert len(rows)==100 and stats["possiblyTruncated"] and "NOT_EXHAUSTIVE" in stats["coverage"]
    assert not parse_rss(rss(date="Sat, 31 Dec 2016 00:30:00 GMT"),"2016-12-30")[0]
    assert len(parse_rss(rss(date="Sun, 01 Jan 2017 00:30:00 +0900"),"2016-12-31")[0])==1


def test_gkg_keeps_full_metadata_without_promoting_mentions_or_dates():
    rows,stats=parse_gkg(gkg(lines=30,bad=True),"20161230141500")
    assert len(rows)==30 and stats["invalid"]==1 and rows[0]["title"] is None
    candidates,observations=normalize(rows,"GDELT")
    assert len(candidates)==len(observations)==1
    m=observations[0]["metadata"]
    assert not m["tickerIdentityVerified"] and not m["ragEligible"] and m["eventDate"] is None and m["body"] is None
    assert "nvidia" in m["organizations"]


def test_url_dedupe_retains_different_provider_observations():
    a={"url":"https://example.org/a#one","title":"A"}
    b={"url":"https://example.org/a#two","title":"B"}
    candidates,observations=normalize([a,a,b],"GDELT")
    assert len(candidates)==1 and len(observations)==2
    assert normalize([dict(a,url="https://example.org/b")],"GDELT")[0][0]["urlHash"]!=candidates[0]["urlHash"]


@pytest.mark.parametrize("mutate",[
    lambda i:setattr(i,"source","SEC"), lambda i:i.parameters.update(pipelineMode="COMBINED"),
    lambda i:i.parameters.update(queries={"wrong":"NVIDIA"}),
    lambda i:setattr(i,"item_key","GKG:20161230141501"),
    lambda i:setattr(i,"item_key","RSS:../../file:2016-12-30"),
])
def test_invalid_units_never_reach_network(mutate):
    i=item();mutate(i)
    with pytest.raises(ValueError):validate_unit(i)


def test_timeout_then_success_reserves_every_attempt_and_continues(storage):
    repo=Repository();c=client(errors=[NewsFetchError("NEWS_TRANSPORT_UNAVAILABLE",True)])
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="COLLECTED"
    assert repo.reservations==2 and c.fetch.call_count==2 and repo.checkpoint["attempts"]==2
    assert "error" not in repo.checkpoint and len(repo.checkpoint["requestHistory"])==2
    assert repo.finishes[0][2][0]["metadata"]["query"]=="NVIDIA earnings"


def test_exhaustion_is_failed_not_empty_success_next_item_can_run(storage):
    repo=Repository();c=Mock();c.fetch.side_effect=NewsFetchError("NEWS_TRANSPORT_UNAVAILABLE",True)
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="FAILED"
    assert c.fetch.call_count==3 and not repo.finishes[0][1]
    assert collect(item(),Repository(),storage,client(),wait=lambda *_:None)=="COLLECTED"
    # Replay cannot reset the persisted attempt budget.
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="FAILED" and c.fetch.call_count==3


def test_nonretryable_response_is_not_retried(storage):
    repo=Repository();c=client(errors=[NewsFetchError("NEWS_HTTP_404")])
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="FAILED" and c.fetch.call_count==1


def test_crash_after_raw_registration_resumes_without_provider_request(storage):
    repo=Repository();original=repo.finish
    repo.finish=Mock(side_effect=RuntimeError("database unavailable"))
    c=client()
    with pytest.raises(RuntimeError):collect(item(),repo,storage,c,wait=lambda *_:None)
    assert repo.checkpoint["rawSource"] and c.fetch.call_count==1
    repo.finish=original
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="COLLECTED" and c.fetch.call_count==1


def test_raw_tamper_is_global_failure_no_refetch_or_false_success(storage):
    from pathlib import Path
    from collectors.s3_raw import RawStorageError
    repo=Repository();collect(item(),repo,storage,client(),wait=lambda *_:None)
    source=repo.checkpoint["rawSource"]
    from workers.raw_repository import local_path_from_source_object
    Path(local_path_from_source_object(source)).write_bytes(b"tamper")
    c=client()
    with pytest.raises(RawStorageError):collect(item(),repo,storage,c,wait=lambda *_:None)
    c.fetch.assert_not_called()


def test_cancel_before_request_and_during_backoff(storage):
    repo=Repository();repo.cancelled=True;c=client()
    assert collect(item(),repo,storage,c)=="CANCELLED";c.fetch.assert_not_called()
    repo=Repository();c=client(errors=[NewsFetchError("TIMEOUT",True)])
    def wait(seconds,repo):
        if repo.checkpoint.get("retryAt"):raise NewsCancelled()
    assert collect(item(),repo,storage,c,wait=wait)=="CANCELLED" and c.fetch.call_count==1


def test_schema_failure_preserves_raw_for_review_without_retry(storage):
    repo=Repository();c=client(raw=b"not xml")
    assert collect(item(),repo,storage,c,wait=lambda *_:None)=="FAILED"
    assert repo.checkpoint["sourceId"]==7 and c.fetch.call_count==1


def test_partial_gkg_preserves_valid_candidates_and_reports_failure(storage):
    repo=Repository()
    assert collect(item("GDELT"),repo,storage,client(gkg(bad=True)),wait=lambda *_:None)=="FAILED"
    assert repo.finishes[0][1] and repo.finishes[0][3]=="NEWS_PARTIAL_SCHEMA_REVIEW_REQUIRED"


def test_transport_fixed_endpoint_no_redirect_or_hidden_retry():
    session=Mock();response=Mock(status_code=503,headers={"Retry-After":"999"})
    session.get.return_value.__enter__=Mock(return_value=response)
    session.get.return_value.__exit__=Mock(return_value=False)
    transport=NewsHTTP(session)
    with pytest.raises(NewsFetchError) as error:transport.fetch("RSS","2016-12-30","NVIDIA")
    assert error.value.retryable and error.value.retry_after==60 and session.get.call_count==1
    args,kwargs=session.get.call_args
    assert args[0]=="https://news.google.com/rss/search" and not kwargs["allow_redirects"] and kwargs["timeout"]==(5,60)
    with pytest.raises(ValueError):transport.fetch("OTHER","","")


def test_transport_size_cap_and_redirects_are_not_success():
    for status,headers in [(200,{"Content-Length":str(1024**2+1)}),(302,{})]:
        session=Mock();response=Mock(status_code=status,headers=headers)
        session.get.return_value.__enter__=Mock(return_value=response);session.get.return_value.__exit__=Mock(return_value=False)
        with pytest.raises(NewsFetchError):NewsHTTP(session).fetch("RSS","2016-12-30","NVIDIA")


def test_dispatch_uses_news_worker_and_rejects_combined(monkeypatch):
    from workers.kafka_dispatcher import dispatch_item
    fn=Mock(return_value="COLLECTED");monkeypatch.setattr("workers.news_collector.process_item",fn)
    i=item();assert dispatch_item(None,i)=="COLLECTED";fn.assert_called_once()
    i.parameters["pipelineMode"]="COMBINED"
    with pytest.raises(ValueError):dispatch_item(None,i)


def test_unknown_news_failure_does_not_overwrite_or_ack_checkpoint(monkeypatch):
    from workers import kafka_pipeline_consumer as worker
    monkeypatch.setattr(worker,"find_processed_event",lambda *_:None)
    monkeypatch.setattr(worker,"claim_queued_item_by_id",lambda *_,**kw:item())
    monkeypatch.setattr(worker,"dispatch_item",Mock(side_effect=RuntimeError("unclassified")))
    recorded,failed=Mock(),Mock()
    monkeypatch.setattr(worker,"record_processed_event",recorded)
    monkeypatch.setattr(worker,"fail_claimed_item",failed)
    message=SimpleNamespace(value=dict(idempotencyKey="fixture",itemId=2,source="NEWS",jobType="NEWS_RSS_COLLECT"))
    with pytest.raises(RuntimeError):worker.process_message(Mock(),message,"fixture")
    recorded.assert_not_called();failed.assert_not_called()


def test_disabled_news_does_not_touch_database_or_network(monkeypatch):
    from workers.news_collector import process_item
    monkeypatch.setenv("NEWS_COLLECTION_ENABLED","false")
    connection=Mock()
    with pytest.raises(RuntimeError,match="DISABLED"):process_item(connection,item())
    assert not connection.mock_calls


def test_contract_mismatch_on_resume_never_fetches(storage):
    repo=Repository();repo.checkpoint={"contractHash":"incorrect","attempts":0}
    c=client()
    with pytest.raises(RuntimeError,match="CONTRACT_CONFLICT"):collect(item(),repo,storage,c)
    c.fetch.assert_not_called()


def test_repository_reservation_commits_before_external_wait():
    from datetime import datetime,timezone
    from unittest.mock import MagicMock
    from workers.news_repository import NewsRepository
    conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
    cursor.rowcount=1;cursor.fetchone.return_value=(datetime.now(timezone.utc),)
    c={"attempts":1}
    delay=NewsRepository(conn,item()).reserve(c,"RSS")
    assert delay>=0 and c["attempts"]==2 and c["requestSlot"]
    assert "INTERVAL '5.1 seconds'" in cursor.execute.call_args_list[1].args[0]
    assert "checkpoint" in cursor.execute.call_args_list[2].args[0]
    conn.commit.assert_called_once()


def test_repository_finalization_does_not_overwrite_candidate_and_preserves_cancel(monkeypatch):
    from unittest.mock import MagicMock
    from workers.news_repository import NewsRepository
    import psycopg2.extras
    conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
    cursor.rowcount=1;cursor.fetchone.side_effect=[("CANCEL_REQUESTED",),("RUNNING",1),("CANCELLED",)]
    values=Mock();monkeypatch.setattr(psycopg2.extras,"execute_values",values)
    checkpoint={"sourceId":7,"stage":"CANDIDATES_STORED"}
    result=NewsRepository(conn,item()).finish("COLLECTED",checkpoint,[{"urlHash":"hash","url":"https://example.org"}],[])
    assert result=="CANCELLED" and checkpoint["stage"]=="CANCELLED"
    values.assert_not_called();conn.commit.assert_called_once()


def test_repository_commit_failure_propagates_instead_of_marking_success(monkeypatch):
    from unittest.mock import MagicMock
    from workers.news_repository import NewsRepository
    conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
    cursor.rowcount=1;cursor.fetchone.side_effect=[("RUNNING",),("RUNNING",1),("SUCCESS",)]
    conn.commit.side_effect=RuntimeError("commit unknown")
    with pytest.raises(RuntimeError,match="commit unknown"):
        NewsRepository(conn,item()).finish("COLLECTED",{})


def test_stale_worker_cannot_publish_after_item_is_reclaimed(monkeypatch):
    from unittest.mock import MagicMock
    from workers.news_repository import NewsRepository
    import psycopg2.extras
    conn=MagicMock();cursor=conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect=[("RUNNING",),("RUNNING",2)]
    writes=Mock();monkeypatch.setattr(psycopg2.extras,"execute_values",writes)
    with pytest.raises(RuntimeError,match="OWNERSHIP_LOST"):
        NewsRepository(conn,item()).finish("COLLECTED",{},[{"urlHash":"hash","url":"https://example.org"}])
    writes.assert_not_called();conn.commit.assert_not_called()


def test_selected_rss_scope_reaches_stored_observation(storage):
    i=item();query='("nvidia" OR "NVDA stock")';key=hashlib.sha256(query.encode()).hexdigest()[:16]
    i.item_key=f"RSS:{key}:2016-12-30"
    i.parameters.update(contract="news-v2",queries={key:query},targets=[dict(assetId=42,ticker="NVDA",name="NVIDIA CORP",exchange="NASDAQ",organizationNames=["nvidia"],queryKey=key)])
    repo=Repository()
    assert collect(i,repo,storage,client(),wait=lambda *_:None)=="COLLECTED"
    metadata=repo.finishes[0][2][0]["metadata"]
    assert metadata["targetMatches"][0]["assetId"]==42 and not metadata["tickerIdentityVerified"]
    assert metadata["collectionContractHash"]==repo.checkpoint["contractHash"]
    assert repo.checkpoint["stats"]["targets"]==1


def test_selected_gkg_excludes_other_companies_without_false_no_news_claim(storage):
    i=item("GDELT");i.parameters.update(contract="news-v2",targets=[dict(assetId=43,ticker="IBM",name="International Business Machines Corp.",exchange="NYSE",organizationNames=["international business machines"])])
    repo=Repository()
    assert collect(i,repo,storage,client(gkg()),wait=lambda *_:None)=="COLLECTED"
    assert not repo.finishes[0][1] and repo.checkpoint["stats"]["excludedByTargetMetadata"]==1
    assert repo.checkpoint["rawSource"] and "NOT_ALL_NEWS" in repo.checkpoint["stats"]["coverage"]
