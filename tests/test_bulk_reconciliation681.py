import gzip,hashlib,json
from decimal import Decimal
import pytest
from workers.bulk_reconciliation681 import load_plan,read_document,screen,run,cache_catalog
from workers.bulk_reconciliation681 import go_batches

def test_go_batches_are_bounded_explicit_and_stable():
 items=[{**item(),'companyId':i} for i in range(1,165)]
 a=go_batches(items,'a'*64);assert a==go_batches(list(reversed(items)),'a'*64)
 assert [len(b['companyIds']) for b in a]==[50,50,50,14]
 assert len({cid for b in a for cid in b['companyIds']})==164
 assert len({b['requestKey'] for b in a})==4
 assert all(not b['allCompanies'] and b['annualOnly'] and b['mode']=='reviewedReconciliation' for b in a)

def item():
 return {'companyId':1,'cik':'0000000001','start':'2017-01-01','end':'2017-12-31','original':'100000000','chosen':'100010000','adjustment':'10000','urls':['https://www.sec.gov/Archives/edgar/data/1/000000000118000001/a.htm']}

def plan(tmp_path,items):
 p=tmp_path/'plan.json';p.write_text(json.dumps({'candidates':items}));return p

@pytest.mark.parametrize('change',[{'chosen':'NaN'},{'adjustment':'11'},{'urls':['https://evil.test/a']},{'urls':[]},{'urls':['https://www.sec.gov/Archives/edgar/data/2/000000000118000001/a.htm']}])
def test_invalid_scope_never_fetches_or_approves(tmp_path,change):
 with pytest.raises(ValueError):load_plan(plan(tmp_path,[{**item(),**change}]))

def test_duplicate_scope_rejected(tmp_path):
 with pytest.raises(ValueError,match='DUPLICATE'):load_plan(plan(tmp_path,[item(),item()]))

def test_hash_changed_cache_rejected(tmp_path):
 p=tmp_path/'doc.gz'
 with gzip.open(p,'wb') as f:f.write(b'changed')
 with pytest.raises(ValueError,match='HASH'):read_document(p,'a'*64)

def test_rounding_is_only_a_review_hint():
 i=item();i.update(original='10424858000',chosen='10425000000',adjustment='142000')
 r=screen(i,[({'url':i['urls'][0],'sha256':'a'*64},b'<html>ASC 606 discontinued operations</html>')])
 assert r['precisionCandidates'] and r['approvalGenerated'] is False and r['requiresHumanReview']
 assert r['hintsAreNotCauseConclusions'] and r['status']=='PRECISION_COMPATIBLE_REQUIRES_DOCUMENT_REVIEW'

def test_cache_only_run_records_incomplete_not_successful_approval(tmp_path):
 r=run(plan(tmp_path,[item()]),tmp_path/'out',[],False)
 assert r['classification']=={'DOCUMENTS_INCOMPLETE':1} and r['databaseWrites']==r['approvalsGenerated']==0

def cache(tmp_path,body):
 root=tmp_path/'cache';root.mkdir();(root/'documents').mkdir();sha=hashlib.sha256(body).hexdigest()
 with gzip.open(root/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
 (root/'report.json').write_text(json.dumps({'documents':[{'url':item()['urls'][0],'sha256':sha}]}));return root

def test_cache_only_reproducible_no_approval(tmp_path):
 root=cache(tmp_path,b'<html>No accounting conclusion</html>');p=plan(tmp_path,[item()]);r=run(p,tmp_path/'out',[root])
 assert r['cached']==1 and r['downloaded']==0 and r['classification']=={'NO_EXACT_RECONCILIATION_FOUND':1}
 assert r['inputSha256']==hashlib.sha256(p.read_bytes()).hexdigest() and r['approvalsGenerated']==0
 with pytest.raises(FileExistsError):run(p,tmp_path/'out',[root])

def test_budget_stop_preserves_receipt_and_does_not_approve(tmp_path):
 root=cache(tmp_path,b'12345');r=run(plan(tmp_path,[item()]),tmp_path/'out',[root],max_bytes=4)
 assert r['stopReason']=='TOTAL_BYTES_BUDGET' and r['status']=='SCREENING_STOPPED'
 assert r['classification']=={'DOCUMENTS_INCOMPLETE':1} and r['databaseWrites']==0

def test_conflicting_cached_document_is_not_silently_selected(tmp_path):
 root=cache(tmp_path,b'a');other=tmp_path/'other';other.mkdir()
 (other/'report.json').write_text(json.dumps({'documents':[{'url':item()['urls'][0],'sha256':'b'*64}]}))
 with pytest.raises(ValueError,match='CONFLICT'):cache_catalog([root,other])

@pytest.mark.parametrize('status',[403,429,302,500])
def test_common_fetch_failures_stop_without_retry_or_approval(tmp_path,monkeypatch,status):
 import requests
 from unittest.mock import MagicMock
 session=MagicMock();response=session.get.return_value.__enter__.return_value;response.status_code=status
 monkeypatch.setattr(requests,'Session',lambda:session)
 r=run(plan(tmp_path,[item()]),tmp_path/'out',[],True)
 assert r['status']=='SCREENING_STOPPED' and r['stopReason']=='SEC_FETCH_STOP_'+str(status)
 assert r['approvalsGenerated']==r['databaseWrites']==0 and session.get.call_count==1
 assert session.get.call_args.kwargs['allow_redirects'] is False
 session.close.assert_called_once()

def test_404_is_unresolved_document_and_does_not_abort_next_source(tmp_path,monkeypatch):
 import requests
 from unittest.mock import MagicMock
 session=MagicMock();response=session.get.return_value.__enter__.return_value;response.status_code=404
 monkeypatch.setattr(requests,'Session',lambda:session)
 r=run(plan(tmp_path,[item()]),tmp_path/'out',[],True)
 assert r['stopReason'] is None and r['classification']=={'DOCUMENTS_INCOMPLETE':1}
 assert r['approvalsGenerated']==0


def test_expired_deadline_never_fetches_or_screens_as_complete(tmp_path,monkeypatch):
 import requests
 from unittest.mock import MagicMock
 session=MagicMock();monkeypatch.setattr(requests,'Session',lambda:session)
 r=run(plan(tmp_path,[item()]),tmp_path/'out',[],True,deadline_utc='2000-01-01T00:00:00+00:00')
 assert r['stopReason']=='DEADLINE_REACHED' and r['classification']=={'NOT_SCREENED_DEADLINE':1}
 session.get.assert_not_called();session.close.assert_called_once()
 assert r['databaseWrites']==r['approvalsGenerated']==0


@pytest.mark.parametrize('budget,reason',[(1,'NEW_DOCUMENT_COUNT_BUDGET'),(10,'NEW_DOCUMENT_BYTES_BUDGET')])
def test_new_fetch_budget_stops_and_keeps_earlier_receipts(tmp_path,monkeypatch,budget,reason):
 import requests
 from unittest.mock import MagicMock
 session=MagicMock();response=session.get.return_value.__enter__.return_value;response.status_code=200
 response.iter_content.return_value=[b'<html>test</html>']
 monkeypatch.setattr(requests,'Session',lambda:session)
 i=item();i['urls']+=[i['urls'][0].replace('a.htm','b.htm')]
 r=run(plan(tmp_path,[i]),tmp_path/'out',[],True,max_new_documents=budget,max_new_bytes=budget if budget==10 else 1000)
 assert r['stopReason']==reason and r['approvalsGenerated']==0
 assert session.get.call_count==1


def test_deadline_must_have_timezone(tmp_path):
 with pytest.raises(ValueError,match='TIMEZONE'):run(plan(tmp_path,[item()]),tmp_path/'out',[],deadline_utc='2026-10-31T12:00:00')
