import gzip,hashlib
from copy import deepcopy
import pytest
from tests.test_precision_evidence682 import precision_case
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.precision_series684 import BASIS,POLICY,series
from workers.prepare_precision684 import bindings

def fixture(tmp_path,dates=False):
 scope,e,_=precision_case(tmp_path);r=e['review'];r['basis']=BASIS;nodes=[]
 for i,values in enumerate([['105.499','89.751'],['105','90']]):
  labels=['June 30, 2023','June 30, 2022'] if dates else ['2023','2022']
  body=('<table><tr><td>Years ended (in thousands)</td></tr><tr>'+''.join('<td>'+x+'</td>' for x in labels)+'</tr><tr><td>Total revenue</td>'+''.join('<td>'+x+'</td>' for x in values)+'</tr></table>').encode();sha=hashlib.sha256(body).hexdigest()
  with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
  e['documents'][i]['sha256']=sha;p=Tables();p.feed(body.decode());nodes.append({'url':scope[i]['source_url'],'sha256':sha,'filingDate':scope[i]['filing_date'],'tableHash':digest(p.tables[0]),'periodRow':1,'valueRow':2,'periodMode':'DATE' if dates else 'YEAR','unitScale':1000,'decimalPlaces':3 if i==0 else 0})
 r['precisionSeriesEvidence']={'targetPeriodKey':r['end'] if dates else '2023','nodes':nodes};return scope,e

@pytest.mark.parametrize('dates',[False,True])
def test_multi_period_published_precision_is_separate_from_error_correction(tmp_path,dates):
 scope,e=fixture(tmp_path,dates);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
 assert a['proof']['policy']==POLICY and len(a['proof']['overlappingPeriods'][0])==2 and scope==before
 assert 'Not proof of unchanged economics' in a['proof']['meaning']

@pytest.mark.parametrize('case',['single_node','duplicate_url','wrong_raw','wrong_target','reverse_dates','future_date','wrong_table','wrong_scale','wrong_decimals','bad_scope','bad_filing_metadata'])
def test_inconsistent_precision_evidence_is_rejected(tmp_path,case):
 scope,e=fixture(tmp_path);r=e['review'];v=r['precisionSeriesEvidence'];nodes=v['nodes']
 if case=='single_node':v['nodes']=nodes[:1]
 if case=='duplicate_url':nodes[1]['url']=nodes[0]['url']
 if case=='wrong_raw':r['originalValue']='105900'
 if case=='wrong_target':v['targetPeriodKey']='2022'
 if case=='reverse_dates':nodes[1]['filingDate']='2022-01-01'
 if case=='future_date':nodes[1]['filingDate']='2030-01-01'
 if case=='wrong_table':nodes[0]['tableHash']='a'*64
 if case=='wrong_scale':nodes[0]['unitScale']=1000000
 if case=='wrong_decimals':nodes[0]['decimalPlaces']=0
 if case=='bad_scope':scope.append(dict(scope[1],source_fact_hash='c'*64))
 if case=='bad_filing_metadata':nodes[0]['filingDate']='2023-08-02'
 with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)

def test_candidate_binding_is_review_only_and_preserves_date_keys(tmp_path):
 _,e=fixture(tmp_path,True);v=e['review']['precisionSeriesEvidence']['nodes'][0]
 with gzip.open(tmp_path/'documents'/(v['sha256']+'.html.gz'),'rb') as f:body=f.read()
 result=bindings(body,{k:v[k] for k in ['url','sha256','filingDate']})
 assert result and result[0]['binding']['periodMode']=='DATE' and result[0]['approvalGenerated'] is False and result[0]['requiresHumanReview']
 assert '2023-06-30' in result[0]['values']

def test_neighbor_year_conflict_rejected_even_if_target_rounds_correctly(tmp_path):
 scope,e=fixture(tmp_path);v=e['review']['precisionSeriesEvidence']['nodes'][1]
 with gzip.open(tmp_path/'documents'/(v['sha256']+'.html.gz'),'rb') as f:body=f.read().replace(b'<td>90</td>',b'<td>95</td>')
 sha=hashlib.sha256(body).hexdigest();p=Tables();p.feed(body.decode())
 with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
 e['documents'][1]['sha256']=sha;v.update(sha256=sha,tableHash=digest(p.tables[0]))
 with pytest.raises(ValueError,match='COMPARATIVE_VALUE_CONFLICT'):validate_entry(scope,e,tmp_path)

def test_malformed_issuer_date_is_skipped_without_guessing(tmp_path):
 body=b'<table><tr><td>Years ended Decmber 10 2015</td><td>December 10 2014</td></tr><tr><td>Total revenue</td><td>100</td><td>90</td></tr></table>'
 assert bindings(body,{'url':'https://www.sec.gov/x','sha256':'a'*64,'filingDate':'2016-01-01'})==[]

def test_full_date_keys_do_not_collapse_two_distinct_dates_in_one_calendar_year(tmp_path):
 scope,e=fixture(tmp_path,True)
 for i,v in enumerate(e['review']['precisionSeriesEvidence']['nodes']):
  with gzip.open(tmp_path/'documents'/(v['sha256']+'.html.gz'),'rb') as f:body=f.read().replace(b'June 30, 2022',b'January 1, 2023')
  sha=hashlib.sha256(body).hexdigest();p=Tables();p.feed(body.decode())
  with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
  e['documents'][i]['sha256']=sha;v.update(sha256=sha,tableHash=digest(p.tables[0]))
 a=validate_entry(scope,e,tmp_path);assert a['proof']['overlappingPeriods']==[['2023-01-01','2023-06-30']]

def test_intermediate_report_requires_two_shared_periods_on_every_edge(tmp_path):
 scope,e=fixture(tmp_path);r=e['review'];nodes=r['precisionSeriesEvidence']['nodes'];latest=nodes[-1]
 def make(url,filed,years,values):
  body=('<table><tr><td>Fiscal years ended (in thousands)</td></tr><tr>'+''.join('<td>'+str(y)+'</td>' for y in years)+'</tr><tr><td>Total revenue</td>'+''.join('<td>'+str(v)+'</td>' for v in values)+'</tr></table>').encode();sha=hashlib.sha256(body).hexdigest();p=Tables();p.feed(body.decode())
  with gzip.open(tmp_path/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
  return {'url':url,'sha256':sha,'filingDate':filed,'tableHash':digest(p.tables[0]),'periodRow':1,'valueRow':2,'periodMode':'YEAR','unitScale':1000,'decimalPlaces':0}
 midurl=latest['url'].replace('125000001','124000001');mid=make(midurl,'2024-08-01',[2024,2023,2022],[200,105,90]);last=make(latest['url'],latest['filingDate'],[2025,2024,2023],[300,200,105])
 e['documents'][1]['sha256']=last['sha256'];e['documents'].append({'url':midurl,'sha256':mid['sha256']});r['officialDocuments'].append(midurl);r['precisionSeriesEvidence']['nodes']=[nodes[0],mid,last]
 a=validate_entry(scope,e,tmp_path);assert [len(k) for k in a['proof']['overlappingPeriods']]==[2,2]
