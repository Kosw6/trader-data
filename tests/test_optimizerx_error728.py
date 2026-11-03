import gzip,hashlib,json
from copy import deepcopy
import pytest
import pipeline.optimizerx_error728 as m
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry,entry_scope_query

INTRO='NOTE 18 - RESTATEMENT '+' '.join(m.CLAUSES)+' The restated Consolidated Balance Sheet, Consolidated Statement of Operations and Consolidated Statement of Cash Flows for the year ended December 31, 2012 is as follows:'

def primary(i):
    if i==0:
        header=[['','','For the','','For the'],['','','year ended','','year ended'],['','','December 31,','','December 31,'],['','','2012','','2011']]
        return header+[['TOTAL REVENUE','2,048,699','1,111,520'],['Operating expenses (See note 17)','2,413,044','2,565,497'],['LOSS FROM OPERATIONS','(364,345)','(1,453,977)'],['TOTAL OTHER INCOME (EXPENSE)','369','(666,785)'],['LOSS BEFORE PROVISION FOR INCOME TAXES','(363,976)','(2,120,762)'],['NET LOSS','$','(363,976)','$','(2,120,762)']]
    header=[['','','','','For the'],['','','For the','','year ended'],['','','year ended','','December 31,'],['','','December 31,','','2012'],['','','2013','','(restated)']]
    return header+[['NET REVENUE','4,957,016','1,989,086'],['REVENUE SHARE EXPENSE','1,767,425','115,360'],['GROSS MARGIN','3,189,591','1,873,726'],['Operating expenses (See Note 17)','2,973,990','2,328,648'],['INCOME (LOSS) FROM OPERATIONS','215,601','(454,922)'],['TOTAL OTHER INCOME (EXPENSE)','246','369'],['INCOME (LOSS) BEFORE PROVISION FOR INCOME TAXES','215,847','(454,553)'],['NET INCOME (LOSS)','$','215,847','$','(454,553)']]

def correction():
    return [['Year ended December 31, 2012','','','',''],['Financial Statement','','Line Item','','','Corrected','','','','Previously Stated',''],['Balance sheet','','Revenue share payable','','$','90,577','','','$','0',''],['Income statement','','Revenue share expense','','$','115,360','','','$','24,783',''],['Income statement','','Loss from operations','','$','(454,922',')','','$','(364,345',')'],['Income statement','','Loss before provision for income taxes','','$','(454,553',')','','$','(363,976',')'],['Income statement','','Net loss','','$','(454,553',')','','$','(363,976',')'],['Statement of cash flows','','Net loss','','$','(454,553',')','','$','(363,976',')'],['Statement of cash flows','','Increase in revenue share payable','','$','90,577','','','$','0','']]

def html(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'

def chain(root,monkeypatch,metric='NET_INCOME'):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];bridge=None;payload=dict(cik=1448431,facts={'us-gaap':{}})
    for name,tag in [('NET_INCOME','NetIncomeLoss'),('OPERATING_INCOME','OperatingIncomeLoss')]:payload['facts']['us-gaap'][tag]=dict(units={'USD':[]})
    accs=['0001255294-13-000182','0001255294-14-000254'];filed=['2013-03-12','2014-03-31']
    for i in range(2):
        title='OPTIMIZERx CORPORATION CONSOLIDATED STATEMENTS OF OPERATIONS FOR THE YEARS ENDED DECEMBER 31, '+('2012 AND 2011' if i==0 else '2013 AND 2012')
        body=('<p>'+title+'</p>'+html(primary(i))+('<p>'+INTRO+'</p>'+html(correction()) if i else '')).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        url='https://www.sec.gov/Archives/edgar/data/1448431/'+accs[i].replace('-','')+'/mainbody.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filed[i]))
        if i:bridge=dict(**docs[-1],tableHash=digest(p.tables[1]))
        for name,tag in [('NET_INCOME','NetIncomeLoss'),('OPERATING_INCOME','OperatingIncomeLoss')]:
            value=([-363976,-454553] if name=='NET_INCOME' else [-364345,-454922])[i];obs=dict(start='2012-01-01',end='2012-12-31',val=value,accn=accs[i],filed=filed[i],form='10-K',fy=2012+i,fp='FY');payload['facts']['us-gaap'][tag]['units']['USD'].append(obs)
            if name==metric:scope.append(dict(company_id=2470,metric_id=m.METRICS[metric][0],metric_code=metric,cik='0001448431',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(value),accession_number=accs[i],filing_date=filed[i],form_type='10-K',source_url=url,report_date=str(2012+i)+'-12-31'))
    idx=raw_index(payload,'0001448431')
    for row in scope:row['source_fact_hash']=next(h for h,v in idx.items() if v['accession']==row['accession_number'] and v['tag']==row['source_tag'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body))
    r=dict(companyId=2470,metricId=m.METRICS[metric][0],metric=metric,chosenTag=m.METRICS[metric][1],basis=m.BASIS,start='2012-01-01',end='2012-12-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value=scope[-1]['value'],originalValue=scope[0]['value'],filingDate=filed[-1],accessions=[accs[-1]],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in docs],expenseErrorEvidence=dict(nodes=nodes,correction=bridge),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0001448431.json',sha256=sha),reviewExplanation='Only two named loss metrics, with complete Note 18 payable/expense/loss/cash-flow equations and literal primary values.')
    return scope,dict(cik='0001448431',review=r,documents=docs)

def rewrite(root,e,i,old,new):
    node=e['review']['expenseErrorEvidence']['nodes'][i]['statement'];body=gzip.decompress((root/'documents'/(node['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());node['sha256']=sha;node['tableHash']=digest(p.tables[0]);e['documents'][i]['sha256']=sha
    if i:e['review']['expenseErrorEvidence']['correction'].update(sha256=sha,tableHash=digest(p.tables[1]))

@pytest.mark.parametrize('metric',['NET_INCOME','OPERATING_INCOME'])
def test_complete_correction_preserves_raws_and_binds_both_loss_metrics(metric,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch,metric);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['deltaUSD']=='90577' and a['proof']['policy']==m.POLICY and scope==before and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']
    sql,params=entry_scope_query(2470,e['review']);assert "m.metric_code='"+metric+"'" in sql and "f.unit='USD'" in sql and params==(2470,'2012-01-01','2012-12-31')

@pytest.mark.parametrize('kind',['company','metric','metric_id','tag','period','scope_hash','chosen_value','original_value','raw_value','raw_period','raw_filed','raw_accession','unit','taxonomy','dimensions','latest_hash','latest_accession','latest_date','report_date','missing_middle','reverse','non_sec','raw_url'])
def test_unregistered_metrics_dates_or_changed_original_claims_block(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=2471
    if kind=='metric':r['metric']='REVENUE'
    if kind=='metric_id':r['metricId']=24
    if kind=='tag':r['chosenTag']='ProfitLoss'
    if kind=='period':r['end']='2011-12-31'
    if kind=='chosen_value':r['value']='-454554'
    if kind=='original_value':r['originalValue']='-363977'
    if kind=='raw_value':scope[0]['value']='-363977'
    if kind=='raw_period':scope[0]['period_start']='2012-01-02'
    if kind=='raw_filed':scope[0]['filing_date']='2013-03-13'
    if kind=='raw_accession':scope[0]['accession_number']='0001255294-13-000183'
    if kind=='unit':scope[0]['unit']='USD/shares'
    if kind=='taxonomy':scope[0]['taxonomy']='custom'
    if kind=='dimensions':scope[0]['dimensions']={'segment':'A'}
    if kind=='latest_hash':r['sourceFactHashes']=['f'*64]
    if kind=='latest_accession':r['accessions']=['0001255294-14-000255']
    if kind=='latest_date':r['filingDate']='2014-04-01'
    if kind=='report_date':scope[0]['report_date']='2012-12-30'
    if kind=='missing_middle':r['expenseErrorEvidence']['nodes'].pop()
    if kind=='reverse':r['expenseErrorEvidence']['nodes'].reverse()
    if kind=='non_sec':scope[0]['source_url']='https://example.com/source'
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/raw'
    r['sourceScopeHash']=digest(scope) if kind!='scope_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('old,new',[(b'90,577',b'90,578'),(b'24,783',b'24,784'),(b'(364,345',b'(364,346'),(b'(454,553',b'(454,554'),(b'Corrected',b'Previously Stated'),(b'Year ended December 31, 2012',b'Year ended December 31, 2011'),(b'revenue share expenses.',b'interest expenses.'),(b'will accrue',b'will not accrue'),(b'Note 17',b'Note 18'),(b'2,328,648',b'2,328,649'),(b'1,989,086',b'1,989,087'),(b'2013 AND 2012',b'2014 AND 2012'),(b'(restated)',b'(unaudited)'),(b'Statement of cash flows',b'Income statement'),(b'Revenue share payable',b'Other payable')])
def test_full_primary_and_every_corrected_cash_flow_line_are_required(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,1,old,new)
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('cells',[['(123',''],['123',')'],['-'],[''],['12.3'],['123%'],['-123'],['1,234,56']])
def test_missing_or_malformed_amounts_are_not_zero(cells):
    if cells==['']:assert m.amounts(cells)==[]
    else:
        with pytest.raises(ValueError):m.amounts(cells)

def test_split_negative_html_cells_preserve_sign():
    assert m.amounts(['$','(454,553',')','$','-0-'])==[-454553,0]
