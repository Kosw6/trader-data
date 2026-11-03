import gzip,hashlib,json
from copy import deepcopy
from decimal import Decimal
import pytest
import pipeline.alaska_split727 as m
from pipeline.financial_history674 import raw_index
from pipeline.financial_reconciliation677 import validate_entry,entry_scope_query
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

YEARS=[[2013,2012,2011],[2014,2013,2012],[2015,2014,2013]]
NI=[['508','316','245'],['605','508','316'],['848','605','508']]
EPS=[['7.16','4.40','3.33'],['4.42','3.58','2.20'],['6.56','4.42','3.58']]
SHARES=[['70.939','71.784','73.421'],['136.801','141.878','143.568'],['129.372','136.801','141.878']]
TITLE='ALASKA AIR GROUP, INC. CONSOLIDATED STATEMENTS OF OPERATIONS'

def rows(i):
    return [['Year Ended December 31 (in millions, except per-share amounts)',*[str(y) for y in YEARS[i]]],['Operating Revenues'],['Total Operating Revenues','1','1','1'],['Total Operating Expenses','1','1','1'],['Operating Income','1','1','1'],['Net Income',*NI[i]],['Diluted Earnings Per Share:',*EPS[i]],['Shares used for computation:'],['Diluted',*SHARES[i]]]

def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];nodes=[];docs=[]
    payload=dict(cik=766421,facts={'us-gaap':{'EarningsPerShareDiluted':dict(units={'USD/shares':[]})}})
    accs=['0000766421-14-000010','0000766421-15-000009','0000766421-16-000065'];filed=['2014-02-13','2015-02-11','2016-02-11']
    for i in range(3):
        body=('<p>'+(' '.join(m.CLAUSES) if i==1 else '')+'</p><p>'+TITLE+'</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in rows(i))+'</table>').encode()
        sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        if i==1:monkeypatch.setattr(m,'CAUSE_SHA256',sha)
        url='https://www.sec.gov/Archives/edgar/data/766421/'+accs[i].replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filed[i]))
        obs=dict(start='2013-01-01',end='2013-12-31',val=7.16 if i==0 else 3.58,accn=accs[i],filed=filed[i],form='10-K',fy=2013+i,fp='FY');payload['facts']['us-gaap']['EarningsPerShareDiluted']['units']['USD/shares'].append(obs)
        scope.append(dict(company_id=639,metric_id=28,metric_code='DILUTED_EPS',cik='0000766421',taxonomy='us-gaap',source_tag='EarningsPerShareDiluted',unit='USD/shares',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(obs['val']),accession_number=accs[i],filing_date=filed[i],form_type='10-K',source_url=url,report_date=str(YEARS[i][0])+'-12-31'))
    idx=raw_index(payload,'0000766421')
    for row in scope:row['source_fact_hash']=next(h for h,v in idx.items() if v['accession']==row['accession_number'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body))
    r=dict(companyId=639,metricId=28,metric='DILUTED_EPS',chosenTag='EarningsPerShareDiluted',basis=m.BASIS,start='2013-01-01',end='2013-12-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value='3.58',originalValue='7.16',filingDate=filed[-1],accessions=[accs[-1]],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in docs],splitPrimaryEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000766421.json',sha256=sha),reviewExplanation='Only literal diluted EPS: unchanged net income, explicit split distribution and all original annual observations.')
    return scope,dict(cik='0000766421',review=r,documents=docs)

def rewrite(root,entry,index,old,new,monkeypatch):
    binding=entry['review']['splitPrimaryEvidence']['nodes'][index]['statement'];body=gzip.decompress((root/'documents'/(binding['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());entry['documents'][index]['sha256']=sha;binding['sha256']=sha;binding['tableHash']=digest(p.tables[0])
    if index==1:monkeypatch.setattr(m,'CAUSE_SHA256',sha)

def test_named_split_is_eps_not_total_income_or_share_price(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['source_tag']=='EarningsPerShareDiluted' and a['proof']['split']['ratio']=='2:1' and a['proof']['primaryVectors'][0]['2013-12-31']['netIncome']=='508000000' and scope==before
    assert a['proof_hash']==validate_entry(scope,e,tmp_path)['proof_hash']
    sql,params=entry_scope_query(639,e['review']);assert "m.metric_code='DILUTED_EPS'" in sql and "f.unit='USD/shares'" in sql and params==(639,'2013-01-01','2013-12-31')

@pytest.mark.parametrize('kind',['company','metric_id','metric','tag','period','scope_hash','original_value','chosen_value','raw_start','raw_value','raw_accession','raw_filed','raw_url','unit','taxonomy','dimensions','latest_accession','latest_hash','latest_date','report_date','missing_middle','reverse','non_sec'])
def test_other_periods_metrics_or_incomplete_observations_are_rejected(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric_id':r['metricId']=23
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='tag':r['chosenTag']='EarningsPerShareBasic'
    if kind=='period':r['start']='2012-01-01'
    if kind=='original_value':r['originalValue']='7.17'
    if kind=='chosen_value':r['value']='3.59'
    if kind=='raw_start':scope[0]['period_start']='2013-01-02'
    if kind=='raw_value':scope[0]['value']='7.17'
    if kind=='raw_accession':scope[0]['accession_number']='0000766421-14-000011'
    if kind=='raw_filed':scope[0]['filing_date']='2014-02-14'
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/source'
    if kind=='unit':scope[0]['unit']='USD'
    if kind=='taxonomy':scope[0]['taxonomy']='custom'
    if kind=='dimensions':scope[0]['dimensions']={'segment':'A'}
    if kind=='latest_accession':r['accessions']=['0000766421-16-000066']
    if kind=='latest_hash':r['sourceFactHashes']=['f'*64]
    if kind=='latest_date':r['filingDate']='2016-02-12'
    if kind=='report_date':scope[0]['report_date']='2013-12-30'
    if kind=='missing_middle':r['splitPrimaryEvidence']['nodes'].pop(1)
    if kind=='reverse':r['splitPrimaryEvidence']['nodes'].reverse()
    if kind=='non_sec':scope[0]['source_url']='https://example.com/report'
    r['sourceScopeHash']=digest(scope) if kind!='scope_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):validate_entry(scope,e,tmp_path)

@pytest.mark.parametrize('old,new',[(b'two-for-one stock split',b'three-for-one stock split'),(b'july 9, 2014',b'july 9, 2015'),(b'june 23, 2014',b'june 24, 2014'),(b'has been recast',b'has not been recast'),(b'two-for-one stock split',b'split-scimitar winglets')])
def test_exact_split_cause_and_dates_cannot_be_replaced_by_other_events(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,1,old,new,monkeypatch)
    with pytest.raises(ValueError,match='CLAUSE'):validate_entry(scope,e,tmp_path)

@pytest.mark.parametrize('i,old,new',[(1,b'141.878',b'141.879'),(1,b'143.568',b'143.569'),(1,b'>508<',b'>509<'),(1,b'2.20',b'2.21'),(2,b'3.58',b'3.59'),(2,b'136.801',b'136.802')])
def test_every_comparative_income_eps_and_share_equation_is_required(i,old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,i,old,new,monkeypatch)
    with pytest.raises(ValueError,match='EQUATIONS'):validate_entry(scope,e,tmp_path)

@pytest.mark.parametrize('old,new',[(b'in millions, except per-share amounts',b'in thousands'),(b'Diluted Earnings Per Share:',b'Basic Earnings Per Share:'),(b'Shares used for computation:',b'Authorized shares:'),(b'Net Income',b'Adjusted net income'),(b'2014',b'2016'),(b'3.58',b'3.580')])
def test_primary_unit_hierarchy_dates_and_literal_precision_are_required(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,1,old,new,monkeypatch)
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)

def test_unit_exception_never_scales_eps_to_millions():
    v=m.vector(rows(1),TITLE);assert v['2013-12-31']['eps']==Decimal('3.58') and v['2013-12-31']['dilutedShares']==141878000
