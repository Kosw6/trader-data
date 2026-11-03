import gzip,hashlib,json
from copy import deepcopy
import pytest
import pipeline.ford_total734 as m
from pipeline.financial_history674 import raw_index
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

TITLE='FORD MOTOR COMPANY AND SUBSIDIARIES CONSOLIDATED INCOME STATEMENT (in millions, except per share amounts)'
AUTO=[[139369,126567,128168],[135782,139369,126567],[140566,135782,139369]]
FIN=[[7548,6992,7437],[8295,7548,6992],[8992,8295,7548]]
def rows(i):
    return [['For the years ended December 31,'],[str(y) for y in range(2013+i,2010+i,-1)],['Revenues'],['Automotive',*sum((['$',format(v,',')] for v in AUTO[i]),[])],['Financial Services',*[format(v,',') for v in FIN[i]]],['Total revenues',*[format(a+b,',') for a,b in zip(AUTO[i],FIN[i])]],['Costs and expenses'],['Automotive','1','1','1'],['Financial Services','1','1','1'],['Total costs and expenses','2','2','2'],['Net income','1','1','1']]
def html(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'

def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];payload=dict(cik=37996,facts={'us-gaap':{tag:dict(units={'USD':[]}) for tag in ['SalesRevenueNet','Revenues']}})
    for i in range(3):
        report=2013+i;body=('<p>'+TITLE+'</p>'+html(rows(i))).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());acc=f'0000037996-{report+1-2000:02d}-000010';filed=str(report+1)+'-02-18';url='https://www.sec.gov/Archives/edgar/data/37996/'+acc.replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filed))
        for tag,val in [('SalesRevenueNet',139369000000),('Revenues',146917000000)]:
            obs=dict(start='2013-01-01',end='2013-12-31',val=val,accn=acc,filed=filed,form='10-K',fy=report,fp='FY');payload['facts']['us-gaap'][tag]['units']['USD'].append(obs);scope.append(dict(company_id=145,metric_id=22,metric_code='REVENUE',cik='0000037996',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(val),accession_number=acc,filing_date=filed,form_type='10-K',source_url=url,report_date=str(report)+'-12-31'))
    index=raw_index(payload,'0000037996')
    for row in scope:row['source_fact_hash']=next(h for h,v in index.items() if v['accession']==row['accession_number'] and v['tag']==row['source_tag'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));latest=scope[-1]
    r=dict(companyId=145,metricId=22,metric='REVENUE',chosenTag='Revenues',basis=m.BASIS,start='2013-01-01',end='2013-12-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value=latest['value'],originalValue=scope[0]['value'],filingDate=latest['filing_date'],accessions=[latest['accession_number']],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],consolidatedTotalEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000037996.json',sha256=sha),reviewExplanation='Automotive is a component; explicit consolidated total includes Financial Services. Earlier comparative basis changes and EPS are not approved.')
    return scope,dict(cik='0000037996',review=r,documents=docs)

def rewrite(root,e,i,old,new):
    n=e['review']['consolidatedTotalEvidence']['nodes'][i]['statement'];body=gzip.decompress((root/'documents'/(n['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());n.update(sha256=sha,tableHash=digest(p.tables[0]));e['documents'][i]['sha256']=sha

def test_automotive_plus_financial_services_is_consolidated_revenue_not_growth(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['financialServicesComponentUSD']=='7548000000' and not a['proof']['restatementClaimed'] and before==scope and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']

@pytest.mark.parametrize('kind',['company','metric','metric_id','tag','earlier_year','scope_hash','value','original_value','raw_value','raw_period','raw_accession','raw_filed','unit','dimensions','taxonomy','latest_hash','latest_accession','latest_date','report_date','raw_url','missing_middle','reverse','non_sec'])
def test_unreviewed_years_metrics_and_changed_original_claims_are_blocked(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='DILUTED_EPS'
    if kind=='metric_id':r['metricId']=28
    if kind=='tag':r['chosenTag']='SalesRevenueNet'
    if kind=='earlier_year':r.update(start='2012-01-01',end='2012-12-31')
    if kind=='value':r['value']='139369000000'
    if kind=='original_value':r['originalValue']='146917000000'
    if kind=='raw_value':scope[0]['value']='139370000000'
    if kind=='raw_period':scope[0]['period_start']='2013-01-02'
    if kind=='raw_accession':scope[0]['accession_number']='0000037996-14-000011'
    if kind=='raw_filed':scope[0]['filing_date']='2014-02-19'
    if kind=='unit':scope[0]['unit']='EUR'
    if kind=='dimensions':scope[0]['dimensions']={'segment':'Auto'}
    if kind=='taxonomy':scope[0]['taxonomy']='custom'
    if kind=='latest_hash':r['sourceFactHashes']=['f'*64]
    if kind=='latest_accession':r['accessions']=['0000037996-16-000011']
    if kind=='latest_date':r['filingDate']='2016-02-19'
    if kind=='report_date':scope[0]['report_date']='2013-12-30'
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/source'
    if kind=='missing_middle':r['consolidatedTotalEvidence']['nodes'].pop(1)
    if kind=='reverse':r['consolidatedTotalEvidence']['nodes'].reverse()
    if kind=='non_sec':scope[-1]['source_url']='https://example.com/source'
    r['sourceScopeHash']=digest(scope) if kind!='scope_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('i,old,new',[(0,b'Automotive',b'Other income'),(0,b'Revenues</td>',b'Expenses</td>'),(0,b'Financial Services',b'Finance profit'),(0,b'146,917',b'146,918'),(1,b'6,992',b'6,993'),(2,b'135,782',b'135,783'),(0,b'in millions',b'in thousands'),(0,b'For the years ended December 31,',b'For the quarters ended December 31,'),(0,b'CONSOLIDATED INCOME STATEMENT',b'SECTOR INCOME STATEMENT'),(0,b'Total revenues',b'Automotive subtotal'),(2,b'Net income',b'Other gains')])
def test_all_three_full_primary_equations_and_untargeted_comparatives_are_required(i,old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,i,old,new)
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)
