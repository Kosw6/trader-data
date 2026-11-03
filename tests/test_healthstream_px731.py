import gzip,hashlib,json
from copy import deepcopy
import pytest
import pipeline.healthstream_px731 as m
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry

TITLE='HEALTHSTREAM, INC. CONSOLIDATED STATEMENTS OF INCOME (In thousands, except per share data)'
PX=m.PX_CAUSE+' The following table presents the financial results of the PX business (in thousands):'
V={2016:[225974,209002,170690],2017:[247662,225974,209002],2018:[231616,214899,192124],2019:[254112,231616,214899]}
def table(year,px=False):
    years=[2018,2017] if px and year==2019 else list(range(year,year-3,-1))
    values=[3342,32763] if px and year==2019 else [3342,32763,33850] if px else V[year]
    return [['Year Ended December 31,'],[str(y) for y in years],['Revenues, net',*sum((['$',format(v,',')] for v in values),[])],['Operating costs and expenses:'],['Cost of revenues (excluding depreciation and amortization)','1','1','1']]
def html(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'

def chain(root,monkeypatch,year=2016):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];adoption=None;payload=dict(cik=1095565,facts={'us-gaap':{tag:dict(units={'USD':[]}) for tag in ['SalesRevenueNet','RevenueFromContractWithCustomerExcludingAssessedTax']}})
    for report in range(year,year+3):
        body=('<p>'+(' '.join(m.ADOPTION) if report==2018 else '')+'</p><p>'+TITLE+'</p>'+html(table(report))+('<p>'+PX+'</p>'+html(table(report,px=True)) if report>=2018 else '')).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());acc=f'0001095565-{report+1-2000:02d}-000010';filed=str(report+1)+'-02-25';url='https://www.sec.gov/Archives/edgar/data/1095565/'+acc.replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));n=dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filed)
        if report>=2018:n['px']=dict(**docs[-1],tableHash=digest(p.tables[1]))
        if report==2018:adoption=docs[-1];monkeypatch.setattr(m,'ADOPTION_SHA256',sha)
        nodes.append(n);tag='SalesRevenueNet' if report<2018 else 'RevenueFromContractWithCustomerExcludingAssessedTax';value=V[report][report-year]*1000;obs=dict(start=str(year)+'-01-01',end=str(year)+'-12-31',val=value,accn=acc,filed=filed,form='10-K',fy=report,fp='FY');payload['facts']['us-gaap'][tag]['units']['USD'].append(obs)
        scope.append(dict(company_id=1615,metric_id=22,metric_code='REVENUE',cik='0001095565',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(value),accession_number=acc,filing_date=filed,form_type='10-K',source_url=url,report_date=str(report)+'-12-31'))
    index=raw_index(payload,'0001095565')
    for row in scope:row['source_fact_hash']=next(h for h,v in index.items() if v['accession']==row['accession_number'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));latest=scope[-1]
    r=dict(companyId=1615,metricId=22,metric='REVENUE',chosenTag=latest['source_tag'],basis=m.BASIS,start=str(year)+'-01-01',end=str(year)+'-12-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value=latest['value'],originalValue=scope[0]['value'],filingDate=latest['filing_date'],accessions=[latest['accession_number']],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],pxPrimaryEvidence=dict(nodes=nodes,adoption=adoption),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0001095565.json',sha256=sha),reviewExplanation='Explicit all-period PX discontinued presentation; ASC606 modified transition did not restate prior periods.')
    return scope,dict(cik='0001095565',review=r,documents=docs)

def rewrite(root,e,i,old,new,monkeypatch):
    node=e['review']['pxPrimaryEvidence']['nodes'][i];binding=node['statement'];body=gzip.decompress((root/'documents'/(binding['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());e['documents'][i]['sha256']=sha;binding.update(sha256=sha,tableHash=digest(p.tables[0]))
    if 'px' in node:node['px'].update(sha256=sha,tableHash=digest(p.tables[1]))
    if e['review']['pxPrimaryEvidence']['adoption']['url']==binding['url']:
        e['review']['pxPrimaryEvidence']['adoption']['sha256']=sha;monkeypatch.setattr(m,'ADOPTION_SHA256',sha)

@pytest.mark.parametrize('year,removed',[(2016,'33850000'),(2017,'32763000')])
def test_all_original_reports_and_both_comparatives_support_exact_px_bridge(year,removed,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch,year);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['discontinuedRevenueUSD']==removed and a['proof']['revenueStandard']['priorPeriodsNotRestatedForASC606'] and all(len(v)==2 for v in a['proof']['overlappingActualPeriodEnds']) and before==scope and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']

@pytest.mark.parametrize('kind',['company','metric','metric_id','tag','period','source_hash','value','original_value','raw_start','raw_value','raw_accession','raw_date','unit','dimensions','taxonomy','latest_hash','latest_accession','latest_date','report_date','missing_middle','reverse','raw_url','non_sec'])
def test_only_registered_periods_and_unchanged_original_annual_claims_qualify(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='metric_id':r['metricId']=23
    if kind=='tag':r['chosenTag']='Revenues'
    if kind=='period':r['end']='2015-12-31'
    if kind=='value':r['value']='192125000'
    if kind=='original_value':r['originalValue']='225975000'
    if kind=='raw_start':scope[0]['period_start']='2016-01-02'
    if kind=='raw_value':scope[0]['value']='225975000'
    if kind=='raw_accession':scope[0]['accession_number']='0001095565-17-000011'
    if kind=='raw_date':scope[0]['filing_date']='2017-02-26'
    if kind=='unit':scope[0]['unit']='EUR'
    if kind=='dimensions':scope[0]['dimensions']={'segment':'X'}
    if kind=='taxonomy':scope[0]['taxonomy']='custom'
    if kind=='latest_hash':r['sourceFactHashes']=['f'*64]
    if kind=='latest_accession':r['accessions']=['0001095565-19-000011']
    if kind=='latest_date':r['filingDate']='2019-02-26'
    if kind=='report_date':scope[1]['report_date']='2017-12-30'
    if kind=='missing_middle':r['pxPrimaryEvidence']['nodes'].pop(1)
    if kind=='reverse':r['pxPrimaryEvidence']['nodes'].reverse()
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/raw'
    if kind=='non_sec':scope[-1]['source_url']='https://example.com/source'
    r['sourceScopeHash']=digest(scope) if kind!='source_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('old,new',[(b'prior period amounts continue',b'prior period amounts do not continue'),(b'modified retrospective',b'full retrospective'),(b'for all periods presented',b'for the current period only'),(b'px business',b'another business'),(b'33,850',b'33,851'),(b'32,763',b'32,764'),(b'192,124',b'192,125'),(b'in thousands',b'in millions'),(b'Year Ended December 31,',b'Quarter Ended December 31,'),(b'Revenues, net',b'Gross revenues')])
def test_source_cause_standard_unit_period_and_both_px_amounts_are_required(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,2,old,new,monkeypatch)
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)

def test_unrequested_pre_disposal_comparative_must_also_match(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,1,b'209,002',b'209,003',monkeypatch)
    with pytest.raises(ValueError,match='OTHER_COMPARATIVES'):m.approve(scope,e,tmp_path)

def test_post_disposal_comparative_px_note_cannot_drift(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch,2017);rewrite(tmp_path,e,2,b'3,342',b'3,343',monkeypatch)
    with pytest.raises(ValueError,match='PX_COMPARATIVE'):m.approve(scope,e,tmp_path)
