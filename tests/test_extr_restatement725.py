import gzip,hashlib,json
from copy import deepcopy
from decimal import Decimal
import pytest
import pipeline.extr_restatement725 as m
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.original_sec_scope725 import verify
from pipeline.financial_history674 import raw_index
from pipeline.financial_reconciliation677 import validate_entry


def adjusted(year):
    vals=([['451,459','8,966','460,425'],['146,659','—','146,659'],['598,118','8,966','607,084']] if year==2017 else [['395,464','(8,555)','386,909'],['132,925','—','132,925'],['528,389','(8,555)','519,834']])
    return [[f'Year Ended June 30, {year}'],['As Reported','Adjustment','As Adjusted'],['Net revenues']]+[[label,*v] for label,v in zip(['Product','Service','Total net revenues'],vals)]


def table(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+v+'</td>' for v in row)+'</tr>' for row in rows)+'</table>'


def adoption(monkeypatch,change=None):
    body=('<p>'+m.CAUSE+'</p><p>'+m.DECLARATION+'</p>'+table(adjusted(2017))+table(adjusted(2016))).encode()
    if change:body=body.replace(change[0].encode(),change[1].encode())
    p=Tables();p.feed(body.decode());monkeypatch.setattr(m,'SOURCE_SHA256',hashlib.sha256(body).hexdigest());monkeypatch.setattr(m,'TABLE_HASHES',[digest(rows) for rows in p.tables if rows and rows[0][0].startswith('Year Ended')])
    return body


def test_explicit_plural_units_apply_only_to_two_consecutive_named_year_tables(monkeypatch):
    r=m.adjustment_source(adoption(monkeypatch));assert r['2016-06-30']['adjustment']=='-8555000' and r['2017-06-30']['adjustment']=='8966000'
    assert all(v['unitScale']=='1000' and v['unitSource']=='EXPLICIT_PLURAL_DECLARATION_FOR_TWO_CONSECUTIVE_TABLES' for v in r.values())


@pytest.mark.parametrize('before,after',[
    ('in thousands, except per share amounts','in millions, except per share amounts'),
    ('the following tables present','the following table presents'),
    ('2017 and 2016','2017 and 2015'),('using the full retrospective method','using a prospective method'),
    ('Year Ended June 30, 2016','Quarter Ended June 30, 2016'),('As Adjusted','As Reported'),
    ('528,389','528,390'),('132,925','132,926'),('(8,555)','(8,554)'),
    ('Total net revenues','Subtotal revenues'),('Service','Product'),('8,966','8,966%'),
    ('—',''),
])
def test_changed_source_context_period_units_components_or_roles_fail_even_after_rehash(monkeypatch,before,after):
    with pytest.raises(ValueError):m.adjustment_source(adoption(monkeypatch,(before,after)))


def test_intervening_other_table_breaks_plural_scope(monkeypatch):
    d=adoption(monkeypatch);d=d.replace(table(adjusted(2016)).encode(),b'<table><tr><td>Other table</td></tr></table>'+table(adjusted(2016)).encode());monkeypatch.setattr(m,'SOURCE_SHA256',hashlib.sha256(d).hexdigest())
    with pytest.raises(ValueError,match='CONSECUTIVE'):m.adjustment_source(d)


def test_intervening_conflicting_unit_is_not_inherited(monkeypatch):
    d=adoption(monkeypatch);d=d.replace(table(adjusted(2016)).encode(),b'<p>Amounts in millions</p>'+table(adjusted(2016)).encode());monkeypatch.setattr(m,'SOURCE_SHA256',hashlib.sha256(d).hexdigest())
    with pytest.raises(ValueError,match='UNIT_CONFLICT'):m.adjustment_source(d)


def primary():
    return [['Year Ended'],['June 30, 2018','June 30, 2017','June 30, 2016'],['(As adjusted)','(As adjusted)'],['Net revenues:'],['Product','764,455','460,425','386,909'],['Service','218,687','146,659','132,925'],['Total net revenues','983,142','607,084','519,834'],['Cost of revenues:']]


CONTEXT='Extreme Networks, Inc. Consolidated Statements of Operations (In thousands, except per share amounts)'


def test_primary_components_bind_all_exact_date_columns():
    before=primary();copy=deepcopy(before);r=m.primary_vector(before,CONTEXT)
    assert r['2016-06-30']==Decimal('519834000') and before==copy


@pytest.mark.parametrize('kind',['title','unit','annual','date','component','total','recast','metric'])
def test_primary_unrelated_or_changed_lines_do_not_supply_revenue(kind):
    rows=primary();ctx=CONTEXT
    if kind=='title':ctx=ctx.replace('Extreme Networks','Another company')
    if kind=='unit':ctx=ctx.replace('thousands','millions')
    if kind=='annual':rows[0]=['Three Months Ended']
    if kind=='date':rows[1][1]='June 29, 2017'
    if kind=='component':rows[4][1]='764,456'
    if kind=='total':rows[6][1]='983,143'
    if kind=='recast':rows[2]=['(As adjusted)']
    if kind=='metric':rows[6][0]='Operating income'
    with pytest.raises(ValueError):m.primary_vector(rows,ctx)


def raw_fixture(root):
    (root/'raw-sec').mkdir();obs=dict(start='2015-07-01',end='2016-06-30',val=100,accn='0000000001-17-000001',filed='2017-08-01',form='10-K',fy=2016,fp='FY')
    payload=dict(cik=1,facts={'us-gaap':{'Revenues':{'units':{'USD':[obs]}}}});body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));h=next(iter(raw_index(payload,'1')))
    row=dict(company_id=1,cik='1',metric_code='REVENUE',source_tag='Revenues',taxonomy='us-gaap',dimensions=None,unit='USD',period_start=obs['start'],period_end=obs['end'],value='100',accession_number=obs['accn'],filing_date=obs['filed'],form_type='10-K',source_fact_hash=h)
    evidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json',sha256=sha)
    return [row],evidence,payload


def rawcheck(scope,e,root):return verify(scope,e,root,company=1,cik='1',raw_sha256=e['sha256'],metric='REVENUE',tags={'Revenues'})


def test_original_structured_period_is_complete_and_not_calendar_guessed(tmp_path):
    scope,e,p=raw_fixture(tmp_path);r=rawcheck(scope,e,tmp_path);assert r['claims']==1 and r['actualPeriodStartsSourceBound'] and r['allOfficialAnnualObservationsRepresented']


@pytest.mark.parametrize('field,value',[('company_id',2),('cik','2'),('metric_code','NET_INCOME'),('taxonomy','custom'),('dimensions',{'segment':'A'}),('unit','EUR'),('period_start','2015-07-02'),('value','101'),('filing_date','2017-08-02'),('source_fact_hash','f'*64)])
def test_structured_wrong_identity_period_or_value_never_matches(tmp_path,field,value):
    scope,e,p=raw_fixture(tmp_path);scope[0][field]=value
    with pytest.raises(ValueError):rawcheck(scope,e,tmp_path)


def test_unrepresented_later_original_observation_requires_review(tmp_path):
    scope,e,p=raw_fixture(tmp_path);p['facts']['us-gaap']['Revenues']['units']['USD'].append({**p['facts']['us-gaap']['Revenues']['units']['USD'][0],'val':102,'filed':'2018-08-01','accn':'0000000001-18-000001'})
    body=json.dumps(p).encode();sha=hashlib.sha256(body).hexdigest();(tmp_path/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));e['sha256']=sha
    with pytest.raises(ValueError,match='UNREPRESENTED'):rawcheck(scope,e,tmp_path)


def full_chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];documents=[];nodes=[]
    urls=['https://www.sec.gov/Archives/edgar/data/1078271/000156459016024924/extr-10k_20160630.htm','https://www.sec.gov/Archives/edgar/data/1078271/000156459017018633/extr-10k_20170630.htm',m.URL]
    accs=['0001564590-16-024924','0001564590-17-018633','0001564590-18-022453'];dates=['2016-08-29','2017-08-28','2018-08-29']
    payload=dict(cik=1078271,facts={'us-gaap':{}})
    for i,year in enumerate([2016,2017,2018]):
        if i==2:r=primary()
        else:
            product,service,total=([['395,464','418,046','411,761'],['132,925','134,894','107,793'],['528,389','552,940','519,554']] if i==0 else [['451,459','395,464','418,046'],['146,659','132,925','134,894'],['598,118','528,389','552,940']])
            r=[['Year Ended'],[f'June 30, {year-j}' for j in range(3)],['Net revenues:'],['Product',*product],['Service',*service],['Total net revenues',*total],['Cost of revenues:']]
        html='<p>'+CONTEXT+'</p>'+table(r)
        if i==2:html+='<p>'+m.CAUSE+'</p><p>'+m.DECLARATION+'</p>'+table(adjusted(2017))+table(adjusted(2016))
        body=html.encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));parser=Tables();parser.feed(html)
        if i==2:monkeypatch.setattr(m,'SOURCE_SHA256',sha);monkeypatch.setattr(m,'TABLE_HASHES',[digest(v) for v in parser.tables[1:]])
        documents.append(dict(url=urls[i],sha256=sha));nodes.append(dict(statement=dict(**documents[-1],tableHash=digest(parser.tables[0])),filingDate=dates[i]))
        tag='SalesRevenueNet' if i<2 else 'RevenueFromContractWithCustomerExcludingAssessedTax';value=528389000 if i<2 else 519834000
        obs=dict(start='2015-07-01',end='2016-06-30',val=value,accn=accs[i],filed=dates[i],form='10-K',fy=year,fp='FY');payload['facts']['us-gaap'].setdefault(tag,dict(units={'USD':[]}))['units']['USD'].append(obs)
        scope.append(dict(company_id=1565,metric_id=22,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0001078271',period_start=obs['start'],period_end=obs['end'],value=str(value),accession_number=accs[i],filing_date=dates[i],form_type='10-K',source_tag=tag,source_url=urls[i],report_date=f'{year}-06-30',fiscal_year=year))
    index=raw_index(payload,'0001078271')
    for row in scope:row['source_fact_hash']=next(h for h,v in index.items() if v['accession']==row['accession_number'])
    raw=json.dumps(payload).encode();sha=hashlib.sha256(raw).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(raw))
    review=dict(companyId=1565,metricId=22,metric='REVENUE',start='2015-07-01',end='2016-06-30',basis=m.BASIS,chosenTag=scope[-1]['source_tag'],value='519834000',originalValue='528389000',reviewedOn='2026-10-31',filingDate=dates[-1],sourceScopeHash=digest(scope),accessions=[accs[-1]],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=urls,originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0001078271.json',sha256=sha),allPrimaryEvidence=dict(nodes=nodes),reviewExplanation='Explicit plural-unit two-table adoption schedule, exact original periods and all full statements.')
    return scope,dict(cik='0001078271',documents=documents,review=review)


def test_full_approval_replays_all_raw_periods_and_comparatives_without_editing(tmp_path,monkeypatch):
    scope,entry=full_chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,entry,tmp_path)
    assert a['proof']['policy']==m.POLICY and a['proof']['originalRawEvidence']['allOfficialAnnualObservationsRepresented']
    assert a['proof']['allPrimaryComparativeEnds']==[['2015-06-30','2016-06-30'],['2016-06-30','2017-06-30']]
    assert not a['proof']['otherMetricApproval'] and scope==before and a['proof_hash']==validate_entry(scope,entry,tmp_path)['proof_hash']


@pytest.mark.parametrize('kind',['company','metric','period','original_value','selected_value','scope_hash','missing_middle','reversed','raw_value','raw_start','source_date','document_membership'])
def test_full_approval_does_not_accept_partial_or_other_scopes(tmp_path,monkeypatch,kind):
    scope,entry=full_chain(tmp_path,monkeypatch);r=entry['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='period':r['start']='2015-07-02'
    if kind=='original_value':r['originalValue']='528390000'
    if kind=='selected_value':r['value']='519835000'
    if kind=='missing_middle':r['allPrimaryEvidence']['nodes'].pop(1)
    if kind=='reversed':r['allPrimaryEvidence']['nodes'].reverse()
    if kind=='raw_value':scope[1]['value']='528390000'
    if kind=='raw_start':scope[1]['period_start']='2015-07-02'
    if kind=='source_date':r['allPrimaryEvidence']['nodes'][1]['filingDate']='2017-08-29'
    if kind=='document_membership':entry['documents'].pop(1)
    r['sourceScopeHash']=digest(scope) if kind!='scope_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):validate_entry(scope,entry,tmp_path)
