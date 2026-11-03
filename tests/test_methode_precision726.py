import gzip,hashlib,json
from copy import deepcopy
from decimal import Decimal
import pytest
import pipeline.methode_precision726 as m
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

PREFIX='Methode Electronics, Inc. and Subsidiaries Consolidated Statements of Operations '
DATES=[['April 28, 2012','April 30, 2011','May 1, 2010'],['April 27, 2013','April 28, 2012','April 30, 2011'],['May 3, 2014','April 27, 2013','April 28, 2012']]
VALUES=[['11,357','15,944','7,441'],['39,178','11,357','15,944'],['73.6','39.2','11.4']]


def statement(i):
    return [['Fiscal Year Ended'],DATES[i],['Net sales','465,095','428,215','377,646'],['Cost of products sold','1','1','1'],['Gross profit','1','1','1'],['Selling expenses','1','1','1'],['Other operating expenses','1','1','1'],['Income from operations',*VALUES[i]],['Other income','1','1','1'],['Income before income taxes','1','1','1'],['Net income','1','1','1']]


def context(i):return PREFIX+('(In thousands, except per share data)' if i<2 else '(In millions, except per share data)')


def test_operating_vector_is_distinct_from_income_before_tax_and_calendar_year():
    before=statement(0);copy=deepcopy(before);v,q=m.vector(before,context(0));assert v['2012-04-28']==Decimal(11357000) and q==1000 and before==copy
    v,q=m.vector(statement(2),context(2));assert v['2012-04-28']==Decimal(11400000) and q==100000


@pytest.mark.parametrize('kind',['company','title','unit','quarter','one_date','five_dates','wrong_date_order','tax_proxy','amount','precision','extra_amount','missing_primary'])
def test_rehashed_nonprimary_or_wrong_metric_dates_and_precision_do_not_qualify(kind):
    rows=deepcopy(statement(2));ctx=context(2)
    if kind=='company':ctx=ctx.replace('Methode','Another')
    if kind=='title':ctx=ctx.replace('Statements of Operations','Statements of Cash Flows')
    if kind=='unit':ctx=ctx.replace('millions','billions')
    if kind=='quarter':rows[0]=['Three Months Ended']
    if kind=='one_date':rows[1]=rows[1][:1]
    if kind=='five_dates':rows[1]+=['April 30, 2011','May 1, 2010']
    if kind=='wrong_date_order':rows[1]=list(reversed(rows[1]))
    if kind=='tax_proxy':rows[7][0]='Income before income taxes'
    if kind=='amount':rows[7][1]='73.6%'
    if kind=='precision':rows[7][1]='73.60'
    if kind=='extra_amount':rows[7].append('1.0')
    if kind=='missing_primary':rows[3][0]='Unknown expenses'
    with pytest.raises(ValueError):m.vector(rows,ctx)


def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];nodes=[];docs=[];payload=dict(cik=65270,facts={'us-gaap':{'OperatingIncomeLoss':dict(units={'USD':[]})}})
    accs=['0000065270-12-000017','0000065270-13-000020','0000065270-14-000023'];filings=['2012-06-29','2013-06-28','2014-06-27'];ends=['2012-04-28','2013-04-27','2014-05-03']
    for i in range(3):
        rows=statement(i);body=('<p>'+context(i)+'</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in rows)+'</table>').encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        url='https://www.sec.gov/Archives/edgar/data/65270/'+accs[i].replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filings[i]))
        obs=dict(start='2011-05-01',end='2012-04-28',val=11357000 if i<2 else 11400000,accn=accs[i],filed=filings[i],form='10-K',fy=2012+i,fp='FY');payload['facts']['us-gaap']['OperatingIncomeLoss']['units']['USD'].append(obs)
        scope.append(dict(company_id=229,metric_id=24,metric_code='OPERATING_INCOME',cik='0000065270',taxonomy='us-gaap',source_tag='OperatingIncomeLoss',unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(obs['val']),accession_number=accs[i],filing_date=filings[i],form_type='10-K',source_url=url,report_date=ends[i]))
    idx=raw_index(payload,'0000065270')
    for row in scope:row['source_fact_hash']=next(h for h,v in idx.items() if v['accession']==row['accession_number'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body))
    r=dict(companyId=229,metricId=24,metric='OPERATING_INCOME',chosenTag='OperatingIncomeLoss',basis=m.BASIS,start='2011-05-01',end='2012-04-28',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value='11400000',originalValue='11357000',filingDate=filings[-1],accessions=[accs[-1]],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=[d['url'] for d in docs],primaryPrecisionEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000065270.json',sha256=sha),reviewExplanation='Exact operating-income concept, full primary comparative columns and official original fiscal periods.')
    return scope,dict(cik='0000065270',review=r,documents=docs)


def test_complete_named_metric_approval_binds_raw_periods_and_all_precision_overlaps(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['source_tag']=='OperatingIncomeLoss' and a['proof']['policy']==m.POLICY and a['proof']['quantumsUSD']==['1000','1000','100000']
    assert a['proof']['originalRawEvidence']['allOfficialAnnualObservationsRepresented'] and before==scope and a['proof_hash']==validate_entry(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('kind',['company','metric','metric_id','tag','period','scope_hash','raw_start','raw_value','raw_accession','raw_filed','source_unit','dimensions','taxonomy','selected_value','original_value','latest_accession','latest_fact','latest_date','original_report_date','source_report_end','missing_middle','reverse','raw_source_url','non_sec_source'])
def test_complete_contract_rejects_other_or_changed_observations(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='metric_id':r['metricId']=23
    if kind=='tag':r['chosenTag']='NetIncomeLoss'
    if kind=='period':r['start']='2011-05-02'
    if kind=='raw_start':scope[1]['period_start']='2011-05-02'
    if kind=='raw_value':scope[1]['value']='11358000'
    if kind=='raw_accession':scope[1]['accession_number']='0000065270-13-000021'
    if kind=='raw_filed':scope[1]['filing_date']='2013-06-29'
    if kind=='source_unit':scope[1]['unit']='USD/shares'
    if kind=='dimensions':scope[1]['dimensions']={'segment':'A'}
    if kind=='taxonomy':scope[1]['taxonomy']='custom'
    if kind=='selected_value':r['value']='11401000'
    if kind=='original_value':r['originalValue']='11358000'
    if kind=='latest_accession':r['accessions']=['0000065270-14-000024']
    if kind=='latest_fact':r['sourceFactHashes']=['f'*64]
    if kind=='latest_date':r['filingDate']='2014-06-28'
    if kind=='original_report_date':scope[0]['report_date']='2012-04-29'
    if kind=='source_report_end':scope[1]['report_date']='2013-04-28'
    if kind=='missing_middle':r['primaryPrecisionEvidence']['nodes'].pop(1)
    if kind=='reverse':r['primaryPrecisionEvidence']['nodes'].reverse()
    if kind=='raw_source_url':r['originalRawEvidence']['url']='https://example.com/data'
    if kind=='non_sec_source':scope[1]['source_url']='https://example.com/report'
    r['sourceScopeHash']=digest(scope) if kind!='scope_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):validate_entry(scope,e,tmp_path)


def test_other_comparative_change_is_rejected_even_if_requested_period_is_unchanged(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);node=e['review']['primaryPrecisionEvidence']['nodes'][1];binding=node['statement'];old=tmp_path/'documents'/(binding['sha256']+'.html.gz');body=gzip.decompress(old.read_bytes()).replace(b'39,178',b'39,278');sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());e['documents'][1]['sha256']=sha;binding['sha256']=sha;binding['tableHash']=digest(p.tables[0])
    with pytest.raises(ValueError,match='COMPARATIVE'):validate_entry(scope,e,tmp_path)
