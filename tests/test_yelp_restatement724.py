import gzip, hashlib, json
from copy import deepcopy
from decimal import Decimal
import pytest
import pipeline.yelp_restatement724 as m
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest


CONTEXT='Yelp Inc. Consolidated Statements of Operations (In thousands, except per share data)'


def rows(year=2018, value='716,063'):
    values={2018:['942,773','850,847',value],2017:['846,813','713,069','549,711'],2016:['713,069','549,711','377,536']}[year]
    return [['Year Ended December 31,'],[str(year-i) for i in range(3)],['Net revenue',*values],['Costs and expenses:']]+[[f'Expense {i}','1','1','1'] for i in range(11)]+[['Total costs and expenses','11','11','11']]


def test_statement_vector_keeps_all_three_annual_columns_and_actual_scale():
    assert m.statement_vector(rows(),CONTEXT)=={'2018-12-31':Decimal(942773000),'2017-12-31':Decimal(850847000),'2016-12-31':Decimal(716063000)}


@pytest.mark.parametrize('kind',['quarter','five_year','unit','company','selected_data','revenue_label','amount','extra_amount','missing_statement'])
def test_rehashed_ineligible_statement_or_unit_not_a_primary_vector(kind):
    data=rows();context=CONTEXT
    if kind=='quarter':data[0]=['Quarter Ended December 31,']
    if kind=='five_year':data[1]+=['2015','2014']
    if kind=='unit':context=context.replace('thousands','millions')
    if kind=='company':context=context.replace('Yelp','Another')
    if kind=='selected_data':context='Consolidated statements of operations data: '+context+' selected data'
    if kind=='revenue_label':data[2][0]='Other revenue'
    if kind=='amount':data[2][1]='942,773%'
    if kind=='extra_amount':data[2].append('1')
    if kind=='missing_statement':data[-1][0]='Other subtotal'
    with pytest.raises(ValueError):m.statement_vector(data,context)


def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir()
    urls=['https://www.sec.gov/Archives/edgar/data/1345016/000120677417000642/yelp3063379-10k.htm',
          'https://www.sec.gov/Archives/edgar/data/1345016/000162828018002519/yelp_10-kx12312017.htm',m.URL]
    accs=['0001206774-17-000642','0001628280-18-002519','0001628280-19-002312'];dates=['2017-03-01','2018-02-28','2019-03-01'];scope=[];docs=[];nodes=[];payload=dict(cik=1345016,facts={'us-gaap':{}})
    for i,year in enumerate([2016,2017,2018]):
        table=rows(year);body=('<p>'+CONTEXT+'</p><table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in row)+'</tr>' for row in table)+'</table>').encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        doc=dict(url=urls[i],sha256=sha);docs.append(doc);nodes.append(dict(statement=dict(**doc,tableHash=digest(p.tables[0])),filingDate=dates[i]))
        tag='SalesRevenueNet' if i<2 else 'RevenueFromContractWithCustomerExcludingAssessedTax';value=713069000 if i<2 else 716063000
        obs=dict(start='2016-01-01',end='2016-12-31',val=value,accn=accs[i],filed=dates[i],form='10-K',fy=year,fp='FY')
        payload['facts']['us-gaap'].setdefault(tag,dict(units={'USD':[]}))['units']['USD'].append(obs)
        scope.append(dict(company_id=2149,metric_id=22,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0001345016',period_start=obs['start'],period_end=obs['end'],form_type='10-K',source_tag=tag,value=str(value),report_date=f'{year}-12-31',filing_date=dates[i],accession_number=accs[i],source_url=urls[i],fiscal_year=year))
    idx=raw_index(payload,'0001345016')
    for row in scope:row['source_fact_hash']=next(h for h,v in idx.items() if v['accession']==row['accession_number'])
    raw=json.dumps(payload).encode();rawsha=hashlib.sha256(raw).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',rawsha);(root/'raw-sec'/(rawsha+'.json.gz')).write_bytes(gzip.compress(raw))
    monkeypatch.setattr(m,'SOURCE_SHA256',docs[-1]['sha256'])
    # The separately tested pinned 720 parser is replaced only in this synthetic
    # contract fixture. Real cached-source replay is mandatory in preparation.
    monkeypatch.setattr(m,'source_evidence',lambda body:[dict(end='2016-12-31',previous='713069000',adjustment='2994000',current='716063000'),dict(end='2017-12-31',previous='846813000',adjustment='4034000',current='850847000')])
    review=dict(ticker='YELP',companyId=2149,metricId=22,metric='REVENUE',chosenTag=scope[-1]['source_tag'],start='2016-01-01',end='2016-12-31',basis=m.BASIS,value='716063000',originalValue='713069000',reviewedOn='2026-10-31',filingDate=dates[-1],sourceScopeHash=digest(scope),accessions=[accs[-1]],sourceFactHashes=[scope[-1]['source_fact_hash']],officialDocuments=urls,reviewExplanation='Every original SEC observation period and every full primary comparative is bound.',originalRawEvidence=dict(url=m.RAW_URL,sha256=rawsha),allPrimaryEvidence=dict(nodes=nodes))
    return scope,dict(cik='0001345016',documents=docs,review=review)


def test_complete_chain_binds_actual_periods_and_all_overlapping_years_without_mutation(tmp_path,monkeypatch):
    scope,entry=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,entry,tmp_path)
    assert a['proof']['policy']==m.POLICY and a['proof']['actualPeriodStartsSourceBound'] and a['proof']['originalRawEvidence']['claims']==3
    assert a['proof']['allPrimaryComparativeEnds']==[['2015-12-31','2016-12-31'],['2016-12-31','2017-12-31']]
    assert not a['proof']['otherMetricApproval'] and before==scope and a['proof_hash']==validate_entry(scope,entry,tmp_path)['proof_hash']


@pytest.mark.parametrize('kind',['raw_start','raw_value','raw_hash','raw_tag','raw_filed','missing_middle','reverse','other_company','other_metric','other_period','raw_source','chosen_value','original_value','raw_report_url','source_scope_hash'])
def test_partial_changed_or_other_scopes_never_inherit_the_restatement(kind,tmp_path,monkeypatch):
    scope,entry=chain(tmp_path,monkeypatch);r=entry['review']
    if kind=='raw_start':scope[1]['period_start']='2016-01-02'
    if kind=='raw_value':scope[1]['value']='713070000'
    if kind=='raw_hash':scope[1]['source_fact_hash']='f'*64
    if kind=='raw_tag':scope[1]['source_tag']='Revenues'
    if kind=='raw_filed':scope[1]['filing_date']='2018-03-01'
    if kind=='missing_middle':r['allPrimaryEvidence']['nodes'].pop(1)
    if kind=='reverse':r['allPrimaryEvidence']['nodes'].reverse()
    if kind=='other_company':r['companyId']=1
    if kind=='other_metric':r['metric']='NET_INCOME'
    if kind=='other_period':r['end']='2018-12-31'
    if kind=='raw_source':r['originalRawEvidence']['url']='https://example.com/raw'
    if kind=='chosen_value':r['value']='716064000'
    if kind=='original_value':r['originalValue']='713070000'
    if kind=='raw_report_url':scope[1]['source_url']=scope[0]['source_url']
    r['sourceScopeHash']=digest(scope) if kind!='source_scope_hash' else 'f'*64
    with pytest.raises(ValueError):validate_entry(scope,entry,tmp_path)


def test_changed_comparative_is_rejected_even_if_target_year_still_matches(tmp_path,monkeypatch):
    scope,entry=chain(tmp_path,monkeypatch);node=entry['review']['allPrimaryEvidence']['nodes'][1];e=node['statement'];path=tmp_path/'documents'/(e['sha256']+'.html.gz');body=gzip.decompress(path.read_bytes()).replace(b'846,813',b'846,814');sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());entry['documents'][1]['sha256']=sha;e['sha256']=sha;e['tableHash']=digest(p.tables[0])
    with pytest.raises(ValueError,match='COMPARATIVE'):validate_entry(scope,entry,tmp_path)


def test_original_sec_observation_absent_from_stored_rows_is_not_silently_ignored(tmp_path,monkeypatch):
    scope,entry=chain(tmp_path,monkeypatch)
    old=tmp_path/'raw-sec'/(m.RAW_SHA256+'.json.gz');payload=json.loads(gzip.decompress(old.read_bytes()))
    observations=payload['facts']['us-gaap']['RevenueFromContractWithCustomerExcludingAssessedTax']['units']['USD']
    observations.append({**observations[0],'filed':'2020-03-01','accn':'0001345016-20-000002','val':716064000})
    raw=json.dumps(payload).encode();sha=hashlib.sha256(raw).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(tmp_path/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(raw));entry['review']['originalRawEvidence']['sha256']=sha
    with pytest.raises(ValueError,match='UNREPRESENTED'):validate_entry(scope,entry,tmp_path)
