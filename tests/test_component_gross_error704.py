import gzip
import hashlib
from copy import deepcopy
import pytest
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.component_gross_error704 import BASIS,POLICY
from pipeline.financial_reconciliation677 import validate_entry

CAUSE=('The Company identified an immaterial prior period error in the presentation of premium subscriptions in its Connected Fitness reporting segment. Subscription revenue was previously recorded net of any related commission. Beginning in the first quarter of 2018, subscription revenue is recorded on a gross basis and the related commission cost is included in selling, general and administrative expense in the consolidated statement of operations. The Company has revised the prior periods to be consistent with the current period presentation resulting in an increase in net revenues and selling, general and administrative expense of $12.7 million and $8.0 million for the years ended December 31, 2017 and 2016, respectively. There was no impact in any period on income (loss) from operations. The Company concluded that the error was not material to any of its previously issued financial statements.')


def fixture(tmp_path,raw_scope='PRODUCT_NET_SALES'):
    folder=tmp_path/'documents';folder.mkdir();nodes={};docs=[]
    old=[3229142,1010693,406614,4646449,99849,80447,-1410,4825335];new=[*old];new[5]=88450;new[7]=4833338
    labels=['Apparel','Footwear','Accessories','Total net sales','Licensing revenues','Connected Fitness','Intersegment Eliminations','Total net revenues']
    def tr(label,values):return '<tr>'+''.join('<td>'+str(x)+'</td>' for x in [label,*values])+'</tr>'
    for role,years,vector,sga in [('original',[2016,2015],old,1823140),('selected',[2018,2017,2016],new,1831143)]:
        header=tr('Year ended December 31,',years)
        primary='<p>Consolidated Statements of Operations (in thousands)</p><table>'+header+tr('Net revenues',[vector[7]]*len(years))+tr('Selling, general and administrative expenses',[sga]*len(years))+tr('Income from operations',[417471]*len(years))+'</table>'
        cat='<p>Net revenues by product category (in thousands)</p><table>'+header+''.join(tr(label,[v]*len(years)) for label,v in zip(labels,vector))+'</table>'
        body=(primary+('<p>'+CAUSE+'</p>' if role=='selected' else '')+cat).encode();sha=hashlib.sha256(body).hexdigest();(folder/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        accession='0000000001-17-000001' if role=='original' else '0000000001-19-000001';url='https://www.sec.gov/Archives/edgar/data/1/'+accession.replace('-','')+'/annual.htm'
        nodes[role+'Statement']=dict(url=url,sha256=sha,tableHash=digest(p.tables[0]),periodRow=0,valueRow=1,rowLabel='Net revenues',unitScale=1000,periodMode='YEAR',annualHeading='Year ended December 31,')
        nodes[role+'Category']=dict(url=url,sha256=sha,tableHash=digest(p.tables[1]),yearRow=0,firstValueRow=1);docs.append(dict(url=url,sha256=sha))
    a='4646449000' if raw_scope=='PRODUCT_NET_SALES' else '4825335000'
    first=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start='2016-01-01',period_end='2016-12-31',form_type='10-K',source_tag='SalesRevenueNet',value=a,report_date='2016-12-31',filing_date='2017-02-01',accession_number='0000000001-17-000001',source_fact_hash='a'*64,source_url=docs[0]['url'],fiscal_year=2016)
    last=dict(first,source_tag='Revenues',value='4833338000',report_date='2018-12-31',filing_date='2019-02-01',accession_number='0000000001-19-000001',source_fact_hash='b'*64,source_url=docs[1]['url']);rows=[first,last]
    nodes.update(causeExcerpt=CAUSE,originalRawScope=raw_scope)
    r=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start='2016-01-01',end='2016-12-31',reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value=last['value'],originalValue=a,filingDate=last['filing_date'],accessions=[last['accession_number']],sourceFactHashes=[last['source_fact_hash']],officialDocuments=[d['url'] for d in docs],componentGrossEvidence=nodes,reviewExplanation='Explicit original product coverage and prior period subscription gross/net error, with exact table reconciliation and equal SG&A offset; no revenue-standard adoption inferred.')
    return rows,dict(cik='0000000001',review=r,documents=docs)


@pytest.mark.parametrize('raw_scope',['PRODUCT_NET_SALES','TOTAL_NET_REVENUES'])
def test_partial_and_full_original_scope_require_exact_revenue_and_equal_expense_bridge(tmp_path,raw_scope):
    rows,e=fixture(tmp_path,raw_scope);before=deepcopy(rows);a=validate_entry(rows,e,tmp_path)
    assert rows==before and a['proof']['policy']==POLICY and a['proof']['originalRawScope']==raw_scope
    assert a['proof']['exactSubscriptionGrossUpUSD']=='8003000' and a['proof']['exactSgaIncreaseUSD']=='8003000'
    assert a['proof']['roundedNarrativeCorroborationUSD']=='8000000.0' and a['proof']['operatingIncomeChangeUSD']=='0'
    assert not a['proof']['revenueStandardAdoptionInferred'] and a['proof_hash']==validate_entry(rows,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['raw_scope','source_hash','raw_value','old_url','new_url','year_row','first_row','unit','cause','rounded_delta','cause_period','table_hash'])
def test_unbound_source_scope_period_or_accounting_cause_is_not_approved(tmp_path,change):
    rows,e=fixture(tmp_path);r=e['review'];v=r['componentGrossEvidence']
    if change=='raw_scope':v['originalRawScope']='TOTAL_NET_REVENUES'
    elif change=='source_hash':r['sourceScopeHash']='c'*64
    elif change=='raw_value':rows[0]['value']='4646449001';r['sourceScopeHash']=digest(rows)
    elif change=='old_url':v['originalCategory']['url']=v['selectedCategory']['url']
    elif change=='new_url':v['selectedStatement']['url']=v['originalStatement']['url']
    elif change=='year_row':v['selectedCategory']['yearRow']=1
    elif change=='first_row':v['selectedCategory']['firstValueRow']=2
    elif change=='unit':v['selectedStatement']['unitScale']=1000000
    elif change=='cause':v['causeExcerpt']='The Company adopted ASC606 on a modified retrospective basis and did not restate prior periods.'
    elif change=='rounded_delta':v['causeExcerpt']=CAUSE.replace('$8.0 million','$8.1 million')
    elif change=='cause_period':v['causeExcerpt']=CAUSE.replace('2017 and 2016','2018 and 2017')
    elif change=='table_hash':v['selectedCategory']['tableHash']='c'*64
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


def rewrite(e,root,role,old,new):
    v=e['review']['componentGrossEvidence'];b=v[role+'Statement'];url=b['url'];sha=b['sha256'];text=gzip.decompress((root/'documents'/(sha+'.html.gz')).read_bytes()).decode();assert old in text;body=text.replace(old,new,1).encode();newsha=hashlib.sha256(body).hexdigest();(root/'documents'/(newsha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
    for name,index in [(role+'Statement',0),(role+'Category',1)]:v[name].update(sha256=newsha,tableHash=digest(p.tables[index]))
    next(d for d in e['documents'] if d['url']==url)['sha256']=newsha


@pytest.mark.parametrize('old,new',[
    ('<td>3229142</td>','<td>3229143</td>'),
    ('<td>1831143</td>','<td>1831144</td>'),
    ('<td>417471</td>','<td>417472</td>'),
    ('<td>Licensing revenues</td>','<td>Other business</td>'),
])
def test_rehashed_but_semantically_changed_tables_fail(tmp_path,old,new):
    rows,e=fixture(tmp_path);rewrite(e,tmp_path,'selected',old,new)
    # Changed non-target year columns must not break exact component arithmetic
    # silently; expense tests replace the target column as well.
    if old in ['<td>1831143</td>','<td>417471</td>']:
        rewrite(e,tmp_path,'selected',old,new);rewrite(e,tmp_path,'selected',old,new)
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


def causal_fixture(tmp_path):
    rows,e=fixture(tmp_path);v=e['review']['componentGrossEvidence'];b=v['selectedCategory'];oldurl=b['url'];body=gzip.decompress((tmp_path/'documents'/(b['sha256']+'.html.gz')).read_bytes());sha=hashlib.sha256(body).hexdigest();url='https://www.sec.gov/Archives/edgar/data/1/000000000118000001/middle.htm';e['documents'].append(dict(url=url,sha256=sha));e['review']['officialDocuments'].append(url)
    mid=dict(rows[1],filing_date='2018-02-01',report_date='2017-12-31',source_url=url,source_fact_hash='c'*64,accession_number='0000000001-18-000001');rows.append(mid);b['url']=url;v['causalFactHashes']=['c'*64];e['review']['sourceScopeHash']=digest(rows)
    return rows,e


def test_intermediate_error_disclosure_must_confirm_exact_latest_value(tmp_path):
    rows,e=causal_fixture(tmp_path)
    assert validate_entry(rows,e,tmp_path)['proof']['exactSubscriptionGrossUpUSD']=='8003000'


@pytest.mark.parametrize('change',['hash','value','tag','chronology'])
def test_intermediate_disclosure_is_not_a_license_for_unrelated_later_values(tmp_path,change):
    rows,e=causal_fixture(tmp_path)
    if change=='hash':e['review']['componentGrossEvidence']['causalFactHashes']=['d'*64]
    elif change=='value':rows[-1]['value']='4833338001'
    elif change=='tag':rows[-1]['source_tag']='SalesRevenueNet'
    elif change=='chronology':rows[-1]['filing_date']='2016-01-01'
    e['review']['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)
