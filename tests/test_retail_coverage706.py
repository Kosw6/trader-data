import gzip
import hashlib
from copy import deepcopy
import pytest
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.retail_coverage706 import BASIS, POLICY
from pipeline.financial_reconciliation677 import validate_entry


def fixture(root, mode='fiscal'):
    (root/'documents').mkdir(); docs=[]; bindings={}
    end='2010-01-30' if mode=='fiscal' else '2013-02-02'
    start='2009-02-01' if mode=='fiscal' else '2012-01-29'
    fy=2009 if mode=='fiscal' else 2012; scale=1000000 if mode=='fiscal' else 1000
    def tr(*cells): return '<tr>'+''.join('<td>'+str(c)+'</td>' for c in cells)+'</tr>'
    for role in ['original','selected']:
        if mode=='fiscal':
            years=[2009,2008] if role=='original' else [2011,2010,2009]
            dates={2008:'January 31, 2009',2009:'January 30, 2010',2010:'January 29, 2011',2011:'January 28, 2012'}
            note=' '.join(f'Fiscal year {y} ended {dates[y]}, and consisted of 52 weeks.' for y in years)
            table=tr('(millions, except per share data)',*years)+tr('Sales',*[60]*len(years))+tr('Credit card revenues',*[2]*len(years))+tr('')+tr('Total revenues',*[62]*len(years))
            body=('<p>'+note+'</p><p>Consolidated Statements of Operations</p><table>'+table+'</table>').encode()
            e=dict(yearRow=0,firstRow=1,totalRow=4,componentContract='SALES_CREDIT_CARD706_V1',periodMode='EXPLICIT_FISCAL_NOTE',fiscalNoteExcerpt=note)
        else:
            dates=['February 2, 2013','January 28, 2012'] if role=='original' else ['January 31, 2015','February 1, 2014','February 2, 2013'];n=len(dates)
            table=tr('Fiscal Year Ended',*dates)+tr('(Dollars in thousands, except per share data)')+tr('REVENUES')+tr('Retail sales',*[60]*n)+tr('Other income (principally finance charges,')+tr('late fees and layaway charges)',*[2]*n)+tr('Total revenues',*[62]*n)
            body=('<p>Consolidated Statements of Income and Comprehensive Income</p><table>'+table+'</table>').encode()
            e=dict(yearRow=0,firstRow=3,totalRow=6,componentContract='RETAIL_FINANCE_CHARGES706_V1',periodMode='DATE')
        sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        acc='0000000001-10-000001' if role=='original' else '0000000001-14-000001';url='https://www.sec.gov/Archives/edgar/data/1/'+acc.replace('-','')+'/annual.htm'
        docs.append(dict(url=url,sha256=sha));bindings[role]=dict(e,url=url,sha256=sha,tableHash=digest(p.tables[0]),unitScale=scale)
    a=dict(company_id=1,metric_id=2,metric_code='REVENUE',taxonomy='us-gaap',unit='USD',dimensions=None,cik='0000000001',period_start=start,period_end=end,form_type='10-K',source_tag='SalesRevenueNet',value=str(60*scale),report_date=end,filing_date='2013-04-01' if mode!='fiscal' else '2010-03-01',accession_number='0000000001-10-000001',source_fact_hash='a'*64,source_url=docs[0]['url'],fiscal_year=fy)
    b=dict(a,source_tag='Revenues',value=str(62*scale),report_date='2014-12-31',filing_date='2015-02-01',accession_number='0000000001-14-000001',source_fact_hash='b'*64,source_url=docs[1]['url'],fiscal_year=2014);rows=[a,b]
    review=dict(companyId=1,metricId=2,metric='REVENUE',chosenTag='Revenues',start=start,end=end,reviewedOn='2026-10-31',sourceScopeHash=digest(rows),basis=BASIS,value=b['value'],originalValue=a['value'],originalFiscalYear=fy,filingDate=b['filing_date'],accessions=[b['accession_number']],sourceFactHashes=[b['source_fact_hash']],officialDocuments=[d['url'] for d in docs],retailCoverageEvidence=bindings,reviewExplanation='Named retail and credit components already sum to the same original and selected total. No accounting restatement inferred.')
    return rows,dict(cik='0000000001',review=review,documents=docs)


@pytest.mark.parametrize('mode',['fiscal','date'])
def test_registered_named_components_preserve_raw_values_and_replay(tmp_path,mode):
    rows,e=fixture(tmp_path,mode);before=deepcopy(rows);a=validate_entry(rows,e,tmp_path)
    assert rows==before and a['proof']['policy']==POLICY and not a['proof']['restatementClaimed']
    assert a['proof_hash']==validate_entry(rows,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',['scope_hash','raw_value','raw_unit','raw_period','raw_form','old_url','new_url','table_hash','year_row','first_row','total_row','unit','contract','period_mode','fiscal_year','fiscal_quote'])
def test_unbound_source_company_period_units_and_fiscal_notes_fail_closed(tmp_path,change):
    rows,e=fixture(tmp_path);r=e['review'];v=r['retailCoverageEvidence'];b=v['selected']
    if change=='scope_hash':r['sourceScopeHash']='c'*64
    elif change in ['raw_value','raw_unit','raw_period','raw_form']:
        k,x={'raw_value':('value','61000000'),'raw_unit':('unit','EUR'),'raw_period':('period_start','2009-10-01'),'raw_form':('form_type','8-K')}[change];rows[0][k]=x;r['sourceScopeHash']=digest(rows)
    elif change=='old_url':v['original']['url']=b['url']
    elif change=='new_url':b['url']=v['original']['url']
    elif change=='table_hash':b['tableHash']='c'*64
    elif change=='year_row':b['yearRow']=True
    elif change=='first_row':b['firstRow']=2
    elif change=='total_row':b['totalRow']=2
    elif change=='unit':b['unitScale']=1000
    elif change=='contract':b['componentContract']='GUESS_SUM'
    elif change=='period_mode':b['periodMode']='GUESS_FISCAL_DATES'
    elif change=='fiscal_year':r['originalFiscalYear']=2008
    elif change=='fiscal_quote':b['fiscalNoteExcerpt']='Use the Saturday nearest January 31 to infer all dates.'
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


def rewrite(e,root,old,new):
    b=e['review']['retailCoverageEvidence']['selected'];sha=b['sha256'];body=gzip.decompress((root/'documents'/(sha+'.html.gz')).read_bytes()).replace(old.encode(),new.encode());newsha=hashlib.sha256(body).hexdigest();(root/'documents'/(newsha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());b.update(sha256=newsha,tableHash=digest(p.tables[0]));next(d for d in e['documents'] if d['url']==b['url'])['sha256']=newsha


@pytest.mark.parametrize('old,new',[
    ('<td>Credit card revenues</td>','<td>Credit card expenses</td>'),
    ('<td>62</td>','<td>63</td>'),
    ('<td>2</td>','<td>3</td>'),
    ('Consolidated Statements of Operations','Management discussion of Consolidated Statements of Operations and segment results'),
    ('January 30, 2010','January 31, 2010'),
    ('Fiscal year 2009 ended January 30, 2010','Fiscal year 2009 will end January 30, 2010'),
])
def test_rehashed_documents_still_need_explicit_dates_labels_and_all_column_equations(tmp_path,old,new):
    rows,e=fixture(tmp_path);rewrite(e,tmp_path,old,new)
    if old.startswith('Fiscal year'):e['review']['retailCoverageEvidence']['selected']['fiscalNoteExcerpt']=e['review']['retailCoverageEvidence']['selected']['fiscalNoteExcerpt'].replace(old,new)
    with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)


def test_changed_intermediate_modern_value_requires_separate_review(tmp_path):
    rows,e=fixture(tmp_path);extra=dict(rows[-1],value='63000000',filing_date='2012-03-01');rows.insert(1,extra);e['review']['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError,match='RETAIL_MODERN_OBSERVATIONS_CHANGED'):validate_entry(rows,e,tmp_path)


@pytest.mark.parametrize('zero,accepted',[('—',True),('',False),('-',False)])
def test_only_printed_em_dash_can_represent_zero_in_other_years(tmp_path,zero,accepted):
    rows,e=fixture(tmp_path)
    rewrite(e,tmp_path,'<td>Sales</td><td>60</td>','<td>Sales</td><td>62</td>')
    rewrite(e,tmp_path,'<td>Credit card revenues</td><td>2</td>','<td>Credit card revenues</td><td>'+zero+'</td>')
    if accepted: assert validate_entry(rows,e,tmp_path)['proof']['policy']==POLICY
    else:
        with pytest.raises(ValueError):validate_entry(rows,e,tmp_path)
