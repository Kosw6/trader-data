import gzip,hashlib,json
from copy import deepcopy
import pytest
import pipeline.biomerica_total730 as m
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry

TITLE='BIOMERICA, INC. AND SUBSIDIARIES CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME (LOSS)'
NOTE='8. BUSINESS SEGMENTS Geographic information regarding net sales is approximately as follows:'

def primary(i):
    return [['YEARS ENDED MAY 31','',str(2012+i),str(2011+i)],['Net sales','$',str(6081131 if i==0 else 6472960),'$',str(4899375 if i==0 else 6081131)],['Cost of sales','(3783955)' if i==0 else '(4045099)','(3373786)' if i==0 else '(3783955)'],['GROSS PROFIT','2297176' if i==0 else '2427861','1525589' if i==0 else '2297176'],['Selling, general and administrative','1445049' if i==0 else '1454767','1237279' if i==0 else '1445049'],['Research and development','347128' if i==0 else '459086','420571' if i==0 else '347128'],['Total operating expenses','1792177' if i==0 else '1913853','1657850' if i==0 else '1792177']]

def geographic():
    labels=['Europe','United States','Asia','South America','Middle East','Other foreign','Total net sales'];a=['2,533,000','1,074,000','2,420,000','2,000','22,000','30,000','6,081,000'];b=['2,483,000','1,160,000','1,153,000','28,000','45,000','30,000','4,899,000']
    return [['','','2012','2011'],['Net sales:','','','','']]+[['',label,'$',x,'$',y] for label,x,y in zip(labels,a,b)]

def html(rows):
    import re
    def fmt(c,i):
        if i and re.fullmatch(r'\(?\d{4,}\)?',c):
            v=format(int(c.strip('()')),',');return '('+v+')' if c.startswith('(') else v
        return c
    return '<table>'+''.join('<tr>'+''.join('<td>'+fmt(c,i)+'</td>' for c in r)+'</tr>' for i,r in enumerate(rows))+'</table>'

def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];note=None;payload=dict(cik=73290,facts={'us-gaap':{tag:dict(units={'USD':[]}) for tag in ['SalesRevenueNet','Revenues']}})
    accs=['0001513162-12-000654','0001513162-13-000655'];dates=['2012-08-29','2013-08-29']
    for i in range(2):
        body=('<p>'+TITLE+'</p>'+html(primary(i))+('<p>'+NOTE+'</p>'+html(geographic()) if i==0 else '')).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode())
        url='https://www.sec.gov/Archives/edgar/data/73290/'+accs[i].replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=dates[i]))
        if i==0:note=dict(**docs[-1],tableHash=digest(p.tables[1]))
        for tag in ['SalesRevenueNet','Revenues'] if i==0 else ['SalesRevenueNet']:
            val=6081131 if tag=='SalesRevenueNet' else 6081000;obs=dict(start='2011-06-01',end='2012-05-31',val=val,accn=accs[i],filed=dates[i],form='10-K',fy=2012+i,fp='FY');payload['facts']['us-gaap'][tag]['units']['USD'].append(obs)
            scope.append(dict(company_id=266,metric_id=22,metric_code='REVENUE',cik='0000073290',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(val),accession_number=accs[i],filing_date=dates[i],form_type='10-K',source_url=url,report_date=str(2012+i)+'-05-31'))
    index=raw_index(payload,'0000073290')
    for row in scope:row['source_fact_hash']=next(h for h,x in index.items() if x['accession']==row['accession_number'] and x['tag']==row['source_tag'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));latest=scope[-1]
    r=dict(companyId=266,metricId=22,metric='REVENUE',chosenTag='SalesRevenueNet',basis=m.BASIS,start='2011-06-01',end='2012-05-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value='6081131',originalValue='6081131',filingDate=dates[-1],accessions=[accs[-1]],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],exactPrimaryEvidence=dict(nodes=nodes,geographic=note),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000073290.json',sha256=sha),reviewExplanation='Exact primary net sales versus explicitly approximate geographic note; both original tags stay intact.')
    return scope,dict(cik='0000073290',review=r,documents=docs)

def rewrite(root,e,i,old,new):
    binding=e['review']['exactPrimaryEvidence']['nodes'][i]['statement'];body=gzip.decompress((root/'documents'/(binding['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());binding.update(sha256=sha,tableHash=digest(p.tables[0]));e['documents'][i]['sha256']=sha
    if i==0:e['review']['exactPrimaryEvidence']['geographic'].update(sha256=sha,tableHash=digest(p.tables[1]))

def test_detailed_primary_total_wins_only_for_named_company_period(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['source_tag']=='SalesRevenueNet' and a['proof']['restatementClaimed'] is False and a['proof']['approximateComparativesUSD']['2012-05-31']=='6081000' and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash'] and before==scope

@pytest.mark.parametrize('kind',['company','metric_id','metric','tag','period','source_hash','value','original_value','raw_value','raw_period','raw_unit','raw_dimensions','raw_filed','raw_accession','selected_accession','selected_hash','selected_date','report_date','raw_url','missing_node','reverse','non_sec'])
def test_other_tags_periods_or_changed_original_observations_never_fall_back(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric_id':r['metricId']=23
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='tag':r['chosenTag']='Revenues'
    if kind=='period':r['end']='2013-05-31'
    if kind=='value':r['value']='6081000'
    if kind=='original_value':r['originalValue']='6081000'
    if kind=='raw_value':scope[0]['value']='6081132'
    if kind=='raw_period':scope[0]['period_start']='2011-06-02'
    if kind=='raw_unit':scope[0]['unit']='USD/shares'
    if kind=='raw_dimensions':scope[0]['dimensions']={'region':'A'}
    if kind=='raw_filed':scope[0]['filing_date']='2012-08-30'
    if kind=='raw_accession':scope[0]['accession_number']='0001513162-12-000655'
    if kind=='selected_accession':r['accessions']=['0001513162-13-000656']
    if kind=='selected_hash':r['sourceFactHashes']=['f'*64]
    if kind=='selected_date':r['filingDate']='2013-08-30'
    if kind=='report_date':scope[0]['report_date']='2012-05-30'
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/source'
    if kind=='missing_node':r['exactPrimaryEvidence']['nodes'].pop()
    if kind=='reverse':r['exactPrimaryEvidence']['nodes'].reverse()
    if kind=='non_sec':scope[-1]['source_url']='https://example.com/source'
    r['sourceScopeHash']=digest(scope) if kind!='source_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('i,old,new',[(0,b'approximately',b'exactly'),(0,b'2,533,000',b'2,534,000'),(0,b'2,483,000',b'2,484,000'),(0,b'6,081,000',b'6,082,000'),(0,b'4,899,000',b'4,900,000'),(0,b'Total net sales',b'Net income'),(0,b'South America',b'Other region'),(0,b'2011</td>',b'2010</td>'),(1,b'6,081,131',b'6,081,132'),(1,b'2,427,861',b'2,427,862'),(1,b'1,454,767',b'1,454,768'),(1,b'YEARS ENDED MAY 31',b'QUARTERS ENDED MAY 31'),(1,b'AND COMPREHENSIVE INCOME (LOSS)',b'AND CASH FLOWS')])
def test_full_primary_and_both_approximate_comparatives_are_source_bound(i,old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);rewrite(tmp_path,e,i,old,new)
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('cells',[['12.3'],['--'],['1,000%'],['(123'],['-123'],['1,23']])
def test_literal_dollar_numbers_only(cells):
    with pytest.raises(ValueError):m.amounts(cells)
