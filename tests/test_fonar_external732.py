import gzip,hashlib,json
from copy import deepcopy
import pytest
import pipeline.fonar_external732 as m
from pipeline.financial_history674 import raw_index
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.financial_reconciliation677 import validate_entry

TITLE='FONAR CORPORATION AND SUBSIDIARIES CONSOLIDATED STATEMENTS OF INCOME'
CONTEXT='Summarized financial information concerning the Company’s reportable segments is shown in the following table:'
def total(year):return (year-2001)*10
def inter(year):return year-2005
def primary(report):
    years=list(range(report,report-(3 if report==2015 else 2),-1));rows=[['For the Years Ended June 30,'],[str(y) for y in years],['Revenues']]
    labels=['Product sales – net','Service and repair fees – net','Service and repair fees – related parties – net']
    data=[[10]*len(years),[20]*len(years),[5]*len(years)]
    if report>=2013:
        labels+=['Patient fee revenue, net of contractual allowances and discounts','Provision for bad debts for patient fee'];data+=[[10 if y>=2013 else 0 for y in years],[-2 if y>=2013 else 0 for y in years]]
    labels+=['Management and other fees - net','Management and other fees – related medical practices – net'];data+=[[40]*len(years),[total(y)-75-(8 if y>=2013 and report>=2013 else 0) for y in years]]
    def fmt(v):return '—' if v==0 else '('+format(-v,',')+')' if v<0 else format(v,',')
    for i,(label,values) in enumerate(zip(labels,data)):rows.append([label,*sum((['$' if i==0 else '',fmt(v)] for v in values),[])])
    rows += [['Total Revenues – Net',*[str(total(y)) for y in years]],['Costs and Expenses'],['Total Costs and Expenses',*['50']*len(years)],['Income from Operations',*[str(total(y)-50) for y in years]]]
    return rows
def segments(report):
    years=list(range(report,report-(3 if report==2015 else 2),-1));rows=[['Manufacturing and Servicing of Medical Equipment','Management of Diagnostic Imaging Centers','Totals']]
    for year in years:
        rows += [[f'Fiscal {year}:'],['Net revenues from external customers','$','50','$',str(total(year)-50),'$',str(total(year))],['Intersegment net revenues *','$',str(inter(year)),'$','—','$',str(inter(year))],['Income from operations','$','1','$','2','$','3'],['Depreciation and amortization','$','1','$','2','$','3'],['Compensatory element of stock issuances','$','1','$','2','$','3'],['Total identifiable assets','$','1','$','2','$','3'],['Capital expenditures','$','1','$','2','$','3']]
    return rows
def html(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'

def chain(root,monkeypatch,year=2012):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];payload=dict(cik=355019,facts={'us-gaap':{t:dict(units={'USD':[]}) for t in ['SalesRevenueNet','Revenues']}})
    for report in range(year,year+(2 if year==2012 else 3)):
        body=('<p>'+TITLE+'</p>'+html(primary(report))+'<p>'+CONTEXT+'</p>'+html(segments(report))).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());acc=f'0000355019-{report-2000:02d}-000032';filed=str(report)+'-10-26';url='https://www.sec.gov/Archives/edgar/data/355019/'+acc.replace('-','')+'/annual.htm';docs.append(dict(url=url,sha256=sha));nodes.append(dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),segments=dict(**docs[-1],tableHash=digest(p.tables[1])),filingDate=filed))
        for tag in ['SalesRevenueNet','Revenues']:
            val=total(year) if tag=='SalesRevenueNet' else inter(year);form='10-K/A' if report==2012 else '10-K';obs=dict(start=str(year-1)+'-07-01',end=str(year)+'-06-30',val=val,accn=acc,filed=filed,form=form,fy=report,fp='FY');payload['facts']['us-gaap'][tag]['units']['USD'].append(obs)
            scope.append(dict(company_id=478,metric_id=22,metric_code='REVENUE',cik='0000355019',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(val),accession_number=acc,filing_date=filed,form_type=form,source_url=url,report_date=str(report)+'-06-30'))
    index=raw_index(payload,'0000355019')
    for row in scope:row['source_fact_hash']=next(h for h,v in index.items() if v['accession']==row['accession_number'] and v['tag']==row['source_tag'])
    body=json.dumps(payload).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));latest=scope[-2]
    r=dict(companyId=478,metricId=22,metric='REVENUE',chosenTag='SalesRevenueNet',basis=m.BASIS,start=str(year-1)+'-07-01',end=str(year)+'-06-30',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value=latest['value'],originalValue=scope[0]['value'],filingDate=latest['filing_date'],accessions=[latest['accession_number']],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],externalTotalEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000355019.json',sha256=sha),reviewExplanation='Consolidated external customer total is independently bound by all primary components and both segment columns; intersegment original facts stay separate.')
    return scope,dict(cik='0000355019',review=r,documents=docs)

def rewrite(root,e,i,old,new):
    n=e['review']['externalTotalEvidence']['nodes'][i];b=n['statement'];body=gzip.decompress((root/'documents'/(b['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());b.update(sha256=sha,tableHash=digest(p.tables[0]));n['segments'].update(sha256=sha,tableHash=digest(p.tables[1]));e['documents'][i]['sha256']=sha

@pytest.mark.parametrize('year',[2012,2013])
def test_exact_external_total_and_separate_intersegment_tag_preserve_all_sources(year,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch,year);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['source_tag']=='SalesRevenueNet' and not a['proof']['restatementClaimed'] and a['proof']['originalRawEvidence']['allOfficialAnnualObservationsRepresented'] and before==scope and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']

@pytest.mark.parametrize('kind',['company','metric_id','metric','tag','period','hash','value','original_value','raw_value','raw_period','raw_filed','raw_accession','unit','dimensions','taxonomy','latest_hash','latest_accession','latest_date','report_date','raw_url','missing_node','reverse','non_sec'])
def test_other_metrics_periods_or_changed_original_observations_block(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric_id':r['metricId']=23
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='tag':r['chosenTag']='Revenues'
    if kind=='period':r['start']='2010-07-01'
    if kind=='value':r['value']='7'
    if kind=='original_value':r['originalValue']='7'
    if kind=='raw_value':scope[1]['value']='8'
    if kind=='raw_period':scope[1]['period_start']='2011-07-02'
    if kind=='raw_filed':scope[1]['filing_date']='2012-10-27'
    if kind=='raw_accession':scope[1]['accession_number']='0000355019-12-000033'
    if kind=='unit':scope[1]['unit']='EUR'
    if kind=='dimensions':scope[1]['dimensions']={'segment':'A'}
    if kind=='taxonomy':scope[1]['taxonomy']='custom'
    if kind=='latest_hash':r['sourceFactHashes']=['f'*64]
    if kind=='latest_accession':r['accessions']=['0000355019-13-000033']
    if kind=='latest_date':r['filingDate']='2013-10-27'
    if kind=='report_date':scope[1]['report_date']='2012-06-29'
    if kind=='raw_url':r['originalRawEvidence']['url']='https://example.com/raw'
    if kind=='missing_node':r['externalTotalEvidence']['nodes'].pop()
    if kind=='reverse':r['externalTotalEvidence']['nodes'].reverse()
    if kind=='non_sec':scope[-2]['source_url']='https://example.com/source'
    r['sourceScopeHash']=digest(scope) if kind!='hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('old,new',[(b'Product sales',b'License fees'),(b'Service and repair fees',b'Unknown fees'),(b'Management and other fees',b'Other gains'),(b'Provision for bad debts for patient fee',b'Other expenses'),(b'>120<',b'>121<'),(b'Total Costs and Expenses',b'Total tax expense'),(b'For the Years Ended June 30,',b'For the Quarters Ended June 30,'),(b'Net revenues from external customers',b'Net revenues from internal customers'),(b'Intersegment net revenues',b'Total net revenues'),('—'.encode(),b''),(b'Fiscal 2013:',b'Fiscal 2011:'),(b'Totals',b'Percentages')])
def test_every_primary_component_and_external_internal_segment_identity_is_required(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch,2013);rewrite(tmp_path,e,0,old,new)
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)

@pytest.mark.parametrize('cells',[['(1',''],['1',')'],['--'],['—'],['1.0'],['12%'],['-1']])
def test_blank_or_wrong_numbers_never_repair_intersegment_totals(cells):
    with pytest.raises(ValueError):m.amounts(cells)
