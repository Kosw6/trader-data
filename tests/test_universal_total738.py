import gzip, hashlib, json
from copy import deepcopy
from pathlib import Path
import pytest
import pipeline.universal_total738 as m
from pipeline.financial_history674 import raw_index
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

FIXTURES=json.loads((Path(__file__).parent/'fixtures/universal738/statement-excerpts.json').read_text(encoding='utf8'))


def html(rows):
    return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'


def chain(root, monkeypatch, year=2017):
    (root/'documents').mkdir(); (root/'raw-sec').mkdir()
    ds=deepcopy(FIXTURES[(0 if year==2017 else 1):(3 if year==2017 else 4)])
    scope=[]; docs=[]; nodes=[]; raw=dict(cik=102037,facts={'us-gaap':{t:dict(units={'USD':[]}) for t in ['SalesRevenueNet',m.MODERN]}})
    for d in ds:
        # The public excerpts retain primary unit headings, all three columns
        # and independent operating-equation rows, not just target values.
        text='<p>UNIVERSAL CORPORATION CONSOLIDATED STATEMENTS OF INCOME</p>'+html(d['rows'])
        if d['note']: text+='<p>'+m.NOTE_UNIT+'</p><p>'+d['note']['context']+'</p>'+html(d['note']['rows'])
        body=text.encode(); sha=hashlib.sha256(body).hexdigest(); (root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body))
        p=Tables();p.feed(text); primary=m.primary(p.tables[0],p.contexts[0]); report=max(primary); fy=int(report[:4]); filed=str(fy)+'-05-28'
        docs.append(dict(url=d['url'],sha256=sha)); n=dict(statement=dict(**docs[-1],tableHash=digest(p.tables[0])),filingDate=filed)
        if d['note']: n['contracts']=dict(**docs[-1],tableHash=digest(p.tables[1]))
        nodes.append(n)
        tag=m.MODERN if d['note'] else 'SalesRevenueNet'; val=(m.contracts(p.tables[1],p.contexts[1])[str(year)+'-03-31']['contract'] if d['note'] else primary[str(year)+'-03-31'])
        folder=d['url'].split('/')[-2]; accn=folder[:10]+'-'+folder[10:12]+'-'+folder[12:]
        obs=dict(start=str(year-1)+'-04-01',end=str(year)+'-03-31',val=int(val),accn=accn,filed=filed,form='10-K',fy=fy,fp='FY');raw['facts']['us-gaap'][tag]['units']['USD'].append(obs)
        scope.append(dict(company_id=366,metric_id=22,metric_code='REVENUE',cik='0000102037',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(val),accession_number=accn,filing_date=filed,form_type='10-K',source_url=d['url'],report_date=report))
    index=raw_index(raw,'0000102037')
    for row in scope: row['source_fact_hash']=next(h for h,x in index.items() if x['accession']==row['accession_number'] and x['tag']==row['source_tag'])
    body=json.dumps(raw).encode();sha=hashlib.sha256(body).hexdigest(); monkeypatch.setattr(m,'RAW_SHA256',sha); (root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body))
    latest=max((s for s in scope if s['source_tag']=='SalesRevenueNet'),key=lambda x:x['filing_date'])
    r=dict(companyId=366,metricId=22,metric='REVENUE',chosenTag='SalesRevenueNet',basis=m.BASIS,start=str(year-1)+'-04-01',end=str(year)+'-03-31',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value=latest['value'],originalValue=latest['value'],filingDate=latest['filing_date'],accessions=[latest['accession_number']],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],contractTotalEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000102037.json',sha256=sha),reviewExplanation='Literal original consolidated totals are unchanged in later full primary statements; customer-contract revenue is only a subtotal.')
    return scope,dict(cik='0000102037',review=r,documents=docs)


@pytest.mark.parametrize('year',[2017,2018])
def test_full_primary_and_contract_note_keep_original_total_and_date(tmp_path,monkeypatch,year):
    scope,e=chain(tmp_path,monkeypatch,year); old=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['source_tag']=='SalesRevenueNet' and scope==old and a['proof']['restatementClaimed'] is False
    assert a['proof']['sources'][0]['filing_date']==e['review']['filingDate']
    assert a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('kind',['company','metric','metric_id','period','tag','value','source_hash','raw_value','raw_unit','raw_dimensions','raw_filing','raw_report','selected_accession','selected_hash','selected_date','reverse','missing','note_other_document'])
def test_changed_scope_or_endpoint_is_held(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='metric_id':r['metricId']=23
    if kind=='period':r['end']='2019-03-31'
    if kind=='tag':r['chosenTag']=m.MODERN
    if kind=='value':r['value']='2047382000'
    if kind=='raw_value':scope[0]['value']='1'
    if kind=='raw_unit':scope[0]['unit']='USD/shares'
    if kind=='raw_dimensions':scope[0]['dimensions']={'division':'X'}
    if kind=='raw_filing':scope[0]['filing_date']='2017-05-29'
    if kind=='raw_report':scope[0]['report_date']='2017-04-01'
    if kind=='selected_accession':r['accessions']=['0000102037-18-000099']
    if kind=='selected_hash':r['sourceFactHashes']=['f'*64]
    if kind=='selected_date':r['filingDate']='2019-05-28'
    if kind=='reverse':r['contractTotalEvidence']['nodes'].reverse()
    if kind=='missing':r['contractTotalEvidence']['nodes'].pop()
    if kind=='note_other_document':r['contractTotalEvidence']['nodes'][-1]['contracts']['url']=e['documents'][0]['url']
    r['sourceScopeHash']=digest(scope) if kind!='source_hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)


@pytest.mark.parametrize('old,new',[(b'all dollar amounts are in thousands',b'all dollar amounts are in millions'),(b'2,047,382',b'2,047,383'),(b'23,836',b'23,835'),(b'Other operating sales and revenues',b'Other non-operating sales and revenues'),(b'2,071,218',b'2,071,219'),(b'2017</td>',b'2016</td>'),(b'211,919',b'211,918'),(b'178,401',b'178,400')])
def test_all_note_columns_units_and_primary_equations_are_bound(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);i=2;n=e['review']['contractTotalEvidence']['nodes'][i];b=n['statement'];body=gzip.decompress((tmp_path/'documents'/(b['sha256']+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());e['documents'][i]['sha256']=sha;b.update(sha256=sha,tableHash=digest(p.tables[0]));n['contracts'].update(sha256=sha,tableHash=digest(p.tables[1]))
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)


@pytest.mark.parametrize('cells',[['1,234','2,345'],['1234','2,345','3,456'],['1,23','2,345','3,456'],['1,234%','2,345','3,456'],['(123','2,345','3,456']])
def test_ambiguous_numeric_cells_never_guess(cells):
    with pytest.raises(ValueError):m.amounts(cells)
