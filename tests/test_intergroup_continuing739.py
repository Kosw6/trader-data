import gzip,hashlib,json
from copy import deepcopy
from pathlib import Path
import pytest
import pipeline.intergroup_continuing739 as m
from pipeline.financial_history674 import raw_index
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

FIXTURES=json.loads((Path(__file__).parent/'fixtures/intergroup739/statement-excerpts.json').read_text(encoding='utf8'))


def html(rows):return '<table>'+''.join('<tr>'+''.join('<td>'+c+'</td>' for c in r)+'</tr>' for r in rows)+'</table>'


def chain(root,monkeypatch):
    (root/'documents').mkdir();(root/'raw-sec').mkdir();scope=[];docs=[];nodes=[];raw=dict(cik=69422,facts={'us-gaap':{t:dict(units={'USD':[]}) for t in ['SalesRevenueNet','Revenues']}})
    for i,d in enumerate(deepcopy(FIXTURES)):
        body=''.join('<p>'+d[k]['context']+'</p>'+html(d[k]['rows']) for k in ['primary','segments','discontinued']).encode();sha=hashlib.sha256(body).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());docs.append(dict(url=d['url'],sha256=sha));filed=str(2012+i)+'-09-25';nodes.append(dict(**{k:dict(**docs[-1],tableHash=digest(p.tables[j])) for j,k in enumerate(['statement','segments','discontinued'])},filingDate=filed))
        folder=d['url'].split('/')[-2];accn=folder[:10]+'-'+folder[10:12]+'-'+folder[12:]
        for tag,val in [('SalesRevenueNet',56999000),('Revenues',57207000)]:
            obs=dict(start='2011-07-01',end='2012-06-30',val=val,accn=accn,filed=filed,form='10-K',fy=2012+i,fp='FY');raw['facts']['us-gaap'][tag]['units']['USD'].append(obs)
            scope.append(dict(company_id=244,metric_id=22,metric_code='REVENUE',cik='0000069422',taxonomy='us-gaap',source_tag=tag,unit='USD',dimensions=None,period_start=obs['start'],period_end=obs['end'],value=str(val),accession_number=accn,filing_date=filed,form_type='10-K',source_url=d['url'],report_date=str(2012+i)+'-06-30'))
    index=raw_index(raw,'0000069422')
    for r in scope:r['source_fact_hash']=next(h for h,x in index.items() if x['tag']==r['source_tag'] and x['accession']==r['accession_number'])
    body=json.dumps(raw).encode();sha=hashlib.sha256(body).hexdigest();monkeypatch.setattr(m,'RAW_SHA256',sha);(root/'raw-sec'/(sha+'.json.gz')).write_bytes(gzip.compress(body));latest=scope[-2]
    r=dict(companyId=244,metricId=22,metric='REVENUE',chosenTag='SalesRevenueNet',basis=m.BASIS,start='2011-07-01',end='2012-06-30',sourceScopeHash=digest(scope),reviewedOn='2026-10-31',value='56999000',originalValue='56999000',filingDate=latest['filing_date'],accessions=[latest['accession_number']],sourceFactHashes=[latest['source_fact_hash']],officialDocuments=[d['url'] for d in docs],continuingRevenueEvidence=dict(nodes=nodes),originalRawEvidence=dict(url='https://data.sec.gov/api/xbrl/companyfacts/CIK0000069422.json',sha256=sha),reviewExplanation='Primary continuing revenue versus separately identified segment discontinued revenue. Both original raw tags retained.')
    return scope,dict(cik='0000069422',review=r,documents=docs)


def test_primary_continuing_total_does_not_add_discontinued_revenue(tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert before==scope and a['source_tag']=='SalesRevenueNet' and not a['proof']['restatementClaimed']
    assert a['proof']['discontinuedRevenueUSD']=='208000' and a['proof_hash']==m.approve(scope,e,tmp_path)['proof_hash']


@pytest.mark.parametrize('kind',['company','metric','metric_id','period','tag','value','original','hash','raw_value','raw_unit','raw_dimensions','raw_report','selected_accession','selected_hash','reverse','missing','cross_report'])
def test_changed_identity_or_original_claim_never_falls_back(kind,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);r=e['review']
    if kind=='company':r['companyId']=1
    if kind=='metric':r['metric']='NET_INCOME'
    if kind=='metric_id':r['metricId']=23
    if kind=='period':r['end']='2013-06-30'
    if kind=='tag':r['chosenTag']='Revenues'
    if kind=='value':r['value']='57207000'
    if kind=='original':r['originalValue']='57207000'
    if kind=='raw_value':scope[0]['value']='57207000'
    if kind=='raw_unit':scope[0]['unit']='USD/shares'
    if kind=='raw_dimensions':scope[0]['dimensions']={'segment':'hotel'}
    if kind=='raw_report':scope[0]['report_date']='2012-06-29'
    if kind=='selected_accession':r['accessions']=['0001144204-13-051364']
    if kind=='selected_hash':r['sourceFactHashes']=['f'*64]
    if kind=='reverse':r['continuingRevenueEvidence']['nodes'].reverse()
    if kind=='missing':r['continuingRevenueEvidence']['nodes'].pop()
    if kind=='cross_report':r['continuingRevenueEvidence']['nodes'][1]['segments']['url']=e['documents'][0]['url']
    r['sourceScopeHash']=digest(scope) if kind!='hash' else 'f'*64
    with pytest.raises((ValueError,FileNotFoundError)):m.approve(scope,e,tmp_path)


@pytest.mark.parametrize('old,new',[(b'208,000',b'209,000'),(b'56,999,000',b'57,207,000'),(b'14,537,000',b'14,538,000'),(b'(149,000)',b'(148,000)'),(b'59,000',b'60,000'),(b'Subtotal',b'Total'),(b'Discontinued',b'Continuing'),(b'Hotel operating expenses',b'Other expenses'),(b'2012</td>',b'2011</td>'),(b'$</td>',b'USD thousands</td>')])
def test_full_primary_segment_and_discontinued_note_are_source_bound(old,new,tmp_path,monkeypatch):
    scope,e=chain(tmp_path,monkeypatch);i=1;n=e['review']['continuingRevenueEvidence']['nodes'][i];sha=n['statement']['sha256'];body=gzip.decompress((tmp_path/'documents'/(sha+'.html.gz')).read_bytes());assert old in body;body=body.replace(old,new);sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());e['documents'][i]['sha256']=sha
    for j,k in enumerate(['statement','segments','discontinued']):n[k].update(sha256=sha,tableHash=digest(p.tables[j]))
    with pytest.raises(ValueError):m.approve(scope,e,tmp_path)
