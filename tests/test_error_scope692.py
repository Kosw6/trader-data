import gzip,hashlib
from copy import deepcopy
import pytest
from tests.test_bound_reconciliation685 import case
from pipeline.financial_reconciliation677 import validate_entry
from pipeline.bound_reconciliation685 import ERROR,POLICY as LEGACY
from pipeline.error_scope692 import POLICY,LocatedTables
from pipeline.reviewed_financial_mapping672 import digest

def replace_body(root,e,body):
    ev=e['review']['boundTableEvidence'];sha=hashlib.sha256(body).hexdigest()
    with gzip.open(root/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
    next(d for d in e['documents'] if d['url']==ev['url'])['sha256']=sha;ev['sha256']=sha
    p=LocatedTables();p.feed(body.decode());return p

def annual_range(root):
    scope,e=case(root);r=e['review'];ev=r['boundTableEvidence'];r['basis']=ERROR
    cause='The Company revised consolidated financial statements for each of the two years in the period ended June 30, 2024 to correct other immaterial errors.'
    old=gzip.decompress((root/'documents'/(ev['sha256']+'.html.gz')).read_bytes());body=old.replace(ev['causeExcerpt'].encode(),cause.encode())
    p=replace_body(root,e,body);ev.update(causeExcerpt=cause,tableHash=digest(p.tables[0]),causeAnnualRange={'phrase':'for each of the two years in the period ended June 30, 2024','years':2,'end':'2024-06-30'})
    return scope,e,body

def test_explicit_annual_range_confirms_target_without_inventing_a_cause_year(tmp_path):
    scope,e,_=annual_range(tmp_path);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['structuredErrorBinding']['period']['periodEnds']==['2024-06-30','2023-06-30']
    assert scope==before and '2023' not in e['review']['boundTableEvidence']['causeExcerpt']

@pytest.mark.parametrize('change',['count','different_date','not_explicit','target_outside','future_range','month_end_changed'])
def test_unproven_or_unrelated_range_is_rejected(tmp_path,change):
    scope,e,body=annual_range(tmp_path);ev=e['review']['boundTableEvidence'];x=ev['causeAnnualRange']
    if change=='count':x['years']=3
    if change=='different_date':x['end']='2024-12-31'
    if change=='not_explicit':x['phrase']='for the prior periods'
    if change=='target_outside':x.update(phrase='for each of the two years in the period ended June 30, 2025',end='2025-06-30');ev['causeExcerpt']=ev['causeExcerpt'].replace('2024','2025');body=body.replace(b'2024',b'2025')
    if change=='future_range':x.update(phrase='for each of the two years in the period ended June 30, 2026',end='2026-06-30');ev['causeExcerpt']=ev['causeExcerpt'].replace('2024','2026');body=body.replace(b'2024',b'2026')
    if change=='month_end_changed':x.update(phrase='for each of the two years in the period ended May 31, 2024',end='2024-05-31');ev['causeExcerpt']=ev['causeExcerpt'].replace('June 30, 2024','May 31, 2024');body=body.replace(b'June 30, 2024',b'May 31, 2024')
    p=replace_body(tmp_path,e,body);ev['tableHash']=digest(p.tables[0])
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)

def grouped(root):
    scope,e=case(root);r=e['review'];r['basis']=ERROR;ev=r['boundTableEvidence']
    cause='The Company revised consolidated financial statements to correct other immaterial errors impacting prior years.'
    intro='The effects of the Revision on our consolidated statements of income were as follows:'
    def table(year,a,adj,b):return f'<table><tr><td>Year Ended June 30, {year}</td></tr><tr><td>(in USD)</td></tr><tr><td>As Previously Reported</td><td>Brazil Adjustments</td><td>Other Immaterial Adjustments</td><td>As Revised</td></tr><tr><td>Revenue</td><td>{a}</td><td>—</td><td>{adj}</td><td>{b}</td></tr></table>'
    body=('<p>'+cause+'</p><p>'+intro+'</p>'+table(2024,200,20,220)+table(2023,100,10,110)).encode();p=replace_body(root,e,body)
    ev.update(tableHash=digest(p.tables[-1]),yearRow=0,valueRow=3,rowLabel='Revenue',orderedHeaders=['As Previously Reported','Brazil Adjustments Other Immaterial Adjustments','As Revised'],causeExcerpt=cause,componentLabels=['Brazil Adjustments','Other Immaterial Adjustments'],componentAmountsUSD=['0','10'],errorComponentEvidence=[{'label':'Brazil Adjustments','amountUSD':'0','explicitZero':True},{'label':'Other Immaterial Adjustments','amountUSD':'10','categoryPhrase':'Other Immaterial','causeExcerpt':cause}],causeRevisionGroup={'introExcerpt':intro,'tableHashes':[digest(t) for t in p.tables]})
    return scope,e,body

def test_prior_year_revision_group_and_zero_component_are_explicit(tmp_path):
    scope,e,_=grouped(tmp_path);before=deepcopy(scope);a=validate_entry(scope,e,tmp_path)
    assert a['proof']['policy']==POLICY and a['proof']['tableAmountsUSD']==['100','10','110']
    binding=a['proof']['structuredErrorBinding'];assert len(binding['components'])==2 and binding['components'][0]['explicitZero']
    assert binding['period']['periodEnds']==['2024-06-30','2023-06-30'] and scope==before

@pytest.mark.parametrize('change',['intro','duplicate_intro','wrong_members','order','missing_zero','zero_not_confirmed','missing_cause','unrelated_category','component_amount','component_order','cause_after_intro','unrelated_table'])
def test_group_and_components_cannot_hide_missing_or_unrelated_evidence(tmp_path,change):
    scope,e,body=grouped(tmp_path);ev=e['review']['boundTableEvidence'];group=ev['causeRevisionGroup']
    if change=='intro':group['introExcerpt']='The unrelated note discusses other statements and other issues.'
    if change=='duplicate_intro':body=('<p>'+group['introExcerpt']+'</p>').encode()+body
    if change=='wrong_members':group['tableHashes']=group['tableHashes'][-1:]
    if change=='order':body=body.replace(b'2024',b'2022')
    if change=='missing_zero':body=body.replace('<td>—</td>'.encode(),b'<td></td>')
    if change=='zero_not_confirmed':ev['errorComponentEvidence'][0]['explicitZero']=False
    if change=='missing_cause':ev['errorComponentEvidence'][1]['causeExcerpt']=''
    if change=='unrelated_category':ev['errorComponentEvidence'][1]['categoryPhrase']='Brazil'
    if change=='component_amount':ev['errorComponentEvidence'][1]['amountUSD']='11'
    if change=='component_order':ev['componentLabels'].reverse()
    if change=='cause_after_intro':body=body.replace(('<p>'+ev['causeExcerpt']+'</p>').encode(),b'');body+=('<p>'+ev['causeExcerpt']+'</p>').encode()
    if change=='unrelated_table':body=body.replace(b'</p><table>',b'</p><table><tr><td>Revenue</td><td>999</td></tr></table><table>',1)
    p=replace_body(tmp_path,e,body)
    # Rebind the deliberately changed bytes/table. The semantic guard still fails.
    ev['tableHash']=digest(p.tables[-1]);group['tableHashes']=[digest(t) for t in p.tables] if change not in {'wrong_members'} else group['tableHashes']
    with pytest.raises(ValueError):validate_entry(scope,e,tmp_path)

def test_legacy_proof_stays_on_its_original_policy(tmp_path):
    scope,e=case(tmp_path);assert validate_entry(scope,e,tmp_path)['proof']['policy']==LEGACY


def test_visible_coordinates_do_not_reuse_htmlparser_column_offset():
    parser=LocatedTables();parser.feed('<p data-padding="'+'x'*5000+'">Intro</p><table><tr><td>Revenue</td><td>10</td></tr></table><p>Tail</p>')
    visible=''.join(parser.visible);start,end=parser.spans[0]
    assert 0<=start<end<=len(visible) and visible[start:end].strip()=='Revenue 10'
    assert visible[:start].strip()=='Intro' and visible[end:].strip()=='Tail'
