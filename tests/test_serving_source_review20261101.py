"""Named source evidence, full period coverage and unchanged generic gates."""
import copy,json
from pathlib import Path
import pytest
from pipeline.serving_source_review20261101 import CASES,ALPHABET,TESLA,approve,metadata,scope_query
from pipeline.financial_reconciliation677 import validate_entry,entry_scope_query,SCOPE_SQL
from pipeline.reviewed_financial_mapping672 import digest

OUT=Path(__file__).resolve().parents[2]/'output/financial-service-followup-20261101'
ROOT=OUT/'candidate-bundle'

@pytest.fixture(scope='module')
def cases():
    if not (ROOT/'manifest.json').exists():pytest.skip('Optional exact live-source acceptance cache is absent; portable vectors are tested separately.')
    bundle=json.loads((ROOT/'manifest.json').read_text())
    scopes=json.loads((OUT/'fresh-targets.json').read_text())['targets']
    return [(next(t['scope'] for t in scopes if t['companyId']==e['review']['companyId']),e) for e in bundle['entries'] if e['review']['basis'] in {ALPHABET,TESLA}]

def test_actual_primary_sources_and_replay(cases):
    assert len(cases)==2
    for scope,entry in cases:
        a=validate_entry(scope,entry,ROOT);b=validate_entry(scope,entry,ROOT)
        assert a==b and a['proof_hash']==digest(a['proof'])
        assert a['proof']['originalObservationsPreserved'] and a['proof']['allActualPeriodStartsVerified']
        assert sum(x['claims'] for x in a['proof']['originalRawEvidence'])==len(scope)

@pytest.mark.parametrize('field,value',[
 ('company_id',7349),('metric_id',99),('metric_code','ASSETS'),('taxonomy','ifrs-full'),
 ('source_tag','SalesRevenueNet'),('unit','EUR'),('period_start','2017-10-01'),
 ('period_end','2019-12-31'),('value','999999'),('dimensions',{'Segment':'Other'}),
 ('source_fact_hash','0'*64),('accession_number','0000000000-00-000000'),
 ('filing_date','2027-01-01'),('report_date','2026-01-01'),('source_url','https://example.com/a'),('form_type','10-Q')])
def test_no_changed_observation_can_inherit_approval(cases,field,value):
    for scope,entry in cases:
        changed=copy.deepcopy(scope);changed[0][field]=value;e=copy.deepcopy(entry)
        e['review']['sourceScopeHash']=digest(changed)
        with pytest.raises(ValueError):validate_entry(changed,e,ROOT)

@pytest.mark.parametrize('field,value',[
 ('companyId',7349),('metricId',99),('metric','ASSETS'),('start','2017-10-01'),
 ('end','2019-12-31'),('chosenTag','SalesRevenueNet'),('sourceScopeHash','0'*64),
 ('value','999'),('filingDate','2027-01-01'),('accessions',[]),('sourceFactHashes',[]),
 ('officialDocuments',[]),('reviewExplanation',''),('reviewedOn','2016-01-01')])
def test_review_metadata_cannot_expand_contract(cases,field,value):
    for scope,entry in cases:
        e=copy.deepcopy(entry);e['review'][field]=value
        with pytest.raises(ValueError):validate_entry(scope,e,ROOT)

def test_partial_quarter_and_extra_observation_rejected(cases):
    scope,entry=next((s,e) for s,e in cases if e['review']['companyId']==2064)
    for changed in [scope[:-1],[x for x in scope if x['period_start']=='2018-01-01'],scope+[scope[0]]]:
        e=copy.deepcopy(entry);e['review']['sourceScopeHash']=digest(changed)
        with pytest.raises(ValueError):validate_entry(changed,e,ROOT)

def test_original_primary_witness_required_even_without_original_fact(cases):
    scope,entry=next((s,e) for s,e in cases if e['review']['companyId']==3238)
    assert all(x['report_date']!='2017-12-31' for x in scope)
    e=copy.deepcopy(entry);e['review']['originalFilingWitness']['filing_date']='2019-02-06'
    with pytest.raises(ValueError):validate_entry(scope,e,ROOT)
    sql,args=entry_scope_query(3238,entry['review'])
    assert 'SELECT count(*) FROM public.filing w' in sql and "w.id=1000000477729" in sql
    assert "w.filing_date='2018-02-06'" in sql and args==(3238,'2017-01-01','2017-12-31')

def test_end_only_mapping_binds_all_actual_starts(cases):
    _,entry=next((s,e) for s,e in cases if e['review']['companyId']==2064)
    sql,args=entry_scope_query(2064,entry['review'])
    assert 'f.period_start' not in sql.split('WHERE',1)[1] and args==(2064,'2018-12-31')

def test_unregistered_metrics_still_blocked():
    with pytest.raises(ValueError):entry_scope_query(3238,dict(metric='DILUTED_EPS',basis='GENERIC',start='2017-01-01',end='2017-12-31'))

def test_missing_or_changed_documents_and_raw_rejected(cases,tmp_path):
    import gzip,shutil
    scope,entry=cases[0];e=copy.deepcopy(entry);e['documents'][0]['sha256']='0'*64
    with pytest.raises((ValueError,FileNotFoundError)):validate_entry(scope,e,ROOT)
    # Validate all source documents normally, then point the policy at a
    # corrupt frozen structured artifact. No network fallback is permitted.
    (tmp_path/'raw-sec').mkdir();sha=CASES[entry['review']['companyId']]['raw']
    with gzip.open(tmp_path/'raw-sec'/(sha+'.json.gz'),'wb') as f:f.write(b'{}')
    with pytest.raises(ValueError,match='ORIGINAL_SEC_SOURCE_BYTES'):approve(scope,entry,tmp_path)

@pytest.fixture
def vectors(monkeypatch):
    import pipeline.serving_source_review20261101 as module
    data=json.loads((Path(__file__).parent/'fixtures/serving_source20261101/tables.json').read_text())
    def table(root,report,kind):
        cid=3238 if '1652044/' in report[0] else 2064
        t=data[str(cid)+':'+report[1]+':'+kind]
        return t['rows'],t['context']
    monkeypatch.setattr(module,'table',table)
    return module,data

def test_portable_annual_eps_class_and_quarter_equations(vectors):
    m,_=vectors
    eps=m.alphabet_vectors(None);annual,quarters=m.tesla_vectors(None)
    assert [v['2017']['eps'] for v in eps]==[18]*3
    assert annual[0]['2018']==21461268000 and annual[2]['2018']==21461000000
    assert quarters[0]['2018'][3]==7225873000 and quarters[1]['2018'][3]==7226000000

@pytest.mark.parametrize('kind',['annual_unit','annual_year','annual_value','quarter_unit','quarter_date','quarter_value','eps_value','eps_class','eps_unit'])
def test_portable_source_equation_guards(vectors,kind):
    m,data=vectors
    cid=3238 if kind.startswith('eps') else 2064
    report=m.REPORTS[cid][1];typ='note' if kind.startswith('quarter') or kind=='eps_class' else 'annual'
    t=data[str(cid)+':'+report[1]+':'+typ]
    if kind.endswith('unit'):t['context']=t['context'].replace('millions','thousands')
    elif kind=='annual_year':t['rows'][1]=['','2020','2018','2017']
    elif kind=='annual_value':next(r for r in t['rows'] if r and r[0]=='Total revenues')[7]='21,462'
    elif kind=='quarter_date':t['rows'][1]=['','March 31','June 30','September 30','December 30']
    elif kind=='quarter_value':next(r for r in t['rows'] if r and r[0]=='Total revenues' and '7,226' in r)[15]='7,227'
    elif kind=='eps_value':next(r for r in t['rows'] if r and r[0].startswith('Diluted net income per share of'))[7]='18.01'
    elif kind=='eps_class':t['rows'][4]=['','Class A','Class B','Class D']
    with pytest.raises(ValueError):
        m.alphabet_vectors(None) if cid==3238 else m.tesla_vectors(None)
