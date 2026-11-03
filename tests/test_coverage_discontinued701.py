import gzip
import hashlib
from copy import deepcopy
import pytest
from tests.test_revenue_coverage694 import integrated
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.coverage_discontinued701 import BASIS, POLICY
from pipeline.financial_reconciliation677 import validate_entry


def fixture(tmp_path):
    rows,entry=integrated(tmp_path);r=entry['review'];old=r['selectedTotalEvidence'];original=r['originalCoverageEvidence'];oldbody=gzip.decompress((tmp_path/'documents'/(old['sha256']+'.html.gz')).read_bytes()).decode()
    total=oldbody.replace('<td>70</td>','<td>65</td>').replace('<td>90</td>','<td>85</td>')
    unit='Notes to Consolidated Financial Statements (Tabular dollars in millions, except per share amounts)'
    cause='In connection with the disposal of Taxware, the Company has classified the results of this business as discontinued operations for all periods presented.'
    note='<p>'+unit+'</p><p>'+cause+'</p><p>Operating results for discontinued operations were as follows:</p><table><tr><td>Years ended June 30,</td><td>2012</td><td>2011</td><td>2010</td></tr><tr><td>Revenues</td><td>10</td><td>7</td><td>5</td></tr></table>'
    body=(total+note).encode();sha=hashlib.sha256(body).hexdigest();(tmp_path/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(body));p=Tables();p.feed(body.decode());assert len(p.tables)==2
    old.update(sha256=sha,tableHash=digest(p.tables[0]));entry['documents'][1]['sha256']=sha
    rows[1]['value']='85000000';r.update(basis=BASIS,value='85000000',originalStatementTotal='90000000',discontinuedRevenue='5000000',sourceScopeHash=digest(rows))
    r['discontinuedEvidence']=dict(url=old['url'],sha256=sha,tableHash=digest(p.tables[1]),yearRow=0,valueRow=1,rowLabel='Revenues',fiscalYear=2010,annualHeading='Years ended June 30,',unitScale=1000000,notesUnitDisclosure=unit,tableContextExcerpt=p.contexts[1],businesses=['Taxware'],causeExcerpts=[cause])
    return rows,entry


def test_partial_to_total_then_exact_discontinued_bridge_preserves_raw(tmp_path):
    rows,entry=fixture(tmp_path);before=deepcopy(rows);a=validate_entry(rows,entry,tmp_path)
    assert rows==before and a['proof']['policy']==POLICY
    assert a['proof']['bridge']==dict(originalRawComponentUSD='70000000',originalStatementTotalUSD='90000000',latestDiscontinuedRevenueUSD='5000000',latestContinuingTotalUSD='85000000')
    assert not a['proof']['economicGrowthInferred'] and a['proof']['originalObservationsPreserved']
    assert a['proof_hash']==validate_entry(rows,entry,tmp_path)['proof_hash']


@pytest.mark.parametrize('change',[
    'original_total','removed','rounding_residue','negative_removed','source_scope','original_url','selected_url',
    'component_url','unit','unit_disclosure','context','business','cause','missing_business_cause','fiscal_year',
    'annual_heading','year_row','value_row','label','table_hash','selected_value','original_component',
])
def test_each_stage_requires_its_own_explicit_period_source_and_cause(tmp_path,change):
    rows,entry=fixture(tmp_path);r=entry['review'];e=r['discontinuedEvidence']
    if change=='original_total':r['originalStatementTotal']='91000000';r['discontinuedRevenue']='6000000'
    elif change=='removed':r['discontinuedRevenue']='6000000'
    elif change=='rounding_residue':r['originalStatementTotal']='90100000'
    elif change=='negative_removed':r['discontinuedRevenue']='-5000000'
    elif change=='source_scope':r['sourceScopeHash']='f'*64
    elif change=='original_url':r['originalCoverageEvidence']['url']=r['selectedTotalEvidence']['url']
    elif change=='selected_url':r['selectedTotalEvidence']['url']=r['originalCoverageEvidence']['url']
    elif change=='component_url':e['url']=r['originalCoverageEvidence']['url']
    elif change=='unit':e['unitScale']=1000
    elif change=='unit_disclosure':e['notesUnitDisclosure']='All dollar amounts in millions'
    elif change=='context':e['tableContextExcerpt']='An unrelated note about discontinued operations without matching context.'
    elif change=='business':e['businesses']=['OtherBusiness']
    elif change=='cause':e['causeExcerpts']=['A derivative cash flow is reclassified into income from discontinued operations for all periods presented.']
    elif change=='missing_business_cause':e['businesses']=['Taxware','OHS']
    elif change=='fiscal_year':e['fiscalYear']=2011
    elif change=='annual_heading':e['annualHeading']='Years ended July 31,'
    elif change=='year_row':e['yearRow']=1
    elif change=='value_row':e['valueRow']=0
    elif change=='label':e['rowLabel']='Gain on disposal'
    elif change=='table_hash':e['tableHash']='f'*64
    elif change=='selected_value':rows[1]['value']='86000000';r['sourceScopeHash']=digest(rows)
    elif change=='original_component':rows[0]['value']='71000000';r['sourceScopeHash']=digest(rows)
    with pytest.raises(ValueError):validate_entry(rows,entry,tmp_path)


def test_global_note_unit_is_revalidated_from_actual_document_bytes(tmp_path):
    rows,entry=fixture(tmp_path);validate_entry(rows,entry,tmp_path);e=entry['review']['discontinuedEvidence'];p=tmp_path/'documents'/(e['sha256']+'.html.gz');body=gzip.decompress(p.read_bytes());p.write_bytes(gzip.compress(body.replace(b'Tabular dollars in millions',b'Tabular dollars in thousands')))
    with pytest.raises(ValueError,match='DOCUMENT_CHANGED'):validate_entry(rows,entry,tmp_path)
