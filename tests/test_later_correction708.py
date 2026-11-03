import gzip,hashlib,json
from copy import deepcopy
import pytest
from pipeline.later_correction708 import corrections,validate_watchlist
from pipeline.service_error708 import correction
from pipeline.financial_reconciliation677 import load_bundle,CONTRACT
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

CAUSE='We identified an immaterial error related to the commencement of revenue recognition for certain FortiCare support service contracts, which resulted in an understatement of revenue during the years ended 2019 and 2018, respectively. All financial information has been revised.'
UNIT='The corrections to our consolidated statements of income for the year ended December 31, 2019 and December 31, 2018 were as follows (in millions, except per share amounts):'

def tr(row):return '<tr>'+''.join('<td>'+str(x)+'</td>' for x in row)+'</tr>'

def body(early=False):
    years=['2017','2016'] if early else ['2019','2018']
    rows=[[''],['','Year Ended December 31,'],['',*years],['',*(['As Previously Reported','Corrections','As Corrected']*2)]]
    if early:
        rows += [['(in millions, except per share amounts)'],['Total revenue','1494.9','2.8','1497.7','1275.4','3.1','1278.5']]
    else:
        rows += [['REVENUE:'],['Service','1367.7','6.8','1374.5','1126.8','3.4','1130.2'],['Total revenue','2156.2','6.8','2163.0','1801.2','3.4','1804.6'],['GROSS PROFIT:'],['Service','1186.4','6.8','1193.2','967.4','3.4','970.8'],['Total gross profit','1650.3','6.8','1657.1','1350.8','3.4','1354.2'],['OPERATING INCOME','344.2','6.8','351.0','231.0','3.4','234.4'],['INCOME BEFORE INCOME TAXES','379.2','6.8','386.0','250.9','3.4','254.3'],['PROVISION FOR INCOME TAXES','52.7','1.6','54.3','(81.3)','0.7','(80.6)'],['NET INCOME','326.5','5.2','331.7','332.2','2.7','334.9']]
    return ('<p>'+CAUSE+'</p>'+(('<p>'+UNIT+'</p>') if not early else '')+'<table>'+''.join(tr(r) for r in rows)+'</table>').encode()

def bind(root,data):
    (root/'documents').mkdir(exist_ok=True);sha=hashlib.sha256(data).hexdigest();(root/'documents'/(sha+'.html.gz')).write_bytes(gzip.compress(data));p=Tables();p.feed(data.decode());d=dict(url='https://www.sec.gov/Archives/edgar/data/1262039/000126203921000008/annual.htm',sha256=sha)
    e=dict(**d,tableHash=digest(p.tables[0]),causeExcerpt=CAUSE)
    return d,e

def test_later_explicit_correction_never_becomes_rounding_or_fabricated_fact():
    data=body(True);copy=bytes(data);a=corrections(data,'2017-12-31');assert data==copy and len(a)==1
    assert a[0]['previouslyReportedUSD']=='1494900000.0' and a[0]['correctionUSD']=='2800000.0' and a[0]['correctedUSD']=='1497700000.0'
    assert corrections(data,'2015-12-31')==[] and corrections(data,'2017-09-30')==[]

@pytest.mark.parametrize('old,new',[
    ('immaterial error','presentation change'),('revenue recognition','reclassification'),('understatement of revenue','precision change'),
    ('2017','2016'),('Year Ended December 31,','Nine Months Ended December 31,'),('Corrections','As corrected'),
    ('1497.7','1497.8'),('in millions','in thousands'),('Total revenue','Product revenue'),('2.8','0.0')])
def test_detector_does_not_infer_cause_units_periods_or_balancing_values(old,new):
    assert corrections(body(True).replace(old.encode(),new.encode()),'2017-12-31')==[]

def test_verified_watchlist_rejects_same_scope_in_registered_entries(tmp_path):
    data=body(True);d,e=bind(tmp_path,data);item=dict(companyId=1919,metricId=22,cik='1262039',end='2017-12-31',document=d,evidence=corrections(data,'2017-12-31')[0]);bundle=dict(contract=CONTRACT,entries=[],lateCorrectionWatchlist=[item]);assert len(validate_watchlist(bundle,tmp_path))==1
    path=tmp_path/'manifest.json';path.write_text(json.dumps(bundle));assert load_bundle(path)[0]==bundle
    bundle['entries']=[dict(review=dict(companyId=1919,metricId=22,end='2017-12-31'))];path.write_text(json.dumps(bundle))
    with pytest.raises(ValueError,match='REGISTERED_SCOPE_BLOCKED'):load_bundle(path)

@pytest.mark.parametrize('change',['sha','url','cik','value','duplicate','string_id','evidence_period','budget'])
def test_watchlist_identity_exact_source_and_budget_fail_closed(tmp_path,change):
    data=body(True);d,e=bind(tmp_path,data);item=dict(companyId=1919,metricId=22,cik='1262039',end='2017-12-31',document=d,evidence=corrections(data,'2017-12-31')[0]);b=dict(lateCorrectionWatchlist=[item])
    if change=='sha':d['sha256']='a'*64
    elif change=='url':d['url']='https://example.com/annual.htm'
    elif change=='cik':item['cik']='1'
    elif change=='value':item['evidence']['correctedUSD']='1494900000'
    elif change=='duplicate':b['lateCorrectionWatchlist'].append(deepcopy(item))
    elif change=='string_id':item['companyId']='1919'
    elif change=='evidence_period':item['end']='2016-12-31'
    elif change=='budget':b['lateCorrectionWatchlist']*=257
    with pytest.raises((ValueError,FileNotFoundError)):validate_watchlist(b,tmp_path)

def test_explicit_service_error_binds_both_years_and_net_income_tax_equation(tmp_path):
    d,e=bind(tmp_path,body());p=correction(dict(documents=[d]),tmp_path,e,'2018-12-31')
    assert p['previouslyReportedUSD']=='1801200000.0' and p['correctionUSD']=='3400000.0' and p['correctedUSD']=='1804600000.0' and p['allComparativeColumnsVerified']

@pytest.mark.parametrize('old,new',[
    ('FortiCare support service contracts','other contracts'),('immaterial error','rounding precision'),('in millions','in thousands'),
    ('As Previously Reported','As Corrected'),('2018','2017'),('1804.6','1804.7'),('1367.7','1367.8'),
    ('234.4','234.5'),('334.9','334.8'),('0.7','0.8'),('Total revenue','Product revenue')])
def test_service_error_rehashed_but_wrong_cause_or_other_year_equations_rejected(tmp_path,old,new):
    data=body().replace(old.encode(),new.encode());d,e=bind(tmp_path,data)
    with pytest.raises(ValueError):correction(dict(documents=[d]),tmp_path,e,'2018-12-31')
