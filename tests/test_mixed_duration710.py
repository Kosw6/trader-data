from copy import deepcopy
from datetime import date
import pytest
from pipeline.mixed_duration710 import review


def fixture(quarter_original=False):
    rows=[];index={};url='https://www.sec.gov/Archives/edgar/data/1/000000000121000001/annual.htm'
    for i,(start,value,report,filed) in enumerate([('2020-01-01','100','2020-12-31','2021-02-01'),('2020-01-01','100','2021-12-31','2022-02-01'),('2020-10-01','30','2020-12-31' if quarter_original else '2021-12-31','2021-02-01' if quarter_original else '2022-02-01')]):
        acc='0000000001-'+filed[2:4]+'-000001';h=hex(i+1)[2:]*64;u=url.replace('000000000121000001',acc.replace('-',''));r=dict(fact_id=i+1,company_id=1,metric_id=24,metric_code='NET_INCOME',taxonomy='us-gaap',source_tag='NetIncomeLoss',unit='USD',period_start=start,period_end='2020-12-31',value=value,dimensions=None,source_fact_hash=h,accession_number=acc,filing_date=filed,report_date=report,source_url=u,form_type='10-K',cik='1')
        rows.append(r);index[h]=dict(tag=r['source_tag'],unit=r['unit'],start=start,end=r['period_end'],value=value,accession=acc,filed=filed,form='10-K')
    return rows,index,dict(canonicalJsonSha256='a'*64,objectUri='local-cache:verified-fixture')


def test_verified_annual_does_not_authorize_unverified_quarter_or_whole_end_mapping():
    rows,index,source=fixture();before=deepcopy(rows);result=review(rows,index,source,today=date(2026,10,31))
    assert rows==before and not result['approvalGenerated'] and not result['publicationAllowed']
    a,q=result['subgroups'];assert a['sourceContract']=='VERIFIED_SUBGROUP_ONLY' and q['reason']=='ORIGINAL_REPORT_NOT_FOUND'
    assert result['wholeScope']==dict(existingPolicyWouldPass=False,reason='ORIGINAL_REPORT_NOT_FOUND')


def test_even_all_subgroups_verified_remain_diagnostic_without_a_publish_operation():
    rows,index,source=fixture(True);result=review(rows,index,source,today=date(2026,10,31))
    assert result['wholeScope']['existingPolicyWouldPass'] and all(s['sourceContract']=='VERIFIED_SUBGROUP_ONLY' for s in result['subgroups'])
    assert result['approvalGenerated']==result['publicationAllowed']==False


@pytest.mark.parametrize('change',['raw_missing','raw_value','raw_form','source_url','unit','annual_restatement'])
def test_exact_source_contracts_are_not_relaxed_by_splitting_periods(change):
    rows,index,source=fixture()
    if change=='raw_missing':del index[rows[0]['source_fact_hash']]
    elif change=='raw_value':index[rows[0]['source_fact_hash']]['value']='101'
    elif change=='raw_form':index[rows[0]['source_fact_hash']]['form']='10-K/A'
    elif change=='source_url':rows[0]['source_url']='https://example.com/annual.htm'
    elif change=='unit':rows[0]['unit']='EUR'
    elif change=='annual_restatement':rows[1]['value']='101';index[rows[1]['source_fact_hash']]['value']='101'
    result=review(rows,index,source,today=date(2026,10,31));assert result['subgroups'][0]['sourceContract']=='REVIEW_REQUIRED' and not result['publicationAllowed']


@pytest.mark.parametrize('change',['empty','single','mixed_company','missing_start','budget'])
def test_mixed_review_has_bounded_exact_identity(change):
    rows,index,source=fixture()
    if change=='empty':rows=[]
    elif change=='single':rows=rows[:2]
    elif change=='mixed_company':rows[-1]['company_id']=2
    elif change=='missing_start':rows[-1]['period_start']=None
    elif change=='budget':rows*=50
    with pytest.raises(ValueError):review(rows,index,source,today=date(2026,10,31))
