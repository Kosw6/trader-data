from copy import deepcopy
import pytest
from pipeline.source_inventory716 import classify

URL='https://www.sec.gov/Archives/edgar/data/1/000000000118000001/report.htm'


def scope():
    row=dict(company_id=1,cik='0000000001',metric_id=22,metric_code='REVENUE',period_start='2017-01-01',period_end='2017-12-31',report_date='2017-12-31',form_type='10-K',source_url=URL,taxonomy='us-gaap',unit='USD',dimensions=None)
    return dict(companyId=1,cik='0000000001',metricId=22,metric='REVENUE',end='2017-12-31',rows=[row])


def test_cached_original_and_all_reports_are_only_content_review_candidates():
    s=scope();before=deepcopy(s);r=classify(s,{URL});assert r['sourceClass']=='CACHED_CHAIN_REQUIRES_ACCOUNTING_CONTENT_REVIEW' and s==before
    assert not r['approvalGenerated'] and not r['publicationAllowed']


@pytest.mark.parametrize('key,value',[('period_start','2017-10-01'),('report_date','2018-12-31'),('form_type','10-Q')])
def test_later_or_quarterly_reports_do_not_become_original_annual_source(key,value):
    s=scope();s['rows'][0][key]=value;assert classify(s,{URL})['sourceClass']=='NO_EXACT_ORIGINAL_ANNUAL_OBSERVATION'


def test_an_uncached_original_or_later_report_is_distinguished():
    s=scope();assert classify(s,set())['sourceClass']=='ORIGINAL_DOCUMENT_CACHE_INCOMPLETE'
    s['rows'].append(dict(s['rows'][0],report_date='2018-12-31',source_url=URL+'-later'))
    assert classify(s,{URL})['sourceClass']=='ORIGINAL_CACHED_STORED_REPORT_CHAIN_INCOMPLETE'


def test_cached_form_can_still_lack_the_explicitly_referenced_primary_exhibit():
    assert classify(scope(),{URL},{URL})['sourceClass']=='INCORPORATED_PRIMARY_DOCUMENT_NOT_CACHED'


def test_an_end_scoped_mapping_is_not_approved_when_durations_are_mixed():
    s=scope();s['rows'].append(dict(s['rows'][0],period_start='2017-10-01'))
    r=classify(s,{URL});assert r['mixedAnnualAndShortDuration'] and r['durationDays']==[91,364] and not r['approvalGenerated']


@pytest.mark.parametrize('field,value,flag',[('unit','EUR','UNREVIEWED_UNIT'),('taxonomy','ifrs-full','UNREVIEWED_TAXONOMY'),('dimensions',{'segment':'A'},'DIMENSIONED_OBSERVATION'),('period_start','2017-99-01','INVALID_DATE')])
def test_source_cache_does_not_clear_raw_taxonomy_unit_dimension_or_date_review(field,value,flag):
    s=scope();s['rows'][0][field]=value;r=classify(s,{URL});assert flag in r['rawPolicyIssues'] and not r['approvalGenerated']


@pytest.mark.parametrize('key,value',[('company_id',2),('metric_id',1),('metric_code','NET_INCOME'),('period_end','2016-12-31')])
def test_mixed_company_metric_or_end_is_not_silently_merged(key,value):
    s=scope();s['rows'][0][key]=value;assert classify(s,{URL})['sourceClass']=='FROZEN_SCOPE_IDENTITY_REVIEW'


def test_instant_assets_use_actual_report_date_without_inventing_a_duration():
    s=scope();s.update(metric='ASSETS');s['rows'][0].update(metric_code='ASSETS',period_start=None)
    r=classify(s,{URL});assert r['originalObservationCount']==1 and r['durationDays']==[] and not r['approvalGenerated']


def test_other_form_and_malformed_url_are_held():
    s=scope();s['rows'][0]['source_url']=None
    assert classify(s,{URL})['sourceClass']=='ORIGINAL_SOURCE_REFERENCE_REVIEW'


def test_other_company_source_path_or_cik_is_not_a_verified_original_document():
    s=scope();s['rows'][0]['source_url']=URL.replace('/data/1/','/data/2/')
    assert classify(s,{s['rows'][0]['source_url']})['sourceClass']=='ORIGINAL_SOURCE_REFERENCE_REVIEW'
    s['rows'][0]['cik']='2';assert classify(s,set())['sourceClass']=='FROZEN_SCOPE_IDENTITY_REVIEW'
