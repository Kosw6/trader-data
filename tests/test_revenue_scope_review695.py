import pytest
from pipeline.revenue_scope_review695 import classify,statement_cards


def doc(amount,*,label='Total revenues',unit='In millions',year=2010,title='Statements of Consolidated Earnings'):
    return (f'<p>{title} ({unit})</p><table><tr><td>Years ended June 30,</td><td>{year}</td></tr><tr><td>{label}</td><td>{amount}</td></tr></table>').encode()


def item():
    return {'end':'2010-06-30','original':'70000000','chosen':'90000000','originalReportUrls':['https://www.sec.gov/original'],'selectedReportUrls':['https://www.sec.gov/later']}


@pytest.mark.parametrize('original,later,raw,chosen,expected',[
    (90,90,70,90,'PARTIAL_ORIGINAL_SOURCE_COVERAGE_REVIEW_REQUIRED'),
    (90,90,90,15,'SELECTED_TAG_MAY_NOT_BE_TOTAL_REVENUE'),
    (90,89,70,89,'COMPONENT_COVERAGE_AND_TOTAL_CHANGE_REVIEW_REQUIRED'),
    (90,89,90,89,'REPORTED_TOTAL_CHANGE_REVIEW_REQUIRED'),
])
def test_coverage_types_are_review_hints_never_accounting_cause_or_approval(original,later,raw,chosen,expected):
    q=item();q.update(original=str(raw*1000000),chosen=str(chosen*1000000))
    result=classify(q,[({'url':q['originalReportUrls'][0]},doc(original)),({'url':q['selectedReportUrls'][0]},doc(later))])
    assert result['classification']==expected
    assert result['requiresHumanReview'] and not result['approvalGenerated'] and not result['restatementCauseInferred']
    assert result['databaseWrites']==0
    assert all(c['periodBinding']=='YEAR_COLUMN_ONLY_ACTUAL_PERIOD_REVIEW_REQUIRED' for c in result['statementCards'])


@pytest.mark.parametrize('kwargs',[{'label':'Cost of revenues'},{'unit':'USD'},{'year':2011},{'title':'Business segment revenue'},{'label':'Sales of one business'}])
def test_wrong_scope_cost_units_or_year_do_not_make_total_cards(kwargs):
    assert not statement_cards(doc(90,**kwargs),item())


def test_unbound_urls_and_multiple_totals_do_not_choose_a_value():
    q=item();documents=[({'url':'https://www.sec.gov/unknown'},doc(90))]
    assert classify(q,documents)['classification']=='INSUFFICIENT_BOUND_STATEMENT_CARDS'
    documents=[({'url':q['originalReportUrls'][0]},doc(90)),({'url':q['originalReportUrls'][0]},doc(89)),({'url':q['selectedReportUrls'][0]},doc(90))]
    assert classify(q,documents)['classification']=='AMBIGUOUS_STATEMENT_TOTALS_REVIEW_REQUIRED'


def test_bulk_screen_keeps_original_accounting_review_status_and_adds_cards():
    from workers.bulk_reconciliation681 import screen
    q=item();q.update(companyId=1,start='2009-07-01',adjustment='20000000')
    r=screen(q,[({'url':q['originalReportUrls'][0]},doc(90)),({'url':q['selectedReportUrls'][0]},doc(90))])
    assert r['status']=='NO_EXACT_RECONCILIATION_FOUND' and r['approvalGenerated'] is False
    assert r['scopeCoverageReview']['classification']=='PARTIAL_ORIGINAL_SOURCE_COVERAGE_REVIEW_REQUIRED'


def test_parse_cache_is_bounded_and_bound_to_actual_bytes_not_url_or_period():
    from pipeline.revenue_scope_review695 import StatementCache
    cache=StatementCache();q=item()
    first=statement_cards(doc(90),q,cache=cache)
    assert first==statement_cards(doc(90),q,cache=cache)
    assert statement_cards(doc(91),q,cache=cache)[0]['documentSha256']!=first[0]['documentSha256']
    assert not statement_cards(doc(90),dict(q,end='2011-06-30'),cache=cache)
    statement_cards(doc(92),q,cache=cache);assert len(cache._items)==2


def test_cached_classification_matches_uncached_without_reusing_financial_conclusion():
    from pipeline.revenue_scope_review695 import StatementCache
    q=item();docs=[({'url':q['originalReportUrls'][0]},doc(90)),({'url':q['selectedReportUrls'][0]},doc(90))];cache=StatementCache()
    assert classify(q,docs,cache=cache)==classify(q,docs)
    assert classify(dict(q,chosen='80000000'),docs,cache=cache)['classification']=='SOURCE_TAG_AND_STATEMENT_COVERAGE_REVIEW_REQUIRED'
