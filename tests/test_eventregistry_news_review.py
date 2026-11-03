from pipeline.eventregistry_news_review import review_article


def sample(title, excerpt='', case='recent_NVDA'):
    return dict(uri='sample', title=title, excerpt=excerpt, date='2026-11-01', observedCases=[case])


def test_broker_symbol_or_brand_in_title_is_candidate_not_truth_or_direct_impact():
    r = review_article(sample('NVDA Price Prediction', "NVIDIA's tokenized stock on Binance Futures"))
    assert r['companyAssociations'][0]['classification'] == 'TITLE_MENTION_CANDIDATE'
    assert 'TOKENIZED_PRODUCT_DO_NOT_TREAT_AS_CASH_EQUITY_PRICE' in r['flags']
    assert not r['articleTruthVerified'] and r['eventDate'] is None


def test_other_company_title_stays_provider_association_only():
    r = review_article(sample('Is Palantir Stock a Buy?'))
    assert r['companyAssociations'][0]['classification'] == 'PROVIDER_ASSOCIATION_ONLY'


def test_old_year_in_current_provider_date_is_review_flag_not_claimed_event_date():
    r = review_article(sample('AFRICA: 05 MAI 2010', case='macro_rates'))
    assert 'OLDER_YEAR_IN_TITLE_POSSIBLE_REPUBLICATION_EVENT_DATE_UNCONFIRMED' in r['flags']
    assert r['eventDate'] is None
