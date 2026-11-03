import pytest
from pipeline.fiscal_period723 import VisibleText, declared_periods, netapp_period_evidence

POLICY = 'Our fiscal year is reported on a 52- or 53-week year that ends on the last Friday in April.'
DECLARATION = ('Fiscal year 2019, which ended on April 26, 2019, fiscal year 2018, '
               'which ended on April 27, 2018, and fiscal year 2017, which ended '
               'on April 28, 2017, were each 52-week years.')


def test_annual_start_comes_from_explicit_week_duration_not_january_first():
    r = declared_periods(POLICY + ' ' + DECLARATION)
    assert [(p['start'], p['end']) for p in r['periods']] == [
        ('2018-04-28', '2019-04-26'), ('2017-04-29', '2018-04-27'),
        ('2016-04-30', '2017-04-28')]
    assert all(p['inclusiveDays'] == 364 and not p['publicationAllowed'] for p in r['periods'])
    assert not r['publicationAllowed']


@pytest.mark.parametrize('before,after', [('were each 52-week', 'were each 53-week'),
    ('April 28, 2017', 'April 27, 2017'), ('fiscal year 2018', 'fiscal quarter 2018'),
    ('last Friday in April', 'last Friday in May'), ('were each', 'may have been'),
    ('2019, which', '2020, which')])
def test_ambiguous_or_different_calendar_cannot_derive_original_starts(before, after):
    with pytest.raises(ValueError): declared_periods((POLICY + ' ' + DECLARATION).replace(before, after))


def test_conflicting_declarations_require_review_instead_of_selecting_one():
    with pytest.raises(ValueError): declared_periods(POLICY + DECLARATION + POLICY + DECLARATION.replace('52-week', '53-week'))


def test_identical_management_and_notes_declarations_are_deduplicated():
    r = declared_periods(POLICY + DECLARATION + ' ' + POLICY.replace('that ends', 'ending') + DECLARATION)
    assert r['equivalentOccurrences'] == 2 and len(r['periods']) == 3 and not r['publicationAllowed']


def test_script_and_style_cannot_supply_a_fiscal_declaration():
    p = VisibleText()
    p.feed('<script>' + POLICY + DECLARATION + '</script><p>not established</p>')
    assert '52-week' not in ' '.join(p.parts)
    with pytest.raises(ValueError): declared_periods(' '.join(p.parts))


def test_unrelated_source_must_not_borrow_netapp_fiscal_policy():
    with pytest.raises(ValueError, match='SOURCE_CHANGED'): netapp_period_evidence((POLICY + DECLARATION).encode())
