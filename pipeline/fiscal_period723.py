"""Local evidence for an explicit 52-week annual declaration, not approval.

Calendar-year assumptions are forbidden. A pinned statement witness and a
declaration explicitly naming every fiscal year are both required.
"""
import hashlib
import re
from datetime import date, timedelta
from html.parser import HTMLParser

from pipeline.legacy_statement_total699 import clean
from pipeline.statement_witness722 import PROFILES, source_witness


class VisibleText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'head'):
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'head'):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, value):
        if not self.hidden:
            self.parts.append(value)


def declared_periods(text):
    normalized = clean(text)
    declaration = ('fiscal year 2019, which ended on april 26, 2019, '
                   'fiscal year 2018, which ended on april 27, 2018, '
                   'and fiscal year 2017, which ended on april 28, 2017, '
                   'were each 52-week years.')
    matches = list(re.finditer(r'fiscal year 2019, which ended on .{0,350}?were each (?:52|53)-week years\.', normalized))
    if not matches or any(m.group() != declaration for m in matches):
        raise ValueError('FISCAL_DECLARATION_EXACT_YEARS_REQUIRED')
    # Restrict the company policy to the paragraph immediately before it.
    for match in matches:
        pos = match.start()
        context = normalized[max(0, pos - 500):pos]
        if not re.search(r'our fiscal year is reported on a 52- or 53-week year (?:that ends|ending) on the last friday in april\.', context):
            raise ValueError('FISCAL_DECLARATION_POLICY_REQUIRED')
    periods = []
    for end in ['2019-04-26', '2018-04-27', '2017-04-28']:
        start = date.fromisoformat(end) - timedelta(days=52 * 7 - 1)
        periods.append(dict(start=start.isoformat(), end=end, inclusiveDays=364,
                            basis='EXPLICIT_NAMED_52_WEEK_YEAR', publicationAllowed=False))
    return dict(declaration=declaration, equivalentOccurrences=len(matches), declarationSha256=hashlib.sha256(declaration.encode()).hexdigest(),
                periods=periods, publicationAllowed=False)


def netapp_period_evidence(body):
    # This also verifies exact source/table identity and all statement columns.
    witness = source_witness(body, 'NTAP')
    parser = VisibleText()
    parser.feed(body.decode('utf8', 'replace'))
    declaration = declared_periods(' '.join(parser.parts))
    assert [p['end'] for p in declaration['periods']] == PROFILES['NTAP']['ends']
    return dict(ticker='NTAP', documentSha256=witness['documentSha256'],
                statementTableHash=witness['tableHash'], **declaration,
                approvalGenerated=False, currentDatabaseRevalidated=False,
                requires=['ADJUSTMENT_SCHEDULE_UNIT_SCOPE', 'ALL_SOURCE_REPORTS',
                          'FRESH_DATABASE_SCOPE_AND_SERVING_VALIDATION'])
