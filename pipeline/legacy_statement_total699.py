"""Explicit full-statement legacy totals despite conflicting modern raw tags.

No global alias, no numeric-priority fallback, and no approval without the
original, latest legacy, and conflicting modern report's full statement.
"""
import gzip
import hashlib
import re
from datetime import datetime
from decimal import Decimal
from pipeline.revenue_scope_review695 import StatementCache
from pipeline.reconciliation_review679 import numeric
from pipeline.bound_reconciliation685 import norm, period_years
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.financial_mapping667 import invalid

BASIS = 'REVIEWED_LEGACY_FULL_STATEMENT_TOTAL'
POLICY = 'sec-reviewed-legacy-full-statement-total699-v1'
DATE = re.compile(r'([A-Za-z]+)\s+(\d{1,2}),?\s*((?:19|20)\d{2})')
LABELS = {'net sales', 'net revenues', 'revenues', 'total revenues', 'total revenue', 'total net sales', 'sales'}
_CACHE = StatementCache()


def clean(value):
    text = norm(value.replace('\u200b', '').replace('\ufeff', ''))
    return re.sub(r'\bst at ements\b', 'statements', text)


def document(binding, root):
    sha = binding['sha256']
    if not re.fullmatch('[a-f0-9]{64}', sha):
        raise ValueError('LEGACY_TOTAL_DOCUMENT_HASH')
    # Read and hash actual bytes on every replay. Only parsing is cached, with
    # a private two-document maximum; URL/period decisions are never cached.
    with gzip.open(root/'documents'/(sha+'.html.gz'), 'rb') as stream:
        body = stream.read(32*1024**2+1)
    if len(body) > 32*1024**2 or hashlib.sha256(body).hexdigest() != sha:
        raise ValueError('LEGACY_TOTAL_DOCUMENT_CHANGED')
    _, (tables, contexts) = _CACHE.parse(body)
    matches = [(rows, context) for rows,context in zip(tables,contexts) if digest(rows) == binding['tableHash']]
    if len(matches) != 1:
        raise ValueError('LEGACY_TOTAL_EXACT_TABLE_REQUIRED')
    return *matches[0], None


def statement(entry, root, binding, end, expected, *, _check_competing=True):
    if (binding['url'], binding['sha256']) not in {(d['url'], d['sha256']) for d in entry['documents']}:
        raise ValueError('LEGACY_TOTAL_DOCUMENT_IDENTITY')
    rows, context, _ = document(binding, root)
    yr, vr = binding['periodRow'], binding['valueRow']
    if type(yr) is not int or type(vr) is not int or not 0 <= yr < vr < len(rows):
        raise ValueError('LEGACY_TOTAL_ROW_BINDING')
    label = clean(rows[vr][0])
    related = binding.get('totalContract') == 'NET_SALES_RELATED_PARTY_SUM699_V1'
    if (not related and label not in LABELS) or label != clean(binding['rowLabel']):
        raise ValueError('LEGACY_TOTAL_FULL_REVENUE_LABEL_REQUIRED')
    top = clean(context + ' ' + ' '.join(' '.join(r) for r in rows[:vr]))
    if not re.search(r'consolidated (?:statements? of (?:income|operations|earnings)|results of operations)', top):
        raise ValueError('LEGACY_TOTAL_CONSOLIDATED_STATEMENT_REQUIRED')
    if not re.search(r'\byears?[- ]ended\b', top):
        raise ValueError('LEGACY_TOTAL_ANNUAL_HEADING')
    if any(period_years(r) or DATE.search(' '.join(r)) for r in rows[yr+1:vr]):
        raise ValueError('LEGACY_TOTAL_CROSS_PERIOD_BINDING')
    mode = binding['periodMode']
    if mode == 'DATE':
        keys = [datetime.strptime(' '.join(m.groups()), '%B %d %Y').date().isoformat() for c in rows[yr] for m in DATE.finditer(c)]
    elif mode == 'YEAR':
        if any(DATE.search(cell) for cell in rows[yr]):
            raise ValueError('LEGACY_TOTAL_FULL_DATE_AS_YEAR_NOT_ALLOWED')
        years = period_years(rows[yr]);heading = binding['annualHeading']
        if not re.fullmatch(r'(?:for the )?years? ended (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2},?', heading, re.I) or norm(heading) not in top:
            raise ValueError('LEGACY_TOTAL_ANNUAL_DATE_REQUIRED')
        md = re.sub(r'^(?:for the )?years? ended ', '', heading, flags=re.I)
        keys = [datetime.strptime(md.rstrip(',')+', '+y, '%B %d, %Y').date().isoformat() for y in years]
    elif mode == 'ANNUAL_HEADER_DATES':
        heading = binding['annualDateHeading']
        if len(heading) > 500 or clean(heading) not in top or not re.match(r'^(?:for the )?years? ended ', clean(heading)):
            raise ValueError('LEGACY_TOTAL_EXPLICIT_ANNUAL_DATES')
        keys = [datetime.strptime(' '.join(m.groups()), '%B %d %Y').date().isoformat() for m in DATE.finditer(heading)]
        fiscal = period_years(rows[yr])
        if len(fiscal) != len(keys) or any(int(a)-int(b) != 1 for a,b in zip(fiscal,fiscal[1:])) or end not in keys or fiscal[keys.index(end)] != str(binding['targetFiscalYear']):
            raise ValueError('LEGACY_TOTAL_FISCAL_DATE_ALIGNMENT')
        dates = [datetime.strptime(k, '%Y-%m-%d').date() for k in keys]
        if any(not 350 <= (a-b).days <= 380 for a,b in zip(dates,dates[1:])) or any(not 0 <= d.year-int(fy) <= 1 for d,fy in zip(dates,fiscal)):
            raise ValueError('LEGACY_TOTAL_ANNUAL_DATE_SPACING')
    else:
        raise ValueError('LEGACY_TOTAL_PERIOD_MODE')
    if not 2 <= len(keys) <= 3 or len(set(keys)) != len(keys) or keys.count(end) != 1:
        raise ValueError('LEGACY_TOTAL_PERIOD_COLUMNS')
    scale = Decimal(str(binding['unitScale']))
    expected_unit = {Decimal(1000): 'thousands', Decimal(1000000): 'millions'}.get(scale)
    units = re.findall(r'\bin\s+(thousands|millions)\b', top)
    if not expected_unit or set(units) != {expected_unit}:
        raise ValueError('LEGACY_TOTAL_UNIT')
    values = [v for c in rows[vr][1:] if (v := numeric(c)) is not None]
    if len(values) != len(keys) or any(not v.is_finite() for v in values):
        raise ValueError('LEGACY_TOTAL_VALUE_COLUMNS')
    total = values[keys.index(end)]*scale
    if total != expected:
        raise ValueError('LEGACY_TOTAL_STATEMENT_VALUE')
    if related:
        if label not in {'', 'total net sales'} or vr < yr+4 or [clean(rows[i][0]) for i in range(vr-3, vr)] != ['net sales', 'unrelated parties', 'related parties']:
            raise ValueError('LEGACY_TOTAL_RELATED_PARTY_HIERARCHY')
        if any(numeric(c) is not None for c in rows[vr-3][1:]):
            raise ValueError('LEGACY_TOTAL_RELATED_PARTY_HEADING')
        components = [[v for c in rows[i][1:] if (v := numeric(c)) is not None] for i in [vr-2, vr-1]]
        if any(len(v) != len(keys) for v in components) or any(a+b != t for a,b,t in zip(*components, values)):
            raise ValueError('LEGACY_TOTAL_RELATED_PARTY_EQUATION')
    modern_value = Decimal(entry['review']['conflictingModernValue']) if _check_competing else None
    # A different legitimate total/net-revenue line cannot be dismissed as a
    # component just because the operator prefers an older tag. Unlabelled
    # statement subtotals also require separate concept/adjustment review.
    for row in rows[yr+1:min(len(rows), vr+5)]:
        if _check_competing and (clean(row[0]) in LABELS or not clean(row[0])) and any((v:=numeric(c)) is not None and v*scale == modern_value for c in row[1:]):
            raise ValueError('LEGACY_TOTAL_COMPETING_STATEMENT_TOTAL')
    return {'documentSha256': binding['sha256'], 'tableHash': binding['tableHash'], 'periodEnd': end, 'rowLabel': binding['rowLabel'], 'totalUSD': str(total), 'periodMode': mode}


def approve(scope, entry, root):
    r = entry['review']
    if r['basis'] != BASIS or r['chosenTag'] != 'SalesRevenueNet':
        raise ValueError('LEGACY_TOTAL_BASIS_OR_TAG')
    checked = reviewed_approval(scope, {**r, 'basis': 'UNCHANGED_TOTAL_CONFIRMED'}, _reviewed_legacy_total=True)
    value = Decimal(r['value'])
    # The old tag must itself be unchanged across EVERY stored annual filing.
    legacy = [x for x in scope if x['source_tag'] == 'SalesRevenueNet']
    if not legacy or any(Decimal(str(x['value'])) != value for x in legacy):
        raise ValueError('LEGACY_TOTAL_CHANGED_OBSERVATION')
    for row in scope:
        shadow = dict(row, report_date=row['period_end'])
        if row['source_tag'] == 'SalesRevenueNet':
            shadow['source_tag'] = 'Revenues'
        reason = invalid(shadow, annual_only=True)
        if reason:
            raise ValueError('LEGACY_TOTAL_SOURCE_'+reason)
    tag = r['conflictingModernTag']
    if tag not in {'Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax'}:
        raise ValueError('LEGACY_TOTAL_MODERN_TAG')
    modern = [x for x in scope if x['source_tag'] == tag]
    if not modern:
        raise ValueError('LEGACY_TOTAL_CONFLICT_NOT_FOUND')
    latest = max(str(x['filing_date']) for x in modern)
    modern = [x for x in modern if str(x['filing_date']) == latest]
    modern_value = Decimal(r['conflictingModernValue'])
    if {Decimal(str(x['value'])) for x in modern} != {modern_value} or modern_value == value:
        raise ValueError('LEGACY_TOTAL_CONFLICT_VALUE')
    if {x['source_fact_hash'] for x in modern} != set(r['conflictingModernFactHashes']) or {x['accession_number'] for x in modern} != set(r['conflictingModernAccessions']) or latest != r['conflictingModernFilingDate']:
        raise ValueError('LEGACY_TOTAL_CONFLICT_SOURCE_CHANGED')
    e = r['statementTotalEvidence']
    roles = [('original', checked['proof']['originalSources']), ('latestLegacy', checked['proof']['sources']), ('modernReport', modern)]
    statements = {}
    for role, sources in roles:
        binding = e[role]
        if {x['source_url'] for x in sources} != {binding['url']}:
            raise ValueError('LEGACY_TOTAL_FILING_BINDING')
        if binding.get('periodMode') == 'ANNUAL_HEADER_DATES' and any(str(x['fiscal_year']) != str(binding.get('targetFiscalYear')) for x in checked['proof']['originalSources']):
            raise ValueError('LEGACY_TOTAL_ORIGINAL_FISCAL_YEAR')
        statements[role] = statement(entry, root, binding, r['end'], value)
    proof = {**checked['proof'], 'policy': POLICY, 'review': r, 'accountingCause': 'SOURCE_TAG_REQUIRES_DOCUMENT_TOTAL_SELECTION', 'statementTotals': statements, 'conflictingModernSources': modern, 'restatementClaimed': False, 'servingTagContract': 'EXPLICIT_COMPANY_PERIOD_LEGACY_STATEMENT_TOTAL699_V1', 'mappingScope': 'COMPANY_METRIC_PERIOD_END_ONLY'}
    return {**checked, 'proof': proof, 'proof_hash': digest(proof)}
