"""Registered nested revenue totals and original rounded-to-detailed bridges.

These contracts replay source documents. They never infer a restatement from a
numeric difference, and do not broaden any previous precision/coverage policy.
"""
import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pipeline.reviewed_evidence683 import document
from pipeline.bound_reconciliation685 import period_years, norm
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest

NESTED = 'REVIEWED_NESTED_REVENUE_COMPONENT_COVERAGE'
PRECISE = 'REVIEWED_ORIGINAL_ROUNDED_TO_DETAILED_REVENUE'


def table(entry, root, binding, end, *, summary=False):
    if (binding['url'], binding['sha256']) not in {(d['url'], d['sha256']) for d in entry['documents']}:
        raise ValueError('BRIDGE_DOCUMENT_IDENTITY')
    rows, context, _ = document(binding, root)
    yr = binding['yearRow']
    if type(yr) is not int or not 0 <= yr < len(rows):
        raise ValueError('BRIDGE_YEAR_ROW')
    years = period_years(rows[yr])
    target = str(binding['fiscalYear'])
    if not 2 <= len(years) <= 3 or len(set(years)) != len(years) or years.count(target) != 1:
        raise ValueError('BRIDGE_YEAR_COLUMNS')
    heading = binding['annualHeading']
    if not re.fullmatch(r'for the years ended (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2},', heading, re.I):
        raise ValueError('BRIDGE_ANNUAL_HEADING')
    top = norm(context + ' ' + ' '.join(' '.join(r) for r in rows[:yr+1]))
    if norm(heading) not in top:
        raise ValueError('BRIDGE_ANNUAL_HEADING')
    if datetime.strptime(re.sub(r'^for the years ended ', '', heading, flags=re.I) + ' ' + target, '%B %d, %Y').date().isoformat() != end:
        raise ValueError('BRIDGE_PERIOD_END')
    if summary:
        if 'consolidated net sales' not in top:
            raise ValueError('BRIDGE_CONSOLIDATED_SALES_REQUIRED')
    elif not re.search(r'consolidated statements of (?:operations|comprehensive income)', top):
        raise ValueError('BRIDGE_CONSOLIDATED_STATEMENT_REQUIRED')
    scale = Decimal(str(binding['unitScale']))
    unit = {Decimal(1000): 'in thousands', Decimal(1000000): 'in millions'}.get(scale)
    if not unit or unit not in top:
        raise ValueError('BRIDGE_UNIT')

    def values(index, label, *, zero_dash=False):
        if type(index) is not int or not yr < index < len(rows) or norm(rows[index][0]) != norm(label):
            raise ValueError('BRIDGE_ROW_LABEL')
        if any(period_years(r) for r in rows[yr+1:index]):
            raise ValueError('BRIDGE_CROSS_PERIOD_BINDING')
        cells = [c for c in rows[index][1:] if numeric(c) is not None or (zero_dash and c.strip() in {'—', '–', '-'})]
        if len(cells) != len(years):
            raise ValueError('BRIDGE_VALUE_COLUMNS')
        result = [(numeric(c) if numeric(c) is not None else Decimal(0)) * scale for c in cells]
        if any(not v.is_finite() for v in result):
            raise ValueError('BRIDGE_NONFINITE_VALUE')
        return result, cells

    return rows, years, values


def nested(entry, root, binding, end, original, total):
    rows, years, values = table(entry, root, binding, end)
    yr = binding['yearRow']
    # A complete, explicitly named hierarchy. Never sum details AND subtotal.
    labels = [norm(r[0]) for r in rows[yr+1:yr+9]]
    if labels[:3] != ['', 'revenue:', 'asset liquidation'] or set(labels[3:5]) != {'asset sales', 'commissions and other'} or labels[5:] != ['total asset liquidation revenue', 'intellectual property licensing', 'total revenue']:
        raise ValueError('BRIDGE_NESTED_HIERARCHY')
    if any(any(c.strip() for c in rows[i][1:]) for i in [yr+1, yr+2, yr+3]):
        raise ValueError('BRIDGE_SECTION_HAS_VALUES')
    a, _ = values(yr+4, rows[yr+4][0])
    b, _ = values(yr+5, rows[yr+5][0])
    subtotal, _ = values(yr+6, 'Total asset liquidation revenue')
    licensing, _ = values(yr+7, 'Intellectual property licensing', zero_dash=True)
    totals, _ = values(yr+8, 'Total revenue')
    if any(x+y != z or z+w != t for x,y,z,w,t in zip(a,b,subtotal,licensing,totals)):
        raise ValueError('BRIDGE_NESTED_EQUATION')
    index = years.index(str(binding['fiscalYear']))
    if subtotal[index] != original or totals[index] != total:
        raise ValueError('BRIDGE_NESTED_SOURCE_VALUES')
    return {'years': years, 'subtotalUSD': list(map(str, subtotal)), 'otherRevenueUSD': list(map(str, licensing)), 'totalUSD': list(map(str, totals)), 'detailsCountedOnce': True}


def detail(entry, root, binding, end, total):
    _, years, values = table(entry, root, binding, end)
    amounts, _ = values(binding['valueRow'], 'Net sales')
    if amounts[years.index(str(binding['fiscalYear']))] != total:
        raise ValueError('BRIDGE_DETAILED_VALUE')
    return dict(zip(years, amounts))


def rounded_summary(entry, root, binding, end, original, detailed):
    rows, years, values = table(entry, root, binding, end, summary=True)
    yr = binding['yearRow']
    expected = ['Consolidated product sales', 'Consolidated pack sales and associate fees(a)', 'Consolidated other, including freight', 'Total']
    if binding['valueRow'] != yr+4 or len(rows) != yr+5:
        raise ValueError('BRIDGE_SUMMARY_HIERARCHY')
    vectors = [values(yr+i+1, label) for i,label in enumerate(expected)]
    totals, cells = vectors[-1]
    if binding['unitScale'] != 1000000 or binding['decimalPlaces'] != 1:
        raise ValueError('BRIDGE_SUMMARY_PRECISION')
    for cell in cells:
        if not re.fullmatch(r'\$?\s*\d+(?:,\d{3})*\.\d', cell.strip()):
            raise ValueError('BRIDGE_SUMMARY_DISPLAY_DECIMALS')
    quantum = Decimal(100000)
    if any(sum(vectors[k][0][i] for k in range(3)) != totals[i] for i in range(len(years))):
        raise ValueError('BRIDGE_SUMMARY_COMPONENT_EQUATION')
    if set(years) != set(detailed) or any((detailed[y]/quantum).quantize(Decimal(1), rounding=ROUND_HALF_UP)*quantum != totals[i] for i,y in enumerate(years)):
        raise ValueError('BRIDGE_ROUNDED_COMPARATIVE_CONFLICT')
    if totals[years.index(str(binding['fiscalYear']))] != original:
        raise ValueError('BRIDGE_ROUNDED_SOURCE_VALUE')
    return {'quantumUSD': str(quantum), 'years': years, 'summaryUSD': list(map(str, totals)), 'originalDetailedUSD': {y: str(v) for y,v in detailed.items()}}


def approve(scope, entry, root):
    r = entry['review']
    a, b = Decimal(r['originalValue']), Decimal(r['value'])
    checked = reviewed_approval(scope, {**r, 'basis': 'FULL_RETROSPECTIVE_REVENUE_STANDARD', 'revenueStandardAdjustment': str(b-a)})
    e = r['originalBridgeEvidence']
    original_urls = {x['source_url'] for x in checked['proof']['originalSources']}
    latest_urls = {x['source_url'] for x in checked['proof']['sources']}
    original_nodes = [e['originalStatement']] + ([e['originalSummary']] if r['basis'] == PRECISE else [])
    if any(original_urls != {n['url']} for n in original_nodes) or latest_urls != {e['selectedStatement']['url']}:
        raise ValueError('BRIDGE_SAME_REPORT_IDENTITY')
    nodes = original_nodes + [e['selectedStatement']]
    if len({str(n['fiscalYear']) for n in nodes}) != 1 or any(str(x['fiscal_year']) != str(nodes[0]['fiscalYear']) for x in checked['proof']['originalSources']):
        raise ValueError('BRIDGE_FISCAL_YEAR')
    if r['basis'] == NESTED:
        original = nested(entry, root, e['originalStatement'], r['end'], a, b)
        selected = nested(entry, root, e['selectedStatement'], r['end'], a, b)
        policy, cause = 'sec-reviewed-nested-revenue-coverage698-v1', 'SOURCE_TAG_COMPONENT_COVERAGE'
    elif r['basis'] == PRECISE:
        first = detail(entry, root, e['originalStatement'], r['end'], b)
        last = detail(entry, root, e['selectedStatement'], r['end'], b)
        common = set(first) & set(last)
        if any(first[y] != last[y] for y in common):
            raise ValueError('BRIDGE_DETAILED_COMPARATIVE_CONFLICT')
        original = rounded_summary(entry, root, e['originalSummary'], r['end'], a, first)
        selected = {'detailUSD': {y: str(v) for y,v in last.items()}, 'matchingDetailedYears': sorted(common)}
        policy, cause = 'sec-reviewed-original-rounded-to-detailed698-v1', 'ORIGINAL_PRESENTATION_PRECISION'
    else:
        raise ValueError('BRIDGE_BASIS')
    proof = {**checked['proof'], 'policy': policy, 'review': r, 'accountingCause': cause, 'originalBridge': original, 'selectedBridge': selected, 'restatementClaimed': False}
    return {**checked, 'proof': proof, 'proof_hash': digest(proof)}
