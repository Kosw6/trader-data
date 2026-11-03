"""Registered, byte-bound annual revenue reconciliation tables; no discovery approval."""
import gzip
import hashlib
import html
import re
from datetime import datetime
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables, numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest

STANDARD = 'REVIEWED_BOUND_REVENUE_STANDARD_TABLE'
AMENDMENT = 'REVIEWED_AMENDED_ORIGINAL_CONFIRMATION'
ERROR = 'REVIEWED_BOUND_ERROR_CORRECTION_TABLE'
POLICY = 'sec-bound-reconciliation685-v1'


def norm(value):
    return re.sub(r'\s+', ' ', value).strip().casefold()


def period_years(row):
    """Recognize period columns, never ASU 2017-07 or a footnote number."""
    years = []
    for cell in row:
        bare = re.fullmatch(r'(?:Fiscal(?: Year)?\s+)?((?:19|20)\d{2})', cell.strip(), re.I)
        dates = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+((?:19|20)\d{2})', cell)
        years.extend([bare[1]] if bare else dates)
    return years


def approve(scope, entry, root):
    r = entry['review']; e = r.get('boundTableEvidence', {})
    if r.get('basis') not in {STANDARD, AMENDMENT, ERROR}:
        raise ValueError('BOUND_BASIS_INVALID')
    if not any(d['url'] == e.get('url') and d['sha256'] == e.get('sha256') for d in entry['documents']):
        raise ValueError('BOUND_DOCUMENT_IDENTITY')
    with gzip.open(root / 'documents' / (e['sha256'] + '.html.gz'), 'rb') as f:
        body = f.read(32 * 1024**2 + 1)
    if len(body) > 32 * 1024**2 or hashlib.sha256(body).hexdigest() != e['sha256']:
        raise ValueError('BOUND_DOCUMENT_CHANGED')
    text = body.decode('utf8', errors='replace')
    clean = re.sub(r'<(script|style)\b[^>]*>.*?</\1>', ' ', text, flags=re.I|re.S)
    plain = norm(html.unescape(re.sub('<[^>]+>', ' ', clean)))
    cause = norm(e.get('causeExcerpt', ''))
    if len(cause) < 40 or cause not in plain:
        raise ValueError('BOUND_CAUSE_NOT_CONFIRMED')
    if r['basis'] == STANDARD:
        if not ('full retrospective' in cause and re.search(r'standard|asc|topic|asu|guidance', cause)):
            raise ValueError('BOUND_RETROSPECTIVE_BASIS_REQUIRED')
        shadow = {**r, 'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD',
                  'revenueStandardAdjustment':str(Decimal(r['value']) - Decimal(r['originalValue']))}
    elif r['basis'] == AMENDMENT:
        if not re.search(r'misstatement|correction|restatement', cause):
            raise ValueError('BOUND_ERROR_CAUSE_REQUIRED')
        shadow = {**r, 'basis':'UNCHANGED_TOTAL_CONFIRMED'}
    else:
        if not re.search(r'misstatement|correction|restatement|errors?',cause):
            raise ValueError('BOUND_ERROR_CAUSE_REQUIRED')
        shadow = {**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD',
                  'revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))}
    checked = reviewed_approval(scope, shadow)
    filed = e.get('filingDate')
    evidence_rows = [x for x in scope if x['source_url'] == e['url'] and x['form_type'] in {'10-K','10-K/A'}]
    if not evidence_rows or {str(x['filing_date']) for x in evidence_rows} != {filed}:
        raise ValueError('BOUND_FILING_METADATA_REQUIRED')
    if not min(str(x['filing_date']) for x in checked['proof']['originalSources']) <= filed <= r['filingDate']:
        raise ValueError('BOUND_FILING_CHRONOLOGY')
    parser = Tables(); parser.feed(text)
    matching = [(rows, context) for rows, context in zip(parser.tables, parser.contexts) if digest(rows) == e.get('tableHash')]
    if len(matching) != 1:
        raise ValueError('BOUND_EXACT_TABLE_REQUIRED')
    rows, context = matching[0]
    yr, vr = e.get('yearRow'), e.get('valueRow')
    if type(vr) is not int or not 0 <= vr < len(rows):
        raise ValueError('BOUND_ROW_POSITION_INVALID')
    target = str(e.get('fiscalYear'))
    originals = checked['proof']['originalSources']
    if not originals or {str(x['fiscal_year']) for x in originals} != {target}:
        raise ValueError('BOUND_FISCAL_YEAR_MISMATCH')
    if yr is None:
        heading = e.get('annualContextHeading', '')
        match = re.fullmatch(r'for the fiscal year ended ([A-Z][a-z]+ \d{1,2}, \d{4})', heading)
        if not match or norm(heading) not in norm(context):
            raise ValueError('BOUND_ANNUAL_CONTEXT_REQUIRED')
        if datetime.strptime(match[1], '%B %d, %Y').date().isoformat() != r['end']:
            raise ValueError('BOUND_ANNUAL_CONTEXT_PERIOD')
        years = [target]
    else:
        if type(yr) is not int or not 0 <= yr < vr:
            raise ValueError('BOUND_ROW_POSITION_INVALID')
        years = period_years(rows[yr])
        if any(period_years(head) for head in rows[yr+1:vr]):
            raise ValueError('BOUND_CROSS_PERIOD_BINDING')
        dates = re.findall(r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+(?:19|20)\d{2}', ' '.join(rows[yr]))
        target_dates = [datetime.strptime(v, '%B %d, %Y').date().isoformat() for v in dates if v.endswith(target)]
        if target_dates and set(target_dates) != {r['end']}:
            raise ValueError('BOUND_DATE_COLUMN_MISMATCH')
    if not 1 <= len(years) <= 3 or len(set(years)) != len(years) or years.count(target) != 1:
        raise ValueError('BOUND_YEAR_COLUMNS_AMBIGUOUS')
    if norm(rows[vr][0]) != norm(e.get('rowLabel', '')) or not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b', rows[vr][0], re.I):
        raise ValueError('BOUND_REVENUE_ROW_REQUIRED')
    table = norm(' '.join(' '.join(row) for row in rows))
    headers = e.get('orderedHeaders', [])
    after_header = r'as (?:adjusted|restated|recast|revised)' if r['basis']==ERROR else r'as (?:adjusted|restated|recast)'
    if len(headers) != 3 or not re.search(r'\bas\s*(?:previously\s*)?reported\b', norm(headers[0])) or not re.search(after_header, norm(headers[2])):
        raise ValueError('BOUND_HEADER_CONTRACT_INVALID')
    header_text = norm(' '.join(' '.join(row) for row in rows[:vr]))
    position = 0
    for h in headers:
        position = header_text.find(norm(h), position)
        if position < 0: raise ValueError('BOUND_HEADERS_NOT_ORDERED')
        position += len(norm(h))
    scale = Decimal(str(e.get('unitScale', 0)))
    unit = {Decimal(1):'in usd', Decimal(1000):'in thousands', Decimal(1000000):'in millions'}.get(scale)
    if not unit or unit not in norm(context + ' ' + table):
        raise ValueError('BOUND_UNIT_NOT_CONFIRMED')
    labels = e.get('componentLabels', [])
    if labels:
        if r['basis'] not in {STANDARD,ERROR} or not 2 <= len(labels) <= 4 or len(set(labels)) != len(labels):
            raise ValueError('BOUND_COMPONENT_CONTRACT')
        position = 0
        for label in labels:
            position = header_text.find(norm(label), position)
            if position < 0: raise ValueError('BOUND_COMPONENT_HEADERS')
            position += len(norm(label))
    width = len(labels) + 2 if labels else 3
    values = []
    for cell in rows[vr]:
        value = numeric(cell)
        if value is not None: values.append(value)
        elif cell.strip() == '\u2014': values.append(Decimal(0))
    if len(values) != width * len(years):
        raise ValueError('BOUND_VALUE_COLUMN_COUNT')
    index = years.index(target) * width
    group = [v * scale for v in values[index:index+width]]
    a, adjustment, b = group[0], sum(group[1:-1]), group[-1]
    if labels and [str(v) for v in group[1:-1]] != [str(Decimal(str(v))) for v in e.get('componentAmountsUSD', [])]:
        raise ValueError('BOUND_COMPONENT_AMOUNTS')
    if a + adjustment != b or b != Decimal(r['value']):
        raise ValueError('BOUND_EXACT_EQUATION_MISMATCH')
    if r['basis'] in {STANDARD,ERROR} and a != Decimal(r['originalValue']):
        raise ValueError('BOUND_ORIGINAL_AMOUNT_MISMATCH')
    structured={}
    if r['basis'] == ERROR:
        from pipeline.error_scope692 import period_binding,component_bindings
        if not re.fullmatch(r'as (?:restated|revised)',norm(headers[2])):raise ValueError('BOUND_ERROR_CORRECTION_PERIOD_CAUSE_REQUIRED')
        period=period_binding(r,e,cause,text)
        if period:structured['period']=period
        if labels:structured['components']=component_bindings(e,labels,group[1:-1],plain)
    if r['basis'] == AMENDMENT:
        if Decimal(r['originalValue']) != b or not any(x['source_url'] == e['url'] and x['form_type'] == '10-K/A' for x in originals):
            raise ValueError('BOUND_AMENDED_ORIGINAL_REQUIRED')
        initial = [x for x in scope if x['source_tag'] == 'SalesRevenueNet' and str(x['report_date']) == r['end'] and x['form_type'] == '10-K' and Decimal(str(x['value'])) == a]
        if not initial or any(x['source_url'] not in r['officialDocuments'] for x in initial):
            raise ValueError('BOUND_INITIAL_REPORT_REQUIRED')
        reference = e.get('reference', '')
        if not re.fullmatch(r'\([a-z]\)', reference) or reference not in [norm(c) for c in rows[vr]] or not cause.startswith(reference):
            raise ValueError('BOUND_ERROR_REFERENCE_MISMATCH')
    proof = {**checked['proof'], 'policy':POLICY, 'review':r,
             'accountingCause':r['basis'], 'tableAmountsUSD':[str(a), str(adjustment), str(b)]}
    if structured:
        from pipeline.error_scope692 import POLICY as STRUCTURED_POLICY
        proof.update(policy=STRUCTURED_POLICY,structuredErrorBinding=structured)
    return {**checked, 'proof':proof, 'proof_hash':digest(proof)}
