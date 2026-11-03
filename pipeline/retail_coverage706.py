"""Named retail/credit components that already sum to the reported total.

An unchanged total is not a revenue restatement. Fiscal dates must be stated
in the same report; the Saturday-nearest-January formula is never inferred.
"""
from datetime import datetime
from decimal import Decimal
import gzip
import hashlib
import html
import re
from pipeline.legacy_statement_total699 import document, clean, DATE
from pipeline.bound_reconciliation685 import period_years, norm
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.financial_mapping667 import invalid

BASIS = 'REVIEWED_RETAIL_AND_CREDIT_REVENUE_COMPONENT_COVERAGE'
POLICY = 'sec-reviewed-retail-credit-coverage706-v1'
CONTRACTS = {'RETAIL_FINANCE_CHARGES706_V1', 'SALES_CREDIT_CARD706_V1'}


def plain_document(root, sha):
    with gzip.open(root / 'documents' / (sha + '.html.gz'), 'rb') as f:
        body = f.read(32 * 1024**2 + 1)
    if len(body) > 32 * 1024**2 or hashlib.sha256(body).hexdigest() != sha:
        raise ValueError('RETAIL_DOCUMENT_CHANGED')
    text = re.sub(r'<(script|style)\b[^>]*>.*?</\1>', ' ', body.decode('utf8', 'replace'), flags=re.S | re.I)
    return norm(html.unescape(re.sub('<[^>]+>', ' ', text)))


def statement(entry, root, e, end, fiscal):
    if (e['url'], e['sha256']) not in {(d['url'], d['sha256']) for d in entry['documents']}:
        raise ValueError('RETAIL_DOCUMENT_BINDING')
    rows, context, _ = document(e, root)
    yr, first, total = e['yearRow'], e['firstRow'], e['totalRow']
    if any(type(i) is not int for i in [yr, first, total]) or not 0 <= yr < first < total < len(rows):
        raise ValueError('RETAIL_ROW_BINDING')
    if not re.search(r'consolidated statements of (?:operations|income(?: and comprehensive income)?)$', clean(context)):
        raise ValueError('RETAIL_PRIMARY_STATEMENT_TITLE')
    contract = e['componentContract']
    if contract not in CONTRACTS or clean(rows[total][0]) != 'total revenues':
        raise ValueError('RETAIL_NAMED_COMPONENT_CONTRACT')
    top = clean(' '.join(' '.join(r) for r in rows[:first]))
    if e['periodMode'] == 'DATE':
        keys = [datetime.strptime(' '.join(m.groups()), '%B %d %Y').date().isoformat()
                for c in rows[yr] for m in DATE.finditer(c)]
        if not re.search(r'fiscal years? ended', top): raise ValueError('RETAIL_ANNUAL_HEADING')
        if contract != 'RETAIL_FINANCE_CHARGES706_V1': raise ValueError('RETAIL_PERIOD_CONTRACT')
    elif e['periodMode'] == 'EXPLICIT_FISCAL_NOTE':
        years = period_years(rows[yr]); quote = norm(e['fiscalNoteExcerpt'])
        if not 100 <= len(quote) <= 2200 or quote not in plain_document(root, e['sha256']):
            raise ValueError('RETAIL_FISCAL_NOTE_BINDING')
        pattern = r'fiscal (?:year )?((?:19|20)\d{2})(?: \(\1\))? ended ([a-z]+ \d{1,2},? (?:19|20)\d{2})'
        pairs = re.findall(pattern, quote)
        dates = {}
        for year, date_text in pairs:
            day = datetime.strptime(date_text.replace(',', ''), '%B %d %Y').date().isoformat()
            if year in dates and dates[year] != day: raise ValueError('RETAIL_FISCAL_DATE_CONFLICT')
            dates[year] = day
        if not 2 <= len(years) <= 3 or any(y not in dates for y in years) or str(fiscal) not in years:
            raise ValueError('RETAIL_EXPLICIT_FISCAL_DATES_REQUIRED')
        keys = [dates[y] for y in years]
        if keys[years.index(str(fiscal))] != end or any(int(a)-int(b) != 1 for a,b in zip(years,years[1:])):
            raise ValueError('RETAIL_FISCAL_COLUMN_ALIGNMENT')
        if contract != 'SALES_CREDIT_CARD706_V1': raise ValueError('RETAIL_PERIOD_CONTRACT')
    else: raise ValueError('RETAIL_PERIOD_MODE')
    if not 2 <= len(keys) <= 3 or len(set(keys)) != len(keys) or keys.count(end) != 1:
        raise ValueError('RETAIL_ANNUAL_COLUMNS')
    dates = [datetime.fromisoformat(k).date() for k in keys]
    if any(not 350 <= (a-b).days <= 380 for a,b in zip(dates,dates[1:])):
        raise ValueError('RETAIL_ANNUAL_DATE_SPACING')
    if any(period_years(r) or DATE.search(' '.join(r)) for r in rows[yr+1:first]):
        raise ValueError('RETAIL_CROSS_PERIOD_BINDING')
    scale = Decimal(str(e['unitScale']))
    expected = Decimal(1000) if contract == 'RETAIL_FINANCE_CHARGES706_V1' else Decimal(1000000)
    unit_phrase = ('dollars in thousands, except per share data' if scale == 1000 else 'millions, except per share data')
    if scale != expected or unit_phrase not in top: raise ValueError('RETAIL_UNIT')
    occupied = [i for i in range(first,total) if any(c.strip() for c in rows[i])]
    if contract == 'RETAIL_FINANCE_CHARGES706_V1':
        if occupied != [first,first+1,first+2] or clean(rows[first][0]) != 'retail sales' or clean(rows[first+1][0]) != 'other income (principally finance charges,' or clean(rows[first+2][0]) != 'late fees and layaway charges)' or clean(' '.join(rows[first-1])) not in {'revenues','revenues:'}:
            raise ValueError('RETAIL_FINANCE_LABEL_HIERARCHY')
        if any(numeric(c) is not None for c in rows[first+1][1:]): raise ValueError('RETAIL_SPLIT_LABEL_HAS_VALUES')
        components = [first,first+2]
    else:
        if occupied != [first,first+1] or [clean(rows[i][0]) for i in occupied] != ['sales','credit card revenues']:
            raise ValueError('RETAIL_CREDIT_LABEL_HIERARCHY')
        components = occupied
    def values(i):
        # A printed em dash is an explicit statement zero. An absent cell or
        # a missing column never supplies zero.
        v = [numeric(c) if numeric(c) is not None else Decimal(0)
             for c in rows[i][1:] if numeric(c) is not None or c.strip() == '—']
        if len(v) != len(keys) or any(not x.is_finite() for x in v): raise ValueError('RETAIL_VALUE_COLUMNS')
        return [x * scale for x in v]
    a,b,t = values(components[0]),values(components[1]),values(total)
    if any(x+y != z for x,y,z in zip(a,b,t)): raise ValueError('RETAIL_EXACT_COMPONENT_EQUATION')
    pos = keys.index(end)
    return {'documentSha256':e['sha256'],'tableHash':e['tableHash'],'periodEnd':end,
            'firstComponentUSD':str(a[pos]),'creditComponentUSD':str(b[pos]),'totalUSD':str(t[pos]),
            'componentContract':contract,'periodMode':e['periodMode']}


def approve(scope, entry, root):
    r = entry['review']; a,b = Decimal(r['originalValue']),Decimal(r['value'])
    if r['basis'] != BASIS or a == b: raise ValueError('RETAIL_COVERAGE_BASIS')
    checked = reviewed_approval(scope, {**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
    for row in scope:
        shadow = dict(row,report_date=row['period_end'])
        if row['source_tag']=='SalesRevenueNet': shadow['source_tag']='Revenues'
        if invalid(shadow, annual_only=True): raise ValueError('RETAIL_SOURCE_NOT_EXACT_ANNUAL')
        if row['source_tag']=='SalesRevenueNet' and Decimal(str(row['value'])) != a: raise ValueError('RETAIL_LEGACY_OBSERVATIONS_CHANGED')
        if row['source_tag'] in {'Revenues','RevenueFromContractWithCustomerExcludingAssessedTax'} and Decimal(str(row['value'])) != b: raise ValueError('RETAIL_MODERN_OBSERVATIONS_CHANGED')
    fiscal = r['originalFiscalYear']
    if any(str(x['fiscal_year']) != str(fiscal) for x in checked['proof']['originalSources']):
        raise ValueError('RETAIL_ORIGINAL_FISCAL_IDENTITY')
    proofs = {}
    for role,sources in [('original',checked['proof']['originalSources']),('selected',checked['proof']['sources'])]:
        e = r['retailCoverageEvidence'][role]
        if {x['source_url'] for x in sources} != {e['url']}: raise ValueError('RETAIL_SOURCE_REPORT_BINDING')
        proofs[role] = statement(entry,root,e,r['end'],fiscal)
        if Decimal(proofs[role]['firstComponentUSD']) != a or Decimal(proofs[role]['totalUSD']) != b:
            raise ValueError('RETAIL_COMPONENT_AND_TOTAL_VALUE')
    if proofs['original']['creditComponentUSD'] != proofs['selected']['creditComponentUSD']:
        raise ValueError('RETAIL_COMPONENT_CHANGED')
    proof = {**checked['proof'],'policy':POLICY,'review':r,'statements':proofs,
             'accountingCause':'UNCHANGED_RETAIL_PLUS_CREDIT_REVENUE_COMPONENT_COVERAGE',
             'restatementClaimed':False,'originalObservationsPreserved':True}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
