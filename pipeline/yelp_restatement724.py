"""Yelp ASC606, exact original SEC periods and every full primary statement.

Approval is scoped to two reviewed revenue periods. Neither other metrics nor
unknown future periods inherit the accounting interpretation.
"""
import gzip
import hashlib
import json
import re
from decimal import Decimal

from pipeline.financial_history674 import raw_index
from pipeline.financial_mapping667 import invalid
from pipeline.legacy_statement_total699 import document, clean
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.yelp_source720 import source_evidence, SOURCE_SHA256, URL

BASIS = 'REVIEWED_YELP_FULL_RETROSPECTIVE_PRIMARY724'
POLICY = 'sec-yelp-all-primary-raw-periods724-v1'
RAW_SHA256 = '14a14a40562321dd8bfd570daaca6d4f4d1e49673ce2e49b07e48ce15edcc081'
RAW_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK0001345016.json'


def statement_vector(rows, context):
    """Require the full annual primary statement, not selected/quarterly data."""
    ctx = clean(context)
    heading = 'yelp inc. consolidated statements of operations (in thousands, except per share data)'
    if not ctx.endswith(heading):
        raise ValueError('YELP_PRIMARY_HEADING_UNIT')
    nonempty = [(i, [clean(c) for c in row if clean(c)]) for i, row in enumerate(rows)
                if any(clean(c) for c in row)]
    if len(nonempty) < 15 or nonempty[0][1] != ['year ended december 31,']:
        raise ValueError('YELP_PRIMARY_ANNUAL_HEADER')
    years = nonempty[1][1]
    if len(years) != 3 or any(not re.fullmatch(r'20\d{2}', y) for y in years) or any(int(a)-int(b) != 1 for a,b in zip(years, years[1:])):
        raise ValueError('YELP_PRIMARY_ACTUAL_YEAR_COLUMNS')
    i, label = nonempty[2]
    if label[0] != 'net revenue' or not any(clean(row[0]) == 'costs and expenses:' for row in rows[i+1:] if row):
        raise ValueError('YELP_PRIMARY_TOTAL_REVENUE')
    if not any(clean(row[0]) == 'total costs and expenses' for row in rows[i+1:] if row):
        raise ValueError('YELP_PRIMARY_FULL_STATEMENT')
    cells = [c.strip() for c in rows[i][1:] if numeric(c) is not None]
    if len(cells) != 3 or any(not re.fullmatch(r'\d{1,3}(?:,\d{3})*', c) for c in cells):
        raise ValueError('YELP_PRIMARY_EXACT_REVENUE_COLUMNS')
    return {year+'-12-31': numeric(cell)*1000 for year,cell in zip(years,cells)}


def official_raw_periods(scope, evidence, root):
    if evidence != dict(url=RAW_URL, sha256=RAW_SHA256):
        raise ValueError('YELP_ORIGINAL_RAW_SOURCE_IDENTITY')
    path = root/'raw-sec'/(RAW_SHA256+'.json.gz')
    if path.is_symlink():
        raise ValueError('YELP_ORIGINAL_RAW_PATH')
    with gzip.open(path, 'rb') as stream:
        body = stream.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=RAW_SHA256:
        raise ValueError('YELP_ORIGINAL_RAW_CHANGED')
    raw = raw_index(json.loads(body), '0001345016')
    for row in scope:
        observation = raw.get(row['source_fact_hash'])
        if not observation or (observation['tag'],observation['unit'],observation['start'],observation['end'],
            Decimal(observation['value']),observation['accession'],observation['filed'],observation['form']) != (
            row['source_tag'],row['unit'],str(row['period_start']),str(row['period_end']),Decimal(str(row['value'])),
            row['accession_number'],str(row['filing_date']),row['form_type']):
            raise ValueError('YELP_ORIGINAL_RAW_EXACT_PERIOD_OR_VALUE')
    start,end=str(scope[0]['period_start']),str(scope[0]['period_end'])
    eligible={h for h,r in raw.items() if r['tag'] in {'SalesRevenueNet','RevenueFromContractWithCustomerExcludingAssessedTax','Revenues'} and r['unit']=='USD' and r['start']==start and r['end']==end and r['form'] in {'10-K','10-K/A'}}
    if eligible!={r['source_fact_hash'] for r in scope}:
        raise ValueError('YELP_UNREPRESENTED_ORIGINAL_RAW_ANNUAL_OBSERVATION')
    return dict(url=RAW_URL,sha256=RAW_SHA256,claims=len(scope),actualPeriodStartsSourceBound=True)


def approve(scope, entry, root):
    review = entry['review']
    if review['basis']!=BASIS or review['companyId']!=2149 or review['metricId']!=22 or review['metric']!='REVENUE' or str(int(entry['cik']))!='1345016':
        raise ValueError('YELP_REVIEW_SCOPE_IDENTITY')
    if (review['start'],review['end']) not in [('2016-01-01','2016-12-31'),('2017-01-01','2017-12-31')]:
        raise ValueError('YELP_REVIEW_EXPLICIT_PERIODS_ONLY')
    original_raw = official_raw_periods(scope,review['originalRawEvidence'],root)
    with gzip.open(root/'documents'/(SOURCE_SHA256+'.html.gz'),'rb') as stream:
        adjustment = source_evidence(stream.read(32*1024**2+1))
    by_end = {r['end']:r for r in adjustment}
    target = by_end[review['end']]
    if Decimal(review['value'])!=Decimal(target['current']) or Decimal(review['originalValue'])!=Decimal(target['previous']):
        raise ValueError('YELP_REVIEW_ADJUSTMENT_ENDPOINTS')
    checked = reviewed_approval(scope,{**review,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD',
                                      'revenueStandardAdjustment':target['adjustment']})
    nodes = review['allPrimaryEvidence']['nodes']
    if len(nodes)!=3:
        raise ValueError('YELP_REVIEW_ALL_PRIMARY_REPORTS_REQUIRED')
    urls = [node['statement']['url'] for node in nodes]
    dates = [node['filingDate'] for node in nodes]
    if len(set(urls))!=3 or dates!=sorted(set(dates)) or dates[-1]!=review['filingDate'] or set(urls)!={r['source_url'] for r in scope}:
        raise ValueError('YELP_REVIEW_ALL_RAW_REPORTS_IN_ORDER')
    if {r['source_url'] for r in checked['proof']['originalSources']}!={urls[0]} or {r['source_url'] for r in checked['proof']['sources']}!={urls[-1]}:
        raise ValueError('YELP_REVIEW_PRIMARY_ENDPOINTS')
    vectors = []
    for node in nodes:
        e = node['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:
            raise ValueError('YELP_PRIMARY_DOCUMENT_BINDING')
        rows, context, _ = document(e, root)
        vector = statement_vector(rows, context)
        if review['end'] not in vector or max(vector)>node['filingDate']:
            raise ValueError('YELP_PRIMARY_TARGET_OR_FUTURE')
        vectors.append(vector)
    if URL not in urls:
        raise ValueError('YELP_PRIMARY_ADOPTION_REPORT_REQUIRED')
    change_index = urls.index(URL)
    if change_index==0:
        raise ValueError('YELP_PRIMARY_ORIGINAL_BEFORE_ADOPTION')
    overlaps = []
    for i,(before,after) in enumerate(zip(vectors,vectors[1:])):
        common = sorted(set(before)&set(after))
        if len(common)<2:
            raise ValueError('YELP_PRIMARY_COMPARATIVE_COVERAGE')
        for day in common:
            expected = Decimal(by_end[day]['adjustment']) if i+1==change_index and day in by_end else Decimal(0)
            if after[day]-before[day]!=expected:
                raise ValueError('YELP_PRIMARY_ALL_COMPARATIVE_EQUATIONS')
        overlaps.append(common)
    for row in scope:
        shadow=dict(row,report_date=row['period_end'])
        if shadow['source_tag']=='SalesRevenueNet': shadow['source_tag']='Revenues'
        if invalid(shadow,annual_only=True):
            raise ValueError('YELP_REVIEW_EXACT_ANNUAL_SOURCE')
        i = urls.index(row['source_url'])
        if str(row['filing_date'])!=dates[i] or Decimal(str(row['value']))!=vectors[i][review['end']]:
            raise ValueError('YELP_PRIMARY_EVERY_RAW_REPORT_VALUE')
    proof={**checked['proof'],'policy':POLICY,'review':review,'originalRawEvidence':original_raw,
           'allPrimaryComparativeEnds':overlaps,'adjustmentSource':target,
           'actualPeriodStartsSourceBound':True,'allSourceReportsBound':True,
           'originalObservationsPreserved':True,'otherMetricApproval':False,
           'accountingCause':'EXPLICIT_ASC606_FULL_RETROSPECTIVE_ADOPTION'}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
