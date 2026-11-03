"""Explicit two-stage bridge: original partial tag -> original total -> continuing total."""
import gzip
import hashlib
import html
import re
from datetime import datetime
from decimal import Decimal
from pipeline.revenue_coverage694 import bound_table
from pipeline.legacy_statement_total699 import document
from pipeline.discontinued_bridge687 import representation_cause
from pipeline.bound_reconciliation685 import norm, period_years
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest

BASIS = 'REVIEWED_COMPONENT_COVERAGE_THEN_DISCONTINUED_REVENUE'
POLICY = 'sec-reviewed-coverage-discontinued701-v1'


def approve(scope, entry, root):
    r = entry['review']
    if r['basis'] != BASIS:
        raise ValueError('COVERAGE_DISCONTINUED_BASIS')
    raw, total, final, removed = [Decimal(r[k]) for k in ['originalValue','originalStatementTotal','value','discontinuedRevenue']]
    if not all(x.is_finite() for x in [raw,total,final,removed]) or removed <= 0 or total-removed != final:
        raise ValueError('COVERAGE_DISCONTINUED_EXACT_TWO_STAGE_EQUATION')
    checked = reviewed_approval(scope, {**r, 'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD', 'revenueStandardAdjustment':str(final-raw)})
    original = r['originalCoverageEvidence']; selected = r['selectedTotalEvidence']; e = r['discontinuedEvidence']
    if {x['source_url'] for x in checked['proof']['originalSources']} != {original['url']} or {x['source_url'] for x in checked['proof']['sources']} != {selected['url']} or e['url'] != selected['url']:
        raise ValueError('COVERAGE_DISCONTINUED_REPORT_BINDING')
    if any(str(x['fiscal_year']) != str(original['fiscalYear']) for x in checked['proof']['originalSources']) or str(e['fiscalYear']) != str(original['fiscalYear']):
        raise ValueError('COVERAGE_DISCONTINUED_FISCAL_YEAR')
    a = bound_table(entry, root, original, r['end'], total, original=True)
    b = bound_table(entry, root, selected, r['end'], final)
    if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:
        raise ValueError('COVERAGE_DISCONTINUED_DOCUMENT')
    rows, context, _ = document(e, root)
    with gzip.open(root/'documents'/(e['sha256']+'.html.gz'), 'rb') as stream:
        body = stream.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=e['sha256']:
        raise ValueError('COVERAGE_DISCONTINUED_DOCUMENT_CHANGED')
    text=re.sub(r'<(script|style)\b[^>]*>.*?</\1>', ' ', body.decode('utf8','replace'), flags=re.S|re.I)
    plain=norm(html.unescape(re.sub('<[^>]+>', ' ', text)))
    unit=norm(e['notesUnitDisclosure'])
    if unit not in plain or not re.fullmatch(r'notes to (?:the )?consolidated financial statements \(tabular dollars in millions, except per share amounts\)',unit) or e['unitScale']!=1000000:
        raise ValueError('COVERAGE_DISCONTINUED_NOTES_UNIT_CONTRACT')
    near=norm(e['tableContextExcerpt']);businesses=e['businesses'];causes=e['causeExcerpts']
    if not 30<=len(near)<=1600 or near not in norm(context) or 'discontinued operations' not in near or not 1<=len(businesses)<=4 or len(set(map(norm,businesses)))!=len(businesses) or len(causes)!=len(businesses):
        raise ValueError('COVERAGE_DISCONTINUED_BUSINESS_TABLE_CONTEXT')
    for business,cause in zip(businesses,causes):
        cause=norm(cause);business=norm(business)
        if not business or business not in near or business not in cause or not 60<=len(cause)<=1200 or cause not in plain or not representation_cause(cause) or not re.search(r'all (?:prior )?periods',cause):
            raise ValueError('COVERAGE_DISCONTINUED_EXPLICIT_BUSINESS_CAUSE')
    yr,vr=e['yearRow'],e['valueRow']
    if type(yr) is not int or type(vr) is not int or not 0<=yr<vr<len(rows):
        raise ValueError('COVERAGE_DISCONTINUED_TABLE_POSITION')
    years=period_years(rows[yr]);fy=str(e['fiscalYear'])
    if not 1<=len(years)<=3 or len(set(years))!=len(years) or years.count(fy)!=1 or any(period_years(row) for row in rows[yr+1:vr]):
        raise ValueError('COVERAGE_DISCONTINUED_YEAR_COLUMNS')
    heading=e['annualHeading']
    if not re.fullmatch(r'Years ended (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2},',heading,re.I) or norm(heading) not in norm(' '.join(' '.join(row) for row in rows[:vr])) or datetime.strptime(re.sub(r'^Years ended ', '', heading,flags=re.I)+' '+fy,'%B %d, %Y').date().isoformat()!=r['end']:
        raise ValueError('COVERAGE_DISCONTINUED_ANNUAL_PERIOD')
    row=rows[vr]
    if norm(row[0])!='revenues' or norm(e['rowLabel'])!='revenues':
        raise ValueError('COVERAGE_DISCONTINUED_REVENUE_ROW')
    values=[numeric(c) if numeric(c) is not None else Decimal(0) for c in row[1:] if numeric(c) is not None or c.strip()=='—']
    if len(values)!=len(years) or values[years.index(fy)]*1000000!=removed:
        raise ValueError('COVERAGE_DISCONTINUED_COMPONENT_AMOUNT')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':BASIS,'originalCoverage':a,'selectedTotal':b,'bridge':{'originalRawComponentUSD':str(raw),'originalStatementTotalUSD':str(total),'latestDiscontinuedRevenueUSD':str(removed),'latestContinuingTotalUSD':str(final)},'economicGrowthInferred':False,'originalObservationsPreserved':True}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
