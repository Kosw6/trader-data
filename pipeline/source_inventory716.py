"""Offline source availability triage, never a numeric or mapping approval."""
from datetime import date

ANNUAL_FORMS={'10-K','10-K/A','20-F','20-F/A','40-F','40-F/A'}
INSTANT={'ASSETS','LIABILITIES'}
DURATION={'REVENUE','NET_INCOME','OPERATING_INCOME','DILUTED_EPS'}


def classify(scope,verified_cached_urls,missing_reference_covers=()):
    end=scope['end'];metric=scope['metric'];rows=scope['rows'];durations=set();original=[];raw_issues=set()
    if metric not in INSTANT|DURATION:return dict(sourceClass='UNSUPPORTED_METRIC',approvalGenerated=False)
    try:expected_cik=int(scope['cik'])
    except (ValueError,TypeError,KeyError):return dict(sourceClass='FROZEN_SCOPE_IDENTITY_REVIEW',approvalGenerated=False)
    for row in rows:
        if row.get('company_id')!=scope['companyId'] or row.get('metric_id')!=scope['metricId'] or row.get('metric_code')!=metric or row.get('period_end')!=end:
            return dict(sourceClass='FROZEN_SCOPE_IDENTITY_REVIEW',approvalGenerated=False)
        try:
            if int(row['cik'])!=expected_cik:return dict(sourceClass='FROZEN_SCOPE_IDENTITY_REVIEW',approvalGenerated=False)
        except (ValueError,TypeError,KeyError):return dict(sourceClass='FROZEN_SCOPE_IDENTITY_REVIEW',approvalGenerated=False)
        try:
            finish=date.fromisoformat(row['period_end']);start=date.fromisoformat(row['period_start']) if row.get('period_start') else None
            duration=(finish-start).days if start else None
            if duration is not None:durations.add(duration)
        except (TypeError,ValueError):raw_issues.add('INVALID_DATE');continue
        unit='USD/shares' if metric=='DILUTED_EPS' else 'USD'
        if row.get('unit')!=unit:raw_issues.add('UNREVIEWED_UNIT')
        if row.get('taxonomy')!='us-gaap':raw_issues.add('UNREVIEWED_TAXONOMY')
        if row.get('dimensions') not in (None,{}):raw_issues.add('DIMENSIONED_OBSERVATION')
        eligible=(metric in INSTANT and start is None) or (metric in DURATION and duration is not None and 335<=duration<=395)
        if eligible and row.get('report_date')==end and row.get('form_type') in ANNUAL_FORMS:original.append(row)
    original_urls={r.get('source_url') for r in original};all_urls={r.get('source_url') for r in rows};bad_reference=any(not isinstance(u,str) or not u.startswith(f'https://www.sec.gov/Archives/edgar/data/{expected_cik}/') for u in original_urls)
    original_urls={u for u in original_urls if isinstance(u,str)};all_urls={u for u in all_urls if isinstance(u,str)}
    if not original:kind='NO_EXACT_ORIGINAL_ANNUAL_OBSERVATION'
    elif bad_reference:kind='ORIGINAL_SOURCE_REFERENCE_REVIEW'
    elif not original_urls<=verified_cached_urls:kind='ORIGINAL_DOCUMENT_CACHE_INCOMPLETE'
    elif original_urls & set(missing_reference_covers):kind='INCORPORATED_PRIMARY_DOCUMENT_NOT_CACHED'
    elif not all_urls<=verified_cached_urls:kind='ORIGINAL_CACHED_STORED_REPORT_CHAIN_INCOMPLETE'
    else:kind='CACHED_CHAIN_REQUIRES_ACCOUNTING_CONTENT_REVIEW'
    return dict(sourceClass=kind,originalObservationCount=len(original),originalUrls=sorted(original_urls),uncachedOriginalUrls=sorted(original_urls-verified_cached_urls),allStoredReportCount=len(all_urls),uncachedStoredReports=len(all_urls-verified_cached_urls),durationDays=sorted(durations),mixedAnnualAndShortDuration=any(335<=d<=395 for d in durations) and any(not 335<=d<=395 for d in durations),rawPolicyIssues=sorted(raw_issues),approvalGenerated=False,publicationAllowed=False)
