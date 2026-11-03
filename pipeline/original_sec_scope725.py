"""Exact original SEC structured periods, including missing stored observations.

The caller supplies an explicitly reviewed company/artifact contract. No URL
fetch, period synthesis, fact insertion or cross-metric tag alias is allowed.
"""
import gzip,hashlib,json
from decimal import Decimal
from pipeline.financial_history674 import raw_index


def verify(scope,evidence,root,*,company,cik,raw_sha256,metric,tags):
    expected_url='https://data.sec.gov/api/xbrl/companyfacts/CIK'+str(int(cik)).zfill(10)+'.json'
    if evidence!=dict(url=expected_url,sha256=raw_sha256):
        raise ValueError('ORIGINAL_SEC_SOURCE_IDENTITY725')
    path=root/'raw-sec'/(raw_sha256+'.json.gz')
    if path.is_symlink():raise ValueError('ORIGINAL_SEC_SOURCE_PATH725')
    with gzip.open(path,'rb') as stream:body=stream.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=raw_sha256:
        raise ValueError('ORIGINAL_SEC_SOURCE_BYTES725')
    index=raw_index(json.loads(body),cik)
    if not scope:raise ValueError('ORIGINAL_SEC_SCOPE_REQUIRED725')
    unit='USD/shares' if metric=='DILUTED_EPS' else 'USD'
    periods={(str(r['period_start']),str(r['period_end'])) for r in scope}
    if len(periods)!=1:raise ValueError('ORIGINAL_SEC_MIXED_PERIODS725')
    start,end=next(iter(periods));stored=set()
    for row in scope:
        if row['company_id']!=company or str(int(row['cik']))!=str(int(cik)) or row['metric_code']!=metric or row['source_tag'] not in tags or row['unit']!=unit or row['taxonomy']!='us-gaap' or row['dimensions'] not in (None,{}):
            raise ValueError('ORIGINAL_SEC_SCOPE_IDENTITY725')
        obs=index.get(row['source_fact_hash'])
        if not obs or (obs['tag'],obs['unit'],obs['start'],obs['end'],Decimal(obs['value']),obs['accession'],obs['filed'],obs['form'])!=(row['source_tag'],row['unit'],start,end,Decimal(str(row['value'])),row['accession_number'],str(row['filing_date']),row['form_type']):
            raise ValueError('ORIGINAL_SEC_ACTUAL_PERIOD_VALUE725')
        stored.add(row['source_fact_hash'])
    eligible={h for h,r in index.items() if r['tag'] in tags and r['unit']==unit and r['start']==start and r['end']==end and r['form'] in {'10-K','10-K/A'}}
    if eligible!=stored or len(stored)!=len(scope):
        raise ValueError('ORIGINAL_SEC_UNREPRESENTED_ANNUAL_OBSERVATION725')
    return dict(url=expected_url,sha256=raw_sha256,claims=len(scope),actualPeriodStartsSourceBound=True,allOfficialAnnualObservationsRepresented=True)
