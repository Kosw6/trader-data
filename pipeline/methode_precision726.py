"""Two Methode operating-income periods at explicitly printed precision.

This contract approves no revenue, EPS, before-tax or net-income substitute.
Original SEC periods and every complete primary comparative are mandatory.
"""
import re
from datetime import datetime
from decimal import Decimal,ROUND_HALF_UP
from pipeline.legacy_statement_total699 import document,clean,DATE
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reviewed_metric_identity726 import checked_identity
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_METHODE_OPERATING_PRIMARY_PRECISION726'
POLICY='sec-methode-operating-primary-precision726-v1'
RAW_SHA256='f77c70c1b979c88ce9584e5d49e12911dc24eb31a1ecc54b35f6bca1a611d361'
PERIODS={('2011-05-01','2012-04-28'),('2012-04-29','2013-04-27')}


def metadata(company,r):
    if company!=229 or r['companyId']!=229 or r['metric']!='OPERATING_INCOME' or r['chosenTag']!='OperatingIncomeLoss' or (r['start'],r['end']) not in PERIODS:
        raise ValueError('METHODE_REGISTERED_OPERATING_SCOPE726')


def vector(rows,context):
    ctx=clean(context)
    prefix='methode electronics, inc. and subsidiaries consolidated statements of operations '
    if not (ctx.endswith(prefix+'(in thousands, except per share data)') or ctx.endswith(prefix+'(in millions, except per share data)')):
        raise ValueError('METHODE_FULL_PRIMARY_TITLE_UNIT726')
    scale=Decimal(1000) if ctx.endswith('(in thousands, except per share data)') else Decimal(1000000)
    nonempty=[(i,clean(' '.join(row))) for i,row in enumerate(rows) if any(clean(c) for c in row)]
    if len(nonempty)<10 or nonempty[0][1]!='fiscal year ended':raise ValueError('METHODE_PRIMARY_FISCAL_HEADER726')
    date_row=nonempty[1][0];matches=list(DATE.finditer(' '.join(rows[date_row])))
    if len(matches)!=3:raise ValueError('METHODE_PRIMARY_THREE_ACTUAL_DATES726')
    ends=[datetime.strptime(' '.join(m.groups()),'%B %d %Y').date().isoformat() for m in matches]
    if any(not 335<=(datetime.fromisoformat(a)-datetime.fromisoformat(b)).days<=395 for a,b in zip(ends,ends[1:])):raise ValueError('METHODE_PRIMARY_ACTUAL_PERIOD_ORDER726')
    revenue=[row for row in rows if row and clean(row[0])=='net sales'];found=[row for row in rows if row and clean(row[0])=='income from operations']
    if len(revenue)!=1 or len(found)!=1 or not any(row and clean(row[0])=='cost of products sold' for row in rows):raise ValueError('METHODE_OPERATING_PRIMARY_HIERARCHY726')
    cells=[c.strip() for c in found[0][1:] if numeric(c) is not None]
    places=0 if scale==1000 else 1
    pattern=r'\d{1,3}(?:,\d{3})*' if places==0 else r'\d+(?:,\d{3})*\.\d'
    if len(cells)!=3 or any(not re.fullmatch(pattern,c) for c in cells):raise ValueError('METHODE_PUBLISHED_OPERATING_PRECISION726')
    return dict(zip(ends,[numeric(c)*scale for c in cells])),scale/(Decimal(10)**places)


def approve(scope,entry,root):
    r=entry['review'];metadata(r['companyId'],r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='65270':raise ValueError('METHODE_COMPANY_BASIS726')
    checked=checked_identity(scope,r,company=229,cik='0000065270',metric='OPERATING_INCOME',tag='OperatingIncomeLoss',periods=PERIODS)
    raw=verify(scope,r['originalRawEvidence'],root,company=229,cik='0000065270',raw_sha256=RAW_SHA256,metric='OPERATING_INCOME',tags={'OperatingIncomeLoss'})
    nodes=r['primaryPrecisionEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:
        raise ValueError('METHODE_ALL_PRIMARY_REPORTS726')
    if {x['source_url'] for x in checked['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['sources']}!={urls[-1]}:raise ValueError('METHODE_PRIMARY_ENDPOINT_REPORTS726')
    vectors=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('METHODE_PRIMARY_DOCUMENT_BINDING726')
        rows,context,_=document(e,root);values,q=vector(rows,context)
        if r['end'] not in values or max(values)>n['filingDate']:raise ValueError('METHODE_TARGET_OR_FUTURE726')
        vectors.append((values,q))
    if vectors[-1][1]<=vectors[0][1]:raise ValueError('METHODE_EXPLICIT_PRECISION_REDUCTION726')
    def rounded(v,q):return (v/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q
    overlaps=[]
    for (a,aq),(b,bq) in zip(vectors,vectors[1:]):
        common=sorted(set(a)&set(b))
        if len(common)<2 or bq<aq or any(rounded(a[day],bq)!=b[day] for day in common):raise ValueError('METHODE_ALL_COMPARATIVE_PRECISION726')
        overlaps.append(common)
    for row in scope:
        i=urls.index(row['source_url']);values,q=vectors[i]
        if str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(values) or Decimal(str(row['value']))!=values[r['end']]:raise ValueError('METHODE_EVERY_ORIGINAL_REPORT_VALUE_PERIOD726')
    proof=dict(policy=POLICY,review=r,sources=checked['sources'],originalSources=checked['originalSources'],originalRawEvidence=raw,
               quantumsUSD=[str(q) for values,q in vectors],overlappingActualPeriodEnds=overlaps,
               allSourceReportsBound=True,originalObservationsPreserved=True,
               accountingCause='PUBLISHED_PRIMARY_OPERATING_INCOME_PRECISION_COMPATIBILITY',
               meaning='Only the two named operating-income periods. Values match at printed precision; no finer economic equivalence or other metric inference.')
    return {k:checked[k] for k in ['company_id','metric_id','source_tag','end']}|dict(proof=proof,proof_hash=digest(proof))
