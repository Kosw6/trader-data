"""Named Alaska 2013 diluted-EPS split bridge, preserving every observation.

Approval requires the explicit distribution/record dates, all original primary
statements, unchanged net income, doubled diluted shares and halved literal EPS.
It never derives EPS from rounded net income or changes any price observations.
"""
import gzip, hashlib, re
from decimal import Decimal
from pipeline.legacy_statement_total699 import document, clean
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.reviewed_metric_identity726 import checked_identity
from pipeline.original_sec_scope725 import verify

BASIS='REVIEWED_ALASKA_EXPLICIT_SPLIT_DILUTED_EPS727'
POLICY='sec-alaska-explicit-split-diluted-eps727-v1'
RAW_SHA256='c511c7f55479ea30cc08657f29faa7a7e1eb7c11d52fede4978a55ad400f04c6'
PERIODS={('2013-01-01','2013-12-31')}
CAUSE_SHA256='9a1cedf70e271236a7cd362de4580722e3a1b75270b340a70c8ac664977dc3b9'
CLAUSES=("the board of directors declared a two-for-one stock split by means of a stock distribution.",
         "the additional shares were distributed on july 9, 2014, to the shareholders of record on june 23, 2014.",
         "all historical share and per share information has been recast to reflect the changes in the company's equity structure.")

def metadata(company,r):
    if company!=639 or r['companyId']!=639 or r['metricId']!=28 or r['metric']!='DILUTED_EPS' or r['chosenTag']!='EarningsPerShareDiluted' or (r['start'],r['end']) not in PERIODS:
        raise ValueError('ALASKA_NAMED_EPS_SCOPE727')

def vector(rows,context):
    if not clean(context).endswith('consolidated statements of operations'):
        raise ValueError('ALASKA_FULL_PRIMARY_TITLE727')
    nonempty=[(i,row) for i,row in enumerate(rows) if any(clean(c) for c in row)]
    if not nonempty or clean(nonempty[0][1][0])!='year ended december 31 (in millions, except per-share amounts)':
        raise ValueError('ALASKA_EXPLICIT_PRIMARY_UNIT727')
    years=[int(c) for c in nonempty[0][1][1:] if c.strip()]
    if len(years)!=3 or years!=list(range(years[0],years[0]-3,-1)):
        raise ValueError('ALASKA_THREE_ANNUAL_COLUMNS727')
    def locate(label):
        hits=[(i,r) for i,r in enumerate(rows) if r and clean(r[0])==label]
        if len(hits)!=1:raise ValueError('ALASKA_UNIQUE_PRIMARY_LINE727')
        return hits[0]
    for name in ('operating revenues','total operating revenues','total operating expenses','operating income'):
        locate(name)
    income,ir=locate('net income');eps,er=locate('diluted earnings per share:')
    shares_header,_=locate('shares used for computation:');shares,sr=locate('diluted')
    if not income<eps<shares_header<shares:raise ValueError('ALASKA_PRIMARY_SHARE_HIERARCHY727')
    def values(row,pattern,scale):
        cells=[c.strip() for c in row[1:] if numeric(c) is not None]
        if len(cells)!=3 or any(not re.fullmatch(pattern,c) for c in cells):raise ValueError('ALASKA_LITERAL_PRIMARY_VALUE727')
        return [numeric(c)*scale for c in cells]
    ni=values(ir,r'\d{1,3}(?:,\d{3})*',Decimal(1000000))
    ep=values(er,r'\d+\.\d{2}',Decimal(1))
    sh=values(sr,r'\d+\.\d{3}',Decimal(1000000))
    return {str(y)+'-12-31':dict(netIncome=n,eps=e,dilutedShares=s) for y,n,e,s in zip(years,ni,ep,sh)}

def split_cause(e,root):
    if e['sha256']!=CAUSE_SHA256:raise ValueError('ALASKA_SPLIT_SOURCE_PIN727')
    path=root/'documents'/(e['sha256']+'.html.gz')
    if path.is_symlink():raise ValueError('ALASKA_SPLIT_SOURCE_PATH727')
    with gzip.open(path,'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=e['sha256']:raise ValueError('ALASKA_SPLIT_SOURCE_BYTES727')
    from pipeline.reconciliation_review679 import Tables
    class Visible(Tables):
        def __init__(self):super().__init__();self.visible=[]
        def handle_data(self,data):
            if not self.hidden:self.visible.append(data)
            super().handle_data(data)
    p=Visible();p.feed(body.decode('utf8','replace'));text=clean(' '.join(p.visible));positions=[]
    for clause in CLAUSES:
        starts=[m.start() for m in re.finditer(re.escape(clause),text)]
        if len(starts)!=1:raise ValueError('ALASKA_EXPLICIT_SPLIT_CLAUSE727')
        positions.append(starts[0])
    if positions!=sorted(positions) or positions[-1]-positions[0]>2000:raise ValueError('ALASKA_BOUND_SPLIT_SECTION727')
    return dict(ratio='2:1',distributionDate='2014-07-09',recordDate='2014-06-23',sha256=e['sha256'],url=e['url'],clauses=list(CLAUSES))

def approve(scope,entry,root):
    r=entry['review'];metadata(r['companyId'],r)
    if r['basis']!=BASIS or str(int(entry['cik']))!='766421':raise ValueError('ALASKA_COMPANY_BASIS727')
    checked=checked_identity(scope,r,company=639,cik='0000766421',metric='DILUTED_EPS',tag='EarningsPerShareDiluted',periods=PERIODS)
    raw=verify(scope,r['originalRawEvidence'],root,company=639,cik='0000766421',raw_sha256=RAW_SHA256,metric='DILUTED_EPS',tags={'EarningsPerShareDiluted'})
    nodes=r['splitPrimaryEvidence']['nodes'];urls=[n['statement']['url'] for n in nodes];dates=[n['filingDate'] for n in nodes]
    if len(nodes)!=3 or len(set(urls))!=3 or dates!=sorted(set(dates)) or set(urls)!={x['source_url'] for x in scope} or dates[-1]!=r['filingDate']:
        raise ValueError('ALASKA_ALL_PRIMARY_REPORTS727')
    if {x['source_url'] for x in checked['originalSources']}!={urls[0]} or {x['source_url'] for x in checked['sources']}!={urls[-1]}:
        raise ValueError('ALASKA_PRIMARY_ENDPOINT_REPORTS727')
    vectors=[]
    for n in nodes:
        e=n['statement']
        if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('ALASKA_PRIMARY_DOCUMENT_BINDING727')
        rows,context,_=document(e,root);v=vector(rows,context)
        if r['end'] not in v or max(v)>n['filingDate']:raise ValueError('ALASKA_TARGET_OR_FUTURE727')
        vectors.append(v)
    cause=split_cause(nodes[1]['statement'],root)
    if not dates[0]<'2014-07-09'<dates[1]:raise ValueError('ALASKA_SPLIT_REPORT_CHRONOLOGY727')
    overlaps=[]
    for i,(a,b) in enumerate(zip(vectors,vectors[1:])):
        common=sorted(set(a)&set(b))
        if len(common)!=2:raise ValueError('ALASKA_ALL_COMPARATIVE_COLUMNS727')
        for day in common:
            expected=dict(netIncome=a[day]['netIncome'],eps=a[day]['eps']/(2 if i==0 else 1),dilutedShares=a[day]['dilutedShares']*(2 if i==0 else 1))
            if b[day]!=expected:raise ValueError('ALASKA_INCOME_SHARES_EPS_SPLIT_EQUATIONS727')
        overlaps.append(common)
    for row in scope:
        i=urls.index(row['source_url']);v=vectors[i]
        if str(row['filing_date'])!=dates[i] or str(row['report_date'])!=max(v) or Decimal(str(row['value']))!=v[r['end']]['eps']:
            raise ValueError('ALASKA_EVERY_ORIGINAL_REPORT_VALUE_PERIOD727')
    proof=dict(policy=POLICY,review=r,sources=checked['sources'],originalSources=checked['originalSources'],originalRawEvidence=raw,split=cause,
               primaryVectors=[{d:{k:str(v) for k,v in x.items()} for d,x in values.items()} for values in vectors],overlappingActualPeriodEnds=overlaps,
               allSourceReportsBound=True,originalObservationsPreserved=True,accountingCause='EXPLICIT_TWO_FOR_ONE_STOCK_SPLIT',
               meaning='Only named 2013 diluted EPS. Literal historical EPS and diluted shares are split-recast; total net income is unchanged. No price adjustment or other metric approval.')
    return {k:checked[k] for k in ['company_id','metric_id','source_tag','end']}|dict(proof=proof,proof_hash=digest(proof))
