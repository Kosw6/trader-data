"""Two finite source-reviewed gaps; no generic missing-original fallback.

Bind exact current observations, original SEC companyfacts and all primary
reports. Tesla's end-only mapping includes the separately verified Q4 scope.
Alphabet's original filing must exist live even though its EPS observation is
absent from the frozen companyfacts. Never insert or rewrite a source fact.
"""
from decimal import Decimal, ROUND_HALF_UP
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.original_sec_scope725 import verify
from pipeline.legacy_statement_total699 import document, clean
from pipeline.reconciliation_review679 import numeric

ALPHABET='REVIEWED_ALPHABET_2017_ORIGINAL_PRIMARY_WITNESS20261101'
TESLA='REVIEWED_TESLA_2018_ANNUAL_Q4_PRECISION20261101'
POLICY='sec-serving-source-review20261101-v1'
ROOT='https://www.sec.gov/Archives/edgar/data/'
CASES={
 3238:dict(basis=ALPHABET,cik='0001652044',metric='DILUTED_EPS',metricId=28,start='2017-01-01',end='2017-12-31',tag='EarningsPerShareDiluted',value='18',raw='4ae19ee116eed252562b95f6881e5e9f4bf67e6106782ddb8b72cc9508b16ca7',scope='5d3bf2aef6bc684e97c24cd5174cb38cf8e23bb27378be02ddf5fa098139e57c'),
 2064:dict(basis=TESLA,cik='0001318605',metric='REVENUE',metricId=22,start='2018-01-01',end='2018-12-31',tag='Revenues',value='21461000000',raw='54c7dec9d0aa7f695536b54fd617abc982deef820b75990d70d42ba2848bcec7',scope='022df7667c5b52f3a4cc2d6bc545b3e72d6f3804f5f0bcc4416de10dfc4ff851'),
}
# (SEC path, full HTML SHA, primary statement table SHA, optional note table SHA)
REPORTS={
 3238:[
 ('1652044/000165204418000007/goog10-kq42017.htm','7fd72deeac3cd2d03def351341032e385552c7bd795192a3f40ad460690f2fe0','47a46e51c3365abf7789531426e744172fe724f5c14178ffcc818c4daa038d9a','7dd8c3816add6f5ad96d6c25c236b7ce5b193521ac85b5c1de5081f1d4d4de7c'),
 ('1652044/000165204419000004/goog10-kq42018.htm','d584f25841513a4b35e785bd191284063130bf13c1500586507450073bf29afe','d79ae72438849ea779b29ec32f920e1c986054724bc7218040b0651d25f67419','7dd8c3816add6f5ad96d6c25c236b7ce5b193521ac85b5c1de5081f1d4d4de7c'),
 ('1652044/000165204420000008/goog10-k2019.htm','a55ec0e8d23d9081a7e6c15f9d9031434ece7d46d2f0cbb818ce297f8480c2a1','1eecb66e207ce6b0715de2eab23a7102350b2b2a552187a83ebfc07f8d307837','7dd8c3816add6f5ad96d6c25c236b7ce5b193521ac85b5c1de5081f1d4d4de7c')],
 2064:[
 ('1318605/000156459019003165/tsla-10k_20181231.htm','75232abcaaf063e714f6a1852ca608cb2d68c5f17efee1a1800b566d54a5cc95','c5f77654f3a3e6a3094b5497fc99008f1f65568f7dd954e3faeb3299a38c43a2','03163e6092c9810bd9b3919bd1583fd559ee3ea9bb909021ae0a3c1d720d0722'),
 ('1318605/000156459020004475/tsla-10k_20191231.htm','5eb85c83bf11e0d8ed94e3481229498a76820ad283010c2e1e073b9dbcac3811','ff3fc76bfc7b393487882701903e95591efceb193b8c4403092d0921577cd66d','8433bec0d28543a61910f74e07d94cf178e24266e85ffeec42589f149fbfc5ba'),
 ('1318605/000156459021004599/tsla-10k_20201231.htm','f82bc57cbe340ac036e1c2dc946209c088259906260b83545d1cbd39f911dc65','44d95c3606f127bb2c2272c734c7a21ae5c20c5d9c1f5e0b4fc5d4603a5b006f',None)]}
ORIGINAL=dict(id=1000000477729,company_id=3238,accession_number='0001652044-18-000007',filing_date='2018-02-06',report_date='2017-12-31',document_url=ROOT+REPORTS[3238][0][0],form_type='10-K')

def metadata(company,r):
    c=CASES.get(company)
    if not c or (r['basis'],r['companyId'],r['metricId'],r['metric'],r['start'],r['end'],r['chosenTag'])!=(c['basis'],company,c['metricId'],c['metric'],c['start'],c['end'],c['tag']):
        raise ValueError('SERVING_NAMED_SOURCE_SCOPE20261101')
    return c

def scope_query(sql,company,r):
    c=metadata(company,r)
    if company==2064:
        # The approval is keyed by period end. Bind ALL stored starts at that
        # end, including Q4, so an unseen quarter can never inherit approval.
        return sql.replace('f.period_start=%s AND f.period_end=%s','f.period_end=%s'),(company,r['end'])
    sql=sql.replace("m.metric_code='REVENUE'","m.metric_code='DILUTED_EPS'").replace("f.unit='USD'","f.unit='USD/shares'")
    witness=" AND (SELECT count(*) FROM public.filing w WHERE w.company_id=3238 AND w.id=1000000477729 AND w.accession_number='0001652044-18-000007' AND w.filing_date='2018-02-06' AND w.report_date='2017-12-31' AND w.form_type='10-K' AND w.document_url='"+ORIGINAL['document_url']+"')=1 "
    return sql.replace('ORDER BY f.id',witness+' ORDER BY f.id'),(company,r['start'],r['end'])

def numbers(row):
    return [n for c in row[1:] if (n:=numeric(c)) is not None]

def line(rows,label):
    hits=[r for r in rows if r and clean(r[0])==label]
    if len(hits)!=1:raise ValueError('SERVING_UNIQUE_PRIMARY_LINE20261101')
    return numbers(hits[0])

def table(root,report,kind):
    path,sha,annual,note=report
    return document(dict(url=ROOT+path,sha256=sha,tableHash=annual if kind=='annual' else note),root)[:2]

def alphabet_vectors(root):
    vectors=[]
    for i,report in enumerate(REPORTS[3238]):
        rows,context=table(root,report,'annual')
        if not clean(context).endswith('alphabet inc. consolidated statements of income (in millions, except per share amounts)') or clean(' '.join(rows[2]))!='year ended december 31,':
            raise ValueError('ALPHABET_PRIMARY_TITLE_UNIT20261101')
        years=[int(v) for v in numbers(rows[3])]
        if years!=list(range(2015+i,2018+i)):raise ValueError('ALPHABET_ACTUAL_COLUMNS20261101')
        if i==0:
            a=line(rows,'diluted net income per share of class a and b common stock');b=line(rows,'diluted net income per share of class c capital stock')
            if a[-1]!=b[-1]:raise ValueError('ALPHABET_TARGET_CLASS_AGREEMENT20261101')
        else:a=line(rows,'diluted net income per share of class a and b common stock and class c capital stock')
        income=line(rows,'net income')
        if len(a)!=3 or len(income)!=3:raise ValueError('ALPHABET_PRIMARY_COLUMNS20261101')
        v={str(y):dict(eps=e,netIncome=n*1000000) for y,e,n in zip(years,a,income)}
        note,_=table(root,report,'note')
        if numbers(note[3])!=[Decimal(2017)] or [clean(c) for c in note[4] if clean(c)]!=['class a','class b','class c'] or line(note,'diluted net income per share')!=[Decimal(18)]*3 or v['2017']['eps']!=18:
            raise ValueError('ALPHABET_ORIGINAL_ALL_CLASSES20261101')
        vectors.append(v)
    for a,b in zip(vectors,vectors[1:]):
        common=set(a)&set(b)
        if len(common)!=2 or any(a[k]!=b[k] for k in common):raise ValueError('ALPHABET_ALL_COMPARATIVES20261101')
    return vectors

def tesla_vectors(root):
    vectors=[];quarters=[]
    for i,report in enumerate(REPORTS[2064]):
        rows,context=table(root,report,'annual');scale=Decimal(1000 if i==0 else 1000000)
        unit='thousands' if i==0 else 'millions'
        if not clean(context).endswith('tesla, inc. consolidated statements of operations (in '+unit+', except per share data)') or clean(' '.join(rows[0]))!='year ended december 31,':
            raise ValueError('TESLA_PRIMARY_TITLE_UNIT20261101')
        years=[int(n) for n in numbers(rows[1])];values=line(rows,'total revenues')
        if years!=[2018+i,2017+i,2016+i] or len(values)!=3:raise ValueError('TESLA_PRIMARY_ANNUAL_COLUMNS20261101')
        vectors.append({str(y):n*scale for y,n in zip(years,values)})
        if i<2:
            qr,qc=table(root,report,'note')
            if not clean(qc).endswith('(in '+unit+', except per share amounts):') or 'quarterly results of operations (unaudited)' not in clean(qc) or clean(' '.join(qr[0]))!='three months ended' or [clean(c) for c in qr[1] if clean(c)]!=['march 31','june 30','september 30','december 31']:
                raise ValueError('TESLA_QUARTER_DATES_UNIT20261101')
            out={};year=None
            for row in qr[2:]:
                nonempty=[clean(c) for c in row if clean(c)]
                if len(nonempty)==1 and nonempty[0] in {'2017','2018','2019'}:year=nonempty[0]
                if row and clean(row[0])=='total revenues':
                    ns=numbers(row)
                    if year is None or year in out or len(ns)!=4:raise ValueError('TESLA_QUARTER_YEAR_BINDING20261101')
                    out[year]=[n*scale for n in ns]
            if set(out)!={str(2018+i),str(2017+i)}:raise ValueError('TESLA_QUARTER_COMPLETE_YEARS20261101')
            quarters.append(out)
    def rounded(n):return (n/1000000).quantize(Decimal(1),rounding=ROUND_HALF_UP)*1000000
    for i,(a,b) in enumerate(zip(vectors,vectors[1:])):
        common=set(a)&set(b)
        if len(common)!=2 or any((rounded(a[k]) if i==0 else a[k])!=b[k] for k in common):raise ValueError('TESLA_ANNUAL_PRECISION_EQUATION20261101')
    if [rounded(n) for n in quarters[0]['2018']]!=quarters[1]['2018'] or sum(quarters[0]['2018'])!=vectors[0]['2018']:
        raise ValueError('TESLA_ALL_FOUR_QUARTERS_PRECISION20261101')
    return vectors,quarters

def approve(scope,entry,root):
    r=entry['review'];company=r['companyId'];c=metadata(company,r)
    if str(entry['cik'])!=c['cik'] or digest(scope)!=c['scope'] or r['sourceScopeHash']!=c['scope'] or Decimal(r['value'])!=Decimal(c['value']):
        raise ValueError('SERVING_EXACT_FROZEN_OBSERVATIONS20261101')
    reports=REPORTS[company];docs=[dict(url=ROOT+p,sha256=h) for p,h,_,_ in reports]
    if entry['documents']!=docs or r['officialDocuments']!=[d['url'] for d in docs]:raise ValueError('SERVING_ALL_PRIMARY_DOCUMENTS20261101')
    starts=sorted({str(x['period_start']) for x in scope});expected=[c['start']] if company==3238 else ['2018-01-01','2018-10-01']
    if starts!=expected:raise ValueError('SERVING_ALL_ACTUAL_PERIODS20261101')
    raw=[]
    for start in starts:
        raw.append(verify([x for x in scope if str(x['period_start'])==start],r['originalRawEvidence'],root,company=company,cik=c['cik'],raw_sha256=c['raw'],metric=c['metric'],tags={x['source_tag'] for x in scope}))
    selected=[x for x in scope if x['source_tag']==c['tag'] and str(x['filing_date'])==max(str(y['filing_date']) for y in scope)]
    if {str(x['filing_date']) for x in selected}!={r['filingDate']} or {x['accession_number'] for x in selected}!=set(r['accessions']) or {x['source_fact_hash'] for x in selected}!=set(r['sourceFactHashes']) or not r.get('reviewExplanation') or r['reviewedOn']<r['filingDate']:
        raise ValueError('SERVING_LATEST_SOURCE_SELECTION20261101')
    if company==3238:
        vectors=alphabet_vectors(root);quarters=[]
        if r.get('originalFilingWitness')!=ORIGINAL:raise ValueError('ALPHABET_ORIGINAL_FILING_WITNESS20261101')
        for row in scope:
            i=[d['url'] for d in docs].index(row['source_url'])
            if Decimal(str(row['value']))!=vectors[i]['2017']['eps']:raise ValueError('ALPHABET_EVERY_SOURCE_EPS20261101')
        cause='ORIGINAL_PRIMARY_AND_ALL_COMPARATIVES_UNCHANGED'
        meaning='2017 literal historical USD/share, as reported before the 2022 split. Not a current split-adjusted EPS or a newly synthesized original fact.'
    else:
        vectors,quarters=tesla_vectors(root)
        for row in scope:
            i=[d['url'] for d in docs].index(row['source_url'])
            v=vectors[i]['2018'] if str(row['period_start'])==c['start'] else quarters[i]['2018'][3]
            if Decimal(str(row['value']))!=v:raise ValueError('TESLA_EVERY_ANNUAL_Q4_SOURCE20261101')
        cause='PRIMARY_THOUSANDS_TO_MILLIONS_PRESENTATION_PRECISION'
        meaning='Original annual 21461268000 USD and Q4 7225873000 USD remain unchanged in storage. Later statements report 21461000000 and 7226000000 USD at million-dollar precision. No economic restatement inferred.'
    proof=dict(policy=POLICY,review=r,sources=selected,allStoredSourceHashes=sorted(x['source_fact_hash'] for x in scope),originalRawEvidence=raw,primaryVectors=vectors,quarterVectors=quarters,accountingCause=cause,meaning=meaning,originalObservationsPreserved=True,allActualPeriodStartsVerified=True)
    return dict(company_id=company,metric_id=c['metricId'],source_tag=c['tag'],end=c['end'],proof=proof,proof_hash=digest(proof))
