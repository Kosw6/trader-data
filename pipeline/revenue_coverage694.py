"""Reviewed component-versus-total revenue coverage. Not a restatement policy."""
import gzip,hashlib,re
from datetime import datetime
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.bound_reconciliation685 import period_years,norm
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest

BASIS='REVIEWED_ORIGINAL_REVENUE_COMPONENT_COVERAGE'
POLICY='sec-reviewed-original-revenue-coverage694-v1'


def bound_table(entry,root,e,end,value,*,original=False):
    if not any(d['url']==e.get('url') and d['sha256']==e.get('sha256') for d in entry['documents']):raise ValueError('COVERAGE_DOCUMENT_IDENTITY')
    with gzip.open(root/'documents'/(e['sha256']+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=e['sha256']:raise ValueError('COVERAGE_DOCUMENT_CHANGED')
    parser=Tables();parser.feed(body.decode('utf8',errors='replace'))
    matches=[(rs,c) for rs,c in zip(parser.tables,parser.contexts) if digest(rs)==e['tableHash']]
    if len(matches)!=1:raise ValueError('COVERAGE_EXACT_TABLE_REQUIRED')
    rows,context=matches[0];yr=e['yearRow'];total=e['totalRow']
    if type(yr) is not int or type(total) is not int or not 0<=yr<total<len(rows):raise ValueError('COVERAGE_ROW_POSITION')
    compound=e.get('componentCoverageContract')=='OPERATING_REVENUE_PLUS_INVESTMENT696_V1'
    title='consolidated statements of income' if compound else 'statements of consolidated earnings'
    if title not in norm(context+' '+' '.join(' '.join(r) for r in rows[:total])):raise ValueError('COVERAGE_CONSOLIDATED_STATEMENT_REQUIRED')
    years=period_years(rows[yr]);target=str(e['fiscalYear'])
    if not 1<=len(years)<=3 or len(set(years))!=len(years) or years.count(target)!=1:raise ValueError('COVERAGE_YEAR_COLUMNS')
    if any(period_years(r) for r in rows[yr+1:total]):raise ValueError('COVERAGE_CROSS_PERIOD_BINDING')
    heading=e.get('annualHeading','')
    if not re.fullmatch(r'(?:Years?|For the years?) ended (?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2},?',heading,re.I):raise ValueError('COVERAGE_ANNUAL_HEADING')
    if norm(heading) not in norm(context+' '+' '.join(' '.join(r) for r in rows[:total])):raise ValueError('COVERAGE_ANNUAL_HEADING')
    date_text=re.sub(r'^(?:Years?|For the years?) ended ','',heading,flags=re.I).rstrip(',')+', '+target
    if datetime.strptime(date_text,'%B %d, %Y').date().isoformat()!=end:raise ValueError('COVERAGE_PERIOD_END')
    scale=Decimal(str(e['unitScale']));unit={Decimal(1000):'in thousands',Decimal(1000000):'in millions'}.get(scale)
    if not unit or unit not in norm(context+' '+' '.join(' '.join(r) for r in rows)):raise ValueError('COVERAGE_UNIT')
    if norm(rows[total][0])!='total revenues' or norm(e['totalLabel'])!='total revenues':raise ValueError('COVERAGE_TOTAL_LABEL')
    def amount(index):
        vals=[v for cell in rows[index] if (v:=numeric(cell)) is not None]
        if len(vals)!=len(years):raise ValueError('COVERAGE_VALUE_COLUMNS')
        return vals[years.index(target)]*scale
    if amount(total)!=Decimal(str(value)):raise ValueError('COVERAGE_TOTAL_VALUE')
    result={'documentSha256':e['sha256'],'tableHash':e['tableHash'],'periodEnd':end,'totalUSD':str(amount(total))}
    if original:
        components=e.get('componentRows',[]);labels=e.get('componentLabels',[])
        if not 2<=len(components)<=4 or len(labels)!=len(components) or len(set(components))!=len(components):raise ValueError('COVERAGE_COMPONENT_CONTRACT')
        expected=list(range(total-len(components),total))
        if compound:
            if components!=[i for i in range(components[0],total) if any(c.strip() for c in rows[i])]:raise ValueError('COVERAGE_NONCONTIGUOUS_COMPONENTS')
        elif components!=expected:raise ValueError('COVERAGE_NONCONTIGUOUS_COMPONENTS')
        starts=e.get('labelStartRows',components)
        if len(starts)!=len(components):raise ValueError('COVERAGE_COMPONENT_LABEL')
        for n,(i,label,begin) in enumerate(zip(components,labels,starts)):
            if type(begin) is not int or begin not in ([i-1,i] if n==0 else [i]):raise ValueError('COVERAGE_COMPONENT_LABEL')
            if begin<i and (begin<=yr or any(numeric(c) is not None for c in rows[begin])):raise ValueError('COVERAGE_COMPONENT_LABEL')
            if norm(' '.join(rows[j][0] for j in range(begin,i+1)))!=norm(label):raise ValueError('COVERAGE_COMPONENT_LABEL')
        first=norm(labels[0]);terms=[re.sub(r'\s*\([a-z]\)$','',norm(label)) for label in labels[1:]]
        if compound:
            section=e.get('revenueSectionRow')
            if [norm(label) for label in labels]!=['premiums','services','investment income'] or type(section) is not int or section!=components[0]-1 or norm(' '.join(rows[section]))!='revenues:':raise ValueError('COVERAGE_PREMIUM_SERVICE_INVESTMENT_SECTION')
        elif not first.startswith('revenues, other than ') or any(term not in first for term in terms):raise ValueError('COVERAGE_NAMED_EXCLUSIONS_REQUIRED')
        values=[amount(i) for i in components]
        original_amount=sum(values[:2]) if compound else values[0]
        if original_amount!=Decimal(entry['review']['originalValue']) or sum(values)!=Decimal(str(value)):raise ValueError('COVERAGE_COMPONENT_EQUATION')
        result.update(componentLabels=labels,componentAmountsUSD=list(map(str,values)),classification='PARTIAL_SOURCE_TAG_VERSUS_TOTAL_REVENUE_NOT_A_RESTATEMENT')
        if compound:result.update(originalSubset=['Premiums','Services'],originalSubsetUSD=str(original_amount))
    return result


def approve(scope,entry,root):
    r=entry['review']
    if r.get('basis')!=BASIS:raise ValueError('COVERAGE_BASIS')
    # Reuse source-identity/annual/hash guards, then replace the internal numeric
    # delta contract with the independently checked statement coverage proof.
    shadow={**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(Decimal(r['value'])-Decimal(r['originalValue']))}
    checked=reviewed_approval(scope,shadow)
    original=r.get('originalCoverageEvidence',{});later=r.get('selectedTotalEvidence',{})
    if {x['source_url'] for x in checked['proof']['originalSources']}!={original.get('url')}:raise ValueError('COVERAGE_ORIGINAL_FILING')
    if {x['source_url'] for x in checked['proof']['sources']}!={later.get('url')}:raise ValueError('COVERAGE_SELECTED_FILING')
    if any(str(x['fiscal_year'])!=str(original.get('fiscalYear')) for x in checked['proof']['originalSources']):raise ValueError('COVERAGE_FISCAL_YEAR')
    a=bound_table(entry,root,original,r['end'],r['value'],original=True)
    b=bound_table(entry,root,later,r['end'],r['value'])
    policy='sec-reviewed-compound-operating-revenue696-v1' if original.get('componentCoverageContract')=='OPERATING_REVENUE_PLUS_INVESTMENT696_V1' else POLICY
    proof={**checked['proof'],'policy':policy,'review':r,'accountingCause':'SOURCE_TAG_COMPONENT_COVERAGE','originalStatement':a,'selectedStatement':b,'restatementClaimed':False}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
