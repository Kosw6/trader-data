"""Exact product/subscription revenue coverage plus a documented gross/net error.

The narrative's rounded corroboration never replaces the exact component,
total-revenue and equal-expense equations from the published tables.
"""
import gzip
import hashlib
import html
import re
from decimal import Decimal, ROUND_HALF_UP
from pipeline.legacy_statement_total699 import document, statement
from pipeline.bound_reconciliation685 import norm, period_years
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest

BASIS='REVIEWED_PRODUCT_COVERAGE_AND_GROSS_NET_ERROR'
POLICY='sec-reviewed-product-gross-error704-v1'
LABELS=[{'apparel'},{'footwear'},{'accessories'},{'total net sales'},{'license','license revenues','licensing revenues'},{'connected fitness'},{'intersegment eliminations'},{'total net revenues'}]


def category(entry,root,e,end):
    if (e['url'],e['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:
        raise ValueError('GROSS_ERROR_CATEGORY_DOCUMENT')
    rows,context,_=document(e,root);yr,first=e['yearRow'],e['firstValueRow']
    if type(yr) is not int or type(first) is not int or not 0<=yr<first or first+8!=len(rows):
        raise ValueError('GROSS_ERROR_CATEGORY_HIERARCHY')
    years=period_years(rows[yr]);fy=end[:4]
    top=norm(context+' '+' '.join(' '.join(r) for r in rows[:first]))
    if not 2<=len(years)<=3 or len(set(years))!=len(years) or years.count(fy)!=1 or not end.endswith('-12-31') or not re.search(r'year ended december 31,?',top) or 'net revenues by product category' not in top or 'in thousands' not in top:
        raise ValueError('GROSS_ERROR_CATEGORY_ANNUAL_UNIT')
    if any(period_years(r) for r in rows[yr+1:first]):
        raise ValueError('GROSS_ERROR_CATEGORY_PERIOD')
    result=[]
    for i,allowed in enumerate(LABELS):
        row=rows[first+i]
        if norm(row[0]) not in allowed:
            raise ValueError('GROSS_ERROR_CATEGORY_LABEL')
        values=[numeric(c) if numeric(c) is not None else Decimal(0) for c in row[1:] if numeric(c) is not None or c.strip()=='—']
        if len(values)!=len(years) or any(not x.is_finite() for x in values):
            raise ValueError('GROSS_ERROR_CATEGORY_COLUMNS')
        result.append([x*1000 for x in values])
    for col in range(len(years)):
        if sum(result[i][col] for i in range(3))!=result[3][col] or sum(result[i][col] for i in [3,4,5,6])!=result[7][col]:
            raise ValueError('GROSS_ERROR_CATEGORY_EXACT_EQUATION')
    return [r[years.index(fy)] for r in result]


def income_lines(root,e,end):
    rows,_,_=document(e,root);years=period_years(rows[e['periodRow']]);fy=end[:4]
    if years.count(fy)!=1 or e['unitScale']!=1000:
        raise ValueError('GROSS_ERROR_INCOME_PERIOD_UNIT')
    out={}
    for label in ['selling, general and administrative expenses','income (loss) from operations','income from operations']:
        matches=[r for r in rows if norm(r[0])==label]
        if len(matches)>1:raise ValueError('GROSS_ERROR_AMBIGUOUS_INCOME_LINE')
        if matches:
            values=[numeric(c) for c in matches[0][1:] if numeric(c) is not None]
            if len(values)!=len(years):raise ValueError('GROSS_ERROR_INCOME_COLUMNS')
            out['sga' if label.startswith('selling') else 'operating']=values[years.index(fy)]*1000
    if set(out)!={'sga','operating'}:raise ValueError('GROSS_ERROR_INCOME_LINES_REQUIRED')
    return out


def approve(scope,entry,root):
    r=entry['review'];a,b=Decimal(r['originalValue']),Decimal(r['value'])
    if r['basis']!=BASIS:raise ValueError('GROSS_ERROR_BASIS')
    checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
    evidence=r['componentGrossEvidence'];roles=[('original',checked['proof']['originalSources']),('selected',checked['proof']['sources'])];vectors={};financial={};totals={}
    for role,sources in roles:
        cat=evidence[role+'Category'];inc=evidence[role+'Statement']
        if {x['source_url'] for x in sources}!={inc['url']}:
            raise ValueError('GROSS_ERROR_SAME_REPORT_BINDING')
        if cat['url']!=inc['url']:
            if role!='selected':raise ValueError('GROSS_ERROR_ORIGINAL_SAME_REPORT_REQUIRED')
            causal=[x for x in scope if x['source_url']==cat['url'] and x['source_tag']==r['chosenTag'] and x['form_type'] in {'10-K','10-K/A'}]
            if not causal or {Decimal(str(x['value'])) for x in causal}!={b} or {x['source_fact_hash'] for x in causal}!=set(evidence.get('causalFactHashes',[])):
                raise ValueError('GROSS_ERROR_CAUSAL_VALUE_CHAIN')
            if not min(str(x['filing_date']) for x in checked['proof']['originalSources'])<=min(str(x['filing_date']) for x in causal)<=r['filingDate']:
                raise ValueError('GROSS_ERROR_CAUSAL_CHRONOLOGY')
        vector=category(entry,root,cat,r['end']);vectors[role]=vector
        totals[role]=statement(entry,root,inc,r['end'],vector[7],_check_competing=False)
        financial[role]=income_lines(root,inc,r['end'])
    old,new=vectors['original'],vectors['selected'];delta=new[5]-old[5]
    raw_scope=evidence['originalRawScope']
    if {'PRODUCT_NET_SALES':old[3],'TOTAL_NET_REVENUES':old[7]}.get(raw_scope)!=a or new[7]!=b or delta<=0 or any(old[i]!=new[i] for i in [0,1,2,3,4,6]) or old[7]+delta!=b:
        raise ValueError('GROSS_ERROR_COMPONENT_SCOPE_EQUATION')
    if financial['selected']['sga']-financial['original']['sga']!=delta or financial['selected']['operating']!=financial['original']['operating']:
        raise ValueError('GROSS_ERROR_EXPENSE_OFFSET_AND_OPERATING_INCOME')
    e=evidence['selectedCategory'];sha=e['sha256']
    with gzip.open(root/'documents'/(sha+'.html.gz'),'rb') as stream:body=stream.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('GROSS_ERROR_DOCUMENT_CHANGED')
    text=re.sub(r'<(script|style)\b[^>]*>.*?</\1>',' ',body.decode('utf8','replace'),flags=re.S|re.I);plain=norm(html.unescape(re.sub('<[^>]+>',' ',text)));cause=norm(evidence['causeExcerpt'])
    if not 200<=len(cause)<=2400 or cause not in plain or not all(x in cause for x in ['immaterial prior period error','premium subscriptions','connected fitness','net of any related commission','gross basis','revised the prior periods','no impact in any period on income (loss) from operations']):
        raise ValueError('GROSS_ERROR_EXPLICIT_CAUSE')
    match=re.search(r'increase in net revenues and selling, general and administrative expense of \$\s*(\d+\.\d) million and \$\s*(\d+\.\d) million for the years ended december 31, ((?:19|20)\d{2}) and ((?:19|20)\d{2}), respectively',cause)
    if not match or len(set(match.group(3,4)))!=2 or r['end'][:4] not in match.group(3,4):
        raise ValueError('GROSS_ERROR_ROUNDED_NARRATIVE_PERIOD')
    index=list(match.group(3,4)).index(r['end'][:4]);reported=Decimal(match.group(1+index))*1000000
    if (delta/100000).quantize(Decimal(1),rounding=ROUND_HALF_UP)*100000!=reported:
        raise ValueError('GROSS_ERROR_ROUNDED_NARRATIVE_CORROBORATION')
    proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':'IMMATERIAL_PRIOR_PERIOD_GROSS_NET_PRESENTATION_ERROR','originalRawScope':raw_scope,'originalRawValueUSD':str(a),'originalProductSalesUSD':str(old[3]),'originalTotalRevenueUSD':str(old[7]),'revisedTotalRevenueUSD':str(b),'exactSubscriptionGrossUpUSD':str(delta),'roundedNarrativeCorroborationUSD':str(reported),'exactSgaIncreaseUSD':str(delta),'operatingIncomeChangeUSD':'0','fullStatements':totals,'originalObservationsPreserved':True,'revenueStandardAdoptionInferred':False}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
