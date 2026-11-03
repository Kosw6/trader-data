"""Candidate discovery and table review cards. Output is NEVER an approval."""
import re
from datetime import date
from decimal import Decimal,InvalidOperation
from html.parser import HTMLParser

TAGS={'Revenues','RevenueFromContractWithCustomerExcludingAssessedTax'}

def candidate(scope):
    try:return _candidate(scope)
    except (ValueError,TypeError,KeyError,InvalidOperation):return None

def _candidate(scope):
    rows=scope['rows']
    if scope['metric']!='REVENUE' or not rows:return None
    if any(r['taxonomy']!='us-gaap' or r['unit']!='USD' or r['dimensions'] not in (None,{}) for r in rows):return None
    ends={str(r['period_end']) for r in rows};companies={int(r['company_id']) for r in rows};ciks={int(r['cik']) for r in rows}
    if ends!={scope['end']} or companies!={scope['companyId']} or len(ciks)!=1:return None
    starts=set()
    for r in rows:
        if r['period_start'] and r['form_type'] in {'10-K','10-K/A'} and 335<=(date.fromisoformat(r['period_end'])-date.fromisoformat(r['period_start'])).days<=395:starts.add(r['period_start'])
    if len(starts)!=1:return None
    start=next(iter(starts));annual=[r for r in rows if r['period_start']==start and r['form_type'] in {'10-K','10-K/A'}]
    original=[r for r in annual if r['source_tag']=='SalesRevenueNet' and r['report_date']==scope['end']]
    if not original:return None
    tag='RevenueFromContractWithCustomerExcludingAssessedTax' if any(r['source_tag']=='RevenueFromContractWithCustomerExcludingAssessedTax' for r in annual) else 'Revenues'
    selected=[r for r in annual if r['source_tag']==tag]
    if not selected:return None
    latest=max(r['filing_date'] for r in selected);selected=[r for r in selected if r['filing_date']==latest]
    a={Decimal(str(r['value'])) for r in original};b={Decimal(str(r['value'])) for r in selected}
    if len(a)!=1 or len(b)!=1:return None
    a=next(iter(a));b=next(iter(b))
    if not a.is_finite() or not b.is_finite() or a==b:return None
    return {'companyId':scope['companyId'],'cik':str(next(iter(ciks))).zfill(10),'metric':'REVENUE','start':start,'end':scope['end'],
        'original':str(a),'chosen':str(b),'adjustment':str(b-a),'tag':tag,
        'urls':sorted({r['source_url'] for r in original+selected}),
        'originalReportUrls':sorted({r['source_url'] for r in original}),
        'selectedReportUrls':sorted({r['source_url'] for r in selected}),
        'requiresHumanReview':True,'approvalGenerated':False}

class Tables(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True);self.stack=[];self.tables=[];self.contexts=[];self.flow='';self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}:self.hidden+=1
        if self.hidden:return
        if tag=='table':self.stack.append({'rows':[],'row':None,'cell':None,'context':self.flow[-1200:]})
        elif self.stack and tag=='tr':self.stack[-1]['row']=[]
        elif self.stack and tag in {'td','th'}:self.stack[-1]['cell']=[]
    def handle_data(self,data):
        if not self.hidden:self.flow=(self.flow+' '+data)[-2000:]
        if not self.hidden and self.stack and self.stack[-1]['cell'] is not None:self.stack[-1]['cell'].append(data)
    def handle_endtag(self,tag):
        if tag in {'script','style'}:self.hidden=max(0,self.hidden-1);return
        if self.hidden or not self.stack:return
        item=self.stack[-1]
        if tag in {'td','th'} and item['cell'] is not None:
            if item['row'] is not None:item['row'].append(re.sub(r'\s+',' ',''.join(item['cell'])).strip())
            item['cell']=None
        elif tag=='tr' and item['row'] is not None:
            item['rows'].append(item['row']);item['row']=None
        elif tag=='table':
            item=self.stack.pop();self.tables.append(item['rows']);self.contexts.append(item['context'])

def numeric(cell):
    cell=cell.strip().replace('$','').replace(',','').replace('−','-')
    if not re.fullmatch(r'\(?-?\d+(?:\.\d+)?\)?',cell):return None
    try:return -Decimal(cell.strip('()')) if cell.startswith('(') else Decimal(cell.strip('()'))
    except InvalidOperation:return None

def review_cards(body,item):
    if len(body)>32*1024**2:raise ValueError('REVIEW_DOCUMENT_BUDGET')
    parser=Tables();parser.feed(body.decode('utf8',errors='replace'));cards=[]
    for rows,context in zip(parser.tables,parser.contexts):
        text=' '.join(' '.join(row) for row in rows);unit_text=context+' '+text
        if not re.search(r'(?:in\s+millions|in\s+thousands)',unit_text,re.I):continue
        if not re.search(r'\bas\s*(?:previously\s*)?reported\b',text,re.I) or not re.search(r'as\s+(?:adjusted|restated|recast|revised)|adjusted\s+for\s+new\s+standards',text,re.I):continue
        units=re.findall(r'in\s+(millions|thousands)',text,re.I) or re.findall(r'in\s+(millions|thousands)',context,re.I)
        scale=Decimal(1000000 if units[-1].lower()=='millions' else 1000)
        expected=[Decimal(item[k])/scale for k in ['original','adjustment','chosen']]
        for row in rows:
            label=next((cell.strip() for cell in row if cell.strip()),'')
            if not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b',label,re.I):continue
            values=[n for cell in row if (n:=numeric(cell)) is not None]
            # An ordered reconciliation is a review aid, not proof of year/basis.
            matches=[]
            for i,value in enumerate(values):
                if value!=expected[0]:continue
                for j in range(i+2,min(i+7,len(values))):
                    if values[j]==expected[2] and sum(values[i+1:j])==expected[1]:matches.append([str(v) for v in values[i+1:j]])
            if matches:
                cards.append({'rowCells':row,'headerRows':rows[:5],'precedingContext':context,'scale':str(scale),'numericEquationMatches':True,'componentCandidates':matches,
                    'requiresHumanReview':True,'approvalGenerated':False})
    return cards
