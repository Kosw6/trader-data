"""Statement coverage diagnostics. Cards are never mapping approvals."""
import hashlib,re
from collections import OrderedDict
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.bound_reconciliation685 import period_years,norm
from pipeline.reviewed_financial_mapping672 import digest


class StatementCache:
    """Two private, byte-hash keyed parse results; never an evidence shortcut."""
    def __init__(self):self._items=OrderedDict()
    def parse(self,body):
        if len(body)>32*1024**2:raise ValueError('COVERAGE_REVIEW_DOCUMENT_BUDGET')
        sha=hashlib.sha256(body).hexdigest()
        if sha not in self._items:
            parser=Tables();parser.feed(body.decode('utf8','replace'));self._items[sha]=(parser.tables,parser.contexts)
            while len(self._items)>2:self._items.popitem(last=False)
        self._items.move_to_end(sha)
        return sha,self._items[sha]


def statement_cards(body,item,*,cache=None):
    if len(body)>32*1024**2:raise ValueError('COVERAGE_REVIEW_DOCUMENT_BUDGET')
    sha,(tables,contexts)=(cache or StatementCache()).parse(body);cards=[];target=item['end'][:4]
    for rows,context in zip(tables,contexts):
        for pos,row in enumerate(rows):
            if not row or not re.fullmatch(r'(?:total\s+)?(?:net\s+)?(?:revenues?|sales)',norm(row[0])):continue
            prefix=context+' '+' '.join(' '.join(r) for r in rows[:pos])
            if not (re.search(r'consolidated',prefix,re.I) and re.search(r'statements?',prefix,re.I)):continue
            headers=[(i,period_years(r)) for i,r in enumerate(rows[:pos]) if period_years(r)]
            if not headers:continue
            year_row,years=headers[-1]
            if not 1<=len(years)<=5 or len(set(years))!=len(years) or years.count(target)!=1:continue
            values=[v for c in row if (v:=numeric(c)) is not None]
            if len(values)!=len(years):continue
            units=re.findall(r'in\s+(millions|thousands)', ' '.join(' '.join(r) for r in rows[:pos+1]),re.I) or re.findall(r'in\s+(millions|thousands)',context,re.I)
            if len({u.casefold() for u in units})!=1:continue
            scale=Decimal(1000000 if units[-1].lower()=='millions' else 1000);amount=values[years.index(target)]*scale
            cards.append({'tableHash':digest(rows),'documentSha256':sha,'yearRow':year_row,'valueRow':pos,'targetYear':target,'rowLabel':row[0],'valueUSD':str(amount),'unitScale':str(scale),'yearColumns':years,'periodBinding':'YEAR_COLUMN_ONLY_ACTUAL_PERIOD_REVIEW_REQUIRED','requiresHumanReview':True,'approvalGenerated':False})
    return cards


def classify(item,documents,*,cache=None):
    """Only caller-identified original/selected URLs can supply coverage roles."""
    original=set(item.get('originalReportUrls',[]));selected=set(item.get('selectedReportUrls',[]));cards=[]
    for doc,body in documents:
        for card in statement_cards(body,item,cache=cache):
            cards.append({**card,'url':doc['url'],'role':'ORIGINAL' if doc['url'] in original else 'SELECTED' if doc['url'] in selected else 'UNBOUND_DOCUMENT'})
    a={Decimal(c['valueUSD']) for c in cards if c['role']=='ORIGINAL'};b={Decimal(c['valueUSD']) for c in cards if c['role']=='SELECTED'};status='INSUFFICIENT_BOUND_STATEMENT_CARDS'
    if len(a)>1 or len(b)>1:status='AMBIGUOUS_STATEMENT_TOTALS_REVIEW_REQUIRED'
    elif len(a)==len(b)==1:
        old=next(iter(a));new=next(iter(b));raw=Decimal(item['original']);chosen=Decimal(item['chosen'])
        if old==chosen==new and old!=raw:status='PARTIAL_ORIGINAL_SOURCE_COVERAGE_REVIEW_REQUIRED'
        elif old==raw==new and new!=chosen:status='SELECTED_TAG_MAY_NOT_BE_TOTAL_REVENUE'
        elif old!=raw and new==chosen and old!=new:status='COMPONENT_COVERAGE_AND_TOTAL_CHANGE_REVIEW_REQUIRED'
        elif old==raw and new==chosen and old!=new:status='REPORTED_TOTAL_CHANGE_REVIEW_REQUIRED'
        else:status='SOURCE_TAG_AND_STATEMENT_COVERAGE_REVIEW_REQUIRED'
    return {'classification':status,'statementCards':cards,'requiresHumanReview':True,'approvalGenerated':False,'databaseWrites':0,'restatementCauseInferred':False}
