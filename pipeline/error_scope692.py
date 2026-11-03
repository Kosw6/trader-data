"""Explicit annual error scope and component causes; never infer from an amount alone."""
import re
from datetime import date,datetime
from decimal import Decimal
from pipeline.reconciliation_review679 import Tables
from pipeline.reviewed_financial_mapping672 import digest

POLICY='sec-bound-structured-error692-v1'
def norm(s):return re.sub(r'\s+',' ',s).strip().casefold()


class LocatedTables(Tables):
    """Same table parser, with independent visible-text coordinates for note groups."""
    def __init__(self):
        super().__init__();self.visible=[];self.visible_length=0;self.spans=[]
    def handle_data(self,data):
        if not self.hidden:self.visible.append(' '+data);self.visible_length+=len(data)+1
        super().handle_data(data)
    def handle_starttag(self,tag,attrs):
        super().handle_starttag(tag,attrs)
        if tag=='table' and not self.hidden:self.stack[-1]['visibleStart']=self.visible_length
    def handle_endtag(self,tag):
        start=self.stack[-1].get('visibleStart') if tag=='table' and self.stack and not self.hidden else None
        super().handle_endtag(tag)
        if start is not None:self.spans.append((start,self.visible_length))


def period_binding(review,evidence,cause,text):
    """Return a stronger registered binding, or keep the prior literal-year contract."""
    target=str(evidence['fiscalYear']);day=date.fromisoformat(review['end'])
    annual=evidence.get('causeAnnualRange');group=evidence.get('causeRevisionGroup')
    if annual and group:raise ValueError('ERROR_PERIOD_MULTIPLE_BINDINGS')
    if annual:
        phrase=norm(annual.get('phrase',''));end=annual.get('end');count=annual.get('years')
        m=re.fullmatch(r'for each of the (two|three) years in the period ended ([a-z]+ \d{1,2}, \d{4})',phrase)
        if not m or phrase not in cause or type(count) is not int or count!={'two':2,'three':3}[m[1]]:raise ValueError('ERROR_EXPLICIT_ANNUAL_RANGE_REQUIRED')
        stated=datetime.strptime(m[2],'%B %d, %Y').date()
        if stated.isoformat()!=end or stated>date.fromisoformat(evidence['filingDate']):raise ValueError('ERROR_ANNUAL_RANGE_DATE')
        try:days=[stated.replace(year=stated.year-i).isoformat() for i in range(count)]
        except ValueError:raise ValueError('ERROR_NONCALENDAR_RANGE_REVIEW_REQUIRED')
        if day.isoformat() not in days or str(day.year)!=target:raise ValueError('ERROR_TARGET_NOT_IN_ANNUAL_RANGE')
        return {'method':'EXPLICIT_ANNUAL_RANGE','phrase':annual['phrase'],'periodEnds':days,'target':day.isoformat()}
    if group:
        intro=norm(group.get('introExcerpt',''))
        if len(intro)<50 or not re.search(r'effects.{0,80}revision.{0,100}consolidated statements of income.{0,50}as follows',intro):raise ValueError('ERROR_REVISION_GROUP_INTRO_REQUIRED')
        if not re.search(r'prior (?:years|periods)',cause) or not re.search(r'errors?|misstat',cause) or not re.search(r'revis|correct|restat',cause):raise ValueError('ERROR_PRIOR_PERIOD_CAUSE_REQUIRED')
        parser=LocatedTables();parser.feed(text);visible=''.join(parser.visible)
        if norm(visible).count(intro)!=1:raise ValueError('ERROR_REVISION_GROUP_INTRO_NOT_UNIQUE')
        matches=[i for i,rows in enumerate(parser.tables) if digest(rows)==evidence['tableHash']]
        if len(matches)!=1:raise ValueError('ERROR_REVISION_GROUP_TARGET_TABLE')
        target_index=matches[0];before=norm(visible[:parser.spans[target_index][0]]);at=before.find(intro);causal=before.rfind(cause,0,at)
        if causal<0 or at-causal>6000:raise ValueError('ERROR_CAUSE_NOT_BOUND_TO_REVISION_GROUP')
        selected=[];ends=[]
        for i,(rows,span) in enumerate(zip(parser.tables,parser.spans)):
            if span[0]>parser.spans[target_index][0] or intro not in norm(visible[:span[0]]):continue
            # Ignore layout/footer tables, but never skip another revenue table.
            if not any(row and re.fullmatch(r'(?:(?:total|net|operating)\s+)*(?:revenues?|sales)',norm(row[0])) for row in rows):continue
            top=norm(' '.join(' '.join(row) for row in rows))
            if not re.search(r'as\s*(?:previously\s*)?reported',top) or not re.search(r'as (?:revised|restated)',top):raise ValueError('ERROR_UNRELATED_REVENUE_TABLE_IN_GROUP')
            dates=re.findall(r'year ended ([a-z]+ \d{1,2}, \d{4})',top)
            if len(dates)!=1:raise ValueError('ERROR_GROUP_ANNUAL_PERIOD_REQUIRED')
            ends.append(datetime.strptime(dates[0],'%B %d, %Y').date().isoformat());selected.append(digest(rows))
        if not 1<=len(selected)<=3 or selected!=group.get('tableHashes') or selected[-1]!=evidence['tableHash']:raise ValueError('ERROR_REVISION_GROUP_MEMBERS_CHANGED')
        if len(set(ends))!=len(ends) or ends!=sorted(ends,reverse=True) or ends[-1]!=review['end'] or any(date.fromisoformat(d)>date.fromisoformat(evidence['filingDate']) for d in ends):raise ValueError('ERROR_REVISION_GROUP_PERIOD_ORDER')
        return {'method':'EXPLICIT_PRIOR_PERIOD_REVISION_GROUP','intro':group['introExcerpt'],'tableHashes':selected,'periodEnds':ends,'target':day.isoformat()}
    if target not in cause:raise ValueError('BOUND_ERROR_CORRECTION_PERIOD_CAUSE_REQUIRED')
    return None


def component_bindings(evidence,labels,amounts,plain):
    items=evidence.get('errorComponentEvidence',[])
    if len(items)!=len(labels):raise ValueError('ERROR_EXACT_COMPONENT_EVIDENCE_REQUIRED')
    result=[]
    for label,amount,item in zip(labels,amounts,items):
        if item.get('label')!=label or Decimal(str(item.get('amountUSD','NaN')))!=amount:raise ValueError('ERROR_COMPONENT_LABEL_AMOUNT_BINDING')
        if amount==0:
            if item.get('explicitZero') is not True:raise ValueError('ERROR_COMPONENT_EXPLICIT_ZERO_REQUIRED')
            result.append({'label':label,'amountUSD':str(amount),'explicitZero':True});continue
        cause=norm(item.get('causeExcerpt',''));category=norm(item.get('categoryPhrase',''))
        if len(cause)<60 or cause not in plain or not re.search(r'errors?|misstat|correct|restat',cause):raise ValueError('ERROR_COMPONENT_CAUSE_NOT_CONFIRMED')
        if len(category)<5 or category not in cause or not (norm(label)==category or norm(label).startswith(category+' ')):raise ValueError('ERROR_COMPONENT_CATEGORY_NOT_BOUND')
        result.append({'label':label,'amountUSD':str(amount),'categoryPhrase':item['categoryPhrase'],'causeExcerpt':item['causeExcerpt']})
    return result
