"""Extract review-only annual precision-series bindings from immutable SEC HTML."""
import re
from decimal import Decimal
from datetime import datetime
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.reviewed_financial_mapping672 import digest
from pipeline.precision_series684 import DATE

def bindings(body,metadata):
 p=Tables();p.feed(body.decode('utf8',errors='replace'));result=[]
 for rows,ctx in zip(p.tables,p.contexts):
  for pr,head in enumerate(rows[:8]):
   try:dates=[datetime.strptime(' '.join(m.groups()),'%B %d %Y').date().isoformat() for c in head for m in DATE.finditer(c)]
   except ValueError:continue  # Reject malformed issuer dates; never guess the intended month.
   years=[x for c in head for x in re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)',c)]
   keys=dates or years;mode='DATE' if dates else 'YEAR'
   if not 2<=len(keys)<=10 or len(set(keys))!=len(keys):continue
   for vr,row in enumerate(rows):
    if not row or not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b',row[0],re.I):continue
    heading=' '.join(' '.join(r) for r in rows[:vr]);top=heading+' '+ctx
    if not re.search('year|fiscal',heading,re.I) or re.search('quarter|three months|six months|nine months',heading,re.I):continue
    cells=[c for c in row if numeric(c) is not None];vals=[numeric(c) for c in cells]
    if len(vals)!=len(keys):continue
    def precision(c):
     c=c.replace(',','').replace('$','').strip().strip('()').strip();return len(c.split('.')[1]) if '.' in c else 0
    places={precision(c) for c in cells}
    if len(places)!=1 or not places<={0,1,2,3}:continue
    places=next(iter(places));local=' '.join(' '.join(r) for r in rows[:vr+1]).casefold()
    units=[v for token,v in [('thousands',1000),('millions',1000000)] if token in local]
    if not units:units=[v for token,v in [('thousands',1000),('millions',1000000)] if token in ctx.casefold()]
    if not units and '$' in ' '.join(row):units=[1]
    if len(units)!=1:continue
    scale=units[0];node={**metadata,'tableHash':digest(rows),'periodRow':pr,'valueRow':vr,'periodMode':mode,'unitScale':scale,'decimalPlaces':places}
    result.append({'binding':node,'values':dict(zip(keys,[str(v*scale) for v in vals])),'quantum':str(Decimal(scale)/(Decimal(10)**places)),'rowLabel':row[0],'requiresHumanReview':True,'approvalGenerated':False})
 if len(result)>100:raise ValueError('PRECISION_BINDING_REVIEW_BUDGET')
 return result
