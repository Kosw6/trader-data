"""Registered percentage-column precision and multi-component error evidence."""
import gzip,hashlib,html,re
from datetime import datetime
from decimal import Decimal,ROUND_HALF_UP
from pipeline.reconciliation_review679 import Tables,numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
from pipeline.precision_evidence682 import verify_binding
LAYOUT='EXPLICIT_PRESENTATION_PRECISION_LAYOUT'
ERROR='EXPLICIT_MULTICOMPONENT_ERROR_CORRECTION'

def document(binding,root):
 sha=binding['sha256']
 if not re.fullmatch('[a-f0-9]{64}',sha):raise ValueError('REVIEW_DOCUMENT_HASH_INVALID')
 with gzip.open(root/'documents'/(sha+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
 if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('REVIEW_DOCUMENT_CHANGED')
 text=body.decode('utf8',errors='replace');p=Tables();p.feed(text)
 matches=[(t,c) for t,c in zip(p.tables,p.contexts) if digest(t)==binding['tableHash']]
 if len(matches)!=1:raise ValueError('REVIEW_TABLE_NOT_UNIQUE')
 plain=re.sub(r'\s+',' ',html.unescape(re.sub('<[^>]+>',' ',text))).casefold()
 return *matches[0],plain

def layout_binding(v,root,b,r):
 rows,context,_=document(v,root);header=rows[v['yearRow']];row=rows[v['valueRow']]
 years=[y for cell in header for y in re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)',cell)]
 n=v['yearValueCount'];tail=v['percentValueCount'];fy=str(v['fiscalYear'])
 if not 1<=n<=5 or not 1<=tail<=5 or len(years)<n or len(set(years))!=n or len(set(years[:n]))!=n or years[:n].count(fy)!=1:raise ValueError('LAYOUT_FINANCIAL_YEAR_PREFIX_INVALID')
 top=' '.join(' '.join(t) for t in rows[:v['valueRow']])
 if not re.search(r'%|percent',top,re.I):raise ValueError('LAYOUT_PERCENT_HEADER_REQUIRED')
 if not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b',row[0],re.I):raise ValueError('LAYOUT_REVENUE_REQUIRED')
 cells=[c for c in row if numeric(c) is not None];vals=[numeric(c) for c in cells]
 if len(vals)!=n+tail:raise ValueError('LAYOUT_PERCENT_COUNT_MISMATCH')
 scale=Decimal(str(v['unitScale']));places=v['decimalPlaces']
 if scale not in {Decimal(1000),Decimal(1000000)} or places not in {0,1,2}:raise ValueError('LAYOUT_UNIT_INVALID')
 if ('thousands' if scale==1000 else 'millions') not in (top+' '+context).casefold():raise ValueError('LAYOUT_UNIT_NOT_CONFIRMED')
 index=years[:n].index(fy);display=cells[index].replace(',','').replace('$','').strip().strip('()').strip();actual=len(display.split('.')[1]) if '.' in display else 0
 if actual!=places or vals[index]*scale!=b:raise ValueError('LAYOUT_PUBLISHED_VALUE_MISMATCH')
 if scale/(Decimal(10)**places)!=Decimal(str(r['precisionQuantum'])):raise ValueError('LAYOUT_QUANTUM_MISMATCH')

def approve(scope,entry,root):
 r=entry['review'];a=Decimal(r['originalValue']);b=Decimal(r['value']);docs={(d['url'],d['sha256']) for d in entry['documents']}
 if r['basis']==LAYOUT:
  q=Decimal(str(r['precisionQuantum']))
  if q not in {Decimal(10)**i for i in range(1,7)} or (a/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q!=b:raise ValueError('LAYOUT_ROUNDING_MISMATCH')
  original=[x for x in scope if x['source_tag']=='SalesRevenueNet' and str(x['report_date'])==r['end'] and x['form_type'] in {'10-K','10-K/A'}]
  latest=[x for x in scope if x['accession_number'] in r['accessions'] and x['source_tag']==r['chosenTag']]
  for role,sources in [('originalRounded',original),('latestRounded',latest)]:
   v=r['precisionEvidence'][role]
   if (v['url'],v['sha256']) not in docs or not any(x['source_url']==v['url'] for x in sources) or not any(str(x['fiscal_year'])==str(v['fiscalYear']) for x in original):raise ValueError('LAYOUT_ORIGINAL_BRIDGE_REQUIRED')
   if v.get('yearValueCount'):layout_binding(v,root,b,r)
   else:verify_binding(v,root,b,r)
  policy='sec-reviewed-percentage-layout-precision683-v1'
 elif r['basis']==ERROR:
  v=r['errorComponentsEvidence'];components=v['components']
  if (v['url'],v['sha256']) not in docs or not 2<=len(components)<=6 or len({c['label'] for c in components})!=len(components):raise ValueError('ERROR_COMPONENT_BINDINGS_INVALID')
  if datetime.strptime(v['periodHeading'],'For the Year Ended %B %d, %Y').date().isoformat()!=r['end']:raise ValueError('ERROR_COMPONENT_PERIOD_MISMATCH')
  amounts=[Decimal(c['amount']) for c in components];scale=Decimal(str(v['unitScale']))
  if scale not in {Decimal(1),Decimal(1000),Decimal(1000000)} or any(not x.is_finite() for x in amounts) or sum(amounts)!=b-a:raise ValueError('ERROR_COMPONENT_EQUATION_MISMATCH')
  rows,context,plain=document(v,root);text=' '.join(' '.join(t) for t in rows).casefold()
  if v['periodHeading'].casefold() not in text or not all(h in text for h in ['as previously reported','as restated']):raise ValueError('ERROR_COMPONENT_HEADER_MISMATCH')
  if ('thousands' if scale==1000 else 'millions' if scale==1000000 else 'usd') not in (text+' '+context).casefold():raise ValueError('ERROR_COMPONENT_UNIT_REQUIRED')
  for c,amount in zip(components,amounts):
   if c['label'].casefold() not in text:raise ValueError('ERROR_COMPONENT_LABEL_REQUIRED')
   cause=re.sub(r'\s+',' ',c.get('causeExcerpt','')).strip().casefold()
   if amount and (not cause or cause not in plain or not re.search(r'error|misstat|premature|restatement',cause)):raise ValueError('ERROR_COMPONENT_CAUSE_REQUIRED')
  row=rows[v['valueRow']]
  if row[0].casefold()!=v['rowLabel'].casefold():raise ValueError('ERROR_COMPONENT_ROW_MISMATCH')
  actual=[x for cell in row if (x:=numeric(cell)) is not None and x!=0]
  expected=[x/scale for x in [a,*amounts,b] if x!=0]
  if actual!=expected:raise ValueError('ERROR_COMPONENT_TABLE_VALUES_MISMATCH')
  policy='sec-reviewed-multicomponent-error683-v1'
 else:raise ValueError('UNREVIEWED_683_BASIS')
 checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
 proof={**checked['proof'],'policy':policy,'review':r,'accountingCause':r['basis']}
 return {**checked,'proof':proof,'proof_hash':digest(proof)}
