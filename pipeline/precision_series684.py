"""Reviewed multi-period published-precision consistency; no economic restatement inference."""
import re
from datetime import datetime,date
from decimal import Decimal,ROUND_HALF_UP
from pipeline.reviewed_evidence683 import document
from pipeline.reconciliation_review679 import numeric
from pipeline.reviewed_financial_mapping672 import reviewed_approval,digest
BASIS='REVIEWED_MULTI_PERIOD_PRECISION_CONSISTENCY'
POLICY='sec-reviewed-multi-period-precision684-v1'
DATE=re.compile(r'([A-Za-z]+)\s+(\d{1,2}),?\s*((?:19|20)\d{2})')

def series(v,root):
 rows,ctx,_=document(v,root);header=rows[v['periodRow']];row=rows[v['valueRow']]
 if not row or not re.match(r'^(?:(?:total|net|operating)\s+)*(?:revenues?|sales)\b',row[0],re.I):raise ValueError('SERIES_REVENUE_ROW_REQUIRED')
 if v['periodMode']=='DATE':
  keys=[datetime.strptime(' '.join(m.groups()),'%B %d %Y').date().isoformat() for c in header for m in DATE.finditer(c)]
 elif v['periodMode']=='YEAR':
  if any(DATE.search(c) for c in header):raise ValueError('SERIES_FULL_DATE_CANNOT_BE_YEAR_ONLY')
  keys=[m for c in header for m in re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)',c)]
 else:raise ValueError('SERIES_PERIOD_MODE_INVALID')
 cells=[c for c in row if numeric(c) is not None];values=[numeric(c) for c in cells]
 if not 2<=len(keys)<=10 or len(keys)!=len(set(keys)) or len(keys)!=len(values):raise ValueError('SERIES_PERIOD_VALUE_ALIGNMENT')
 filed=date.fromisoformat(v['filingDate'])
 if (v['periodMode']=='DATE' and any(date.fromisoformat(k)>filed for k in keys)) or (v['periodMode']=='YEAR' and any(int(k)>filed.year for k in keys)):raise ValueError('SERIES_FUTURE_PERIOD_NOT_ALLOWED')
 top=' '.join(' '.join(r) for r in rows[:v['valueRow']+1])+' '+ctx
 heading=' '.join(' '.join(r) for r in rows[:v['valueRow']])
 if not re.search('year|fiscal',heading,re.I) or re.search(r'quarter|three months|six months|nine months',heading,re.I):raise ValueError('SERIES_ANNUAL_HEADER_REQUIRED')
 scale=Decimal(str(v['unitScale']));places=v['decimalPlaces']
 if scale not in {Decimal(1),Decimal(1000),Decimal(1000000)} or places not in {0,1,2,3}:raise ValueError('SERIES_UNIT_INVALID')
 if scale!=1 and ('thousands' if scale==1000 else 'millions') not in top.casefold():raise ValueError('SERIES_UNIT_DISCLOSURE_REQUIRED')
 if scale==1 and ('$' not in ' '.join(row) or re.search('thousands|millions',top,re.I)):raise ValueError('SERIES_UNSCALED_DOLLAR_REQUIRED')
 for c in cells:
  display=c.replace(',','').replace('$','').strip().strip('()').strip();actual=len(display.split('.')[1]) if '.' in display else 0
  if actual!=places:raise ValueError('SERIES_DISPLAY_PRECISION_MISMATCH')
 if any(not x.is_finite() for x in values):raise ValueError('SERIES_NONFINITE_VALUE')
 return dict(zip(keys,[x*scale for x in values])),scale/(Decimal(10)**places)

def approve(scope,entry,root):
 r=entry['review'];e=r['precisionSeriesEvidence'];nodes=e['nodes'];a=Decimal(r['originalValue']);b=Decimal(r['value'])
 if r['basis']!=BASIS or not 2<=len(nodes)<=4:raise ValueError('SERIES_BASIS_OR_CHAIN_BUDGET')
 docs={(d['url'],d['sha256']) for d in entry['documents']}
 if any((n['url'],n['sha256']) not in docs for n in nodes) or len({n['url'] for n in nodes})!=len(nodes):raise ValueError('SERIES_DOCUMENT_CHAIN_INVALID')
 originals=[x for x in scope if x['source_tag']=='SalesRevenueNet' and str(x['report_date'])==r['end'] and x['form_type'] in {'10-K','10-K/A'}]
 latest=[x for x in scope if x['source_tag']==r['chosenTag'] and x['accession_number'] in r['accessions']]
 if not any(x['source_url']==nodes[0]['url'] for x in originals) or not any(x['source_url']==nodes[-1]['url'] for x in latest):raise ValueError('SERIES_ORIGINAL_LATEST_BINDING_REQUIRED')
 dates=[date.fromisoformat(n['filingDate']) for n in nodes]
 if dates!=sorted(dates) or len(set(dates))!=len(dates) or dates[-1]>date.fromisoformat(r['reviewedOn']):raise ValueError('SERIES_FILING_SEQUENCE_INVALID')
 if not any(str(x['filing_date'])==nodes[0]['filingDate'] for x in originals) or nodes[-1]['filingDate']!=r['filingDate']:raise ValueError('SERIES_FILING_METADATA_MISMATCH')
 if len({n['periodMode'] for n in nodes})!=1:raise ValueError('SERIES_PERIOD_CONVENTION_CHANGED')
 target=e['targetPeriodKey']
 if nodes[0]['periodMode']=='DATE':
  if target!=r['end']:raise ValueError('SERIES_TARGET_DATE_MISMATCH')
 elif not any(str(x['fiscal_year'])==target for x in originals):raise ValueError('SERIES_TARGET_FISCAL_YEAR_MISMATCH')
 vectors=[series(n,root) for n in nodes]
 if any(target not in data for data,_ in vectors):raise ValueError('SERIES_TARGET_NOT_IN_EVERY_REPORT')
 def rounded(x,q):return (x/q).quantize(Decimal(1),rounding=ROUND_HALF_UP)*q
 first,q0=vectors[0];last,qn=vectors[-1]
 if rounded(a,q0)!=first[target] or last[target]!=b or rounded(a,qn)!=b:raise ValueError('SERIES_TARGET_RAW_OR_DISPLAY_MISMATCH')
 if qn<=q0:raise ValueError('SERIES_NO_REPORTING_PRECISION_REDUCTION')
 overlaps=[]
 for (old,oq),(new,nq) in zip(vectors,vectors[1:]):
  common=sorted(set(old)&set(new))
  if len(common)<2 or nq<oq:raise ValueError('SERIES_INSUFFICIENT_OVERLAP_OR_PRECISION_REVERSAL')
  if any(rounded(old[k],nq)!=new[k] for k in common):raise ValueError('SERIES_COMPARATIVE_VALUE_CONFLICT')
  overlaps.append(common)
 checked=reviewed_approval(scope,{**r,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':str(b-a)})
 proof={**checked['proof'],'policy':POLICY,'review':r,'accountingCause':BASIS,'overlappingPeriods':overlaps,'meaning':'Multiple annual published values are consistent with coarser presentation. Not proof of unchanged economics below displayed precision; original raw values retained.'}
 return {**checked,'proof':proof,'proof_hash':digest(proof)}
