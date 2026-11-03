"""Explicitly reviewed annual revenue error corrections, separate from ASC606."""
import gzip
import hashlib
import html
import re
from decimal import Decimal
from datetime import datetime
from pipeline.reviewed_financial_mapping672 import reviewed_approval, digest
from pipeline.reconciliation_review679 import Tables,numeric

POLICY='sec-reviewed-error-correction680-v1'
BASIS='EXPLICIT_ERROR_CORRECTION'

def normalized(text):return re.sub(r'\s+',' ',text).strip().casefold()

def error_approval(scope,review):
    if review.get('basis')!=BASIS:raise ValueError('ERROR_CORRECTION_BASIS_REQUIRED')
    e=review.get('errorEvidence',{})
    if (e.get('category')!=BASIS or not e.get('causeExcerpt') or not e.get('periodHeading')
        or not e.get('rowLabel') or not re.fullmatch(r'\([a-z]\)',e.get('reference',''))
        or e.get('sourceUrl') not in review.get('officialDocuments',[])
        or not re.fullmatch('[a-f0-9]{64}',e.get('documentSha256',''))):
        raise ValueError('ERROR_CORRECTION_DOCUMENT_EVIDENCE_REQUIRED')
    if e.get('periodEnd')!=review['end'] or e.get('metric')!='REVENUE':raise ValueError('ERROR_CORRECTION_PERIOD_OR_METRIC_MISMATCH')
    try:heading_end=datetime.strptime(e['periodHeading'],'Year ended %B %d, %Y').date().isoformat()
    except ValueError:raise ValueError('ERROR_CORRECTION_ANNUAL_HEADING_INVALID') from None
    if heading_end!=review['end']:raise ValueError('ERROR_CORRECTION_HEADING_PERIOD_MISMATCH')
    scale=Decimal(str(e.get('unitScale','0')))
    if scale not in {Decimal(1),Decimal(1000),Decimal(1000000)}:raise ValueError('ERROR_CORRECTION_UNIT_SCALE_INVALID')
    prior=[r for r in scope if r['accession_number']==e.get('priorAccession') and r['source_tag']==e.get('priorTag') and r['form_type'] in {'10-K','10-K/A'}]
    if not prior or {Decimal(str(r['value'])) for r in prior}!={Decimal(review['originalValue'])}:
        raise ValueError('ERROR_CORRECTION_PRIOR_REPORT_MISMATCH')
    if any(r['source_url'] not in review['officialDocuments'] for r in prior):raise ValueError('ERROR_CORRECTION_PRIOR_DOCUMENT_REQUIRED')
    # Reuse all existing immutable scope/original/latest/URL/annual/Decimal
    # guards. The serving proof retains ERROR_CORRECTION, never the shadow basis.
    shadow={**review,'basis':'FULL_RETROSPECTIVE_REVENUE_STANDARD','revenueStandardAdjustment':review['errorCorrectionAdjustment']}
    checked=reviewed_approval(scope,shadow)
    proof={**checked['proof'],'policy':POLICY,'review':review,'accountingCause':BASIS}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}

def verify_error_document(entry,root):
    r=entry['review'];e=r['errorEvidence'];sha=e['documentSha256']
    if not any(d['url']==e['sourceUrl'] and d['sha256']==sha for d in entry['documents']):raise ValueError('ERROR_CORRECTION_DOCUMENT_BINDING_MISMATCH')
    with gzip.open(root/'documents'/(sha+'.html.gz'),'rb') as stream:body=stream.read(32*1024**2+1)
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=sha:raise ValueError('ERROR_CORRECTION_DOCUMENT_CHANGED')
    text=body.decode('utf8',errors='replace');text=re.sub(r'<(script|style)\b[^>]*>.*?</\1>',' ',text,flags=re.I|re.S)
    plain=normalized(html.unescape(re.sub('<[^>]+>',' ',text)))
    if normalized(e['causeExcerpt']) not in plain or not re.search(r'correct(?:ion|ed).*?misstatements?|restatement',normalized(e['causeExcerpt'])):
        raise ValueError('ERROR_CORRECTION_CAUSE_NOT_IN_DOCUMENT')
    if not normalized(e['causeExcerpt']).startswith(e['reference']):raise ValueError('ERROR_CORRECTION_CAUSE_REFERENCE_MISMATCH')
    parser=Tables();parser.feed(text);expected=[Decimal(r[k])/Decimal(str(e['unitScale'])) for k in ['originalValue','errorCorrectionAdjustment','value']]
    for rows in parser.tables:
        table=normalized(' '.join(' '.join(row) for row in rows))
        if normalized(e['periodHeading']) not in table:continue
        if not all(h in table for h in ['as previously reported','restatement impacts','as restated']):continue
        if str(e['unitScale'])=='1000000' and 'in millions' not in table:continue
        if str(e['unitScale'])=='1000' and 'in thousands' not in table:continue
        if str(e['unitScale'])=='1' and 'in usd' not in table:continue
        for row in rows:
            label=next((normalized(cell) for cell in row if cell.strip()),'')
            if label!=normalized(e['rowLabel']):continue
            if e['reference'] not in [normalized(cell) for cell in row]:continue
            values=[n for cell in row if (n:=numeric(cell)) is not None]
            if values==expected:return
    raise ValueError('ERROR_CORRECTION_EXACT_TABLE_NOT_CONFIRMED')
