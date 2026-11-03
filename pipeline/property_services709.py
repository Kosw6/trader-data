"""Full primary property-plus-services revenue, never a global tag alias."""
from decimal import Decimal
import re
from pipeline.legacy_statement_total699 import document,clean
from pipeline.reconciliation_review679 import numeric

BASIS='REVIEWED_PRIMARY_PROPERTY_SERVICES_TOTAL_PRECISION'
CONTRACT='PRIMARY_PROPERTY_PLUS_SERVICES709_V1'
POLICY='sec-reviewed-property-services-precision709-v1'

def statement(entry,root,binding,end,expected):
    if str(int(entry['cik']))!='1053507' or binding.get('totalContract')!=CONTRACT:
        raise ValueError('PROPERTY_SERVICE_COMPANY_CONTRACT')
    if (binding['url'],binding['sha256']) not in {(d['url'],d['sha256']) for d in entry['documents']}:raise ValueError('PROPERTY_SERVICE_DOCUMENT')
    rows,context,_=document(binding,root);yr,vr=binding['periodRow'],binding['valueRow']
    if (yr,vr)!=(3,7) or binding['periodMode']!='YEAR' or binding['annualHeading'].lower().rstrip(',')!='year ended december 31':raise ValueError('PROPERTY_SERVICE_ANNUAL_LAYOUT')
    if [clean(rows[i][0]) for i in [4,5,6,7]]!=['revenues:','property','services','total operating revenues'] or clean(binding['rowLabel'])!='total operating revenues':raise ValueError('PROPERTY_SERVICE_LABEL_HIERARCHY')
    if clean(' '.join(rows[2]))!='year ended december 31,':raise ValueError('PROPERTY_SERVICE_YEAR_HEADING')
    heading=re.search(r'american tower corporation and subsidiaries consolidated statements of operations \(in (thousands|millions), except (?:share and )?per share data\)$',clean(context))
    if not heading:raise ValueError('PROPERTY_SERVICE_PRIMARY_TITLE')
    years=[c for c in rows[3] if c.strip()]
    if len(years)!=3 or any(not re.fullmatch(r'(?:19|20)\d{2}',y) for y in years) or any(int(a)-int(b)!=1 for a,b in zip(years,years[1:])):raise ValueError('PROPERTY_SERVICE_YEAR_COLUMNS')
    scale=Decimal(str(binding['unitScale']));unit='thousands' if scale==1000 else 'millions' if scale==1000000 else None
    if not unit or heading.group(1)!=unit:raise ValueError('PROPERTY_SERVICE_UNITS')
    values=[]
    for row in rows[5:8]:
        a=[numeric(c) for c in row[1:] if numeric(c) is not None]
        if len(a)!=3 or any(not x.is_finite() for x in a):raise ValueError('PROPERTY_SERVICE_COMPONENT_COLUMNS')
        values.append(a)
    if any(a+b!=c for a,b,c in zip(*values)):raise ValueError('PROPERTY_SERVICE_EXACT_TOTAL_EQUATION')
    key=end[:4]
    if end!=key+'-12-31' or key not in years or values[2][years.index(key)]*scale!=expected:raise ValueError('PROPERTY_SERVICE_TARGET_VALUE')
    return dict(totalUSD=str(expected),allThreeAnnualComponentEquationsVerified=True)
