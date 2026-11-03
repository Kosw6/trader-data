"""Review-only signed operating/tax bridge; deliberately cannot approve mappings.

An exact bridge in one year cannot clear a conflicting companion year. Values
must be literal source-bound USD amounts, not rounded to make an equation fit.
"""
from decimal import Decimal


def diagnose(company,year,original,continuing,note,*,unit):
    if company!=295 or year not in {2012,2013} or unit!='USD':
        raise ValueError('DISCONTINUED_REVIEW_NAMED_SCOPE743')
    required={'operating','pretax','tax','net'}
    if set(original)!=required or set(continuing)!=required or set(note)!={'afterTaxIncome','tax','revenue'}:
        raise ValueError('DISCONTINUED_REVIEW_COMPLETE_COLUMNS743')
    def amounts(d):
        result={k:Decimal(str(v)) for k,v in d.items()}
        if any(not v.is_finite() for v in result.values()):raise ValueError('DISCONTINUED_REVIEW_FINITE_VALUES743')
        return result
    a,b,n=map(amounts,(original,continuing,note))
    if a['pretax']-a['tax']!=a['net'] or b['pretax']-b['tax']!=b['net']:
        raise ValueError('DISCONTINUED_REVIEW_BOTH_PRIMARY_NET_EQUATIONS743')
    if a['net']-b['net']!=n['afterTaxIncome']:
        raise ValueError('DISCONTINUED_REVIEW_LITERAL_NET_ALLOCATION743')
    primary_tax=a['tax']-b['tax'];primary_pretax=a['pretax']-b['pretax'];operating=a['operating']-b['operating']
    nonop_original=a['operating']-a['pretax'];nonop_continuing=b['operating']-b['pretax']
    note_pretax=n['afterTaxIncome']+n['tax'];tax_gap=primary_tax-n['tax']
    if primary_pretax!=n['afterTaxIncome']+primary_tax:
        raise ValueError('DISCONTINUED_REVIEW_PRIMARY_PRETAX_EQUATION743')
    status='EXACT_PRIMARY_AND_NOTE_BRIDGE_REVIEW_ONLY' if not tax_gap and operating-(nonop_original-nonop_continuing)==note_pretax else 'PRIMARY_VS_NOTE_TAX_ALLOCATION_CONFLICT'
    return dict(companyId=company,metric='OPERATING_INCOME',start=str(year-1)+'-10-01',end=str(year)+'-09-30',unit=unit,status=status,
                operatingChangeUSD=str(operating),primaryPretaxChangeUSD=str(primary_pretax),primaryTaxAllocationUSD=str(primary_tax),
                printedNoteTaxUSD=str(n['tax']),printedNotePretaxUSD=str(note_pretax),primaryVsNoteTaxDifferenceUSD=str(tax_gap),
                nonOperatingNetExpenseChangeUSD=str(nonop_original-nonop_continuing),
                amountRoundedOrAdjusted=False,publicationAllowed=False,approvalGenerated=False,
                requires=['EXPLICIT_RECLASSIFICATION_CAUSE','EVERY_ORIGINAL_SEC_OBSERVATION','ALL_SOURCE_REPORTS_AND_COMPARATIVES','FRESH_DATABASE_AND_SERVING_VALIDATION'])
