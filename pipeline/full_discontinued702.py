"""Existing exact discontinued bridge plus replayed original/latest full statements."""
from decimal import Decimal
from pipeline.discontinued_bridge687 import approve as discontinued, BASIS as INNER_BASIS
from pipeline.legacy_statement_total699 import statement
from pipeline.reviewed_financial_mapping672 import digest

BASIS='REVIEWED_FULL_STATEMENT_DISCONTINUED_REVENUE'
POLICY='sec-reviewed-full-statement-discontinued702-v1'


def approve(scope,entry,root):
    r=entry['review']
    if r['basis']!=BASIS:
        raise ValueError('FULL_DISCONTINUED_BASIS')
    # First replay the unchanged strict 687 source, exact subtraction, cause,
    # annual table, unit, fiscal year and selected-document guards.
    checked=discontinued(scope,{**entry,'review':{**r,'basis':INNER_BASIS}},root)
    evidence=r['fullStatementEvidence'];proofs={}
    for role,sources,key in [('original',checked['proof']['originalSources'],'originalValue'),('selected',checked['proof']['sources'],'value')]:
        e=evidence[role]
        if {x['source_url'] for x in sources}!={e['url']}:
            raise ValueError('FULL_DISCONTINUED_STATEMENT_SOURCE')
        proofs[role]=statement(entry,root,e,r['end'],Decimal(r[key]),_check_competing=False)
    proof={**checked['proof'],'policy':POLICY,'review':r,'fullStatements':proofs,'economicGrowthInferred':False}
    return {**checked,'proof':proof,'proof_hash':digest(proof)}
