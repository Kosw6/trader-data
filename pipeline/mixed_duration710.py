"""Period-start-aware review cards, deliberately incapable of publication.

An annual subgroup passing provenance checks cannot approve a period-end-only
mapping that also makes an unverified quarter visible. Existing policies and
the original source rows are left unchanged.
"""
from collections import defaultdict
from datetime import date
from pipeline.financial_history674 import unchanged_approval
from pipeline.reviewed_financial_mapping672 import digest


def review(rows,index,source,*,today=None):
    if not rows or len(rows)>128:raise ValueError('MIXED_DURATION_SOURCE_BUDGET')
    identities={(r['company_id'],r['metric_id'],r['metric_code'],str(r['period_end']),str(int(r['cik']))) for r in rows}
    if len(identities)!=1:raise ValueError('MIXED_DURATION_IDENTITY')
    groups=defaultdict(list)
    for row in rows:groups[str(row['period_start'])].append(row)
    if not 2<=len(groups)<=8 or 'None' in groups:raise ValueError('MIXED_DURATION_REQUIRES_EXPLICIT_STARTS')
    results=[]
    for start,entries in sorted(groups.items()):
        days=(date.fromisoformat(str(entries[0]['period_end']))-date.fromisoformat(start)).days+1
        kind='ANNUAL' if 336<=days<=396 else 'QUARTER' if 71<=days<=106 else 'OTHER'
        result=dict(start=start,end=str(entries[0]['period_end']),days=days,kind=kind,rows=len(entries),sourceScopeHash=digest(entries))
        try:
            approval=unchanged_approval(entries,index,source,annual_only=kind=='ANNUAL',today=today)
            result.update(sourceContract='VERIFIED_SUBGROUP_ONLY',proofHash=approval['proof_hash'],tag=approval['source_tag'])
        except ValueError as exc:result.update(sourceContract='REVIEW_REQUIRED',reason=str(exc))
        results.append(result)
    try:
        unchanged_approval(rows,index,source,today=today);whole=dict(existingPolicyWouldPass=True)
    except ValueError as exc:whole=dict(existingPolicyWouldPass=False,reason=str(exc))
    return dict(contract='MIXED_DURATION_REVIEW_ONLY710_V1',wholeSourceScopeHash=digest(rows),subgroups=results,wholeScope=whole,
                approvalGenerated=False,publicationAllowed=False,financialFactWrites=0,filingWrites=0,
                publicMappingKey=['company_id','metric_id','period_end'],requiredConsumerContract='ALL_DURATION_GROUPS_VERIFIED_OR_PERIOD_START_AWARE_MAPPING',
                limitation='A subgroup proof never authorizes a period-end-only public mapping. This helper emits review cards only.')
