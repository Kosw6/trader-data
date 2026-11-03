"""Prove optional SEC frame metadata evolution without relaxing amount/provenance checks."""
from collections import defaultdict
from decimal import Decimal
import re
from collectors.common import content_hash

POLICY = 'sec-optional-frame-history686-v1'
FRAME = re.compile(r'CY(?:19|20)\d{2}(?:Q[1-4])?I?')


def fact_hash(cik, tag, unit, fact):
    return content_hash({'cik':str(int(cik)).zfill(10),'taxonomy':'us-gaap',
                         'source_tag':tag,'unit':unit,'fact':fact})


def with_frame(fact, frame):
    result = dict(fact)
    if frame is None: result.pop('frame', None)
    else: result['frame'] = frame
    return result


def validate_binding(binding, cik):
    o = binding['observedFact']; old = binding['storedFrame']
    if old is not None and (not isinstance(old,str) or not FRAME.fullmatch(old)):
        raise ValueError('FRAME_STORED_METADATA_INVALID')
    current=o.get('frame')
    if current is not None and (not isinstance(current,str) or not FRAME.fullmatch(current)):
        raise ValueError('FRAME_CURRENT_METADATA_INVALID')
    if o.get('frame') == old:
        raise ValueError('FRAME_METADATA_DID_NOT_CHANGE')
    if (fact_hash(cik,binding['tag'],binding['unit'],o) != binding['rawFullHash'] or
        fact_hash(cik,binding['tag'],binding['unit'],with_frame(o,old)) != binding['storedHash']):
        raise ValueError('FRAME_EXACT_HASH_RECONSTRUCTION_FAILED')


def frame_verified_approval(rows, payload, source, retained_frames, **kwargs):
    # Import lazily: the existing immutable policy remains unchanged and can
    # call this separate contract only after its strict exact-hash check fails.
    from pipeline.financial_history674 import raw_index, unchanged_approval
    from pipeline.financial_mapping667 import digest
    if content_hash(payload) != source.get('canonicalJsonSha256'):
        raise ValueError('FRAME_RAW_SOURCE_HASH_CHANGED')
    ci=str(int(rows[0]['cik'])).zfill(10);index=raw_index(payload,ci)
    candidates=defaultdict(list)
    wanted={(r['source_tag'],r['unit'],str(r['period_start']) if r['period_start'] is not None else None,str(r['period_end'])) for r in rows}
    for tag,concept in payload.get('facts',{}).get('us-gaap',{}).items():
        for unit,observations in concept.get('units',{}).items():
            for o in observations:
                key=(tag,unit,o.get('start'),o.get('end'))
                if key in wanted:candidates[key].append(o)
    bindings=[];aliases={}
    for r in rows:
        stored=r['source_fact_hash']
        if stored in index:continue
        if stored not in retained_frames:
            raise ValueError('FRAME_RETAINED_METADATA_REQUIRED')
        old=retained_frames[stored]
        matches=[]
        key=(r['source_tag'],r['unit'],str(r['period_start']) if r['period_start'] is not None else None,str(r['period_end']))
        for o in candidates[key]:
            # Every monetary/period/filing field must agree BEFORE considering
            # a frame-only reconstruction. fy/fp and all other JSON keys are
            # also retained unmodified in the reconstructed full SHA.
            if (Decimal(str(o.get('val')))!=Decimal(str(r['value'])) or
                o.get('accn')!=r['accession_number'] or o.get('filed')!=str(r['filing_date']) or o.get('form')!=r['form_type']):continue
            if fact_hash(ci,r['source_tag'],r['unit'],with_frame(o,old)) != stored:continue
            full=fact_hash(ci,r['source_tag'],r['unit'],o)
            binding={'storedHash':stored,'rawFullHash':full,'storedFrame':old,'tag':r['source_tag'],'unit':r['unit'],'observedFact':o}
            validate_binding(binding,ci);matches.append(binding)
        unique={digest(b):b for b in matches}
        if len(unique)!=1:raise ValueError('FRAME_EXACT_UNIQUE_OBSERVATION_REQUIRED')
        b=next(iter(unique.values()));aliases[stored]=b;bindings.append(b)
        if b['rawFullHash'] not in index:raise ValueError('FRAME_FULL_OBSERVATION_NOT_INDEXED')
        index[stored]=index[b['rawFullHash']]
    if not bindings:raise ValueError('FRAME_RECONSTRUCTION_NOT_NEEDED')
    a=unchanged_approval(rows,index,source,**kwargs);p=a['proof']
    stored_matched=p['rawMatchedFactHashes']
    # Never claim the reconstructed stored hash is a hash of current raw bytes.
    used=[b for b in bindings if b['storedHash'] in stored_matched]
    if not used:raise ValueError('FRAME_RECONSTRUCTION_NOT_NEEDED')
    p={**p,'policy':POLICY,'frameCompanyCik':ci,'storedMatchedFactHashes':stored_matched,
       'rawMatchedFactHashes':sorted({aliases[h]['rawFullHash'] if h in aliases else h for h in stored_matched}),
       'rawObservationFrameBindings':sorted({digest(b):b for b in used}.values(),key=lambda b:b['storedHash']),
       'hashMeaning':'Current SEC raw JSON SHA and full observation SHA; stored observation SHA is independently reproduced by changing ONLY the retained optional frame field. Amount, unit, period, accession, filing date, form, fy/fp and every other observation key remain exact. Original rows are not edited.'}
    return {**a,'policy_version':POLICY,'proof':p,'proof_hash':digest(p)}


def validate_proposal(d):
    from pipeline.financial_mapping667 import RULES,digest
    p=d.get('proof',{});bindings=p.get('rawObservationFrameBindings',[])
    if (d.get('policy_version')!=POLICY or p.get('policy')!=POLICY or digest(p)!=d.get('proof_hash')
        or d.get('metric_code') not in RULES or d.get('source_tag') not in RULES[d['metric_code']][1]
        or any(p.get(pk)!=d.get(dk) for pk,dk in [('companyId','company_id'),('metricId','metric_id'),('metricCode','metric_code'),('end','end'),('tag','source_tag')])
        or not bindings or len(bindings)>128):
        raise ValueError('FRAME_PREPARED_EVIDENCE_INVALID')
    for b in bindings:
        # The normalized company identity is retained in the source-observation
        # wrapper so validation never relies on a mutable company lookup.
        validate_binding(b,p['frameCompanyCik'])
        if b['storedHash'] not in p['storedMatchedFactHashes'] or b['rawFullHash'] not in p['rawMatchedFactHashes']:
            raise ValueError('FRAME_MATCHED_HASH_SET_INVALID')
