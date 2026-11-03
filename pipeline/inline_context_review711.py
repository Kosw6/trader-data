"""Offline inline-XBRL context cards; never rewrite raw dates or values."""
import hashlib,html,re

def contexts_at_end(body,end):
    if len(body)>32*1024**2 or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',end):raise ValueError('INLINE_CONTEXT_REVIEW_BUDGET_OR_DATE')
    text=body.decode('utf8','replace');contexts=[]
    pattern=r'<(?:[A-Za-z0-9_]+:)?context\b([^>]*)>(.*?)</(?:[A-Za-z0-9_]+:)?context>'
    for pos,m in enumerate(re.finditer(pattern,text,re.I|re.S)):
        if pos>=10000:raise ValueError('INLINE_CONTEXT_COUNT_BUDGET')
        if not re.search(r'<(?:[A-Za-z0-9_]+:)?endDate\b[^>]*>\s*'+re.escape(end)+r'\s*<',m[2],re.I):continue
        identity=re.search(r'\bid=["\x27]([^"\x27]+)',m[1],re.I)
        if not identity:raise ValueError('INLINE_CONTEXT_ID_MISSING')
        contexts.append(dict(contextId=identity[1],dates=re.findall(r'<(?:[A-Za-z0-9_]+:)?(?:startDate|endDate)\b[^>]*>\s*([^<]+)',m[2],re.I),dimensionsPresent=bool(re.search('explicitMember|typedMember',m[2],re.I)),contextSha256=hashlib.sha256(m[0].encode()).hexdigest(),referencingFacts=[]))
    indexed={c['contextId']:c for c in contexts}
    if len(indexed)!=len(contexts):raise ValueError('INLINE_CONTEXT_DUPLICATE_ID')
    for pos,f in enumerate(re.finditer(r'<ix:nonFraction\b([^>]*)>(.*?)</ix:nonFraction>',text,re.I|re.S)):
        if pos>=50000:raise ValueError('INLINE_FACT_COUNT_BUDGET')
        attrs={k.lower():v for k,v in re.findall(r'([A-Za-z0-9_:]+)=["\x27]([^"\x27]*)',f[1])};context=indexed.get(attrs.get('contextref'))
        if context is not None:context['referencingFacts'].append(dict(name=attrs.get('name'),scale=attrs.get('scale'),decimals=attrs.get('decimals'),unitRef=attrs.get('unitref'),printed=html.unescape(re.sub('<[^>]+>',' ',f[2])).strip()[:4096]))
    return dict(documentSha256=hashlib.sha256(body).hexdigest(),targetEnd=end,contexts=contexts,approvalGenerated=False,dateWrites=0,factWrites=0,limitation='Context/value cards only. Presence of an inline context does not resolve disagreement with the annual cover, accounting scope or other source contracts.')
