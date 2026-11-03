"""Discover explicitly linked exhibit-13 annual reports; never fetch or approve.

A Form 10-K cover can incorporate its primary statements from a separate
annual-report exhibit. Absence of tables in that cover is not a parser failure
or permission to use a later report alone.
"""
import hashlib,re
from html.parser import HTMLParser
from urllib.parse import urljoin,urlsplit
from pipeline.legacy_statement_total699 import clean


class Links(HTMLParser):
    def __init__(self):super().__init__();self.active=None;self.links=[];self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}:self.hidden+=1
        if not self.hidden and tag=='a':self.active=[dict(attrs).get('href',''),[]]
    def handle_endtag(self,tag):
        if tag in {'script','style'}:self.hidden=max(0,self.hidden-1)
        if tag=='a' and self.active:self.links.append((self.active[0],' '.join(self.active[1])));self.active=None
    def handle_data(self,data):
        if self.hidden:return
        self.parts.append(data)
        if self.active:self.active[1].append(data)


def review(body,url,cik,expected_sha256,*,cached_urls=()):
    if len(body)>32*1024**2 or hashlib.sha256(body).hexdigest()!=expected_sha256:raise ValueError('INCORPORATED_SOURCE_BYTES_CHANGED')
    u=urlsplit(url);prefix=f'/Archives/edgar/data/{int(cik)}/'
    if u.scheme!='https' or u.netloc!='www.sec.gov' or u.query or u.fragment or not re.fullmatch(re.escape(prefix)+r'\d{18}/[A-Za-z0-9_.-]+',u.path):raise ValueError('INCORPORATED_SOURCE_IDENTITY')
    p=Links();p.feed(body.decode('utf8','replace'));text=clean(' '.join(p.parts));references={};directory=url.rsplit('/',1)[0]+'/'
    deferred=bool(re.search(r'(?:information required.*?item 8|financial statements).*?incorporated by reference',text))
    for href,label in p.links:
        if len(href)>2048:continue
        target=urljoin(url,href);v=urlsplit(target)
        if v.scheme!='https' or v.netloc!='www.sec.gov' or v.query or v.fragment or not target.startswith(directory):continue
        if not re.fullmatch(r'[A-Za-z0-9_-]*(?:dex13|ex13)[A-Za-z0-9_-]*\.html?',v.path.rsplit('/',1)[-1],re.I):continue
        literal=clean(label)
        if 'annual report to shareholders' not in literal or 'incorporated by reference' not in literal:continue
        references[target]=dict(url=target,literalLabel=label,referenceOnly=True,cached=target in set(cached_urls))
    return dict(reasonCode='INCORPORATED_PRIMARY_REPORT_REFERENCE_REQUIRES_REVIEW' if deferred and references else 'NO_BOUND_INCORPORATED_PRIMARY_REPORT_REFERENCE',sourceSha256=expected_sha256,coverDefersFinancialStatements=deferred,references=list(references.values()),approvalGenerated=False,publicationAllowed=False,newDownloads=0)
