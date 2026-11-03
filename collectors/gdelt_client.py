"""Unauthenticated bounded GDELT reads; observation times are not event times."""
import csv
import hashlib
import html
import io
import json
import re
import time
import zipfile
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from collectors.marketaux_client import safe_url

DOC='https://api.gdeltproject.org/api/v2/doc/doc'
DATA='https://data.gdeltproject.org/gdeltv2/'


class GdeltError(RuntimeError):
    def __init__(self,code,receipt=None):
        self.code,self.receipt=code,receipt
        super().__init__(code)


class GdeltClient:
    def __init__(self,session=None,interval=5.1,max_bytes=32*1024**2,archive_limit=10*1024**2):
        if not 1<=max_bytes<=64*1024**2 or not 1<=archive_limit<=20*1024**2:
            raise ValueError('GDELT_LOCAL_SIZE_LIMIT_INVALID')
        self.session=session or requests.Session()
        self.session.trust_env=False
        self.session.mount('https://',HTTPAdapter(max_retries=0))
        self.session.headers.update({'User-Agent':'RevestLab-GDELT-Evaluation/1.0','Accept-Encoding':'identity'})
        self.attempts=self.bytes=0;self.last=0.;self.interval=interval
        self.max_bytes,self.archive_limit=max_bytes,archive_limit

    def close(self):self.session.close()

    def get(self,url,params=None):
        if url!=DOC and not re.fullmatch(re.escape(DATA)+r'2016\d{10}\.gkg\.csv\.zip',url):
            raise ValueError('GDELT_ENDPOINT_NOT_ALLOWED')
        if url==DOC:
            allowed={'query','mode','format','maxrecords','sort','startdatetime','enddatetime'}
            if not isinstance(params,dict) or set(params)-allowed or params.get('maxrecords')!=5 or params.get('mode')!='artlist' or params.get('format')!='json':
                raise ValueError('BOUNDED_DOC_QUERY_REQUIRED')
        elif params:raise ValueError('ARCHIVE_PARAMETERS_NOT_ALLOWED')
        if self.attempts>=12:raise GdeltError('LOCAL_REQUEST_CAP')
        time.sleep(max(0,self.interval-(time.monotonic()-self.last)))
        self.last=time.monotonic();self.attempts+=1;started=time.monotonic()
        try:
            with self.session.get(url,params=params,stream=True,timeout=(5,25),allow_redirects=False) as r:
                receipt=dict(url=url,parameters=params,http=r.status_code,attempt=self.attempts,
                     receivedAt=datetime.now(timezone.utc).isoformat(),seconds=round(time.monotonic()-started,3))
                limit=1024**2 if url==DOC else self.archive_limit
                if r.headers.get('Content-Length','').isdigit() and (int(r.headers['Content-Length'])>limit or self.bytes+int(r.headers['Content-Length'])>self.max_bytes):
                    raise GdeltError('GDELT_DECLARED_RESPONSE_SIZE_CAP',receipt)
                body=bytearray()
                for chunk in r.iter_content(65536):
                    self.bytes+=len(chunk);body.extend(chunk)
                    if len(body)>limit or self.bytes>self.max_bytes:raise GdeltError('GDELT_RESPONSE_SIZE_CAP',receipt)
                receipt.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),
                               seconds=round(time.monotonic()-started,3))
                if r.status_code!=200:raise GdeltError('GDELT_HTTP_'+str(r.status_code),receipt)
                return bytes(body),receipt
        except requests.RequestException as e:
            raise GdeltError('GDELT_'+type(e).__name__,dict(url=url,attempt=self.attempts,
                 seconds=round(time.monotonic()-started,3))) from None


def parse_doc(raw):
    try:payload=json.loads(raw)
    except (ValueError,UnicodeError):
        diagnostic=raw[:500].decode('utf-8',errors='replace')
        code='DOC_DATE_RANGE_REJECTED' if re.search(r'date|time|range|start|end',diagnostic,re.I) else 'DOC_NON_JSON_RESPONSE'
        raise GdeltError(code,{'providerDiagnostic':diagnostic}) from None
    if not isinstance(payload,dict) or not isinstance(payload.get('articles'),list):
        raise GdeltError('DOC_RESPONSE_SCHEMA_INVALID')
    if len(payload['articles'])>5:raise GdeltError('DOC_RECORD_CAP_NOT_HONORED')
    result=[]
    for a in payload['articles']:
        url=safe_url(a.get('url'))
        if not url or not isinstance(a.get('title'),str):raise GdeltError('DOC_ARTICLE_SCHEMA_INVALID')
        try:seen=datetime.strptime(a['seendate'],'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        except (KeyError,TypeError,ValueError):raise GdeltError('DOC_SEEN_DATE_INVALID') from None
        result.append(dict(url=url,title=html.unescape(a['title']),seenAt=seen.isoformat(),
             language=a.get('language'),publisherDomain=a.get('domain'),publisherCountry=a.get('sourcecountry'),
             publishedAt=None,eventDate=None,body=None,association='KEYWORD_MATCH_NOT_TICKER_IDENTITY',
             originalArticleVerified=False,causalPriceImpactVerified=False))
    return result


def labels(organizations,themes):
    org=';'.join(organizations).casefold()
    result=[]
    for symbol,pattern in [('NVDA',r'\bnvidia\b'),('AAPL',r'\bapple(?: inc\.?| computer)?\b'),
                           ('TSLA',r'\btesla(?: motors| inc\.?)?\b')]:
        if re.search(pattern,org):result.append(symbol)
    if re.search(r'\bfederal reserve\b|\bbank of england\b|\beuropean central bank\b',org) or any('INTEREST_RATE' in t for t in themes):
        result.append('MACRO_RATES_CANDIDATE')
    if any(re.search(r'SANCTION|ARMEDCONFLICT|ECON_TRADE_DISPUTE',t) for t in themes):
        result.append('MACRO_GEOPOLITICS_CANDIDATE')
    return result


def parse_gkg(raw,stamp):
    try:z=zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:raise GdeltError('GKG_NOT_A_ZIP') from None
    with z:
        entries=z.infolist()
        if len(entries)!=1 or entries[0].filename!=stamp+'.gkg.csv' or entries[0].file_size>128*1024**2:
            raise GdeltError('GKG_ZIP_MEMBER_OR_SIZE_INVALID')
        counts={};rows=invalid=0;selected=[];all_ids=set();duplicate_ids=0
        with z.open(entries[0]) as f:
            stream=io.TextIOWrapper(f,encoding='utf-8',errors='replace',newline='')
            for fields in csv.reader(stream,delimiter='\t',quoting=csv.QUOTE_NONE):
                rows+=1
                if len(fields)!=27 or not re.fullmatch(r'2016\d{10}',fields[1]) or not safe_url(fields[4]):
                    invalid+=1;continue
                try:datetime.strptime(fields[1],'%Y%m%d%H%M%S')
                except ValueError:
                    invalid+=1;continue
                if fields[0] in all_ids:duplicate_ids+=1
                all_ids.add(fields[0])
                orgs=[x for x in fields[13].split(';') if x]
                themes=[x for x in fields[7].split(';') if x]
                tagged=labels(orgs,themes)
                for tag in tagged:counts[tag]=counts.get(tag,0)+1
                if not tagged:continue
                if not any(sum(tag in x['candidateLabels'] for x in selected)<20 for tag in tagged):continue
                title=re.search(r'<PAGE_TITLE>(.*?)</PAGE_TITLE>',fields[26],re.S)
                selected.append(dict(recordId=fields[0],providerDocumentDateRaw=fields[1],
                     gdeltBatchAt=fields[0].split('-')[0],
                     url=safe_url(fields[4]),publisher=fields[3],
                     title=html.unescape(title[1])[:500] if title else None,
                     organizations=orgs,themes=themes,candidateLabels=tagged,
                     publishedAt=None,eventDate=None,body=None,
                     association='ORGANIZATION_NAME_OR_THEME_HEURISTIC_NOT_EXCHANGE_IDENTITY',
                     sourceSnapshot=stamp,originalArticleVerified=False))
        return dict(snapshot=stamp,rows=rows,invalidRows=invalid,duplicateRecordIds=duplicate_ids,
                    candidateCounts=counts,samples=selected,bodyAvailable=False,
                    fullYearCoverageVerified=False,uncompressedBytes=entries[0].file_size)
