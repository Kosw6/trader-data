"""Bounded unauthenticated public dataset ranges; never fetch an entire large CSV."""
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter

ROOT = Path(__file__).resolve().parents[2]
FILES = ['All_external.csv', 'nasdaq_exteral_data.csv']


def save(path, value):
    with path.open('x', encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


class PublicRanges:
    def __init__(self, out, cap=32*1024**2, max_requests=50):
        self.out, self.cap, self.max_requests = out, cap, max_requests
        self.bytes = self.requests = 0
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.mount('https://', HTTPAdapter(max_retries=0))
        self.session.headers.update({'User-Agent':'RevestLab-FNSPID-Sample/1.0', 'Accept-Encoding':'identity'})

    def get(self, url, start=None, end=None):
        headers = {'Range':f'bytes={start}-{end}'} if start is not None else {}
        for _ in range(6):
            parts = urlsplit(url)
            assert parts.scheme == 'https' and parts.hostname and not parts.username
            assert parts.hostname == 'huggingface.co' or parts.hostname.endswith(('.huggingface.co','.hf.co')), 'UNAPPROVED_DATA_HOST'
            assert self.requests < self.max_requests, 'REQUEST_CAP'
            self.requests += 1
            try:
                r = self.session.get(url, headers=headers, stream=True, timeout=(5,30), allow_redirects=False)
            except requests.RequestException:
                raise RuntimeError('PUBLIC_DATA_TRANSPORT_FAILED') from None
            with r:
                if r.status_code in {301,302,303,307,308}:
                    url = urljoin(url, r.headers['Location'])
                    continue
                assert r.status_code == (206 if start is not None else 200), 'EXPECTED_BOUNDED_HTTP_STATUS_'+str(r.status_code)
                limit = end-start+1 if start is not None else 1024**2
                assert limit+self.bytes <= self.cap, 'BYTE_CAP'
                if start is not None:
                    m = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',r.headers.get('Content-Range',''))
                    assert m and int(m[1])==start and int(m[2])==end, 'RANGE_NOT_HONORED'
                body=bytearray()
                for chunk in r.iter_content(65536):
                    self.bytes += len(chunk)
                    body.extend(chunk)
                    assert len(body)<=limit and self.bytes<=self.cap, 'RESPONSE_SIZE_CAP'
                return bytes(body), dict(status=r.status_code,contentRange=r.headers.get('Content-Range'),
                     bytes=len(body),sha256=hashlib.sha256(body).hexdigest(),host=parts.hostname)
        raise RuntimeError('REDIRECT_CAP')

    def range(self, revision, file, start, length, label):
        url=f'https://huggingface.co/datasets/Zihan1004/FNSPID/resolve/{revision}/Stock_news/{file}?download=true'
        data, receipt=self.get(url,start,start+length-1)
        target=self.out/(label+'.part')
        with target.open('xb') as f:f.write(data)
        save(self.out/(label+'-receipt.json'),dict(file=file,start=start,revision=revision,**receipt))
        return data


def main():
    out=ROOT/'output'/('news-fnspid-eval-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True,exist_ok=False)
    client=PublicRanges(out)
    info,_=client.get('https://huggingface.co/api/datasets/Zihan1004/FNSPID')
    repo=json.loads(info)
    revision=repo['sha']
    tree,_=client.get(f'https://huggingface.co/api/datasets/Zihan1004/FNSPID/tree/{revision}/Stock_news?expand=false')
    files=json.loads(tree)
    save(out/'plan.json',dict(dataset='Zihan1004/FNSPID',revision=revision,gated=repo.get('gated'),
         byteCap=client.cap,maxHttpRequests=client.max_requests,fullDownload=False,authenticationUsed=False,
         files=[{k:x.get(k) for k in ['path','size','oid','lfs']} for x in files],awsChanges=0,dbWrites=0))
    for file in FILES:
        size=next(x['size'] for x in files if x['path']=='Stock_news/'+file)
        for label,start in [('head',0),('tail',size-128*1024)]:
            data=client.range(revision,file,start,128*1024,file+'-'+label)
            # Only a small public file excerpt, not arbitrary headers or redirect URLs.
            print(json.dumps({'file':file,'section':label,'preview':data[:1600].decode('utf-8',errors='replace')},ensure_ascii=True),flush=True)
    save(out/'probe-completion.json',dict(status='INITIAL_BYTE_SAMPLES_SAVED',bytes=client.bytes,httpRequests=client.requests))
    print(json.dumps({'directory':str(out),'bytes':client.bytes,'httpRequests':client.requests}),flush=True)


if __name__=='__main__':main()
