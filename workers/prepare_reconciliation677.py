"""Prepare an operator-reviewed bundle; requests cannot introduce URLs or reviews.

Usage: python -m workers.prepare_reconciliation677 --manifest reviewed.json
 --destination /operator/immutable-bundle [--download]
The destination must be new. Expected byte hashes come from the human review.
"""
import argparse
import gzip
import hashlib
import json
import re
import time
from pathlib import Path
from pipeline.financial_reconciliation677 import load_bundle,verify_documents

def prepare(manifest,destination,download=False):
    manifest=Path(manifest);bundle,_=load_bundle(manifest)
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=False)
    (destination/'documents').mkdir()
    docs={}
    for entry in bundle['entries']:
        cik=str(int(entry['cik']))
        for document in entry['documents']:
            url=document['url'];sha=document['sha256']
            if not re.fullmatch(r'https://www\.sec\.gov/Archives/edgar/data/'+cik+r'/\d{18}/[A-Za-z0-9_.-]+',url) or not re.fullmatch('[a-f0-9]{64}',sha):
                raise ValueError('RECONCILIATION_DOWNLOAD_IDENTITY_INVALID')
            if url in docs and docs[url]!=sha:raise ValueError('RECONCILIATION_DOWNLOAD_CONFLICT')
            docs[url]=sha
    if len(docs)>100:raise ValueError('RECONCILIATION_DOWNLOAD_BUDGET')
    session=None;last=0;total=0
    try:
        if download:
            import requests
            session=requests.Session();session.trust_env=False
            session.headers['User-Agent']='Revest Lab financial-data validation (kosw006@gmail.com)'
        for url,sha in docs.items():
            if session:
                time.sleep(max(0,1-(time.monotonic()-last)));last=time.monotonic()
                with session.get(url,timeout=(5,40),stream=True,allow_redirects=False) as response:
                    if response.status_code!=200:raise ValueError('SEC_DOCUMENT_FETCH_FAILED_'+str(response.status_code))
                    parts=[];size=0
                    for block in response.iter_content(1024*1024):
                        size+=len(block)
                        if size>32*1024**2:raise ValueError('RECONCILIATION_DOCUMENT_BUDGET')
                        parts.append(block)
                    body=b''.join(parts)
            else:
                with gzip.open(manifest.parent/'documents'/(sha+'.html.gz'),'rb') as f:body=f.read(32*1024**2+1)
            total+=len(body)
            if len(body)>32*1024**2 or total>256*1024**2 or hashlib.sha256(body).hexdigest()!=sha:
                raise ValueError('RECONCILIATION_DOCUMENT_CHANGED_OR_BUDGET')
            with gzip.open(destination/'documents'/(sha+'.html.gz'),'wb') as f:f.write(body)
        for entry in bundle['entries']:verify_documents(entry,destination)
        # Publish only after all documents pass; incomplete folders have no manifest.
        result=destination/'manifest.json'
        with result.open('x',encoding='utf8') as f:json.dump(bundle,f,indent=2)
        return result
    finally:
        if session:session.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--destination',required=True);p.add_argument('--download',action='store_true')
    args=p.parse_args();print(prepare(args.manifest,args.destination,args.download))
