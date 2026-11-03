"""Seek limited byte samples; a successful hit proves presence, not total coverage."""
import csv
import hashlib
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'trader-data/experiments'))
from fnspid_range_probe import PublicRanges,save

ROW_START=re.compile(rb'(?m)^(?:(?:[0-9]+(?:\.0)?|),)?(?:19|20)[0-9]{2}-[0-9]{2}-[0-9]{2} [0-9:]{8} UTC,')


def parse_complete_rows(data,indexed=False):
    """Discard both partial edge records; validate recovered complete CSV rows."""
    markers=list(ROW_START.finditer(data))
    rows=[];invalid=0
    for left,right in zip(markers,markers[1:]):
        fragment=data[left.start():right.start()]
        try:
            cells=list(csv.reader(io.StringIO(fragment.decode('utf-8')),strict=True))
            if len(cells)!=1 or len(cells[0])!=(12 if indexed else 11):
                raise ValueError()
            values=cells[0];offset=1 if indexed else 0
            published=datetime.strptime(values[offset],'%Y-%m-%d %H:%M:%S UTC').replace(tzinfo=timezone.utc)
            symbol=values[offset+2]
            if symbol and not re.fullmatch(r'[A-Z0-9.^\-/]+',symbol):raise ValueError()
            url=values[offset+3]
            if not url.startswith(('https://','http://')):raise ValueError()
            rows.append(dict(byteOffset=left.start(),date=published.isoformat(),symbol=symbol,
                 title=values[offset+1],url=url,publisherField=values[offset+4],
                 authorField=values[offset+5],bodyChars=len(values[offset+6]),
                 bodySha256=hashlib.sha256(values[offset+6].encode()).hexdigest(),
                 bodyExcerpt=values[offset+6][:1200],summaryChars={k:len(values[offset+n]) for k,n in
                    [('lsa',7),('luhn',8),('textrank',9),('lexrank',10)]},
                 csvRecordSha256=hashlib.sha256(fragment).hexdigest()))
        except (ValueError,csv.Error,UnicodeError):invalid+=1
    return rows,invalid


def main():
    parent=ROOT/'output/news-fnspid-eval-20261101T131353Z'
    initial=json.loads((parent/'plan.json').read_text(encoding='utf-8'))
    initial_usage=json.loads((parent/'probe-completion.json').read_text(encoding='utf-8'))
    out=parent/'target-seek';out.mkdir(exist_ok=False)
    save(out/'plan.json',dict(initialBytes=initial_usage['bytes'],initialHttpRequests=initial_usage['httpRequests'],
         cumulativeByteCap=32*1024**2,cumulativeHttpRequestCap=250,
         reason='CSV is large and grouped by stock/source; bounded byte seeks for 2016 samples only',
         seekIsHeuristic=True,noWholeFileCoverageClaim=True,fullDownload=False,authentication=False))
    c=PublicRanges(out,cap=32*1024**2-initial_usage['bytes'],max_requests=250-initial_usage['httpRequests'])
    trace=[];samples=[];cache={};counter=0
    revision=initial['revision']
    for item in initial['files']:
        file=item['path'].split('/')[-1];size=item['size']
        indexed=file=='nasdaq_exteral_data.csv'
        for symbol in ['AAPL','NVDA','TSLA']:
            low,high=0,size-128*1024
            found=[]
            for step in range(22):
                if high<low:break
                start=(low+high)//2
                key=(file,start)
                if key not in cache:
                    counter+=1
                    data=c.range(revision,file,start,128*1024,f'probe-{counter:03d}')
                    rows,invalid=parse_complete_rows(data,indexed)
                    cache[key]=(rows,invalid)
                rows,invalid=cache[key]
                company_rows=[r for r in rows if r['symbol']]
                matches=[r for r in company_rows if r['symbol']==symbol and r['date'].startswith('2016-')]
                trace.append(dict(file=file,symbol=symbol,step=step,start=start,
                    recoveredRows=len(rows),invalidRows=invalid,
                    first=[company_rows[0]['symbol'],company_rows[0]['date']] if company_rows else None,
                    last=[company_rows[-1]['symbol'],company_rows[-1]['date']] if company_rows else None,
                    targetMatches=len(matches)))
                print(json.dumps(trace[-1]),flush=True)
                if matches:
                    found=matches[:3]
                    for r in found:
                        r.update(file=file,rangeStart=start,absoluteRecordOffset=start+r['byteOffset'],
                                 sampleOnly=True,originalWebPageVerified=False)
                        samples.append(r)
                    break
                if not company_rows:
                    # The merged file has an untagged macro-news suffix. This
                    # heuristic narrows the tagged prefix, never proves absence.
                    high=start-1;continue
                keys=[(r['symbol'],-datetime.fromisoformat(r['date']).timestamp()) for r in company_rows]
                if keys!=sorted(keys):
                    trace[-1]['hold']='LOCAL_SORT_ASSUMPTION_FAILED';break
                target=(symbol,-datetime(2016,7,1,tzinfo=timezone.utc).timestamp())
                if target<keys[0]:high=start-1
                elif target>keys[-1]:low=start+128*1024
                else:
                    trace[-1]['hold']='TARGET_BRACKETED_WITHOUT_VALID_2016_ROW';break
            save(out/(file+'-'+symbol+'-result.json'),dict(file=file,symbol=symbol,
                 status='2016_SAMPLE_PRESENT' if found else 'NOT_FOUND_IN_BOUNDED_SEEK_NOT_ABSENCE_PROOF',
                 samples=found,fullFileCountKnown=False))
    save(out/'trace.json',trace)
    save(out/'samples.json',samples)
    report=dict(status='BOUNDED_PRESENCE_TEST_COMPLETE',samples=len(samples),
                cumulativeBytes=initial_usage['bytes']+c.bytes,cumulativeHttpRequests=initial_usage['httpRequests']+c.requests,
                fileCount=2,fullFilesDownloaded=False,originalWebPageVerified=False,
                completeCountsKnown=False,awsChanges=0,dbWrites=0,apiKeyUsed=False,
                perFileAndSymbol=[dict(file=f,symbol=s,count=sum(r['file']==f and r['symbol']==s for r in samples),
                    withBody=sum(r['file']==f and r['symbol']==s and r['bodyChars']>0 for r in samples))
                    for f in ['All_external.csv','nasdaq_exteral_data.csv'] for s in ['AAPL','NVDA','TSLA']])
    save(out/'completion.json',report)
    print(json.dumps(report),flush=True)


if __name__=='__main__':main()
