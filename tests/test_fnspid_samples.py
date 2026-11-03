import csv
import io
import sys
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'experiments'))
from fnspid_target_samples import parse_complete_rows


def make_row(date='2016-01-01 12:00:00 UTC',symbol='NVDA',body='line1\nline2, "quoted"',indexed=False):
    stream=io.StringIO();w=csv.writer(stream)
    w.writerow((['0.0'] if indexed else [])+[date,'Nvidia news',symbol,'https://example.org/a','Publisher','Author',body,'','','',''])
    return stream.getvalue().encode()


def test_multiline_csv_preserves_columns_and_discards_incomplete_edges():
    data=b'partial previous body\n'+make_row()+make_row(symbol='TSLA')+make_row()[:40]
    rows,bad=parse_complete_rows(data)
    assert len(rows)==2 and bad==0
    assert rows[0]['symbol']=='NVDA' and rows[1]['symbol']=='TSLA'
    assert rows[0]['bodyExcerpt']=='line1\nline2, "quoted"'


def test_indexed_csv_and_blank_macro_symbol_are_distinct():
    rows,bad=parse_complete_rows(make_row(indexed=True)+make_row(symbol='',indexed=True)+make_row(indexed=True),True)
    assert len(rows)==2 and rows[1]['symbol']=='' and bad==0


def test_invalid_date_url_and_field_counts_are_quarantined():
    malformed=make_row(date='2016-99-99 12:00:00 UTC')
    malformed+=make_row().replace(b'https://example.org/a',b'file:///a')
    malformed+=make_row()+make_row()
    rows,bad=parse_complete_rows(malformed)
    assert len(rows)==1 and bad==2
