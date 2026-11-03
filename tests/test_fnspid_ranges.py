import pytest
from pathlib import Path
from experiments.fnspid_range_probe import PublicRanges


class Reply:
    def __init__(self, status=206, content_range='bytes 0-3/9999999999'):
        self.status_code=status
        self.headers={'Content-Range':content_range}
        self.read=False
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,size):
        self.read=True
        yield b'test'


class Session:
    def __init__(self,reply):self.reply=reply
    def get(self,*args,**kwargs):return self.reply


def test_server_ignoring_range_cannot_trigger_full_csv_download():
    c=PublicRanges(Path('.'));r=Reply(status=200);c.session=Session(r)
    with pytest.raises(AssertionError,match='EXPECTED_BOUNDED_HTTP_STATUS_200'):
        c.get('https://huggingface.co/file',0,3)
    assert not r.read and c.bytes==0


def test_wrong_cached_byte_range_rejected_before_reading():
    c=PublicRanges(Path('.'));r=Reply(content_range='bytes 4-7/9999999999');c.session=Session(r)
    with pytest.raises(AssertionError,match='RANGE_NOT_HONORED'):
        c.get('https://huggingface.co/file',0,3)
    assert not r.read


def test_unapproved_redirect_destination_and_size_limit_rejected():
    c=PublicRanges(Path('.'),cap=3);r=Reply();c.session=Session(r)
    with pytest.raises(AssertionError,match='BYTE_CAP'):c.get('https://huggingface.co/file',0,3)
    assert not r.read
    with pytest.raises(AssertionError,match='UNAPPROVED_DATA_HOST'):
        c.get('https://other.example/file',0,3)
