import hashlib
import pytest
from pipeline.incorporated_source715 import review

URL='https://www.sec.gov/Archives/edgar/data/916076/000119312518055887/d529448d10k.htm'


def run(href='d529448dex1301.htm',label='Excerpts from 2017 Annual Report to Shareholders, portions incorporated by reference.',extra='',cached=()):
    body=('<p>The information required under Item 8 financial statements is incorporated by reference.</p><a href="'+href+'">'+label+'</a>'+extra).encode()
    return review(body,URL,916076,hashlib.sha256(body).hexdigest(),cached_urls=cached)


def test_actual_same_accession_exhibit_13_is_a_reference_not_approval_or_download():
    r=run();assert r['coverDefersFinancialStatements'] and len(r['references'])==1 and not r['references'][0]['cached']
    assert r['reasonCode']=='INCORPORATED_PRIMARY_REPORT_REFERENCE_REQUIRES_REVIEW' and not r['approvalGenerated'] and not r['publicationAllowed'] and r['newDownloads']==0


@pytest.mark.parametrize('href',['https://evil.example/ex13.htm','http://www.sec.gov/Archives/edgar/data/916076/000119312518055887/ex13.htm','../000119312517000001/ex13.htm','https://www.sec.gov/Archives/edgar/data/1/000119312518055887/ex13.htm','javascript:ex13.htm','d529448dex301.htm','ex13.htm?file=other','ex13.htm#part','../../../../ex13.htm'])
def test_unbound_other_accession_company_host_protocol_or_path_is_not_a_source(href):
    r=run(href);assert not r['references'] and not r['approvalGenerated']


def test_contract_exhibit_mentioning_an_old_annual_report_is_not_primary_statement_reference():
    assert not run('d529448dex1001.htm','Contract incorporated by reference to the 2017 Annual Report to Shareholders')['references']
    assert not run('d529448dex1301.htm','Compensation agreement incorporated by reference')['references']


def test_existing_cache_is_an_availability_signal_and_never_an_approval():
    target=URL.rsplit('/',1)[0]+'/d529448dex1301.htm';r=run(cached=[target]);assert r['references'][0]['cached'] and not r['publicationAllowed']


def test_changed_source_or_other_company_is_rejected():
    body=b'<p>source</p>'
    with pytest.raises(ValueError,match='BYTES'):review(body,URL,916076,'f'*64)
    with pytest.raises(ValueError,match='IDENTITY'):review(body,URL,1,hashlib.sha256(body).hexdigest())
