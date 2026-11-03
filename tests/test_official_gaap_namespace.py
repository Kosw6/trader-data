from decimal import Decimal

import pytest

from pipeline.coordinated_financial import gaap_namespace, official_values


@pytest.mark.parametrize(
    "uri", ["http://fasb.org/us-gaap/2019-01-31", "https://fasb.org/us-gaap/2025"]
)
def test_real_annual_namespace_date_and_year_both_supported(uri):
    raw = f'''<html xmlns:x="http://www.xbrl.org/2003/instance" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" xmlns:us-gaap="{uri}" xmlns:iso="http://www.xbrl.org/2003/iso4217">
    <x:context id="c"><x:entity><x:identifier scheme="http://www.sec.gov/CIK">6201</x:identifier></x:entity><x:period><x:instant>2019-12-31</x:instant></x:period></x:context>
    <x:unit id="u"><x:measure>iso:USD</x:measure></x:unit><ix:nonFraction name="us-gaap:Assets" contextRef="c" unitRef="u" scale="6">59,995</ix:nonFraction></html>'''.encode()
    assert official_values(raw, "0000006201", "Assets", "USD", None, "2019-12-31") == {
        Decimal(59995000000)
    }


@pytest.mark.parametrize(
    "uri",
    [
        "http://fasbXorg/us-gaap/2019",
        "https://fasb.org.attacker.example/us-gaap/2019",
        "https://fasb.org/us-gaap/2019-02-30",
        "https://fasb.org/us-gaap/2019-01-31/extra",
    ],
)
def test_taxonomy_domain_and_valid_date_not_relaxed(uri):
    assert not gaap_namespace(uri)
