import hashlib
import json

import pytest
from pipeline.coordinated_financial import official_values
from pipeline.durable_requests import CoordinationError
from workers.coordinated_recovery import pinned_plan, render_status


def test_dashboard_escapes_values_and_does_not_embed_credentials():
    rendered = render_status(
        [{"target": "<script>alert(1)</script>", "state": "REVIEW"}]
    )
    assert "<script>" not in rendered and "&lt;script&gt;" in rendered
    assert "ADMIN_API_KEY" not in rendered


def test_manifest_byte_hash_required(tmp_path):
    raw = json.dumps({"kind": "PRICE", "workKey": "a" * 64}).encode()
    path = tmp_path / "plan.json"
    path.write_bytes(raw)
    assert pinned_plan(path, hashlib.sha256(raw).hexdigest())["kind"] == "PRICE"
    with pytest.raises(CoordinationError):
        pinned_plan(path, "0" * 64)


@pytest.mark.parametrize(
    "invalid",
    ["duplicate_context", "duplicate_unit", "fake_format", "mixed_unit", "double_sign"],
)
def test_official_xbrl_ambiguous_evidence_is_rejected(invalid):
    context = '<x:context id="c"><x:entity><x:identifier scheme="http://www.sec.gov/CIK">0000000001</x:identifier></x:entity><x:period><x:instant>2025-12-31</x:instant></x:period></x:context>'
    unit = '<x:unit id="u"><x:measure>iso:USD</x:measure></x:unit>'
    attr, number = "", "123"
    if invalid == "duplicate_context":
        context += context
    elif invalid == "duplicate_unit":
        unit += unit
    elif invalid == "fake_format":
        attr = ' format="fake:numdotdecimal"'
    elif invalid == "mixed_unit":
        unit = unit.replace("</x:unit>", "<x:measure>iso:EUR</x:measure></x:unit>")
    else:
        attr, number = ' sign="-"', "-123"
    raw = f'<html xmlns:x="http://www.xbrl.org/2003/instance" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL" xmlns:iso="http://www.xbrl.org/2003/iso4217" xmlns:g="http://fasb.org/us-gaap/2025" xmlns:fake="https://invalid.example">{context}{unit}<ix:nonFraction name="g:Assets" contextRef="c" unitRef="u"{attr}>{number}</ix:nonFraction></html>'.encode()
    if invalid == "mixed_unit":
        assert (
            official_values(raw, "0000000001", "Assets", "USD", None, "2025-12-31")
            == set()
        )
    else:
        with pytest.raises(CoordinationError, match="review_required"):
            official_values(raw, "0000000001", "Assets", "USD", None, "2025-12-31")
