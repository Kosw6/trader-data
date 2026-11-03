import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from collectors import article_title as transport
from collectors.article_title import ArticleTitleClient, TitleUnavailable, checked_url, extract_title
from pipeline.news_collection import digest, normalize
from pipeline.news_enrichment import POLICY, enrich_observations, relevance
from workers.news_repository import NewsCancelled


def metadata(title=None, ticker="NVDA", name="NVIDIA CORP", organizations=True):
    return dict(url="https://example.org/story", title=title, publisher="Example", provider="GDELT",
                requestedAssets=[dict(assetId=42, ticker=ticker, name=name, companyName=name, exchange="NASDAQ")],
                targetMatches=[dict(assetId=42, ticker=ticker, exchange="NASDAQ", basis=["ORGANIZATION_NAME_MENTION"] if organizations else ["QUERY_RESULT_ONLY_REVIEW_REQUIRED"])])


@pytest.mark.parametrize("title,ticker,name,expected", [
    ("NVIDIA Announces Financial Results for the Fourth Quarter and Fiscal 2016", "NVDA", "NVIDIA CORP", "DIRECT"),
    ("NVIDIA GPUs Powered Deadpool’s CG Opening Sequence", "NVDA", "NVIDIA CORP", "DIRECT"),
    ("Acer Predator 8 review – the Nvidia Shield Tablet rival", "NVDA", "NVIDIA CORP", "INDIRECT"),
    ("Khronos Group Publishes Vulkan 1.0 Specification – AMD/Nvidia Pledge Support and Release Primary Drivers", "NVDA", "NVIDIA CORP", "INDIRECT"),
    ("Pre-Market Most Active for Dec 30: SMI, CAB, NVDA, BAC", "NVDA", "NVIDIA CORP", "MENTION"),
    ("Bereaved parents sue Apple after motorist distracted by Facetime app killed their daughter", "AAPL", "APPLE INC", "DIRECT"),
    ("New Mass Effect Andromeda gameplay at CES", "NVDA", "NVIDIA CORP", "MENTION"),
    ("A day at the market", "NVDA", "NVIDIA CORP", "MENTION"),
    (None, "NVDA", "NVIDIA CORP", "REVIEW"),
    ("NVIDIA mentioned in analyst conversations", "NVDA", "NVIDIA CORP", "MENTION"),
])
def test_per_asset_policy_examples(title, ticker, name, expected):
    row = relevance(metadata(title, ticker, name))[0]
    assert row["category"] == expected and row["policy"] == POLICY and not row["verified"]


def test_ambiguous_names_publisher_suffix_and_bare_short_tickers_are_not_direct():
    assert relevance(metadata("Apple launches autumn fruit harvest", "AAPL", "Apple Inc", False))[0]["category"] == "REVIEW"
    assert relevance(metadata("On reports rainy weather", "ON", "On Semiconductor Corp", False))[0]["category"] != "DIRECT"
    m = metadata("New research - NVIDIA", organizations=False); m["publisher"] = "NVIDIA"
    assert relevance(m)[0]["category"] != "DIRECT"


def test_same_article_can_have_different_company_relevance():
    m = metadata("NVIDIA announces earnings")
    m["requestedAssets"].append(dict(assetId=43, ticker="AAPL", name="APPLE INC", exchange="NASDAQ"))
    m["targetMatches"].append(dict(assetId=43, ticker="AAPL", basis=["ORGANIZATION_NAME_MENTION"]))
    assert [r["category"] for r in relevance(m)] == ["DIRECT", "MENTION"]


@pytest.mark.parametrize("raw,expected,method", [
    (b'<title>Fallback title</title><meta property="og:title" content="NVIDIA &amp; partners announce GPUs">', "NVIDIA & partners announce GPUs", "og:title"),
    (b'<meta name="twitter:title" content="A recovered headline">', "A recovered headline", "twitter:title"),
    (b'<title>\n A valid article title \t</title>', "A valid article title", "html:title"),
    (b'<meta property="og:title" content="Just a moment"><title>A legitimate fallback headline</title>', "A legitimate fallback headline", "html:title"),
])
def test_metadata_title_extraction(raw, expected, method):
    assert extract_title(raw) == (expected, method)


@pytest.mark.parametrize("raw", [b'<title>Access Denied</title>', b'<title>404 Page not found</title>', b'no metadata', b'<title>Sign in to continue</title>'])
def test_error_pages_are_not_titles(raw):
    with pytest.raises(TitleUnavailable): extract_title(raw)


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/a", "http://169.254.169.254/latest/meta-data/", "http://[::1]/", "file:///etc/passwd",
    "https://user:password@example.org/", "https://example.org:8443/", "http://host.internal/", "https://example.org/a\nX: bad",
    "https://example.org\\@127.0.0.1/", "https://example.org/?api_key=secret", "https://example.org/?X-Amz-Signature=abc",
])
def test_unsafe_url_rejected_before_dns_or_network(url, monkeypatch):
    resolve = Mock(); monkeypatch.setattr(transport, "public_addresses", resolve)
    result = ArticleTitleClient().fetch(url)
    assert result["status"] == "REVIEW_REQUIRED"; resolve.assert_not_called()


@pytest.mark.parametrize("blocked", ["10.0.0.1", "224.0.0.1"])
def test_mixed_private_dns_is_blocked(monkeypatch, blocked):
    monkeypatch.setattr(transport.socket, "getaddrinfo", lambda *a, **k: [
        (None, None, None, None, ("8.8.8.8", 0)), (None, None, None, None, (blocked, 0))])
    with pytest.raises(TitleUnavailable, match="NON_PUBLIC"): transport.public_addresses("example.org", 1)


def test_resolver_timeout_is_bounded(monkeypatch):
    import time
    monkeypatch.setattr(transport.socket, "getaddrinfo", lambda *a, **k: time.sleep(.2))
    with pytest.raises(TitleUnavailable, match="DNS_TIMEOUT"): transport.public_addresses("example.org", .01)


def test_tls_pins_validated_address_and_verifies_original_hostname(monkeypatch):
    sock = Mock(); dial = Mock(return_value=sock); wrap = Mock()
    context = Mock(); context.wrap_socket = wrap
    monkeypatch.setattr(transport.ssl, "create_default_context", lambda: context)
    monkeypatch.setattr(transport.socket, "create_connection", dial)
    conn = transport._TLS("publisher.example.org", "8.8.8.8", 3); conn.connect()
    dial.assert_called_once_with(("8.8.8.8", 443), 3)
    wrap.assert_called_once_with(sock, server_hostname="publisher.example.org")


def test_robots_disallow_and_failure_prevent_article_request():
    for status, body, reason in [(200, b"User-agent: *\nDisallow: /", "ROBOTS_DISALLOWED"), (503, b"", "ROBOTS_UNAVAILABLE"),
                                 (200, b"User-agent: *\nCrawl-delay: 20", "ROBOTS_SLOW_CRAWL_REVIEW")]:
        client = ArticleTitleClient(); client._get = Mock(return_value=(status, {}, body))
        assert client.fetch("https://example.org/a")["reason"] == reason
        assert client._get.call_count == 1


def test_redirect_destination_is_revalidated_and_downgrade_blocked():
    for location in ["http://127.0.0.1/private", "http://example.org/plain"]:
        client = ArticleTitleClient(); client._allowed = Mock()
        client._get = Mock(return_value=(302, {"location": location}, b""))
        assert client.fetch("https://example.org/a")["status"] == "REVIEW_REQUIRED"
        assert client._get.call_count == 1


def test_success_keeps_evidence_not_article_body_and_rechecks_cross_host_robots():
    client = ArticleTitleClient(); client._allowed = Mock()
    client._get = Mock(side_effect=[(301, {"location": "https://publisher.example.org/a"}, b""),
                                   (200, {"content-type": "text/html"}, b"<title>A recovered title</title><p>body not stored</p>")])
    receipt = client.fetch("https://example.org/a")
    assert receipt["status"] == "RECOVERED" and receipt["redirects"] == 1 and len(receipt["responseSha256"]) == 64
    assert "body" not in receipt and "html" not in receipt
    assert client._allowed.call_count == 2


def test_old_http_links_use_https_with_audited_upgrade_not_plaintext_fallback():
    client = ArticleTitleClient(); client._allowed = Mock()
    client._get = Mock(return_value=(200, {"content-type": "text/html"}, b"<title>A recovered article title</title>"))
    receipt = client.fetch("http://example.org/old-article")
    assert receipt["httpsUpgrade"] and receipt["finalUrl"] == "https://example.org/old-article"
    assert all(call.args[0].startswith("https://") for call in client._get.call_args_list)


def test_robots_redirect_target_cannot_reach_private_address():
    client = ArticleTitleClient()
    client._get = Mock(return_value=(302, {"location": "http://127.0.0.1/robots.txt"}, b""))
    result = client.fetch("https://example.org/a")
    assert result["status"] == "REVIEW_REQUIRED" and client._get.call_count == 1


@pytest.mark.parametrize("headers,raw,reason", [
    ({"content-length": str(transport.MAX_BYTES + 1)}, b"", "TITLE_SIZE_LIMIT"),
    ({"content-encoding": "gzip"}, b"", "TITLE_ENCODING_UNSUPPORTED"),
    ({}, b"a" * (transport.MAX_BYTES + 1), "TITLE_SIZE_LIMIT"),
], ids=["length-header", "compressed-body", "stream-overflow"])
def test_transport_caps_without_hidden_retries(headers, raw, reason, monkeypatch):
    response = Mock(status=200); response.getheaders.return_value = list(headers.items())
    chunks = [raw[i:i+8192] for i in range(0, len(raw), 8192)] + [b""]
    response.read1.side_effect = chunks
    conn = Mock(); conn.getresponse.return_value = response
    monkeypatch.setattr(transport, "public_addresses", lambda *_: ["8.8.8.8"])
    factory = Mock(return_value=conn); monkeypatch.setattr(transport, "_TLS", factory)
    with pytest.raises(TitleUnavailable, match=reason): ArticleTitleClient()._get("https://example.org/a", transport.time.monotonic() + 12, transport.MAX_BYTES, lambda: None)
    assert factory.call_count == 1; conn.close.assert_called_once()


class Repo:
    def __init__(self): self.saved = []; self.cancelled = False
    def save(self, c): self.saved.append(copy.deepcopy(c))
    def check_cancel(self):
        if self.cancelled: raise NewsCancelled()


def observations(count=1):
    return normalize([dict(metadata(), url=f"https://example.org/{i}") for i in range(count)], "GDELT")[1]


def test_checkpoint_reservation_precedes_fetch_and_replay_does_not_repeat():
    repo = Repo(); checkpoint = {}; originals = observations(); pristine = copy.deepcopy(originals)
    def fetch(*args, **kwargs):
        assert list(repo.saved[-1]["titleEnrichment"]["urls"].values())[0]["status"] == "RESERVED"
        return dict(status="RECOVERED", title="NVIDIA announces earnings", method="og:title")
    client = Mock(); client.fetch.side_effect = fetch
    enriched = enrich_observations(originals, checkpoint, repo, client)
    again = enrich_observations(originals, checkpoint, repo, client)
    assert originals == pristine and again == enriched and client.fetch.call_count == 1
    m = enriched[0]["metadata"]
    assert m["title"] and m["providerTitle"] is None and m["originalObservationHash"] == originals[0]["observationHash"]
    assert not m["originalArticleVerified"] and not m["tickerIdentityVerified"] and not m["ragEligible"] and m["body"] is None
    assert enriched[0]["observationHash"] == digest(m)


def test_failed_title_continues_next_and_unit_budget_is_durable():
    client = Mock(); client.fetch.return_value = dict(status="REVIEW_REQUIRED", reason="TITLE_HTTP_404")
    checkpoint = {}; repo = Repo(); rows = observations(12)
    output = enrich_observations(rows, checkpoint, repo, client)
    assert len(output) == 12 and client.fetch.call_count == 10
    assert all(r["metadata"]["assetRelevance"][0]["category"] == "REVIEW" for r in output)
    enrich_observations(rows, checkpoint, repo, client)
    assert client.fetch.call_count == 10


def test_unknown_fetch_outcome_is_not_retried_and_fatal_errors_propagate():
    client = Mock(); client.fetch.side_effect = RuntimeError("unclassified")
    checkpoint = {}; repo = Repo(); rows = observations()
    with pytest.raises(RuntimeError): enrich_observations(rows, checkpoint, repo, client)
    output = enrich_observations(rows, checkpoint, repo, client)
    assert client.fetch.call_count == 1 and output[0]["metadata"]["titleEnrichment"]["reason"].startswith("PRIOR_FETCH_OUTCOME_UNKNOWN")
    repo.save = Mock(side_effect=RuntimeError("commit unknown"))
    with pytest.raises(RuntimeError): enrich_observations(observations(), {}, repo, client)


def test_existing_title_not_fetched_cancellation_not_swallowed_and_policy_conflict():
    rows = normalize([metadata("NVIDIA announces earnings")], "RSS")[1]
    client = Mock(); repo = Repo()
    output = enrich_observations(rows, {}, repo, client); client.fetch.assert_not_called()
    assert output[0]["metadata"]["titleEnrichment"]["status"] == "PROVIDER_TITLE_RETAINED"
    repo.cancelled = True
    with pytest.raises(NewsCancelled): enrich_observations(rows, {}, repo, client)
    with pytest.raises(RuntimeError, match="POLICY_CONFLICT"):
        enrich_observations(rows, {"titleEnrichment": {"policy": "other"}}, Repo(), client)


def test_collection_integration_persists_classification(tmp_path, monkeypatch):
    # The production collect() seam is exercised with its real immutable raw storage in the existing test module.
    from tests.test_news_collection import item, Repository, client as provider_client, gkg
    from collectors.common import LocalPipelineStorage
    from workers.news_collector import collect
    i = item("GDELT"); i.parameters.update(contract="news-v2", enrichmentPolicy=POLICY,
        targets=[dict(assetId=42, ticker="NVDA", name="NVIDIA CORP", exchange="NASDAQ", organizationNames=["nvidia"])])
    repo = Repository(); titles = Mock(); titles.fetch.return_value = dict(status="RECOVERED", title="NVIDIA announces earnings", method="og:title")
    monkeypatch.setenv("RAW_STORAGE_BACKEND", "LOCAL"); monkeypatch.delenv("RAW_STORAGE_BASE_URI", raising=False)
    storage = LocalPipelineStorage(tmp_path, 2)
    assert collect(i, repo, storage, provider_client(gkg()), wait=lambda *_: None, title_client=titles) == "COLLECTED"
    stored = repo.finishes[0][2][0]["metadata"]
    assert stored["assetRelevance"][0]["category"] == "DIRECT" and stored["collectionContractHash"] == repo.checkpoint["contractHash"]
    assert repo.checkpoint["titleEnrichment"]["summary"]["titleRecovered"] == 1


def test_backfill_appends_and_reads_back_without_updating_old_records():
    from unittest.mock import MagicMock
    from workers.news_enrichment_backfill import append_revisions
    original = dict(observations()[0], sourceId=7)
    enriched = enrich_observations([original], {}, Repo())
    conn = MagicMock(); cursor = conn.cursor.return_value.__enter__.return_value
    cursor.rowcount = 1
    cursor.fetchone.side_effect = [(original["metadata"],), (enriched[0]["metadata"],)]
    assert append_revisions(conn, [original], enriched) == 1
    sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "INSERT INTO news_collection_observation" in sql and "ON CONFLICT DO NOTHING" in sql
    assert "UPDATE " not in sql and "DELETE " not in sql
    assert conn.commit.call_count == 2


def test_backfill_tamper_and_unknown_commit_do_not_report_success():
    from unittest.mock import MagicMock
    from workers.news_enrichment_backfill import append_revisions
    original = dict(observations()[0], sourceId=7)
    enriched = enrich_observations([original], {}, Repo())
    conn = MagicMock(); conn.commit.side_effect = RuntimeError("commit unknown")
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (original["metadata"],)
    with pytest.raises(RuntimeError, match="commit unknown"): append_revisions(conn, [original], enriched)
    conn.rollback.assert_called_once()
    broken = copy.deepcopy(enriched); broken[0]["metadata"]["originalObservationHash"] = "forged"
    with pytest.raises(ValueError, match="EVIDENCE_CONFLICT"): append_revisions(MagicMock(), [original], broken)


def test_backfill_stop_file_prevents_fetch_and_requires_finished_job(tmp_path):
    from unittest.mock import MagicMock
    from workers.news_enrichment_backfill import ReceiptRepository, select_existing
    (tmp_path / "STOP").touch()
    with pytest.raises(NewsCancelled): enrich_observations(observations(), {}, ReceiptRepository(tmp_path), Mock())
    conn = MagicMock(); conn.cursor.return_value.__enter__.return_value.fetchone.return_value = ("RUNNING", True)
    with pytest.raises(RuntimeError, match="FINISHED_JOB"): select_existing(conn, 7, 50)
    with pytest.raises(ValueError, match="BOUNDS"): select_existing(conn, 7, 101)
