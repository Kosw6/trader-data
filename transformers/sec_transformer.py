from collectors.common import content_hash, to_iso


TRANSFORMER_NAME = "sec-transformer"
TRANSFORMER_VERSION = "2.0.0"

EXCHANGE_DEFAULTS = {
    "NASDAQ": ("NASDAQ", "USD", "America/New_York"),
    "NASDAQ GLOBAL SELECT": ("NASDAQ", "USD", "America/New_York"),
    "NASDAQ GLOBAL MARKET": ("NASDAQ", "USD", "America/New_York"),
    "NASDAQ CAPITAL MARKET": ("NASDAQ", "USD", "America/New_York"),
    "NYSE": ("NYSE", "USD", "America/New_York"),
    "NYSE AMERICAN": ("NYSE_AMERICAN", "USD", "America/New_York"),
    "OTC": ("OTC", "USD", "America/New_York"),
}


TARGET_FORMS = {"10-K", "10-Q", "8-K"}

METRIC_TAG_CANDIDATES = {
    "REVENUE": {
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "Revenues",
    },
    "NET_INCOME": {"NetIncomeLoss"},
    "OPERATING_INCOME": {"OperatingIncomeLoss"},
    "ASSETS": {"Assets"},
    "LIABILITIES": {"Liabilities"},
    "STOCKHOLDERS_EQUITY": {
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    },
    "DILUTED_EPS": {"EarningsPerShareDiluted"},
    "OPERATING_CASH_FLOW": {"NetCashProvidedByUsedInOperatingActivities"},
    "CASH": {
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    },
    "CAPEX": {
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    },
}

TAG_TO_METRIC = {
    tag: metric
    for metric, tags in METRIC_TAG_CANDIDATES.items()
    for tag in tags
}

KNOWN_FACT_FIELDS = {
    "start",
    "end",
    "val",
    "accn",
    "fy",
    "fp",
    "form",
    "filed",
    "frame",
}


def normalize_form(form):
    """10-K/A 같은 수정 공시를 기본 공시 유형인 10-K로 정규화한다."""
    return (form or "").removesuffix("/A")


def normalize_exchange(exchange):
    """SEC 거래소명을 내부 코드·통화·시간대로 정규화한다."""
    original = (exchange or "").strip()
    normalized = " ".join(original.upper().split())
    if normalized in EXCHANGE_DEFAULTS:
        return EXCHANGE_DEFAULTS[normalized]
    return normalized or "UNKNOWN", "XXX", "UTC"


def transform_company(submissions, raw_object_uri, collected_at):
    """SEC Submissions 기업 메타데이터를 company 스키마 레코드로 변환한다."""
    business_address = (submissions.get("addresses") or {}).get("business") or {}
    return {
        "provider": "SEC",
        "cik": str(submissions.get("cik", "")).zfill(10),
        "company_name": submissions.get("name"),
        "sic_code": str(submissions.get("sic") or "") or None,
        "sic_description": submissions.get("sicDescription") or None,
        "fiscal_year_end": submissions.get("fiscalYearEnd") or None,
        "country_code": None,
        "state_of_incorporation": submissions.get("stateOfIncorporation") or None,
        "business_state_or_country": business_address.get("stateOrCountry") or None,
        "business_state_or_country_description": business_address.get(
            "stateOrCountryDescription"
        )
        or None,
        "entity_type": submissions.get("entityType") or None,
        "raw_object_uri": raw_object_uri,
        "collected_at": to_iso(collected_at),
        "updated_at": to_iso(collected_at),
    }


def transform_assets(submissions, raw_object_uri, collected_at):
    """SEC ticker·exchange 배열을 DB 보강 전 단계의 asset 레코드로 변환한다."""
    cik = str(submissions.get("cik", "")).zfill(10)
    tickers = submissions.get("tickers") or []
    exchanges = submissions.get("exchanges") or []
    records = []
    for index, ticker in enumerate(tickers):
        exchange = exchanges[index] if index < len(exchanges) else None
        exchange_code, currency, timezone_name = normalize_exchange(exchange)
        records.append(
            {
                "provider": "SEC",
                "company_cik": cik,
                "asset_type": "UNKNOWN",
                "ticker": ticker,
                "name": submissions.get("name"),
                "share_class": None,
                "security_identifier": None,
                "exchange": exchange_code,
                "currency": currency,
                "timezone": timezone_name,
                "active": True,
                "enrichment_status": "PENDING",
                "listed_at": None,
                "delisted_at": None,
                "raw_object_uri": raw_object_uri,
                "collected_at": to_iso(collected_at),
            }
        )
    return records


def _column_value(columns, name, index):
    """SEC 병렬 배열에서 누락에 안전하게 특정 인덱스의 값을 조회한다."""
    values = columns.get(name) or []
    return values[index] if index < len(values) else None


def transform_filings(submissions, filing_pages, raw_object_uri, collected_at, *, allowed_forms=None):
    """최근·과거 submissions 페이지를 10-K·10-Q·8-K filing 레코드로 변환한다."""
    cik = str(submissions.get("cik", "")).zfill(10)
    records = []
    seen = set()
    forms = TARGET_FORMS if allowed_forms is None else frozenset(allowed_forms)

    for columns in filing_pages:
        accession_numbers = columns.get("accessionNumber") or []
        for index, accession_number in enumerate(accession_numbers):
            if accession_number in seen:
                continue
            form = _column_value(columns, "form", index) or ""
            if normalize_form(form) not in forms:
                continue

            primary_document = _column_value(columns, "primaryDocument", index)
            accession_path = accession_number.replace("-", "")
            document_url = None
            if primary_document:
                document_url = (
                    "https://www.sec.gov/Archives/edgar/data/"
                    f"{int(cik)}/{accession_path}/{primary_document}"
                )

            records.append(
                {
                    "provider": "SEC",
                    "company_cik": cik,
                    "accession_number": accession_number,
                    "form_type": form,
                    "filing_date": _column_value(columns, "filingDate", index),
                    "report_date": _column_value(columns, "reportDate", index) or None,
                    "accepted_at": _column_value(
                        columns, "acceptanceDateTime", index
                    ),
                    "primary_document": primary_document,
                    "document_url": document_url,
                    "is_xbrl": _column_value(columns, "isXBRL", index),
                    "is_inline_xbrl": _column_value(
                        columns, "isInlineXBRL", index
                    ),
                    "amendment": form.endswith("/A"),
                    "raw_object_uri": raw_object_uri,
                    "collected_at": to_iso(collected_at),
                }
            )
            seen.add(accession_number)

    return records


def transform_financial_facts(
    company_facts,
    raw_object_uri,
    collected_at,
    core_metrics_only=True,
    *,
    allowed_forms=None,
    allowed_taxonomies=None,
):
    """Company Facts를 원본 태그를 보존한 financial_fact 레코드로 변환한다."""
    cik = str(company_facts.get("cik", "")).zfill(10)
    records = []
    seen_hashes = set()
    forms = TARGET_FORMS if allowed_forms is None else frozenset(allowed_forms)
    taxonomies = None if allowed_taxonomies is None else frozenset(allowed_taxonomies)

    for taxonomy, concepts in (company_facts.get("facts") or {}).items():
        if taxonomies is not None and taxonomy not in taxonomies:
            continue
        for source_tag, concept in concepts.items():
            metric_code = TAG_TO_METRIC.get(source_tag)
            if core_metrics_only and metric_code is None:
                continue

            for unit, facts in (concept.get("units") or {}).items():
                for fact in facts:
                    form = fact.get("form") or ""
                    if normalize_form(form) not in forms:
                        continue
                    if (
                        fact.get("val") is None
                        or fact.get("end") is None
                        or not fact.get("accn")
                    ):
                        continue

                    identity = {
                        "cik": cik,
                        "taxonomy": taxonomy,
                        "source_tag": source_tag,
                        "unit": unit,
                        "fact": fact,
                    }
                    fact_hash = content_hash(identity)
                    if fact_hash in seen_hashes:
                        continue

                    dimensions = {
                        key: value
                        for key, value in fact.items()
                        if key not in KNOWN_FACT_FIELDS
                    }
                    records.append(
                        {
                            "provider": "SEC",
                            "company_cik": cik,
                            "accession_number": fact.get("accn"),
                            "metric_code": metric_code,
                            "taxonomy": taxonomy,
                            "source_tag": source_tag,
                            "label": concept.get("label"),
                            "description": concept.get("description"),
                            "unit": unit,
                            "period_start": fact.get("start"),
                            "period_end": fact.get("end"),
                            "value": str(fact.get("val")),
                            "fiscal_year": fact.get("fy"),
                            "fiscal_period": fact.get("fp"),
                            "form_type": form,
                            "filed_date": fact.get("filed"),
                            "frame": fact.get("frame"),
                            "dimensions": dimensions or None,
                            "source_fact_hash": fact_hash,
                            "raw_object_uri": raw_object_uri,
                            "collected_at": to_iso(collected_at),
                        }
                    )
                    seen_hashes.add(fact_hash)

    return records
