import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()


SEC_DATA_URL = "https://data.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
OUTPUT_ROOT = Path(__file__).parent / "sec_sample_output"

COMPANIES = {
    "AAPL": "0000320193",
    "NVDA": "0001045810",
}

TARGET_FORMS = {"10-K", "10-Q", "8-K"}

# Keep alternative tags visible instead of forcing one universal schema.
METRIC_TAG_CANDIDATES = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "Revenues",
    ],
    "net_income": ["NetIncomeLoss"],
    "operating_income": ["OperatingIncomeLoss"],
    "assets": ["Assets"],
    "liabilities": ["Liabilities"],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "diluted_eps": ["EarningsPerShareDiluted"],
    "operating_cash_flow": ["NetCashProvidedByUsedInOperatingActivities"],
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "capital_expenditure": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
}


def create_session():
    user_agent = os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise RuntimeError(
            "Set SEC_USER_AGENT first, for example: "
            "$env:SEC_USER_AGENT='Your Name your-email@example.com'"
        )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
    )
    return session


def fetch_json(session, url):
    response = session.get(url, timeout=30)
    response.raise_for_status()
    time.sleep(0.2)
    return response.json()


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_form(form):
    return form.removesuffix("/A")


def extract_recent_filings(submissions):
    recent = submissions.get("filings", {}).get("recent", {})
    accession_numbers = recent.get("accessionNumber", [])
    filings = []

    for index, accession_number in enumerate(accession_numbers):
        form = recent["form"][index]
        if normalize_form(form) not in TARGET_FORMS:
            continue

        primary_document = recent["primaryDocument"][index]
        cik = str(submissions["cik"])
        accession_without_dashes = accession_number.replace("-", "")

        filings.append(
            {
                "accession_number": accession_number,
                "form": form,
                "filing_date": recent["filingDate"][index],
                "report_date": recent["reportDate"][index],
                "acceptance_datetime": recent["acceptanceDateTime"][index],
                "primary_document": primary_document,
                "is_xbrl": recent["isXBRL"][index],
                "is_inline_xbrl": recent["isInlineXBRL"][index],
                "document_url": (
                    f"{SEC_ARCHIVE_URL}/{int(cik)}/"
                    f"{accession_without_dashes}/{primary_document}"
                ),
            }
        )

    return filings


def find_available_tag(us_gaap_facts, candidates):
    available = []

    for tag in candidates:
        concept = us_gaap_facts.get(tag)
        if concept is None:
            continue

        facts = [
            fact
            for unit_facts in concept.get("units", {}).values()
            for fact in unit_facts
        ]
        latest_filed = max(
            (fact.get("filed", "") for fact in facts),
            default="",
        )
        available.append((latest_filed, len(facts), tag))

    if not available:
        return None

    # Companies can migrate between equivalent US-GAAP tags. Prefer the tag
    # that is still used in the most recently filed statement.
    return max(available)[2]


def extract_financial_facts(company_facts, max_facts_per_metric=20):
    us_gaap_facts = company_facts.get("facts", {}).get("us-gaap", {})
    rows = []
    coverage = {}

    for canonical_metric, candidates in METRIC_TAG_CANDIDATES.items():
        source_tag = find_available_tag(us_gaap_facts, candidates)
        coverage[canonical_metric] = {
            "found": source_tag is not None,
            "source_tag": source_tag,
            "candidates": candidates,
        }
        if source_tag is None:
            continue

        concept = us_gaap_facts[source_tag]
        concept_rows = []

        for unit, facts in concept.get("units", {}).items():
            for fact in facts:
                form = fact.get("form", "")
                if normalize_form(form) not in TARGET_FORMS:
                    continue

                concept_rows.append(
                    {
                        "canonical_metric": canonical_metric,
                        "taxonomy": "us-gaap",
                        "source_tag": source_tag,
                        "label": concept.get("label"),
                        "unit": unit,
                        "start_date": fact.get("start"),
                        "end_date": fact.get("end"),
                        "value": fact.get("val"),
                        "accession_number": fact.get("accn"),
                        "fiscal_year": fact.get("fy"),
                        "fiscal_period": fact.get("fp"),
                        "form": form,
                        "filed_date": fact.get("filed"),
                        "frame": fact.get("frame"),
                    }
                )

        concept_rows.sort(
            key=lambda row: (row["filed_date"] or "", row["end_date"] or ""),
            reverse=True,
        )
        rows.extend(concept_rows[:max_facts_per_metric])

    return rows, coverage


def print_summary(ticker, submissions, filings, facts, coverage):
    print(f"\n=== {ticker}: {submissions.get('name')} ===")
    print(f"CIK: {str(submissions.get('cik')).zfill(10)}")
    print(f"Recent target filings: {len(filings)}")

    for filing in filings[:8]:
        print(
            f"  {filing['form']:6} "
            f"filed={filing['filing_date']} "
            f"report={filing['report_date']} "
            f"accn={filing['accession_number']}"
        )

    print("Metric coverage:")
    for metric, result in coverage.items():
        status = "FOUND" if result["found"] else "MISSING"
        print(f"  {metric:24} {status:7} {result['source_tag'] or '-'}")

    print(f"Normalized fact samples: {len(facts)}")


def collect_company(session, ticker, cik):
    submissions_url = f"{SEC_DATA_URL}/submissions/CIK{cik}.json"
    company_facts_url = f"{SEC_DATA_URL}/api/xbrl/companyfacts/CIK{cik}.json"

    submissions = fetch_json(session, submissions_url)
    company_facts = fetch_json(session, company_facts_url)

    filings = extract_recent_filings(submissions)
    facts, coverage = extract_financial_facts(company_facts)

    company_dir = OUTPUT_ROOT / ticker
    write_json(company_dir / "submissions.raw.json", submissions)
    write_json(company_dir / "companyfacts.raw.json", company_facts)
    write_json(company_dir / "filings.sample.json", filings)
    write_json(company_dir / "financial_facts.sample.json", facts)
    write_json(company_dir / "metric_coverage.json", coverage)

    print_summary(ticker, submissions, filings, facts, coverage)


def main():
    session = create_session()
    for ticker, cik in COMPANIES.items():
        collect_company(session, ticker, cik)

    print(f"\nSaved sample data under: {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
