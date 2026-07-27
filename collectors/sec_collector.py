import argparse
import os
from pathlib import Path

from collectors.common import (
    LocalPipelineStorage,
    build_lineage_record,
    build_target_record_key,
    utc_now,
)
from collectors.sec_client import SECClient
from dotenv import load_dotenv
from transformers.sec_transformer import (
    TRANSFORMER_NAME,
    TRANSFORMER_VERSION,
    transform_assets,
    transform_company,
    transform_filings,
    transform_financial_facts,
)


DEFAULT_COMPANIES = {
    "AAPL": "0000320193",
    "NVDA": "0001045810",
}


def build_sec_lineage(records, source_by_uri, target_table, identity_builder, now):
    """SEC 정규화 레코드를 원본 객체와 안정적인 target key로 연결한다."""
    lineage = []
    for record in records:
        source = source_by_uri.get(record.get("raw_object_uri"))
        if source is None:
            raise ValueError(
                f"No source_object for {target_table}: {record.get('raw_object_uri')}"
            )
        target_key = build_target_record_key(
            target_table,
            identity_builder(record),
        )
        lineage.append(
            build_lineage_record(
                source,
                target_table,
                target_key,
                record,
                TRANSFORMER_NAME,
                TRANSFORMER_VERSION,
                now,
            )
        )
    return lineage


def parse_company(value):
    """CLI의 TICKER=CIK 입력을 검증하고 10자리 CIK 튜플로 변환한다."""
    if "=" not in value:
        raise argparse.ArgumentTypeError("Use TICKER=CIK, for example AAPL=0000320193")
    ticker, cik = value.split("=", 1)
    ticker = ticker.strip().upper()
    cik = cik.strip().zfill(10)
    if not ticker or not cik.isdigit():
        raise argparse.ArgumentTypeError(f"Invalid company value: {value}")
    return ticker, cik


def collect_company(
    client,
    storage,
    ticker,
    cik,
    include_history=False,
    all_facts=False,
):
    """한 기업의 SEC 원본을 수집하고 company·asset·filing·fact 레코드로 변환한다."""
    collected_at = utc_now()
    source_objects = []

    submissions = client.get_submissions(cik)
    submissions_source = storage.write_raw_json(
        "SEC",
        "SUBMISSIONS",
        cik,
        submissions,
        collected_at,
    )
    source_objects.append(submissions_source)

    filing_records = transform_filings(
        submissions,
        [submissions.get("filings", {}).get("recent", {})],
        submissions_source["object_uri"],
        collected_at,
    )

    if include_history:
        for historical_file in submissions.get("filings", {}).get("files", []):
            file_name = historical_file.get("name")
            if not file_name:
                continue
            page = client.get_historical_submission_page(file_name)
            page_source = storage.write_raw_json(
                "SEC",
                "SUBMISSIONS_HISTORY",
                file_name,
                page,
                collected_at,
            )
            source_objects.append(page_source)
            filing_records.extend(
                transform_filings(
                    submissions,
                    [page],
                    page_source["object_uri"],
                    collected_at,
                )
            )

    company_facts = client.get_company_facts(cik)
    facts_source = storage.write_raw_json(
        "SEC",
        "COMPANY_FACTS",
        cik,
        company_facts,
        collected_at,
    )
    source_objects.append(facts_source)

    filing_records = list(
        {record["accession_number"]: record for record in filing_records}.values()
    )
    filing_records.sort(
        key=lambda record: (
            record.get("accepted_at") or "",
            record.get("filing_date") or "",
        ),
        reverse=True,
    )

    company_records = [
        transform_company(
            submissions,
            submissions_source["object_uri"],
            collected_at,
        )
    ]
    asset_records = transform_assets(
        submissions,
        submissions_source["object_uri"],
        collected_at,
    )
    financial_fact_records = transform_financial_facts(
        company_facts,
        facts_source["object_uri"],
        collected_at,
        core_metrics_only=not all_facts,
    )
    source_by_uri = {
        source_object["object_uri"]: source_object
        for source_object in source_objects
    }
    lineage_records = []
    lineage_records.extend(
        build_sec_lineage(
            company_records,
            source_by_uri,
            "company",
            lambda record: {"cik": record["cik"]},
            collected_at,
        )
    )
    lineage_records.extend(
        build_sec_lineage(
            asset_records,
            source_by_uri,
            "asset",
            lambda record: {
                "company_cik": record["company_cik"],
                "exchange": record["exchange"],
                "ticker": record["ticker"],
            },
            collected_at,
        )
    )
    lineage_records.extend(
        build_sec_lineage(
            filing_records,
            source_by_uri,
            "filing",
            lambda record: {"accession_number": record["accession_number"]},
            collected_at,
        )
    )
    lineage_records.extend(
        build_sec_lineage(
            financial_fact_records,
            source_by_uri,
            "financial_fact",
            lambda record: {"source_fact_hash": record["source_fact_hash"]},
            collected_at,
        )
    )

    return {
        "company": company_records,
        "asset": asset_records,
        "filing": filing_records,
        "financial_fact": financial_fact_records,
        "source_object": source_objects,
        "record_lineage": lineage_records,
        "ticker": ticker,
    }


def merge_results(results):
    """기업별 수집 결과를 테이블 단위의 단일 레코드 목록으로 합친다."""
    merged = {
        "company": [],
        "asset": [],
        "filing": [],
        "financial_fact": [],
        "source_object": [],
        "record_lineage": [],
    }
    for result in results:
        for table_name in merged:
            merged[table_name].extend(result[table_name])
    return merged


def build_parser():
    """기업, 과거 공시, 전체 Fact, 출력 경로를 지정하는 CLI parser를 만든다."""
    parser = argparse.ArgumentParser(
        description="Collect SEC data and write schema-shaped JSONL previews."
    )
    parser.add_argument(
        "--company",
        action="append",
        type=parse_company,
        help="Company as TICKER=CIK. Repeat for multiple companies.",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).parents[1] / "pipeline_output" / "sec"),
        help="Local preview output root.",
    )
    parser.add_argument(
        "--include-history",
        action="store_true",
        help="Also fetch SEC submissions.files historical pages.",
    )
    parser.add_argument(
        "--all-facts",
        action="store_true",
        help="Output all SEC facts instead of core financial metrics only.",
    )
    return parser


def main():
    """SEC 수집 CLI를 실행하고 raw 파일과 테이블별 JSONL을 출력한다."""
    load_dotenv()
    args = build_parser().parse_args()
    user_agent = os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT is required in .env or the environment")

    companies = dict(args.company or DEFAULT_COMPANIES.items())
    storage = LocalPipelineStorage(args.output)
    client = SECClient(user_agent)

    results = []
    for ticker, cik in companies.items():
        print(f"Collecting SEC data: {ticker} ({cik})")
        result = collect_company(
            client,
            storage,
            ticker,
            cik,
            include_history=args.include_history,
            all_facts=args.all_facts,
        )
        results.append(result)
        print(
            f"  filings={len(result['filing'])} "
            f"facts={len(result['financial_fact'])}"
        )

    merged = merge_results(results)
    for table_name, records in merged.items():
        path, count = storage.write_table(table_name, records)
        print(f"{table_name}: {count} rows -> {path}")


if __name__ == "__main__":
    main()
