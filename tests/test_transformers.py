import unittest
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

from collectors.common import (
    LocalPipelineStorage,
    build_lineage_record,
    build_target_record_key,
)
from transformers.bls_transformer import (
    release_type_for_title,
    transform_observation_statuses,
    transform_observation_vintages,
    transform_observations,
)
from transformers.sec_transformer import (
    normalize_exchange,
    transform_company,
    transform_filings,
    transform_financial_facts,
)


NOW = datetime(2026, 6, 18, 0, 0, tzinfo=timezone.utc)


class SECTransformerTest(unittest.TestCase):
    """SEC 원본이 설계한 company·filing·financial_fact 형태로 변환되는지 검증한다."""

    def test_company_and_filing_fields_match_schema(self):
        """CIK 정규화와 공시 식별·시각 필드가 보존되는지 확인한다."""
        submissions = {
            "cik": "1045810",
            "name": "NVIDIA CORP",
            "sic": "3674",
            "sicDescription": "Semiconductors & Related Devices",
            "fiscalYearEnd": "0131",
            "stateOfIncorporation": "DE",
            "entityType": "operating",
        }
        company = transform_company(submissions, "file:///submissions.json", NOW)
        self.assertEqual("0001045810", company["cik"])
        self.assertEqual("3674", company["sic_code"])
        self.assertEqual(company["collected_at"], company["updated_at"])

        filing_page = {
            "accessionNumber": ["0001045810-26-000052"],
            "form": ["10-Q"],
            "filingDate": ["2026-05-20"],
            "reportDate": ["2026-04-26"],
            "acceptanceDateTime": ["2026-05-20T20:35:52.000Z"],
            "primaryDocument": ["nvda-20260426.htm"],
            "isXBRL": [1],
            "isInlineXBRL": [1],
        }
        filings = transform_filings(
            submissions,
            [filing_page],
            "file:///submissions.json",
            NOW,
        )
        self.assertEqual(1, len(filings))
        self.assertEqual("0001045810-26-000052", filings[0]["accession_number"])
        self.assertFalse(filings[0]["amendment"])

    def test_preserves_all_candidate_tags_across_time(self):
        """기업이 태그를 변경해도 과거와 현재 원본 태그를 모두 보존하는지 확인한다."""
        company_facts = {
            "cik": 1045810,
            "facts": {
                "us-gaap": {
                    "RevenueFromContractWithCustomerExcludingAssessedTax": {
                        "label": "Revenue",
                        "description": "Older revenue tag",
                        "units": {
                            "USD": [
                                {
                                    "start": "2021-01-01",
                                    "end": "2021-12-31",
                                    "val": 100,
                                    "accn": "old-accn",
                                    "fy": 2021,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2022-02-01",
                                }
                            ]
                        },
                    },
                    "Revenues": {
                        "label": "Revenue",
                        "description": "Current revenue tag",
                        "units": {
                            "USD": [
                                {
                                    "start": "2025-01-01",
                                    "end": "2025-12-31",
                                    "val": 200,
                                    "accn": "new-accn",
                                    "fy": 2025,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": "2026-02-01",
                                }
                            ]
                        },
                    },
                }
            },
        }
        facts = transform_financial_facts(
            company_facts,
            "file:///companyfacts.json",
            NOW,
        )
        self.assertEqual(2, len(facts))
        self.assertEqual(
            {
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "Revenues",
            },
            {fact["source_tag"] for fact in facts},
        )
        self.assertTrue(all(fact["metric_code"] == "REVENUE" for fact in facts))
        self.assertEqual(2, len({fact["source_fact_hash"] for fact in facts}))

    def test_exchange_defaults_fill_required_asset_fields(self):
        """SEC 거래소명이 Asset 필수 통화·시간대 값으로 정규화되는지 확인한다."""
        self.assertEqual(
            ("NASDAQ", "USD", "America/New_York"),
            normalize_exchange("Nasdaq"),
        )
        self.assertEqual(("UNKNOWN", "XXX", "UTC"), normalize_exchange(None))


class BLSTransformerTest(unittest.TestCase):
    """BLS 일정 분류와 월별 관측값 변환 규칙을 검증한다."""

    def test_does_not_misclassify_veterans_release(self):
        """Veterans 보고서를 일반 Employment Situation으로 오인하지 않는지 확인한다."""
        self.assertEqual("EMPLOYMENT", release_type_for_title("Employment Situation"))
        self.assertEqual(
            "OTHER",
            release_type_for_title("Employment Situation of Veterans"),
        )

    def test_observation_keeps_period_value_and_footnotes(self):
        """월별 기간·원본 값·주석을 보존하고 연평균 M13을 제외하는지 확인한다."""
        response = {
            "Results": {
                "series": [
                    {
                        "seriesID": "WPUFD4",
                        "data": [
                            {
                                "year": "2025",
                                "period": "M12",
                                "periodName": "December",
                                "value": "150.773",
                                "footnotes": [{}],
                            },
                            {
                                "year": "2025",
                                "period": "M13",
                                "periodName": "Annual",
                                "value": "149.000",
                                "footnotes": [{}],
                            },
                        ],
                    }
                ]
            }
        }
        records = transform_observations(response, NOW, "file:///bls.json")
        self.assertEqual(1, len(records))
        self.assertEqual("2025-12", records[0]["reference_period"])
        self.assertEqual("150.773", records[0]["value"])
        self.assertTrue(records[0]["content_hash"])

        vintages = transform_observation_vintages(records, NOW)
        self.assertEqual(1, len(vintages))
        self.assertEqual("COLLECTED_FALLBACK", vintages[0]["availability_source"])
        self.assertEqual(NOW.isoformat(), vintages[0]["available_at"])
        self.assertFalse(vintages[0]["is_initial"])

    def test_observation_skips_unavailable_values(self):
        """BLS가 결측값 '-'를 주면 NUMERIC 적재 대상 관측값에서 제외하는지 확인한다."""
        response = {
            "Results": {
                "series": [
                    {
                        "seriesID": "CUSR0000SA0",
                        "data": [
                            {
                                "year": "2025",
                                "period": "M10",
                                "periodName": "October",
                                "value": "-",
                                "footnotes": [
                                    {
                                        "code": "X",
                                        "text": "Data unavailable due to the 2025 lapse in appropriations",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        }
        self.assertEqual([], transform_observations(response, NOW, "file:///bls.json"))
        statuses = transform_observation_statuses(response, NOW, "file:///bls.json")
        self.assertEqual(1, len(statuses))
        self.assertEqual("2025-10", statuses[0]["reference_period"])
        self.assertEqual("PROVIDER_UNAVAILABLE", statuses[0]["status"])
        self.assertEqual("BLS_X", statuses[0]["reason_code"])
        self.assertEqual("-", statuses[0]["raw_value"])


class LocalPipelineStorageTest(unittest.TestCase):
    """로컬 raw 보존과 테이블별 JSONL 출력 계약을 검증한다."""

    def test_writes_raw_source_object_and_jsonl_table(self):
        """원본 저장 결과에 URI·해시가 있고 JSONL 파일이 생성되는지 확인한다."""
        with TemporaryDirectory() as directory:
            storage = LocalPipelineStorage(directory)
            source = storage.write_raw_json(
                "BLS",
                "TIMESERIES",
                "2025",
                {"status": "REQUEST_SUCCEEDED"},
                NOW,
            )
            table_path, count = storage.write_table("source_object", [source])

            self.assertEqual(1, count)
            self.assertTrue(table_path.exists())
            self.assertTrue(source["object_uri"].startswith("file:"))
            self.assertTrue(source["content_hash"])
            self.assertTrue(source["dedupe_key"])

            revised_source = storage.write_raw_json(
                "BLS",
                "TIMESERIES",
                "2025",
                {"status": "REQUEST_SUCCEEDED", "revision": 2},
                NOW,
            )
            self.assertNotEqual(source["object_uri"], revised_source["object_uri"])
            self.assertNotEqual(source["dedupe_key"], revised_source["dedupe_key"])

            target_key = build_target_record_key(
                "macro_observation",
                {"series": "WPUFD4", "reference_period": "2025-12"},
            )
            lineage = build_lineage_record(
                source,
                "macro_observation",
                target_key,
                {"value": "150.773", "collected_at": NOW.isoformat()},
                "bls-transformer",
                "2.0.0",
                NOW,
            )
            self.assertEqual(source["dedupe_key"], lineage["source_object_dedupe_key"])
            self.assertEqual(target_key, lineage["target_record_key"])
            self.assertTrue(lineage["output_hash"])


if __name__ == "__main__":
    unittest.main()
