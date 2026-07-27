# Collector and Transformer Preview

DB를 만들기 전에 SEC와 BLS 데이터를 `DATA_SCHEMA_DESIGN.md`의 테이블 형태로 확인하기 위한 실행 코드다.

## 구조

```text
collectors/
  common.py          HTTP 재시도, raw 저장, JSONL 출력
  sec_client.py      SEC API client
  sec_collector.py   SEC raw 수집과 transformer 실행
  bls_client.py      BLS API client
  bls_collector.py   BLS 캘린더·시계열 수집

transformers/
  sec_transformer.py
  bls_transformer.py
```

`collectors`는 외부 응답과 `source_object`를 만들고, `transformers`는 DB 컬럼과 같은 딕셔너리를 반환한다. 현재는 `LocalPipelineStorage.write_table()`이 JSONL을 쓰며, DB 도입 시 이 부분을 repository bulk insert로 교체한다.

DB 내부 ID가 아직 없으므로 JSONL에서는 `company_cik`, `accession_number`, `external_series_id`, `release_external_id`를 관계 연결 키로 사용한다. DB loader를 추가할 때 이 값을 조회해 실제 FK인 `company_id`, `filing_id`, `series_id`, `release_id`로 변환한다.

SEC Submissions만으로는 Asset 통화, 시간대, 주식·ADR 유형을 확정할 수 없다. 따라서 SEC 출력은 해당 필드를 `null` 또는 `UNKNOWN`으로 두고 `enrichment_status=PENDING`을 표시한다. 이후 KIS 또는 거래소 메타데이터로 보강한다.

## 설치

```powershell
cd C:\Users\USER\kosw006\trader\trader\trader-data
pip install -r requirements.txt
```

`.env`에는 다음 값을 둔다.

```dotenv
SEC_USER_AGENT="TraderDataBot-KR/1.0 YourName your-email@example.com"
BLS_API_KEY="optional-registration-key"
DATABASE_URL="postgresql://postgres:password@localhost:55432/trader"
```

## SEC 실행

Apple과 NVIDIA의 최근 공시와 핵심 재무 지표:

```powershell
python -m collectors.sec_collector
```

과거 submissions 페이지까지 포함:

```powershell
python -m collectors.sec_collector --include-history
```

모든 SEC 표준 Fact 출력:

```powershell
python -m collectors.sec_collector --all-facts
```

기업 직접 지정:

```powershell
python -m collectors.sec_collector --company AAPL=0000320193 --company NVDA=0001045810
```

## 레거시·SEC Company 백필

`stock_info` 검증과 실제 반영:

```powershell
python -m backfills.stock_info_backfill --limit 100
python -m backfills.stock_info_backfill --apply --batch-size 500
```

SEC Company는 `company_tickers_exchange.json`의 `ticker + exchange`와 현재 KIS
`asset_provider_symbol.provider_symbol + asset.exchange`가 모두 정확히 일치하는 CIK만
선택한다. ticker 기호 변환이나 이름 유사도는 자동 선택에 사용하지 않는다.

```powershell
# 교집합 중 100개를 DB 쓰기 없이 검증
python -m backfills.sec_company_backfill --matched-assets-only --limit 100

# 교집합 전체를 company·company_alias·source_object·record_lineage에 반영
python -m backfills.sec_company_backfill --apply --matched-assets-only
```

중단 후에는 마지막 출력 CIK를 기준으로 재개한다.

```powershell
python -m backfills.sec_company_backfill `
  --apply `
  --matched-assets-only `
  --start-after-cik 0001045810
```

## BLS 실행

최근 3년 기본 매크로 시리즈와 발표 캘린더:

```powershell
python -m collectors.bls_collector
```

기간 지정:

```powershell
python -m collectors.bls_collector --start-year 2020 --end-year 2026
```

## 출력

```text
pipeline_output/
  sec/
    raw/
    tables/
      company.jsonl
      asset.jsonl
      filing.jsonl
      financial_fact.jsonl
      source_object.jsonl
  bls/
    raw/
    tables/
      macro_series.jsonl
      macro_release.jsonl
      macro_observation.jsonl
      source_object.jsonl
```

재무·매크로 `value`는 JSONL에서 문자열로 보존한다. PostgreSQL 적재 계층에서 `Decimal`로 변환해 `NUMERIC` 컬럼에 저장한다.

## 테스트

```powershell
python -m unittest discover -s tests -v
```
