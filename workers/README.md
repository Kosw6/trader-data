# Trader Data Workers

## Worker 구분

현재 worker는 두 계층으로 나뉩니다.

| 종류 | 역할 |
| --- | --- |
| combined worker | 수집, raw 저장, 변환, DB 적재를 한 번에 수행합니다. MVP 검증과 수동 운영에 계속 사용할 수 있습니다. |
| split worker | collector-only와 ETL-only로 분리합니다. raw 보존, Kafka 이벤트, ETL 실패/재처리 흐름을 검증하기 위한 운영형 구조입니다. |

## Combined Workers

```powershell
python -m workers.bls_macro_worker
python -m workers.sec_financial_worker
python -m workers.kis_price_worker
```

loop 모드:

```powershell
python -m workers.bls_macro_worker --loop --poll-interval-seconds 10
python -m workers.sec_financial_worker --loop --poll-interval-seconds 10
python -m workers.kis_price_worker --loop --poll-interval-seconds 10
```

## Split Workers

collector-only worker는 외부 API를 호출하고 raw 파일을 저장한 뒤 `source_object`를 `COLLECTED`로 남깁니다. 이후 Go controller의 raw-ready API를 호출해 `pipeline_outbox`에 `RAW_OBJECT_READY` 이벤트를 생성합니다.

ETL-only worker는 Kafka raw topic을 소비하거나 `--source-object-id`를 직접 받아 raw 파일을 읽고 정규화 테이블과 `record_lineage`를 적재합니다.

### KIS Price

collector:

```powershell
python -m workers.kis_price_collector
```

ETL consumer:

```powershell
python -m workers.kis_price_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.kis_price_etl_worker --source-object-id 8755
```

### BLS Macro

collector:

```powershell
python -m workers.bls_macro_collector
```

ETL consumer:

```powershell
python -m workers.bls_macro_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.bls_macro_etl_worker --source-object-id 8750
```

### SEC Financial

collector:

```powershell
python -m workers.sec_financial_collector
```

ETL consumer:

```powershell
python -m workers.sec_financial_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.sec_financial_etl_worker --source-object-id 8752
```

SEC는 하나의 item이 `SUBMISSIONS`, `SUBMISSIONS_HISTORY`, `COMPANY_FACTS` raw를 여러 개 만들 수 있습니다. collector는 이 raw들을 모두 `source_object`로 저장하고, 대표 raw 하나만 `RAW_OBJECT_READY`로 보고합니다. ETL worker는 대표 raw가 속한 `pipeline_job_item_id`를 기준으로 같은 item의 SEC raw 묶음을 읽어 `filing`, `financial_fact`, `record_lineage`를 함께 적재합니다.

## Kafka Topics

| topic | 소비 worker | 용도 |
| --- | --- | --- |
| `trader.jobs.events` | `workers.kafka_pipeline_consumer` | 기존 combined worker를 Kafka로 실행하는 control-plane 이벤트 |
| `trader.raw.kis.ready` | `workers.kis_price_etl_worker` | KIS raw price bar ETL |
| `trader.raw.bls.ready` | `workers.bls_macro_etl_worker` | BLS timeseries raw ETL |
| `trader.raw.sec.ready` | `workers.sec_financial_etl_worker` | SEC raw bundle ETL |
| `trader.raw.news.ready` | 추후 news ETL worker | 뉴스 raw ETL 확장 예정 |

## 필수 환경 변수

```text
DATABASE_URL
TRADER_CONTROLLER_URL
ADMIN_API_KEY
KAFKA_BROKERS
```

KIS:

```text
APPKEY
APPSECRET
```

SEC:

```text
SEC_USER_AGENT
```

BLS:

```text
BLS_API_KEY
```

## Item Key

```text
KIS:{asset_id}
BLS:{series_id}
SEC:{ticker}
SEC:{cik}
```

예시:

```text
KIS:2872
BLS:CUSR0000SA0
SEC:AAPL
SEC:0001045810
```
