# Raw 수집과 ETL 분리 구조

## 1. 목적

이 문서는 `trader-data`의 외부 데이터 수집 파이프라인을 `collector-only`와 `etl-only`로 분리하는 기준을 정리한다.

핵심 목적은 다음과 같다.

- 외부 API 호출과 DB 정규화 적재를 분리한다.
- raw 원본을 로컬 또는 S3에 보존한다.
- `source_object`와 `record_lineage`로 원본과 정규화 레코드를 연결한다.
- Kafka lag를 source별로 관측해 ETL 실행 시점과 부하를 제어한다.
- Kafka 장애가 있어도 raw 수집 결과는 DB outbox를 통해 나중에 재발행한다.

## 2. 공통 흐름

```text
Go controller
  -> JOB_ITEM_QUEUED 발행

Python collector-only worker
  -> 외부 API 호출
  -> raw 파일 저장
  -> source_object 저장
  -> Go controller raw-ready API 호출

Go controller
  -> source_object 재조회
  -> provider별 topic 결정
  -> pipeline_outbox에 RAW_OBJECT_READY 저장

Go outbox relay
  -> Kafka 발행

Python etl-only worker
  -> RAW_OBJECT_READY 소비
  -> source_object.storage_key 또는 object_uri로 raw 읽기
  -> transform
  -> 정규화 테이블 적재
  -> record_lineage 저장
  -> source_object.processing_status 갱신
```

## 3. Topic 기준

| 용도 | Topic | 이벤트 |
| --- | --- | --- |
| Job 실행 제어 | `trader.jobs.events` | `JOB_ITEM_QUEUED` |
| KIS raw ETL | `trader.raw.kis.ready` | `RAW_OBJECT_READY` |
| BLS raw ETL | `trader.raw.bls.ready` | `RAW_OBJECT_READY` |
| SEC raw ETL | `trader.raw.sec.ready` | `RAW_OBJECT_READY` |
| 뉴스 raw ETL | `trader.raw.news.ready` | `RAW_OBJECT_READY` |

topic을 나누는 이유는 source별 데이터 특성이 다르기 때문이다.

- KIS: 종목 수가 많고 일별 반복 수집이 많다. lag 기준을 크게 잡고 batch ETL을 해도 된다.
- BLS: 월별 소량 데이터다. lag가 1이어도 운영상 확인 가치가 있다.
- SEC: companyfacts와 submissions가 무겁다. 저부하 시간대에 ETL을 몰아서 처리하기 좋다.
- 뉴스: 빈도가 높고 일부 지연을 허용할 수 있다. 별도 topic으로 격리해야 한다.

## 4. API 계약

collector-only worker는 raw 저장과 `source_object` 저장을 끝낸 뒤 controller에 완료를 보고한다.

```http
POST /api/v1/admin/source-objects/{source_object_id}/raw-ready
X-Admin-API-Key: ...
```

요청 body는 없다.

controller가 `source_object_id`로 DB를 다시 조회해 다음 값을 결정한다.

- `provider`
- `data_type`
- `pipeline_job_item_id`
- `storage_backend`
- `storage_key`
- `object_uri`
- `content_hash`

이벤트 규칙은 Go controller가 중앙에서 관리한다.

```text
message_key     = raw:{source_object_id}
idempotency_key = RAW_OBJECT_READY:{source_object_id}
event_type      = RAW_OBJECT_READY
topic           = provider별 raw topic
```

이렇게 하는 이유는 Python collector마다 topic, idempotency key, payload 규칙을 중복 구현하지 않기 위해서다.

## 5. 상태 모델

### pipeline_job_item.status

| 상태 | 의미 |
| --- | --- |
| `QUEUED` | 실행 대기 |
| `RUNNING` | collector가 처리 중 |
| `COLLECTED` | raw 저장과 source_object 생성 완료 |
| `SUCCESS` | 기존 combined worker처럼 수집과 적재까지 완료 |
| `SKIPPED` | 처리할 대상 없음 |
| `FAILED` | collector 단계 실패 |

분리 구조에서는 `COLLECTED`가 중요하다.

`COLLECTED`는 "정규화 적재 완료"가 아니라 "raw 수집 완료"를 뜻한다. ETL 성공 여부는 `source_object.processing_status`, `record_lineage`, `processed_event`로 확인한다.

### source_object.processing_status

| 상태 | 의미 |
| --- | --- |
| `COLLECTED` | raw 파일 저장 완료, ETL 대기 |
| `PROCESSING` | ETL worker가 처리 중 |
| `PROCESSED` | 정규화 적재와 lineage 기록 완료 |
| `FAILED` | ETL 처리 실패 |

## 6. KIS 구현 현황

### collector-only

파일:

```text
trader-data/workers/kis_price_collector.py
```

역할:

- `pipeline_job_item`을 `QUEUED -> RUNNING`으로 선점한다.
- `KIS:{asset_id}` item key를 해석한다.
- `asset_provider_symbol(provider='KIS')`에서 KIS 종목 코드를 조회한다.
- KIS daily price API를 호출한다.
- raw JSON을 `pipeline_output/kis_price_collector/.../raw/...`에 저장한다.
- `source_object`를 `COLLECTED` 상태로 저장한다.
- Go controller의 raw-ready API를 호출한다.
- item을 `COLLECTED`로 종료한다.

실행 예:

```powershell
python -m workers.kis_price_collector
```

loop 실행:

```powershell
python -m workers.kis_price_collector --loop
```

### etl-only

파일:

```text
trader-data/workers/kis_price_etl_worker.py
```

역할:

- `trader.raw.kis.ready` topic의 `RAW_OBJECT_READY` 이벤트를 소비한다.
- `sourceObjectId`로 `source_object`를 조회한다.
- `object_uri` 또는 `storage_key`로 raw JSON 파일을 읽는다.
- KIS raw page의 `output2`를 다시 bar 구조로 복원한다.
- OHLCV 검증을 수행한다.
- 유효한 row를 `price_bar`에 upsert한다.
- `record_lineage`로 `source_object -> price_bar` 연결을 남긴다.
- `source_object.processing_status`를 `PROCESSED` 또는 `FAILED`로 갱신한다.

Kafka 소비 실행:

```powershell
python -m workers.kis_price_etl_worker --max-records 1
```

Kafka 없이 특정 raw만 직접 처리:

```powershell
python -m workers.kis_price_etl_worker --source-object-id 8751
```

## 7. BLS 확장 방향

현재 BLS는 combined worker 중심이다.

분리 시 구조:

```text
bls_macro_collector.py
  -> BLS API/RSS/calendar 호출
  -> raw 저장
  -> source_object(COLLECTED)
  -> /source-objects/{id}/raw-ready 호출

bls_macro_etl_worker.py
  -> trader.raw.bls.ready 소비
  -> macro_series, macro_release, macro_observation 적재
  -> record_lineage 저장
  -> source_object(PROCESSED)
```

BLS는 데이터가 작기 때문에 분리의 성능 이점은 KIS보다 작다. 대신 발표 예정, 제공불가, 누락 상태를 투명하게 보여주는 운영 가시성 측면에서 의미가 있다.

## 8. SEC 확장 방향

현재 SEC는 companyfacts와 submissions를 combined worker로 처리한다.

분리 시 구조:

```text
sec_financial_collector.py
  -> submissions/companyfacts 호출
  -> raw 저장
  -> source_object(COLLECTED)
  -> /source-objects/{id}/raw-ready 호출

sec_financial_etl_worker.py
  -> trader.raw.sec.ready 소비
  -> filing, financial_fact 적재
  -> record_lineage 저장
  -> source_object(PROCESSED)
```

SEC는 데이터가 무겁고 기업 수가 많으므로 분리 효과가 크다.

- collector는 API rate limit과 raw 보존에 집중한다.
- ETL은 저부하 시간대에 몰아서 실행할 수 있다.
- submissions-first 전략으로 변경 여부만 확인한 뒤 필요한 기업만 companyfacts를 가져올 수 있다.

## 9. 장애 시나리오

### Kafka 장애

collector는 raw 파일과 `source_object`를 먼저 저장한다. 이후 controller API가 `pipeline_outbox`에 이벤트를 저장한다.

Kafka가 내려가 있으면 outbox relay가 발행하지 못하고 `PENDING` 또는 `RETRY_WAIT`에 남는다. Kafka 복구 후 relay가 다시 발행한다.

### ETL 실패

raw 파일은 삭제하지 않는다.

`source_object.processing_status = FAILED`로 남기고, 동일 `source_object_id`로 ETL worker를 다시 실행할 수 있다.

### 데이터 품질 문제

KIS에서 `close > high` 같은 이상 OHLC가 내려와도 raw를 보존한다.

정규화 적재 단계에서는 유효하지 않은 row를 제외하거나 품질 이력 테이블에 남긴다. 이후 보조 공급원이나 재조회로 복구할 때 원본과 결과를 비교할 수 있다.

## 10. 운영 확인 쿼리

raw 대기/처리 상태:

```sql
SELECT id, provider, data_type, external_id, processing_status,
       storage_backend, storage_key, pipeline_job_item_id, collected_at
FROM source_object
WHERE provider IN ('KIS', 'BLS', 'SEC')
ORDER BY id DESC
LIMIT 20;
```

RAW_OBJECT_READY outbox:

```sql
SELECT id, source_object_id, event_type, topic, status,
       kafka_partition, kafka_offset, created_at, published_at
FROM pipeline_outbox
WHERE event_type = 'RAW_OBJECT_READY'
ORDER BY id DESC
LIMIT 20;
```

KIS lineage:

```sql
SELECT so.id AS source_object_id,
       so.storage_key,
       rl.target_table,
       COUNT(*) AS lineage_count
FROM source_object so
JOIN record_lineage rl ON rl.source_object_id = so.id
WHERE so.provider = 'KIS'
GROUP BY so.id, so.storage_key, rl.target_table
ORDER BY so.id DESC
LIMIT 20;
```

consumer 처리 이력:

```sql
SELECT consumer_name, idempotency_key, status, topic,
       partition_no, offset_no, error_message, processed_at
FROM processed_event
ORDER BY processed_at DESC
LIMIT 20;
```

## 11. KIS Split PoC 검증 결과

검증 일자: 2026-07-03

검증 대상:

- Provider: `KIS`
- Asset: `NVDA`
- Asset ID: `2872`
- Job ID: `22`
- Job Item ID: `28`
- Source Object ID: `8755`
- Outbox ID: `14`

### 11.1 Collector-only 검증

`workers.kis_price_collector`를 실행해 KIS API 호출, raw 저장, `source_object` 저장, Go controller raw-ready API 호출까지 검증했다.

생성된 `source_object`:

```text
id = 8755
provider = KIS
data_type = PRICE_BAR
external_id = 2872-NVDA
pipeline_job_item_id = 28
storage_backend = LOCAL
storage_key = kis_price_collector/job-22/item-28/raw/kis/price_bar/2872-NVDA-ea7b2b7068500f01.json
processing_status = PROCESSED
```

`processing_status`는 collector 직후에는 `COLLECTED`였고, ETL worker 처리 이후 `PROCESSED`로 변경되었다.

### 11.2 RAW_OBJECT_READY Outbox 검증

Go controller raw-ready API 호출 결과 `pipeline_outbox`에 `RAW_OBJECT_READY` 이벤트가 생성되었다.

```text
id = 14
source_object_id = 8755
job_item_id = 28
event_type = RAW_OBJECT_READY
topic = trader.raw.kis.ready
message_key = raw:8755
idempotency_key = RAW_OBJECT_READY:8755
```

초기에는 `trader.raw.kis.ready` topic이 없어 발행이 실패했고, outbox 상태가 `RETRY_WAIT`가 되었다.

```text
last_error = Unknown Topic Or Partition
```

로컬 Kafka topic 생성 후 outbox relay가 재시도하여 발행에 성공했다.

```text
status = PUBLISHED
kafka_partition = 2
kafka_offset = 0
published_at = 2026-07-03 15:34:37.810005+09
```

이 검증으로 Kafka topic 장애 또는 미생성 상황에서도 이벤트가 DB outbox에 남고, broker 준비 후 재발행될 수 있음을 확인했다.

### 11.3 ETL-only 검증

`workers.kis_price_etl_worker`를 실행해 `trader.raw.kis.ready` 메시지를 소비하고 raw 파일을 정규화 적재했다.

`processed_event` 결과:

```text
consumer_name = kis-price-etl-worker
idempotency_key = RAW_OBJECT_READY:8755
status = SUCCESS
topic = trader.raw.kis.ready
partition_no = 2
offset_no = 0
processed_at = 2026-07-03 15:45:36.4743+09
```

ETL 성공 후 `source_object.processing_status`는 `PROCESSED`로 변경되었다.

### 11.4 Lineage 검증

`source_object 8755`에서 `price_bar`로 이어지는 `record_lineage`가 4건 생성되었다.

```text
source_object_id = 8755
target_table = price_bar
lineage_count = 4
transformer_name = kis_price_worker
transformer_version = v1
processed_at = 2026-07-03 15:45:36.169865+09
```

확인된 `record_lineage.id`:

```text
2599484
2599485
2599486
2599487
```

이 결과로 raw 원본 파일, `source_object`, `price_bar`, `record_lineage`가 하나의 추적 흐름으로 연결되는 것을 확인했다.

### 11.5 최종 판단

KIS 기준 split pipeline PoC는 성공했다.

검증된 흐름:

```text
KIS collector-only
-> raw 파일 저장
-> source_object 생성
-> Go controller raw-ready API 호출
-> pipeline_outbox RAW_OBJECT_READY 생성
-> Kafka trader.raw.kis.ready 발행
-> KIS etl-only worker 소비
-> price_bar 적재
-> record_lineage 생성
-> source_object PROCESSED
-> processed_event SUCCESS
```

이 구조는 BLS, SEC, News에도 같은 방식으로 확장할 수 있다.

- BLS: `trader.raw.bls.ready`
- SEC: `trader.raw.sec.ready`
- News: `trader.raw.news.ready`

운영 관점에서 얻은 검증 포인트:

- Kafka topic 미생성 장애가 발생해도 outbox가 이벤트를 보존한다.
- topic 생성 후 relay 재시도로 Kafka 발행이 복구된다.
- ETL worker는 Kafka offset과 `processed_event`를 함께 남긴다.
- raw 원본과 정규화 결과는 `record_lineage`로 추적된다.
- collector 성공과 ETL 성공을 `pipeline_job_item.status`와 `source_object.processing_status`로 분리해 볼 수 있다.
## 12. BLS/SEC Split Worker 추가 구현

추가 일자: 2026-07-03

KIS에서 검증한 `collector-only -> RAW_OBJECT_READY -> etl-only` 구조를 BLS와 SEC에도 확장했다.

### 12.1 BLS

추가 파일:

```text
trader-data/workers/bls_macro_collector.py
trader-data/workers/bls_macro_etl_worker.py
```

BLS collector 흐름:

```text
pipeline_job_item(BLS:series_id)
-> BLS API timeseries 호출
-> raw JSON 저장
-> source_object(COLLECTED) 저장
-> Go controller raw-ready API 호출
-> pipeline_outbox(RAW_OBJECT_READY, topic=trader.raw.bls.ready)
-> pipeline_job_item(COLLECTED)
```

BLS ETL 흐름:

```text
trader.raw.bls.ready 소비
-> source_object 조회
-> raw TIMESERIES JSON 읽기
-> macro_series / macro_observation / macro_observation_vintage 변환
-> DB upsert
-> record_lineage 저장
-> source_object(PROCESSED)
-> processed_event(SUCCESS)
```

### 12.2 SEC

추가 파일:

```text
trader-data/workers/sec_financial_collector.py
trader-data/workers/sec_financial_etl_worker.py
```

SEC collector는 `SUBMISSIONS`, `SUBMISSIONS_HISTORY`, `COMPANY_FACTS` raw를 모두 저장하되 대표 source_object 하나만 `RAW_OBJECT_READY`로 보고한다. ETL worker는 대표 source_object가 속한 `pipeline_job_item_id`의 SEC raw 묶음을 읽어 `filing`, `financial_fact`, `record_lineage`를 함께 적재한다.

검증 명령:

```powershell
python -m workers.bls_macro_collector
python -m workers.bls_macro_etl_worker --brokers 127.0.0.1:9092 --max-records 1
python -m workers.sec_financial_collector
python -m workers.sec_financial_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

## 13. BLS/SEC Split Pipeline 검증 결과

검증 일자: 2026-07-03

KIS에 이어 BLS와 SEC도 `collector-only -> source_object -> pipeline_outbox -> Kafka -> etl-only -> record_lineage` 흐름을 실제로 검증했다.

### 13.1 BLS 검증 결과

검증 대상:

```text
Job ID = 23
Job Item ID = 29
Item Key = BLS:CUSR0000SA0
Source Object ID = 8756
Topic = trader.raw.bls.ready
Consumer = bls-macro-etl-worker
```

확인된 상태:

```text
source_object.provider = BLS
source_object.data_type = TIMESERIES
source_object.external_id = 2024-2026-CUSR0000SA0
source_object.pipeline_job_item_id = 29
source_object.processing_status = PROCESSED
processed_event.status = SUCCESS
processed_event.idempotency_key = RAW_OBJECT_READY:8756
```

Lineage 결과:

| source_object_id | target_table | lineage_count |
| --- | ---: | ---: |
| 8756 | macro_series | 1 |
| 8756 | macro_observation | 28 |
| 8756 | macro_observation_vintage | 28 |
| 8756 | macro_observation_status | 1 |

해석:

- BLS collector가 raw TIMESERIES 파일을 저장하고 `source_object`를 생성했다.
- Go controller raw-ready API를 통해 `RAW_OBJECT_READY` 이벤트가 생성됐다.
- Kafka `trader.raw.bls.ready` topic을 통해 BLS ETL worker가 이벤트를 소비했다.
- ETL worker가 raw 파일을 다시 읽어 `macro_series`, `macro_observation`, `macro_observation_vintage`, `macro_observation_status`를 적재했다.
- 결과 row와 원본 raw가 `record_lineage`로 연결됐다.
- 숫자로 제공되지 않은 기간 또는 아직 제공되지 않은 값은 `macro_observation_status`로 분리되어 수집 가능/불가능 상태를 설명할 수 있다.

최종 판단:

```text
BLS split pipeline = 검증 성공
```

### 13.2 SEC 검증 결과

검증 대상:

```text
Job ID = 24
Job Item ID = 30
Item Key = SEC:AAPL
Topic = trader.raw.sec.ready
Consumer = sec-financial-etl-worker
Outbox ID = 18
Representative Source Object ID = 4915
```

Outbox 발행 결과:

```text
pipeline_outbox.id = 18
source_object_id = 4915
job_item_id = 30
event_type = RAW_OBJECT_READY
topic = trader.raw.sec.ready
status = PUBLISHED
kafka_partition = 0
kafka_offset = 0
published_at = 2026-07-03 16:24:44.120725+09
```

Consumer 처리 결과:

```text
consumer_name = sec-financial-etl-worker
idempotency_key = RAW_OBJECT_READY:4915
status = SUCCESS
topic = trader.raw.sec.ready
partition_no = 0
offset_no = 0
processed_at = 2026-07-03 16:27:28.347929+09
```

SEC raw 객체:

| source_object_id | data_type | pipeline_job_item_id | processing_status | storage_key |
| ---: | --- | ---: | --- | --- |
| 459 | SUBMISSIONS | 30 | PROCESSED | `sec_financial_collector/job-24/item-30/raw/sec/submissions/0000320193-49300740ab29cbe0.json` |
| 4915 | COMPANY_FACTS | 30 | PROCESSED | `sec_financial_collector/job-24/item-30/raw/sec/company_facts/0000320193-b8dc854f48abdea4.json` |

Lineage 결과:

| source_object_id | data_type | target_table | lineage_count |
| ---: | --- | --- | ---: |
| 459 | SUBMISSIONS | company | 1 |
| 459 | SUBMISSIONS | filing | 149 |
| 4915 | COMPANY_FACTS | financial_fact | 24,852 |

해석:

- SEC collector가 `SUBMISSIONS`와 `COMPANY_FACTS` raw를 저장했다.
- SEC는 한 item에서 여러 raw 파일이 생기므로 `COMPANY_FACTS`를 대표 source_object로 raw-ready 이벤트를 발행했다.
- SEC ETL worker는 대표 source_object가 속한 `pipeline_job_item_id = 30`을 기준으로 같은 item의 raw 묶음을 읽었다.
- `SUBMISSIONS` raw는 `company`, `filing`으로 변환됐다.
- `COMPANY_FACTS` raw는 `financial_fact`로 변환됐다.
- 각 결과 row는 `record_lineage`로 원본 source_object까지 역추적 가능하다.

주의할 점:

- `source_object.collected_at`은 기존 dedupe row 재사용 때문에 과거 수집 시각이 남을 수 있다.
- 이번 검증에서는 `pipeline_job_item_id`, `storage_key`, `processing_status`, `record_lineage`가 이번 item 기준으로 연결됐으므로 추적성은 확보됐다.
- 향후 운영 문서에서는 `collected_at`은 최초 원본 수집 시각, `pipeline_job_item_id`는 최근 처리 job 연결값으로 설명하거나, 필요하면 별도 `updated_at`/`last_seen_at` 성격의 컬럼을 추가할 수 있다.

최종 판단:

```text
SEC split pipeline = 검증 성공
```

### 13.3 현재 검증 완료 범위

| Source | Collector | Raw 저장 | Outbox | Kafka publish | ETL worker | DB 적재 | Lineage | processed_event |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| KIS | 완료 | 완료 | 완료 | 완료 | 완료 | `price_bar` | 완료 | SUCCESS |
| BLS | 완료 | 완료 | 완료 | 완료 | 완료 | `macro_*` | 완료 | SUCCESS |
| SEC | 완료 | 완료 | 완료 | 완료 | 완료 | `company`, `filing`, `financial_fact` | 완료 | SUCCESS |

검증된 운영 흐름:

```text
collector-only
-> raw storage
-> source_object(COLLECTED)
-> Go controller raw-ready API
-> pipeline_outbox(RAW_OBJECT_READY)
-> Kafka source별 raw topic
-> etl-only worker
-> 정규화 테이블 upsert
-> record_lineage
-> source_object(PROCESSED)
-> processed_event(SUCCESS)
```

이제 `trader-data`의 KIS, BLS, SEC는 모두 같은 운영 모델로 설명할 수 있다. 남은 확장 대상은 News raw collector와 news ETL worker다.
