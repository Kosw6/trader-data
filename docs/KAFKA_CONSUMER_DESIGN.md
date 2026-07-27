# Kafka Consumer Design

## 목적

`trader-controller`는 Job과 Job Item을 PostgreSQL에 저장하고 같은 트랜잭션에서 `pipeline_outbox` 이벤트를 남긴다. Kafka는 원본 저장소가 아니라 전달 로그다. 대용량 원문은 로컬 또는 S3에 저장하고, Kafka에는 `source_object.id`, storage key, hash 같은 참조 정보만 전달한다.

## Topic

| Topic | Consumer | 목적 |
| --- | --- | --- |
| `trader.jobs.events` | `workers.kafka_pipeline_consumer` | 기존 combined worker 실행 |
| `trader.raw.kis.ready` | `workers.kis_price_etl_worker` | KIS raw price ETL |
| `trader.raw.bls.ready` | `workers.bls_macro_etl_worker` | BLS raw macro ETL |
| `trader.raw.sec.ready` | `workers.sec_financial_etl_worker` | SEC raw financial ETL |
| `trader.raw.news.ready` | 추후 news ETL worker | 뉴스 raw ETL 확장 예정 |

토픽을 나눈 이유는 source별 지연 허용치와 처리 비용이 다르기 때문이다. KIS는 종목 수가 많아 lag 기준을 크게 잡을 수 있고, SEC는 companyfacts가 무거워 야간 배치나 낮은 동시성으로 처리하는 편이 좋다. BLS는 데이터량은 작지만 발표 시각과 누락 상태를 운영 지표로 분리해서 보기 좋다.

## JOB_ITEM_QUEUED

```text
Controller API
-> pipeline_job / pipeline_job_item 생성
-> pipeline_outbox(JOB_ITEM_QUEUED, PENDING)
-> Go outbox relay가 trader.jobs.events로 발행
-> Python kafka_pipeline_consumer가 item_id 기준으로 QUEUED item 선점
-> source별 combined worker 호출
-> processed_event 기록
-> Kafka offset commit
```

실행:

```powershell
python -m workers.kafka_pipeline_consumer --brokers 127.0.0.1:9092 --max-records 1
```

## RAW_OBJECT_READY

```text
Python collector-only worker
-> 외부 API 호출
-> raw 파일 저장
-> source_object(COLLECTED) 저장
-> Go controller raw-ready API 호출

Go controller
-> source_object 조회
-> provider별 raw topic 결정
-> pipeline_outbox(RAW_OBJECT_READY, PENDING)

Go outbox relay
-> Kafka raw topic 발행

Python etl-only worker
-> processed_event 중복 확인
-> raw 파일 읽기
-> 정규화 테이블 적재
-> record_lineage 저장
-> source_object(PROCESSED/FAILED) 갱신
-> Kafka offset commit
```

실행:

```powershell
python -m workers.kis_price_etl_worker --brokers 127.0.0.1:9092 --max-records 1
python -m workers.bls_macro_etl_worker --brokers 127.0.0.1:9092 --max-records 1
python -m workers.sec_financial_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw만 직접 검증:

```powershell
python -m workers.kis_price_etl_worker --source-object-id 8755
python -m workers.bls_macro_etl_worker --source-object-id 8750
python -m workers.sec_financial_etl_worker --source-object-id 8752
```

## 중복 처리

Kafka는 같은 메시지를 다시 전달할 수 있으므로 consumer는 `processed_event`를 먼저 확인한다.

| 이벤트 | idempotency key |
| --- | --- |
| `JOB_ITEM_QUEUED` | `JOB_ITEM_QUEUED:{pipeline_job_item_id}` |
| `RAW_OBJECT_READY` | `RAW_OBJECT_READY:{source_object_id}` |

`processed_event`의 `(consumer_name, idempotency_key)`가 이미 존재하면 같은 이벤트는 다시 처리하지 않는다.

## 실패 정책

| 실패 지점 | 처리 |
| --- | --- |
| Kafka publish 실패 | `pipeline_outbox`가 `PENDING` 또는 `RETRY_WAIT`로 남고 relay가 재시도한다. |
| consumer 처리 중 죽음 | offset commit 전이면 Kafka가 다시 전달한다. |
| ETL 실패 | `source_object.processing_status = FAILED`로 남기고 raw 파일은 보존한다. |
| 중복 이벤트 | `processed_event` 기준으로 skip한다. |

## 필수 환경 변수

```text
DATABASE_URL
KAFKA_BROKERS
KAFKA_TOPIC
KAFKA_GROUP_ID
KAFKA_CONSUMER_NAME
KAFKA_RAW_KIS_TOPIC
KAFKA_RAW_KIS_GROUP_ID
KAFKA_RAW_KIS_CONSUMER_NAME
KAFKA_RAW_BLS_TOPIC
KAFKA_RAW_BLS_GROUP_ID
KAFKA_RAW_BLS_CONSUMER_NAME
KAFKA_RAW_SEC_TOPIC
KAFKA_RAW_SEC_GROUP_ID
KAFKA_RAW_SEC_CONSUMER_NAME
```

## Topic별 Lag Monitor

2026-07-03 기준으로 controller의 Kafka Health Checker와 Consumer Lag Monitor는 단일 topic이 아니라 다음 5개 topic을 순회하며 측정하도록 확장했다.

| Topic | Consumer group | 의미 |
| --- | --- | --- |
| `trader.jobs.events` | `trader-data-workers` | Job Item 실행 이벤트 |
| `trader.raw.kis.ready` | `trader-data-kis-etl` | KIS raw price ETL 대기 |
| `trader.raw.bls.ready` | `trader-data-bls-etl` | BLS raw macro ETL 대기 |
| `trader.raw.sec.ready` | `trader-data-sec-etl` | SEC raw financial ETL 대기 |
| `trader.raw.news.ready` | `trader-data-news-etl` | 뉴스 raw ETL 확장 예정 |

측정 결과는 `kafka_consumer_lag_snapshot`에 `topic + consumer_group` 기준으로 저장된다. 관리자 페이지의 Kafka Pipeline 탭은 최신 snapshot을 topic별 카드로 묶어 보여준다.

운영적으로는 source마다 지연 허용치와 처리 비용이 다르기 때문에 topic별 lag 분리가 중요하다. 예를 들어 KIS는 종목 수가 많아 일정량 lag를 허용한 뒤 batch ETL로 처리할 수 있고, SEC는 companyfacts가 무거워 낮은 동시성 또는 야간 ETL로 조절할 수 있다. BLS는 데이터량은 작지만 발표 후 수집 여부를 명확히 보여주는 데 의미가 있다.

## 검증 완료 상태

2026-07-03 실제 검증 결과:

| Source | Raw topic | Consumer | 결과 |
| --- | --- | --- | --- |
| KIS | `trader.raw.kis.ready` | `kis-price-etl-worker` | `price_bar` 적재, `record_lineage`, `processed_event SUCCESS` 확인 |
| BLS | `trader.raw.bls.ready` | `bls-macro-etl-worker` | `macro_series`, `macro_observation`, `macro_observation_vintage`, `macro_observation_status` 적재 확인 |
| SEC | `trader.raw.sec.ready` | `sec-financial-etl-worker` | `company`, `filing`, `financial_fact` 적재 및 lineage 확인 |

따라서 현재 Kafka pipeline은 Job 이벤트와 KIS/BLS/SEC raw ETL 이벤트를 모두 같은 outbox/consumer/idempotency 구조로 설명할 수 있다.
