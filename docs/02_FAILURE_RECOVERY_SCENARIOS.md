# 장애 대응 및 복구 시나리오

> 이 문서는 Trader 데이터 파이프라인에서 Kafka, worker, 외부 API, ETL 처리 중단이 발생했을 때 어떤 상태가 남고 어떻게 복구되는지 정리한다.

---

## Summary

### 목표

- 장애 발생 시 데이터 유실 여부를 판단할 수 있어야 한다.
- 자동 복구 가능한 상황과 관리자 개입이 필요한 상황을 구분한다.
- Kafka 메시지 재전달, DB outbox, source_object, processed_event의 역할을 분리한다.
- 면접/운영 문서에서 "어디까지 검증했고, 어디부터 확장안인지"를 명확히 한다.

### 핵심 원칙

| 원칙 | 설명 |
| --- | --- |
| Kafka publish 전 DB outbox 저장 | Kafka가 죽어도 이벤트 요청은 DB에 남는다. |
| DB 처리 완료 후 Kafka offset commit | worker가 중간에 죽으면 미커밋 메시지가 재전달된다. |
| processed_event로 중복 방지 | 같은 idempotency_key는 중복 처리하지 않는다. |
| source_object + lineage로 완료 판단 | processed_event가 없어도 raw가 이미 정규화되었는지 판단할 수 있다. |
| raw 원본 보존 | ETL 오류나 invalid 데이터의 원인을 원본에서 재검증할 수 있다. |

---

## 1. 상태 모델

### 1.1 pipeline_outbox.status

| 상태 | 의미 | 복구 방식 |
| --- | --- | --- |
| `PENDING` | Kafka 발행 대기 | relay가 publish 시도 |
| `PUBLISHING` | relay가 선점 | 오래 지속되면 relay 중단 여부 확인 |
| `PUBLISHED` | Kafka partition/offset 기록 완료 | 정상 |
| `RETRY_WAIT` | publish 실패 후 backoff 대기 | next_attempt_at 이후 재시도 |
| `DEAD` | 최대 재시도 초과 | 관리자 확인 후 retry |

### 1.2 source_object.processing_status

| 상태 | 의미 | 복구 방식 |
| --- | --- | --- |
| `COLLECTED` | raw 저장 완료, ETL 대기 | RAW_OBJECT_READY 또는 수동 retry |
| `PROCESSING` | ETL worker가 처리 중 | Kafka 미커밋이면 재전달 가능 |
| `PROCESSED` | 정규화 및 lineage 완료 | 재처리 생략 가능 |
| `FAILED` | ETL 실패 | 관리자 retry 또는 정책 기반 재처리 |

### 1.3 processed_event.status

| 상태 | 의미 |
| --- | --- |
| `SUCCESS` | consumer가 메시지를 성공 처리하고 DB에 기록 |
| `FAILED` | consumer 처리 실패. error_message 확인 필요 |
| `SKIPPED` | 중복 또는 처리 대상 없음 |

---

## 2. 장애 시나리오 요약표

| 시나리오 | 남는 상태 | 자동 복구 | 관리자 확인 |
| --- | --- | --- | --- |
| Kafka down 중 job 생성 | `pipeline_outbox=PENDING/RETRY_WAIT` | Kafka 복구 후 relay 재발행 | outbox last_error |
| outbox relay down | `pipeline_outbox=PENDING` 증가 | relay 재기동 후 처리 | relay heartbeat/log |
| Python worker down before DB commit | Kafka offset 미커밋, `source_object=PROCESSING` 가능 | consumer group 재전달 | stale PROCESSING |
| DB commit 완료, Kafka commit 전 down | `source_object=PROCESSED`, lineage 있음, processed_event 없을 수 있음 | 재수신 시 ETL skip 후 processed_event 보강 | Raw Data 상세 |
| 외부 API 실패 | `pipeline_job_item=FAILED` | source별 retry 정책 | error_code/message |
| raw 저장 완료, ETL 미처리 | `source_object=COLLECTED` | RAW_OBJECT_READY 재발행 또는 ETL worker 처리 | Raw Data 미처리 |
| invalid price data | `price_bar_quality_issue` 또는 quality status | 보조 공급원/재조회 | issue report |
| Kafka lag 증가 | lag snapshot 증가 | worker scale-out policy | Worker Control |

---

## 3. Kafka down 시나리오

### 흐름

```text
Go controller
-> pipeline_outbox PENDING
-> outbox relay Kafka publish 실패
-> RETRY_WAIT
-> Kafka 복구
-> relay 재시도
-> PUBLISHED + partition/offset 기록
```

### 확인 SQL

```sql
SELECT id, event_type, topic, status, attempt_count,
       next_attempt_at, kafka_partition, kafka_offset,
       last_error, created_at, published_at
FROM pipeline_outbox
ORDER BY id DESC
LIMIT 20;
```

### 검증 근거 작성 방식

이미지 대신 상태 전이를 텍스트로 남긴다.

```text
Kafka down:
pipeline_outbox.status=RETRY_WAIT
pipeline_outbox.last_error=Unknown Topic Or Partition 또는 connection error

Kafka recovery:
pipeline_outbox.status=PUBLISHED
pipeline_outbox.kafka_partition=...
pipeline_outbox.kafka_offset=...
```

핵심 방어 코드는 다음 부분만 짧게 첨부한다.

```text
PENDING/RETRY_WAIT -> PUBLISHING 선점
publish 성공 -> PUBLISHED + partition/offset 저장
publish 실패 -> RETRY_WAIT + next_attempt_at 저장
```

---

## 4. ETL worker down 시나리오

### 핵심 판단

ETL worker가 죽어도 Kafka offset을 commit하지 않았다면 메시지는 다시 전달된다.

```text
Kafka message 수신
-> source_object PROCESSING
-> raw 읽기
-> 정규화 테이블 적재
-> source_object PROCESSED
-> processed_event SUCCESS
-> DB commit
-> Kafka offset commit
```

위 순서에서 Kafka commit을 마지막에 둔다.

### 경우별 결과

| 죽은 위치 | 남는 상태 | 다음 처리 |
| --- | --- | --- |
| raw 읽기 전 | offset 미커밋 | 다른 worker가 다시 처리 |
| DB 적재 중 | 트랜잭션 rollback 가능 | 다시 처리 |
| DB commit 후 processed_event 전 | source_object/lineage 확인 | ETL skip 후 processed_event 보강 |
| processed_event 기록 후 Kafka commit 전 | processed_event 중복 확인 | 바로 Kafka commit |

---

## 5. source_object PROCESSED + lineage skip

이미 ETL 결과가 DB에 들어갔는데 Kafka commit만 실패한 경우, 같은 메시지가 다시 들어올 수 있다.

이때 다시 ETL을 수행하지 않고 다음 조건으로 완료 처리한다.

```text
source_object.processing_status = PROCESSED
record_lineage 존재
```

그 결과:

- 정규화 테이블 중복 upsert 비용을 줄인다.
- SEC처럼 큰 payload의 재처리를 피한다.
- processed_event SUCCESS를 보강 기록하고 Kafka offset을 commit할 수 있다.

---

## 6. stale PROCESSING 관측

`PROCESSING`은 강한 DB lock이 아니라 운영 관측 상태다. consumer group과 Kafka offset이 실제 동시 처리를 제어하고, DB는 진행 상태를 보여준다.

관리자 페이지 Raw Data 탭에서 다음 항목을 본다.

- PROCESSING 상태 raw 수
- 30분 이상 처리 중인 raw 수
- processing_started_at
- processing_attempt_count
- job item id

현재 정책:

```text
자동으로 stale PROCESSING을 재처리하지 않는다.
기본 복구는 Kafka 미커밋 offset 재전달에 맡긴다.
관리자 페이지에서 관측 후 필요 시 retry 정책을 적용한다.
```

---

## 7. 관리자 페이지 확인 위치

| 확인 항목 | 탭 |
| --- | --- |
| outbox PENDING/RETRY_WAIT/DEAD | Kafka Pipeline |
| topic별 lag | Kafka Pipeline |
| raw 처리 상태 | Raw Data |
| raw 상세 흐름 | Raw Data -> 상세 drawer |
| worker heartbeat | Worker Control |
| scale-out command | Worker Control |
| job/item 실패 | Pipeline Jobs |

---

## 8. 검증 체크리스트

검증 결과는 화면 캡처보다 SQL/로그 요약을 우선한다.

| 검증 | 확인 상태 | 남길 증거 |
| --- | --- | --- |
| Kafka down 중 outbox 적재 | `PENDING` 또는 `RETRY_WAIT` 유지 | `pipeline_outbox` SQL 3~5줄 |
| Kafka 복구 후 outbox relay publish | `PUBLISHED`, partition/offset 기록 | `pipeline_outbox` SQL 3~5줄 |
| BLS raw lag 증가 후 worker scale-out | lag threshold 이상, scale command 생성 | `kafka_consumer_lag_snapshot`, `pipeline_worker_scale_command` SQL |
| worker down 후 미처리 raw 재처리 | offset 미커밋 메시지 재전달 | Python worker 로그 5줄 + `processed_event` SQL |
| processed_event 중복 처리 | 동일 idempotency key 재처리 방지 | `processed_event` SQL + 중복 skip 코드 스니펫 |
| source_object PROCESSED + lineage skip | ETL 재실행 생략, SUCCESS 보강 | `source_object`, `record_lineage`, `processed_event` SQL |

문서에 붙일 코드는 전체 구현이 아니라 방어 판단이 드러나는 부분만 남긴다.

```text
if processed_event SUCCESS exists:
    skip processing
    commit kafka offset

if source_object PROCESSED and lineage exists:
    skip ETL
    insert processed_event SUCCESS
    commit kafka offset
```

---

## 9. 문서에 첨부할 방어 코드 후보

전체 파일을 붙이지 말고, 아래 파일에서 핵심 분기 10~25줄만 발췌한다.

| 방어 주제 | 파일 | 발췌할 핵심 |
| --- | --- | --- |
| Outbox 선점과 재시도 | `trader-controller/internal/outbox/repository.go` | `PENDING/RETRY_WAIT -> PUBLISHING`, stale publishing 복구 |
| Kafka publish 결과 기록 | `trader-controller/internal/outbox/relay.go` | publish 성공 후 partition/offset 저장, 실패 시 retry 상태 전환 |
| Job consumer 중복 처리 | `trader-data/workers/kafka_pipeline_consumer.py` | `processed_event` 조회 후 중복 메시지 skip |
| Raw ETL 중복 처리 | `trader-data/workers/kis_price_etl_worker.py`, `bls_macro_etl_worker.py`, `sec_financial_etl_worker.py` | `source_object_completed_with_lineage`이면 ETL 생략 |
| Raw 상태와 lineage 조회 | `trader-controller/internal/postgres/source_object.go` | raw, outbox, processed_event, lineage를 한 화면에 보여주는 LEFT JOIN |
| Worker scale command 선점 | `trader-controller/internal/postgres/worker_scaling.go` | actuator가 처리할 command 하나를 선점하고 결과 기록 |
| Local/AWS actuator | `trader-controller/internal/worker/local_docker_actuator.go`, `aws_asg_actuator.go` | 같은 scale command를 로컬 Docker 또는 AWS ASG로 실행 |
| Admin session 보호 | `trader-controller/internal/httpapi/server.go`, `internal/postgres/admin_session.go` | 브라우저는 HttpOnly cookie, worker는 내부 API key로 분리 |

코드 스니펫은 구현 세부보다 "왜 이 분기가 장애를 막는지"를 설명하는 주석과 함께 넣는다.

---

## 10. AWS 검증 사례: Kafka 재시작 후 outbox 재발행 복구 - 2026-07-08

AWS 검증 중 Kafka broker 설정을 수정하고 재시작하면서 DB의 outbox 상태와 Kafka topic 상태가 일시적으로 어긋나는 상황이 발생했다.

### 10.1 발생 상태

`pipeline_job_item.id=39`는 아직 실행되지 않은 상태였다.

```text
pipeline_job_item:
id=39
item_key=BLS:CES0000000001
status=QUEUED
started_at=NULL
completed_at=NULL
```

DB outbox에는 이미 Kafka 발행 성공으로 기록되어 있었다.

```text
pipeline_outbox:
id=30
job_item_id=39
event_type=JOB_ITEM_QUEUED
topic=trader.jobs.events
status=PUBLISHED
kafka_partition=0
kafka_offset=0
published_at=2026-07-08 08:52:43+09
```

하지만 consumer 처리 기록은 없었다.

```text
processed_event:
idempotency_key=JOB_ITEM_QUEUED:39
rows=0
```

Kafka lag snapshot에서는 topic에 남은 메시지가 없는 것으로 보였다.

```text
topic=trader.jobs.events
consumer_group=trader-data-workers
total_lag=0
commitOffsets={"0":0}
latestOffsets={"0":0}
```

이 조합은 `DB outbox는 PUBLISHED지만 Kafka topic에는 소비할 메시지가 없고 worker 처리 기록도 없는 상태`를 의미한다.

### 10.2 원인

단일 Kafka broker에서 consumer group 내부 topic 설정이 부족해 `coordinators=0` 상태가 발생했다. 이후 Kafka 설정을 수정하고 broker를 재시작하는 과정에서 기존 Kafka topic 상태와 DB outbox 상태가 어긋났다.

추가한 단일 broker 설정:

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

정상화 후 broker 로그:

```text
Coordinator for GROUP/trader-data-workers
Discovered coordinator coordinator-1
Stabilized group trader-data-workers generation 2
Assignment received from leader
```

### 10.3 복구 방법

DB outbox를 source of truth로 보고, 해당 outbox row를 다시 publish 대상 상태로 되돌렸다.

```sql
UPDATE pipeline_outbox
SET status = 'PENDING',
    attempt_count = 0,
    next_attempt_at = now(),
    kafka_partition = NULL,
    kafka_offset = NULL,
    published_at = NULL,
    last_error = NULL,
    updated_at = now()
WHERE job_item_id = 39
  AND event_type = 'JOB_ITEM_QUEUED';
```

Go outbox relay가 다시 Kafka에 발행했고, Python job worker가 메시지를 소비했다.

```text
kafka message committed topic=trader.jobs.events partition=0 offset=0 result=success
```

### 10.4 복구 결과

```text
pipeline_job_item:
id=39
status=COLLECTED
started_at=2026-07-08 09:04:28+09
completed_at=2026-07-08 09:04:29+09
```

```text
source_object:
id=8775
provider=BLS
data_type=TIMESERIES
processing_status=PROCESSED
processing_attempt_count=1
processing_started_at=2026-07-08 09:04:33+09
processing_finished_at=2026-07-08 09:04:33+09
```

```text
pipeline_outbox:
JOB_ITEM_QUEUED -> PUBLISHED, topic=trader.jobs.events, partition=0, offset=0
RAW_OBJECT_READY -> PUBLISHED, topic=trader.raw.bls.ready, partition=0, offset=0
```

### 10.5 운영 판단

이 사례는 Kafka를 source of truth로 보지 않고 DB outbox를 기준으로 복구해야 하는 이유를 보여준다.

- Kafka 메시지가 유실되거나 topic 상태가 초기화되어도 DB outbox에는 발행 의도가 남는다.
- `pipeline_job_item`, `pipeline_outbox`, `processed_event`, `kafka_consumer_lag_snapshot`을 함께 보면 멈춘 구간을 식별할 수 있다.
- 처리 기록이 없는 `PUBLISHED` outbox는 운영자가 `PENDING`으로 되돌려 재발행할 수 있다.
- 재발행 후 worker는 idempotency key와 job item 상태를 기준으로 중복 처리를 방어한다.

---

## 11. AWS 검증 사례: raw lag 누적 후 ETL worker 재개 - 2026-07-08

BLS ETL worker를 중지한 뒤 BLS raw job 2건을 생성해 raw topic lag가 쌓이고, worker를 다시 실행했을 때 밀린 raw가 처리되는지 검증했다.

### 11.1 발생 상태

BLS ETL worker가 중지된 동안 raw 수집은 완료되었고, raw topic lag가 증가했다.

```text
topic=trader.raw.bls.ready
consumer_group=trader-data-bls-etl
total_lag=2
commitOffsets={"0":1}
latestOffsets={"0":3}
partitionLags={"0":2}
```

이때 raw 객체는 ETL 대기 상태였다.

```text
source_object 8777:
provider=BLS
data_type=TIMESERIES
processing_status=COLLECTED
pipeline_job_item_id=40
processing_started_at=NULL
processing_finished_at=NULL

source_object 8778:
provider=BLS
data_type=TIMESERIES
processing_status=COLLECTED
pipeline_job_item_id=41
processing_started_at=NULL
processing_finished_at=NULL
```

### 11.2 복구 방법

Python worker node 안의 BLS ETL worker 컨테이너를 다시 실행했다.

```bash
cd /data/python-worker
docker compose start trader-data-bls-etl-worker
```

이 검증은 ASG scale-out 검증이 아니라, 이미 존재하는 worker node에서 ETL worker가 재개되었을 때 Kafka에 쌓인 raw 메시지를 이어 처리하는지 확인한 것이다.

### 11.3 복구 결과

Kafka lag는 2에서 0으로 감소했다.

```text
topic=trader.raw.bls.ready
consumer_group=trader-data-bls-etl
total_lag=0
commitOffsets={"0":3}
latestOffsets={"0":3}
partitionLags={"0":0}
```

raw 객체는 `PROCESSED`로 전환되었다.

```text
source_object 8777:
processing_status=PROCESSED
processing_started_at=2026-07-08 09:22:48+09
processing_finished_at=2026-07-08 09:22:48+09
processing_attempt_count=1

source_object 8778:
processing_status=PROCESSED
processing_started_at=2026-07-08 09:22:48+09
processing_finished_at=2026-07-08 09:22:48+09
processing_attempt_count=1
```

worker 로그:

```text
bls-macro-etl-worker macro_observation rows=6
bls-macro-etl-worker macro_observation_vintage rows=6
bls-macro-etl-worker record_lineage rows=13
BLS raw message committed topic=trader.raw.bls.ready partition=0 offset=2 result=success
```

### 11.4 운영 판단

- raw 수집과 ETL이 분리되어 있으므로 ETL worker가 중지되어도 raw는 `source_object=COLLECTED` 상태로 남는다.
- Kafka raw topic lag는 미처리 raw ETL 작업량을 보여준다.
- worker가 재개되면 Kafka offset 기준으로 미처리 raw를 이어 처리한다.
- ETL 완료 여부는 `source_object.processing_status=PROCESSED`, `processing_finished_at`, `record_lineage`, `processed_event`로 확인한다.
- ASG actuator는 worker node 단위 복구/확장을 담당하고, 개별 컨테이너 재시작은 Docker restart policy 또는 node 내부 운영 정책의 영역으로 분리한다.
