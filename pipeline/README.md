# Trader 데이터 파이프라인 실행 경계

이 폴더는 BLS, SEC, KIS Worker가 공통으로 사용하는 파이프라인 단계 이름과 수집/ETL 실행 경계를 정의한다.

현재 Worker는 Combined와 Split 두 가지 실행 방식을 제공한다.

Combined Worker는 한 프로세스에서 다음 단계를 연속으로 처리한다.

```text
pipeline_job_item
  -> collector API call
  -> raw/source preview 저장
  -> transform/validate
  -> PostgreSQL upsert
  -> item terminal status 기록
```

이 모드는 `COMBINED_COLLECT_TRANSFORM_LOAD`다. 간단한 기능 검증과 수동 운영에 사용할 수 있다.

## 수집과 ETL 분리 구조

Split Worker는 코드 구조를 공유하면서 수집과 ETL의 실행 책임을 나눈다.

```text
Collector worker
  -> 외부 API 호출
  -> source_object / S3 또는 local raw object 저장
  -> RAW_OBJECT_READY event 발행

ETL worker
  -> RAW_OBJECT_READY event 소비
  -> transform
  -> PostgreSQL upsert
  -> record_lineage 기록
```

Kafka는 원본 데이터 저장소가 아니다. Kafka에는 큰 SEC companyfacts JSON이나 KIS 응답 전체를 넣지 않고, 다음과 같은 작은 참조값만 넣는다.

```json
{
  "event_type": "RAW_OBJECT_READY",
  "job_item_id": 123,
  "source_object_id": 456,
  "object_uri": "s3://trader-raw/sec/0001045810/companyfacts.json",
  "idempotency_key": "RAW_OBJECT_READY:456"
}
```

원본 payload는 S3 또는 로컬 저장소에 먼저 보관하고, ETL Worker는 해당 위치를 읽어 DB 작업을 수행한다.

## 왜 두 실행 방식을 함께 유지하는가

Combined 방식은 Provider별 수집과 변환을 한 프로세스에서 빠르게 점검하는 기준으로 사용한다. Split 방식은 원본 보존, Kafka 이벤트, ETL 재처리, lag 기반 실행 제어가 필요한 운영형 흐름을 검증하는 데 사용한다.

- 작은 범위의 문제는 Combined 방식에서 외부 API부터 DB 적재까지 빠르게 추적할 수 있다.
- Split 방식에서는 수집에 성공한 원본을 다시 호출하지 않고 ETL만 재실행할 수 있다.
- Source별 수집량과 DB 적재 시간을 관찰해 Kafka lag 임계값과 batch 크기를 조정할 수 있다.
- Kafka를 사용할 수 없는 로컬 환경에서는 Combined Worker나 특정 `source_object_id` 직접 실행 경로를 사용할 수 있다.

두 방식은 공통 단계와 checkpoint를 사용한다.

```text
RESOLVE_TARGET -> COLLECT_RAW -> WRITE_RAW_PREVIEW -> TRANSFORM -> LOAD_DB
```

같은 단계 이름을 사용하므로 Combined와 `COLLECT_ONLY`, `ETL_ONLY` 실행 결과를 동일한 기준으로 추적할 수 있다.

## 폴더별 책임

| 폴더 | 책임 |
|---|---|
| `collectors/` | 외부 API Client 호출과 Provider 원본 payload 수집 |
| `transformers/` | Provider payload를 정규화 레코드로 변환 |
| `backfills/` | 과거 데이터 이관, 일회성 대량 작업 |
| `workers/` | Job Item 선점, 단계 실행 조정, Job 상태 갱신 |
| `pipeline/` | 공통 단계 이름과 실행 metadata |

Go Controller는 Kafka lag, Worker 상태 신호, 처리 가능 시간대 정책을 기준으로 Python Worker 실행 여부와 AWS ASG 용량을 판단한다.
