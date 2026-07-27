# trader-data pipeline boundary

이 폴더는 BLS, SEC, KIS worker가 공통으로 사용하는 pipeline stage 이름과 향후 수집/ETL 분리 기준을 둔다.

현재 worker MVP는 보수적으로 동작한다.

```text
pipeline_job_item
  -> collector API call
  -> raw/source preview 저장
  -> transform/validate
  -> PostgreSQL upsert
  -> item terminal status 기록
```

이 모드는 `COMBINED_COLLECT_TRANSFORM_LOAD`다. 한 프로세스가 수집, raw 저장, 변환, DB 적재까지 처리한다.

## 다음 분리 구조

다음 단계에서는 코드 구조는 최대한 유지하되 실행 책임을 나눈다.

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

원본 payload는 S3 또는 로컬 저장소에 먼저 보관하고, ETL worker는 해당 위치를 읽어서 DB 작업을 수행한다.

## 왜 지금 바로 완전 분리하지 않았나

처음부터 collector와 ETL worker를 완전히 나누면 구조는 더 멋있어 보이지만, 현재 단계에서는 오히려 디버깅 비용이 커진다.

- 스키마와 관리자 페이지가 아직 변하고 있다.
- source별 수집량, DB 적재 시간, 실패 유형을 더 측정해야 한다.
- Kafka outbox, consumer 중복 처리, processed_event 흐름을 먼저 안정화해야 한다.
- 수집/ETL을 분리하면 raw storage, Kafka, ETL consumer, DB loader까지 장애 지점이 늘어난다.

그래서 현재는 combined worker를 유지하되, checkpoint에 공통 stage를 남긴다.

```text
RESOLVE_TARGET -> COLLECT_RAW -> WRITE_RAW_PREVIEW -> TRANSFORM -> LOAD_DB
```

이 경계가 있으면 나중에 같은 코드를 `COLLECT_ONLY`, `ETL_ONLY` 모드로 나누기 쉽다.

## 폴더별 책임

| Folder | Responsibility |
|---|---|
| `collectors/` | 외부 API client와 raw provider payload 수집 |
| `transformers/` | provider payload를 canonical record로 변환 |
| `backfills/` | 과거 데이터 이관, 일회성 대량 작업 |
| `workers/` | job item claim, stage orchestration, job 상태 갱신 |
| `pipeline/` | 공통 stage 이름, runtime metadata, 향후 event helper |

Go controller는 나중에 ETL 실행 시점을 결정한다. 예를 들어 사용자 트래픽이 낮은 시간대, 장 마감 이후, Kafka lag가 기준치를 넘은 시점에 ETL worker를 실행하도록 만들 수 있다.
