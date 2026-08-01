# trader-data

Trader 서비스의 데이터 수집, 변환, 백필 작업을 관리하는 저장소다.

## 디렉터리 구조

| 디렉터리 | 책임 |
|---|---|
| `collectors/` | 외부 API 호출과 원본 payload 수집을 담당한다. |
| `transformers/` | provider payload를 우리 DB 스키마에 맞는 record로 변환한다. |
| `pipeline/` | 공통 stage 이름, checkpoint metadata, 향후 수집/ETL 분리 기준을 둔다. |
| `backfills/` | 과거 데이터 이관, 대량 백필, 관계 매칭처럼 idempotent하게 재실행 가능한 작업을 둔다. |
| `tests/` | transformer와 backfill 단위 테스트를 둔다. |
| `docs/` | 데이터 스키마와 파이프라인 설계 문서를 둔다. |
| `experiments/` | BLS, SEC 등 외부 API 탐색용 실험 코드를 둔다. |
| `benchmarks/` | 저장 방식, 조회 성능 등 일회성 실험 코드를 둔다. |
| `archive/` | 이전 MongoDB/KIS prototype 코드를 보관한다. |
| `pipeline_output/` | 로컬에서 생성된 raw/report 파일을 저장한다. Git에는 올리지 않는다. |

`.env`, `.env.example`, `.gitignore`, `requirements.txt`는 프로젝트 루트에 둔다.
모듈 경로를 일관되게 해석할 수 있도록 이 디렉터리에서 `python -m ...` 형식으로 실행한다.

## 수집과 ETL 책임 분리 방향

현재 SEC, KIS, BLS Worker는 Combined와 Split 두 가지 실행 방식을 제공한다.

Combined Worker는 `COMBINED_COLLECT_TRANSFORM_LOAD` 방식으로 한 프로세스에서 수집부터 DB 적재까지 수행한다.

```text
pipeline_job_item
  -> collector/client로 외부 API 호출
  -> pipeline_output/에 raw 또는 table preview 저장
  -> transform / validation
  -> PostgreSQL upsert
  -> pipeline_job_item 최종 상태 기록
```

Combined 방식은 간단한 기능 검증과 수동 운영에 사용한다. Worker checkpoint에는 공통 `pipeline` 객체를 남겨 BLS, SEC, KIS 처리 과정이 같은 단계 이름으로 보이도록 했다.

```text
RESOLVE_TARGET -> COLLECT_RAW -> WRITE_RAW_PREVIEW -> TRANSFORM -> LOAD_DB
```

운영형 검증에는 Python을 유지하면서 수집과 ETL의 실행 책임을 나눈 Split Worker를 사용한다.

```text
Collector worker
  -> 외부 API 호출
  -> source_object / S3 또는 local raw object 저장
  -> RAW_OBJECT_READY event 발행

ETL worker
  -> RAW_OBJECT_READY event 소비
  -> transform / validation
  -> database upsert
  -> record_lineage
```

Go Controller는 Kafka lag, Worker 상태 신호, 처리 가능 시간대 등의 정책을 기준으로 Python Worker 실행 여부를 판단한다. Kafka에는 큰 원본 payload를 넣지 않고 `source_object_id`, `object_uri`, `job_item_id` 같은 작은 참조값만 넣는다.

### 왜 Combined 방식도 함께 유지하는가

Split 구조를 구현한 뒤에도 Combined 방식을 함께 유지하는 이유는 다음과 같다.

1. **작은 범위의 기능 확인이 빠르다.**
   외부 API 호출부터 DB 적재까지 한 프로세스에서 확인할 수 있어 Provider별 수집과 변환 문제를 빠르게 좁힐 수 있다.

2. **Split 구조와 비교할 기준이 된다.**
   Collector, raw storage, Outbox, Kafka, ETL Consumer, DB Loader가 분리된 흐름과 한 프로세스 흐름을 같은 입력으로 비교할 수 있다.

3. **Kafka 없이도 수동 복구와 검증이 가능하다.**
   Broker를 사용할 수 없는 로컬 환경이나 단건 점검에서는 Combined Worker 또는 `--source-object-id` 직접 실행 경로를 사용할 수 있다.

4. **Provider별 처리량을 관찰할 수 있다.**
   KIS, SEC, BLS의 수집량과 DB 적재 시간을 비교해 source별 lag 임계값과 batch 크기를 조정할 수 있다.

따라서 현재 코드는 Combined Worker를 단건 검증 경로로 유지하고, Split Worker를 원본 보존, 재처리, lag 기반 실행 제어를 검증하는 경로로 함께 제공한다.

## 원본 추적 모델

BLS, SEC, KIS worker는 모두 다음 추적 모델을 목표로 한다.

```text
pipeline_job_item
  -> source_object
      -> record_lineage
          -> normalized table
```

- BLS: `source_object -> record_lineage -> macro_observation / macro_observation_vintage`
- SEC: `source_object -> record_lineage -> filing / financial_fact`
- KIS: `source_object -> record_lineage -> price_bar`

`source_object.pipeline_job_item_id`는 어떤 job item이 원본을 만들었는지 추적하기 위한 값이다. `record_lineage.target_record_key`는 DB PK가 달라져도 같은 결과 레코드를 다시 식별하기 위한 안정 키다.

## Asset-Company 연결

DB를 변경하지 않고 SEC-KIS 정확 일치 결과를 미리 확인한다.

```powershell
python -m backfills.asset_company_match_backfill
```

정확히 일치한 결과를 검토 가능한 후보로 저장한다.

```powershell
python -m backfills.asset_company_match_backfill --apply
```

`/admin/data`에서 후보 근거를 확인한 뒤, ticker와 거래소가 명확하게 일대일로 일치하는 후보를 확정한다.

```powershell
python -m backfills.asset_company_match_backfill `
  --apply `
  --auto-confirm-exact
```

생성기는 기존 `CONFIRMED`, `REJECTED` 결정을 유지한다. 식별자가 충돌하거나 이미 다른 Company와 연결된 Asset은 `REVIEW_REQUIRED` 상태로 남긴다.

## 기존 일봉 가격 백필

기존 `stock.timestamp`는 `2024-06-14 09:00:00+09`처럼 UTC 자정을 나타내는 `TIMESTAMPTZ` 값이다. 백필 작업은 이 값을 `price_bar.bar_time`으로 그대로 복사하고, DB에서 90일 단위로 UPSERT를 실행한다.

먼저 한 종목과 한 구간으로 확인한다.

```powershell
python -m backfills.price_bar_backfill `
  --start 2024-01-01 `
  --end 2024-04-01 `
  --symbol AAPL
```

건수를 확인한 뒤 같은 점검 구간을 DB에 반영한다.

```powershell
python -m backfills.price_bar_backfill `
  --start 2024-01-01 `
  --end 2024-04-01 `
  --symbol AAPL `
  --apply
```

전체 멱등 이관은 다음 명령으로 실행한다.

```powershell
python -m backfills.price_bar_backfill --all --apply
```

`--limit-windows 1`을 사용하면 첫 90일 구간만 이관한다. 성공한 구간은 각각 독립적으로 커밋되며, 작업이 중단되면 `last_completed_end`를 다음 실행의 `--start` 값으로 사용할 수 있다.

## 잘못된 일봉 가격 복구

기존 KIS Loader는 참고용으로 `archive/legacy_kis/`에 보관한다. 현재 복구 흐름은 Provider HTTP 처리와 DB 복구 책임을 다음과 같이 분리한다.

- `collectors/kis_client.py`: KIS 토큰, 재시도, 호출 제한, 페이지 순회, 응답 파싱
- `backfills/price_bar_invalid_repair.py`: 잘못된 데이터 추출, 그룹화, 검증, 원본 응답 보존, 멱등 `price_bar` UPSERT

KIS를 호출하지 않고 기존의 잘못된 행을 모두 추출한다.

```powershell
python -m backfills.price_bar_invalid_repair
```

DB를 변경하지 않고 한 종목을 조회해 비교 보고서를 생성한다.

```powershell
python -m backfills.price_bar_invalid_repair `
  --symbol XLF `
  --fetch
```

KIS가 반환한 값 중 OHLCV 검증을 통과한 값만 반영한다.

```powershell
python -m backfills.price_bar_invalid_repair `
  --symbol XLF `
  --fetch `
  --apply
```

여러 종목을 검토한 뒤 전체 복구를 명시적으로 실행한다.

```powershell
python -m backfills.price_bar_invalid_repair `
  --all `
  --fetch `
  --apply
```

KIS 원본 응답은 `pipeline_output/raw/kis/price_bar_repair`에 보존한다. CSV 보고서는 `pipeline_output/reports/price_bar_invalid`에 저장한다. 필터를 적용한 실행은 종목과 기간별 하위 디렉터리를 사용하므로 전체 오류 보고서를 덮어쓰지 않는다.

이 작업은 고가나 저가를 임의로 만들지 않는다. KIS가 반환하지 않았거나 여전히 OHLC 규칙을 위반하는 행은 이후 확인할 수 있도록 복구 보고서에 남긴다.

## Pipeline Worker 기본 동작 확인

`workers/dry_run_worker.py`는 Go Controller가 생성한 Job을 실제 외부 API 호출 없이 점검한다. 대기 중인 `pipeline_job_item`을 선점하고 `RUNNING`으로 변경한 뒤 종료 상태로 완료할 수 있는지 확인한다.

먼저 Controller에서 수동 Job을 생성하고 다음 Item을 미리 확인한다.

```powershell
python -m workers.dry_run_worker --source BLS
```

상태 전이를 실제 DB에 반영한다.

```powershell
python -m workers.dry_run_worker --source BLS --apply
```

관리자 페이지를 점검하기 위해 Item을 `SKIPPED` 또는 `FAILED`로 완료할 수도 있다.

```powershell
python -m workers.dry_run_worker --source BLS --complete-status SKIPPED --apply
python -m workers.dry_run_worker --source BLS --complete-status FAILED --apply
```

Worker는 `trader-data/.env`에서 `DATABASE_URL`을 읽는다. Item 선점에는 `FOR UPDATE SKIP LOCKED`를 사용하므로 여러 Worker 프로세스가 같은 Item을 동시에 가져가지 않는다.

## BLS 거시경제 Worker

`/admin/data`에서 BLS Job을 생성한 뒤 대기 중인 BLS Item 하나를 처리한다.

```powershell
python -m workers.bls_macro_worker
```

장기 실행 Worker로 동작시키려면 다음과 같이 실행한다.

```powershell
python -m workers.bls_macro_worker --loop --poll-interval-seconds 10
```

Worker는 `BLS:{seriesId}` Item 하나를 선점하고 상위 Job parameter에서 `startYear`, `endYear`를 읽는다. 이후 BLS 시계열 데이터를 수집하고 `pipeline_output/bls_macro_worker` 아래에 원본과 보고서 파일을 저장한 다음, `macro_*` 테이블을 UPSERT하고 lineage를 기록한다. Item은 결과에 따라 `SUCCESS` 또는 `SKIPPED`로 완료한다.

`--loop` 모드에서는 대기 Item이 없을 때 기다린 뒤 다시 조회한다. Series 메타데이터는 `macro_series`에서 읽으므로 테이블에 새 BLS Series를 추가하면 관리자 선택 화면, Schedule Form, Worker에서도 사용할 수 있다.
