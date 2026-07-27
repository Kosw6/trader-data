# trader-data

Data ingestion and backfill workspace for the Trader service.

## 디렉터리 구조

| Directory | Responsibility |
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

Keep `.env`, `.env.example`, `.gitignore`, and `requirements.txt` at the root.
Run package commands from this directory with `python -m ...` so imports resolve
consistently.

## 수집과 ETL 책임 분리 방향

현재 MVP에서는 SEC, KIS, BLS worker가 `COMBINED_COLLECT_TRANSFORM_LOAD` 방식으로 동작한다.

```text
pipeline_job_item
  -> collector/client로 외부 API 호출
  -> pipeline_output/에 raw 또는 table preview 저장
  -> transform / validation
  -> PostgreSQL upsert
  -> pipeline_job_item 최종 상태 기록
```

이렇게 한 이유는 아직 스키마, 관리자 페이지, 수동 job 생성 흐름이 계속 바뀌는 단계이기 때문이다. 이 시점에 수집 프로세스와 ETL 프로세스를 완전히 분리하면 장애 지점과 디버깅 포인트가 급격히 늘어난다. 대신 worker checkpoint에 공통 `pipeline` 객체를 남겨 BLS, SEC, KIS가 같은 단계 이름으로 읽히도록 했다.

```text
RESOLVE_TARGET -> COLLECT_RAW -> WRITE_RAW_PREVIEW -> TRANSFORM -> LOAD_DB
```

다음 단계에서는 Python을 유지하되 프로세스 책임을 분리한다.

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

Go controller는 ETL 실행 가능 시간을 결정한다. 예를 들어 장 마감 이후, 사용자 트래픽이 적은 시간대, 또는 Kafka lag/backlog가 기준치를 넘었을 때 ETL worker를 실행할 수 있다. Kafka에는 큰 원본 payload를 넣지 않고, `source_object_id`, `object_uri`, `job_item_id` 같은 작은 참조값만 넣는다.

### 왜 처음부터 완전히 분리하지 않았나

지금 단계에서 바로 `collector worker -> RAW_OBJECT_READY -> ETL worker`로 나누지 않은 이유는 다음과 같다.

1. **스키마와 관리자 기능이 아직 고정되는 중이다.**  
   수집 결과를 어떤 테이블에 어떤 상태값으로 저장할지 바뀌는 동안 프로세스까지 분리하면 수정 비용이 커진다.

2. **장애 지점이 너무 빨리 늘어난다.**  
   완전 분리 구조에서는 collector, raw storage, outbox, Kafka, ETL consumer, DB loader를 모두 동시에 봐야 한다. MVP 단계에서는 원인 추적이 어려워진다.

3. **현재 목표는 기능 완성보다 운영 경계 확정이다.**  
   먼저 job/item, outbox, processed_event, checkpoint, 관리자 페이지 흐름을 안정화한 뒤 수집/ETL을 나누는 편이 더 안전하다.

4. **Kafka lag 기준도 실제 데이터로 잡아야 한다.**  
   KIS, SEC, BLS의 수집량과 DB 적재 시간을 먼저 관찰해야 source별 threshold를 현실적으로 정할 수 있다.

따라서 현재 코드는 combined worker로 유지하되, checkpoint와 문서에서 stage boundary를 명확히 남기는 방식으로 갔다. 이후에는 같은 stage를 기준으로 `COLLECT_ONLY`, `ETL_ONLY` 모드로 나누면 된다.

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

## Asset-Company matching

Preview SEC-KIS exact matching without changing the database:

```powershell
python -m backfills.asset_company_match_backfill
```

Persist exact matches as reviewable candidates:

```powershell
python -m backfills.asset_company_match_backfill --apply
```

After inspecting candidate evidence in `/admin/data`, explicitly confirm all
unambiguous one-to-one ticker and exchange matches:

```powershell
python -m backfills.asset_company_match_backfill `
  --apply `
  --auto-confirm-exact
```

The generator preserves existing `CONFIRMED` and `REJECTED` decisions. Identity
collisions and Assets already linked to another Company remain
`REVIEW_REQUIRED`.

## Legacy daily-price backfill

The legacy `stock.timestamp` is already a `TIMESTAMPTZ` value such as
`2024-06-14 09:00:00+09`, which is UTC midnight. The backfill copies it directly
to `price_bar.bar_time` and executes database-side UPSERTs in 90-day windows.

Start with one symbol and one window:

```powershell
python -m backfills.price_bar_backfill `
  --start 2024-01-01 `
  --end 2024-04-01 `
  --symbol AAPL
```

Apply the same smoke-test range after checking the counts:

```powershell
python -m backfills.price_bar_backfill `
  --start 2024-01-01 `
  --end 2024-04-01 `
  --symbol AAPL `
  --apply
```

The full idempotent migration is:

```powershell
python -m backfills.price_bar_backfill --all --apply
```

Use `--limit-windows 1` to migrate only the first 90-day window. Each successful
window commits independently, and `last_completed_end` can be reused as the
next `--start` value after an interruption.

## Invalid daily-price repair

The legacy KIS loader is preserved under `archive/legacy_kis/` for reference.
The current repair flow separates provider HTTP concerns from database repair:

- `collectors/kis_client.py`: KIS token, retry, throttling, pagination, parsing
- `backfills/price_bar_invalid_repair.py`: invalid export, grouping, validation,
  raw-response preservation, and idempotent `price_bar` UPSERT

Export all invalid legacy rows without calling KIS:

```powershell
python -m backfills.price_bar_invalid_repair
```

Fetch one symbol and write comparison reports without changing the database:

```powershell
python -m backfills.price_bar_invalid_repair `
  --symbol XLF `
  --fetch
```

Apply only values returned by KIS that pass the OHLCV checks:

```powershell
python -m backfills.price_bar_invalid_repair `
  --symbol XLF `
  --fetch `
  --apply
```

After reviewing several symbols, the complete repair can be explicitly enabled:

```powershell
python -m backfills.price_bar_invalid_repair `
  --all `
  --fetch `
  --apply
```

Raw KIS pages are preserved under `pipeline_output/raw/kis/price_bar_repair`.
CSV reports are written under `pipeline_output/reports/price_bar_invalid`.
Filtered runs use a symbol-and-period subdirectory so they do not overwrite the
complete invalid report.
The job never fabricates high or low values: rows not returned by KIS or still
violating OHLC rules remain in the repair report for later investigation.

## Pipeline worker MVP

`workers/` contains the first worker-plane skeleton for jobs created by the Go
controller. The initial worker does not call SEC/BLS/KIS yet. It verifies that a
queued `pipeline_job_item` can be claimed, marked `RUNNING`, and completed with
a terminal status.

Create a manual job through the controller first, then preview the next item:

```powershell
python -m workers.dry_run_worker --source BLS
```

Apply the status transition:

```powershell
python -m workers.dry_run_worker --source BLS --apply
```

Mark the item as skipped or failed for admin-page testing:

```powershell
python -m workers.dry_run_worker --source BLS --complete-status SKIPPED --apply
python -m workers.dry_run_worker --source BLS --complete-status FAILED --apply
```

The worker reads `DATABASE_URL` from `trader-data/.env`. The claim operation uses
`FOR UPDATE SKIP LOCKED`, so multiple worker processes can run without claiming
the same item.

## BLS macro worker

After creating a BLS job from `/admin/data`, process one queued BLS item with:

```powershell
python -m workers.bls_macro_worker
```

Run it as a long-lived worker process:

```powershell
python -m workers.bls_macro_worker --loop --poll-interval-seconds 10
```

The worker claims one `BLS:{seriesId}` item, reads `startYear` and `endYear` from
the parent job parameters, collects that BLS timeseries, writes raw/report files
under `pipeline_output/bls_macro_worker`, upserts `macro_*` tables, records
lineage, and completes the item as `SUCCESS` or `SKIPPED`. In `--loop` mode it
waits and polls again when there is no queued item. Series metadata is read from
`macro_series`, so adding a new BLS series to the table also makes it available
to the admin picker, schedule form, and worker.
