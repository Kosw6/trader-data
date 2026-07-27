# SEC Financial Backfill

SEC `submissions`와 `companyfacts`를 이용해 `filing`, `financial_metric`,
`financial_fact`, `source_object`, `record_lineage`를 백필한다.

## 1. 샘플 검증

DB에 쓰지 않고 AAPL/NVDA 원본 수집과 변환만 확인한다.

```powershell
python -m backfills.sec_financial.backfill
```

명시 기업을 지정할 수도 있다.

```powershell
python -m backfills.sec_financial.backfill `
  --company AAPL=0000320193 `
  --company NVDA=0001045810
```

## 2. DB 반영

현재 DB의 `asset.company_id -> company.cik` 연결을 기준으로 ticker를 조회해 반영한다.

```powershell
python -m backfills.sec_financial.backfill `
  --ticker AAPL `
  --ticker NVDA `
  --apply
```

매칭된 기업 일부를 먼저 반영한다.

```powershell
python -m backfills.sec_financial.backfill `
  --matched-companies `
  --include-history `
  --limit 200 `
  --apply
```

다음 200개는 직전 실행의 `summary_*.json`에 있는 `next_start_after_cik` 값을
`--start-after-cik`에 넣어 이어서 실행한다.

```powershell
python -m backfills.sec_financial.backfill `
  --matched-companies `
  --include-history `
  --start-after-cik 0001045810 `
  --limit 200 `
  --apply
```

커서를 사람이 옮기지 않고 자동으로 끝까지 이어가려면 auto batch 모드를 사용한다.
네트워크가 불안정할 수 있으므로 처음에는 50~100개 단위로 확인하고, 안정적이면 200개로 올린다.

```powershell
python -m backfills.sec_financial.backfill `
  --matched-companies `
  --include-history `
  --auto-batches `
  --batch-size 100 `
  --request-interval 1.0 `
  --batch-sleep-seconds 10 `
  --apply
```

처음부터 다시 200개씩 자동 진행하려면 시작 커서를 기본값으로 두고 실행한다.

```powershell
python -m backfills.sec_financial.backfill `
  --matched-companies `
  --include-history `
  --auto-batches `
  --batch-size 200 `
  --request-interval 1.0 `
  --batch-sleep-seconds 10 `
  --apply
```

전체 매칭 기업 반영은 충분히 배치 실행을 확인한 뒤 제한 없이 실행한다.

```powershell
python -m backfills.sec_financial.backfill `
  --matched-companies `
  --include-history `
  --apply
```

## 3. 옵션 기준

- 기본은 core financial metrics only다.
- `--all-facts`는 표준 지표로 매핑되지 않은 SEC fact까지 저장한다.
- `--include-history`는 SEC recent filing 외 과거 submissions page까지 조회한다.
- 기업 하나 단위로 transaction을 나누어 한 기업 실패가 전체 백필을 중단시키지 않는다.
- 저장하지 못한 fact, 실패 기업, companyfacts 미제공/핵심 지표 없음 기업은
  `pipeline_output/sec_financial_backfill/reports`에 남긴다.
- `processed_companies_*.jsonl`은 성공, 스킵, 실패를 포함한 시도 목록이다. 마지막 줄의
  `cik` 또는 `summary_*.json`의 `next_start_after_cik`를 다음 배치 커서로 사용한다.
- `--auto-batches`는 배치가 끝날 때마다 마지막 시도 CIK를 다음 커서로 넘긴다.
  단, 한 배치 전체가 네트워크 오류 등으로 실패하면 자동 반복을 중단한다.

## 4. 필요한 환경 변수

```dotenv
DATABASE_URL="postgresql://postgres:<password>@localhost:55432/trader"
SEC_USER_AGENT="TraderDataBot-KR/1.0 YourName your-email@example.com"
```
