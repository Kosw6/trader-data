# BLS Macro Backfill

BLS 발표 캘린더와 Timeseries API 응답을 수집해 다음 테이블에 반영한다.

- `source_object`
- `macro_series`
- `macro_release`
- `macro_observation`
- `macro_observation_status`
- `macro_observation_vintage`
- `record_lineage`

## 1. Dry Run

DB에 쓰지 않고 raw 원본과 테이블별 JSONL만 생성한다.

```powershell
python -m backfills.bls_macro.backfill `
  --start-year 2020 `
  --end-year 2026
```

출력 위치는 기본적으로 `pipeline_output/bls_macro_backfill`이다.

## 2. DB 반영

```powershell
python -m backfills.bls_macro.backfill `
  --start-year 2020 `
  --end-year 2026 `
  --apply
```

필요한 환경 변수:

```dotenv
DATABASE_URL="postgresql://postgres:<password>@localhost:55432/trader"
BLS_API_KEY="<optional>"
BLS_USER_AGENT="TraderDataBot-KR/1.0"
```

`BLS_API_KEY`는 없으면 비등록 호출로 진행한다. 호출 한도와 안정성을 위해 운영에서는
등록 키를 사용하는 편이 좋다.

## 3. 처리 기준

- `macro_series`는 BLS Series ID 정의다.
- `macro_release`는 BLS ICS Calendar의 예정 발표 이벤트다.
- `macro_observation`은 Series ID와 기준월의 현재 관측값이다.
- `macro_observation_vintage`는 같은 관측값의 버전 이력이다.
- 현재 `available_at`은 `COLLECTED_FALLBACK`으로 저장한다.
- 실제 발표시각과 기준월 매칭은 RSS/보도자료 보강 단계에서 추가한다.

## 4. 1차 범위

현재 기본 수집 시리즈:

- `CUSR0000SA0`: CPI
- `CUSR0000SA0L1E`: Core CPI
- `WPUFD4`: PPI Final Demand
- `LNS14000000`: Unemployment Rate
- `CES0000000001`: Total Nonfarm Payroll
- `CES0500000003`: Average Hourly Earnings

## 5. Unavailable observations

BLS can return a monthly period with a non-numeric value such as `-` and a
footnote explaining why the data is unavailable. Those rows are not inserted
into numeric `macro_observation`; they are preserved in
`macro_observation_status` so the admin inventory can distinguish "not
collected yet" from "the provider explicitly marked this period unavailable."
