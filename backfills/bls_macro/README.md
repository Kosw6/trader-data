# BLS 거시경제 데이터 백필

BLS 발표 캘린더와 Timeseries API 응답을 수집해 다음 테이블에 반영한다.

- `source_object`
- `macro_series`
- `macro_release`
- `macro_observation`
- `macro_observation_status`
- `macro_observation_vintage`
- `record_lineage`

## 1. 사전 실행

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

- `CUSR0000SA0`: 소비자물가지수(CPI)
- `CUSR0000SA0L1E`: 근원 소비자물가지수(Core CPI)
- `WPUFD4`: 최종수요 생산자물가지수(PPI)
- `LNS14000000`: 실업률
- `CES0000000001`: 비농업 부문 전체 고용
- `CES0500000003`: 시간당 평균 임금

## 5. 수집 불가 관측값

BLS는 특정 월의 값이 제공되지 않았을 때 `-` 같은 비숫자 값과 사유가 담긴 각주를 반환할 수 있다.
이 행은 숫자 컬럼을 사용하는 `macro_observation`에 넣지 않고 `macro_observation_status`에 보존한다.
관리자 화면에서는 이를 통해 아직 수집하지 않은 기간과 Provider가 명시적으로 수집 불가 상태를 반환한 기간을 구분할 수 있다.
