# 원본 데이터와 정규화 데이터 Lineage 검증 결과

## 1. 목적

외부 API에서 받은 원본 데이터는 DB에 직접 저장하지 않고 로컬 또는 S3 같은 object storage에 보존한다.
DB의 `source_object`에는 원본 위치, 저장소 key, hash, 수집 job 정보를 기록하고,
`record_lineage`에는 해당 원본이 어떤 정규화 테이블 row를 만들었는지 기록한다.

이번 검증의 목적은 다음 흐름이 실제 DB에서 연결되는지 확인하는 것이다.

```text
pipeline_job
-> pipeline_job_item
-> source_object
-> record_lineage
-> price_bar / filing / financial_fact / macro_observation
```

## 2. 검증 기준

검증 성공 기준은 다음과 같다.

| 기준 | 설명 |
| --- | --- |
| `source_object.storage_key` 존재 | 로컬/S3 환경과 무관하게 원본을 식별할 수 있는 상대 key다. |
| `source_object.object_uri` 존재 | 현재 환경에서 원본 파일을 열 수 있는 실제 URI다. |
| `source_object.pipeline_job_item_id` 존재 | 어떤 세부 job item이 원본을 만들었는지 추적한다. |
| `record_lineage.source_object_id` 연결 | 원본과 정규화 결과 사이의 연결점이다. |
| `record_lineage.target_table` 확인 | 원본이 어떤 결과 테이블을 만들었는지 확인한다. |

## 3. KIS 검증 결과

NVDA `asset_id=2872`에 대해 KIS 증분 수집을 실행했다.

확인된 흐름은 다음과 같다.

```text
pipeline_job 19
-> pipeline_job_item 25
-> source_object 8751
-> record_lineage 2596949, 2596950
-> price_bar 21946870, 21946871
```

확인된 원본 정보는 다음과 같다.

```text
provider: KIS
data_type: PRICE_BAR
external_id: 2872-NVDA
storage_backend: LOCAL
storage_key: kis_price_worker/job-19/item-25/raw/kis/price_bar/2872-NVDA-8701803771bd35c3.json
processing_status: PROCESSED
```

정규화 결과는 NVDA의 `2025-07-21`, `2025-07-22` 일봉 `price_bar` 2건으로 연결됐다.

이로써 특정 `price_bar` row에서 KIS 원본 JSON까지 역추적할 수 있음을 확인했다.
예를 들어 `close > high` 같은 품질 문제가 발생하면 `record_lineage`를 통해 원본 응답을 열어
API 원본 문제인지, 변환 로직 문제인지, 옵션 문제인지 확인할 수 있다.

## 4. SEC 검증 결과

SEC NVDA 재무 수집 결과도 `source_object`와 `record_lineage` 연결이 확인됐다.

확인된 결과는 다음과 같다.

| source_object_id | data_type | pipeline_job_item_id | target_table | lineage_count |
| --- | --- | ---: | --- | ---: |
| 8752 | `SUBMISSIONS` | 26 | `filing` | 86 |
| 8753 | `SUBMISSIONS_HISTORY` | 26 | `filing` | 270 |
| 4917 | `COMPANY_FACTS` | 26 | `financial_fact` | 2177 |

의미는 다음과 같다.

```text
SEC 수집 item 26 하나가
-> submissions 원본
-> historical submissions 원본
-> companyfacts 원본
을 만들었고,
각 원본이 filing / financial_fact 정규화 row로 연결됐다.
```

SEC는 원본 하나가 여러 정규화 결과를 만드는 대표적인 사례다.
따라서 `record_lineage`가 없으면 특정 `financial_fact`가 어떤 SEC 원본에서 생성됐는지 추적하기 어렵다.

## 5. BLS 검증 상태

BLS는 이전 검증에서 다음 흐름을 확인했다.

```text
BLS TIMESERIES source_object
-> record_lineage
-> macro_observation
-> macro_observation_vintage
```

BLS는 KIS나 SEC보다 데이터 크기는 작지만,
발표 시각과 기준 기간을 분리해야 하므로 원본 응답과 정규화 결과를 연결하는 것이 중요하다.

## 6. 설계 판단

`source_object`는 원본 카탈로그이므로 강한 부모 역할을 한다.
반면 `record_lineage`는 감사와 추적을 위한 테이블이며, 결과 테이블의 필수 부모가 아니다.

현재 구조는 다음 원칙을 따른다.

```text
source_object까지는 강하게 연결한다.
record_lineage는 원본과 결과 row를 논리적으로 연결한다.
price_bar, financial_fact, macro_observation은 lineage 없이도 독립적으로 존재할 수 있다.
```

이 구조를 선택한 이유는 결과 테이블의 형태가 서로 다르기 때문이다.
예를 들어 `price_bar`는 TimescaleDB hypertable이고, `financial_fact`와 `macro_observation`은 일반 정규화 테이블이다.
따라서 `record_lineage`가 모든 결과 테이블에 물리 FK를 강하게 거는 방식보다
`target_table`, `target_record_id`, `target_record_key`로 느슨하게 연결하는 방식이 운영상 안전하다.

## 7. 결론

KIS, SEC, BLS 모두 원본 보존과 정규화 결과 추적이 가능한 구조로 검증됐다.

검증된 핵심은 다음과 같다.

```text
원본 파일은 로컬/S3에 보존한다.
DB에는 source_object로 URI, storage_key, hash, job item을 기록한다.
정규화 결과는 record_lineage로 원본과 연결한다.
품질 문제 발생 시 결과 row에서 원본 API 응답까지 역추적할 수 있다.
```

이 구조는 이후 `RAW_OBJECT_READY` 이벤트 기반으로 수집과 ETL을 분리할 때도 그대로 확장할 수 있다.
