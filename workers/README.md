# Trader 데이터 Worker

## Worker 구분

현재 worker는 두 계층으로 나뉩니다.

| 종류 | 역할 |
| --- | --- |
| combined worker | 수집, raw 저장, 변환, DB 적재를 한 번에 수행합니다. MVP 검증과 수동 운영에 계속 사용할 수 있습니다. |
| split worker | collector-only와 ETL-only로 분리합니다. raw 보존, Kafka 이벤트, ETL 실패/재처리 흐름을 검증하기 위한 운영형 구조입니다. |

## 통합 Worker

```powershell
python -m workers.bls_macro_worker
python -m workers.sec_financial_worker
python -m workers.kis_price_worker
```

반복 실행 모드:

```powershell
python -m workers.bls_macro_worker --loop --poll-interval-seconds 10
python -m workers.sec_financial_worker --loop --poll-interval-seconds 10
python -m workers.kis_price_worker --loop --poll-interval-seconds 10
```

## 분리 Worker

collector-only worker는 외부 API를 호출하고 raw 파일을 저장한 뒤 `source_object`를 `COLLECTED`로 남깁니다. 이후 Go controller의 raw-ready API를 호출해 `pipeline_outbox`에 `RAW_OBJECT_READY` 이벤트를 생성합니다.

ETL-only worker는 Kafka raw topic을 소비하거나 `--source-object-id`를 직접 받아 raw 파일을 읽고 정규화 테이블과 `record_lineage`를 적재합니다.

### KIS 가격 데이터

Collector:

```powershell
python -m workers.kis_price_collector
```

ETL Consumer:

```powershell
python -m workers.kis_price_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.kis_price_etl_worker --source-object-id 8755
```

### BLS 거시경제 데이터

Collector:

```powershell
python -m workers.bls_macro_collector
```

ETL Consumer:

```powershell
python -m workers.bls_macro_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.bls_macro_etl_worker --source-object-id 8750
```

### SEC 재무 데이터

Collector:

```powershell
python -m workers.sec_financial_collector
```

ETL Consumer:

```powershell
python -m workers.sec_financial_etl_worker --brokers 127.0.0.1:9092 --max-records 1
```

Kafka 없이 특정 raw 직접 처리:

```powershell
python -m workers.sec_financial_etl_worker --source-object-id 8752
```

SEC는 하나의 item이 `SUBMISSIONS`, `SUBMISSIONS_HISTORY`, `COMPANY_FACTS` raw를 여러 개 만들 수 있습니다. collector는 이 raw들을 모두 `source_object`로 저장하고, 대표 raw 하나만 `RAW_OBJECT_READY`로 보고합니다. ETL worker는 대표 raw가 속한 `pipeline_job_item_id`를 기준으로 같은 item의 SEC raw 묶음을 읽어 `filing`, `financial_fact`, `record_lineage`를 함께 적재합니다.

## Kafka Topic

| topic | 소비 worker | 용도 |
| --- | --- | --- |
| `trader.jobs.events` | `workers.kafka_pipeline_consumer` | Kafka를 통해 수집 작업을 전달하는 제어 이벤트 |
| `trader.raw.kis.ready` | `workers.kis_price_etl_worker` | KIS 원본 가격 데이터 ETL |
| `trader.raw.bls.ready` | `workers.bls_macro_etl_worker` | BLS 원본 시계열 데이터 ETL |
| `trader.raw.sec.ready` | `workers.sec_financial_etl_worker` | SEC 원본 묶음 ETL |
| `trader.raw.news.ready` | 향후 News ETL Worker | 뉴스 원본 ETL 확장 예정 |

## 필수 환경 변수

```text
DATABASE_URL
TRADER_CONTROLLER_URL
ADMIN_API_KEY
KAFKA_BROKERS
```

KIS:

```text
APPKEY
APPSECRET
```

SEC:

```text
SEC_USER_AGENT
```

BLS:

```text
BLS_API_KEY
```

## Item 식별 키

```text
KIS:{asset_id}
BLS:{series_id}
SEC:{ticker}
SEC:{cik}
```

예시:

```text
KIS:2872
BLS:CUSR0000SA0
SEC:AAPL
SEC:0001045810
```


## 재무 지표 매핑 자동화 (667)

`pipeline/financial_mapping667.py`는 저장된 SEC 정규화 결과를 대상으로 조회용 태그 매핑을 검증합니다. `workers/financial_mapping_worker.py`는 별도 큐를 소비합니다. 기존 Python 수집 워커와 같은 이미지로 기존 색인 서버에서 실행하며 외부 SEC/KIS 수집이나 AI 호출을 하지 않습니다.

실행 전 `trader-deploy/ops/mapper667_schema.sql`의 별도 스키마가 필요합니다. 이 마이그레이션은 기존 `finance_review665` 검토 매핑이 있는 통합 DB를 전제로 합니다. 원본 facts/filings/원래 매핑을 바꾸지 않고 `finance_mapping667.approved`의 활성 행만 공개 매핑 뷰에 추가합니다.

- SEC ETL 적재 함수의 커밋 직전 `enqueue_if_enabled(cursor, company_id)`를 호출합니다. `FINANCIAL_MAPPING_QUEUE_ENABLED=true`인 배포에서만 활성화되며 적재가 롤백되면 큐 등록도 롤백됩니다. 수집 작업 자체를 새로 예약하지 않습니다.
- 큐 소비는 기업별 트랜잭션과 잠금으로 직렬화합니다. 승인·보류·영수증·큐 상태를 같은 트랜잭션으로 커밋하고 새 읽기로 영수증 ID를 확인합니다. 재등록 세대와 기존 영수증을 보존하며 같은 입력으로 승인 행을 중복 생성하지 않습니다.
- 현재 지원하는 매출·순이익·영업이익·자산·부채·희석 EPS 여섯 지표만 처리합니다. `us-gaap`, 검토된 태그, 정확한 단위, 무차원 값, 실제 보고기간, 10-K/10-Q 및 정정 공시, SEC CIK/접수번호 URL과 저장된 fact 해시 형식을 검증합니다. 자산·부채는 시점값, 다른 지표는 허용된 분기/연간 길이만 인정합니다. 비교기간만 있는 행·미래 날짜는 제외합니다.
- 승인 범위는 해당 기업·지표·실제 보고종료일 한 점입니다. 매출 별칭 충돌, 동일일 상충값, 기존 매핑 충돌은 임의로 선택하지 않고 보류합니다. 새로운 충돌이 생기면 해당 자동 승인만 비활성화합니다. 원본 및 수동 검토 매핑은 수정하지 않습니다.
- 이번 역사 보완은 2014년 이후 **연간** 자료이며 회사 마스터 4,869개를 순회합니다. 자료 없는 기업의 처리 완료도 포함하므로 처리 기업 수를 재무 데이터 보유 기업 수로 해석하지 않습니다. 이후 SEC 적재로 등록되는 큐는 전체 보유 기간의 분기·연간을 검증합니다. 기존 역사 큐가 먼저 처리되는 동안 새 큐는 뒤에서 대기할 수 있습니다.
- 기업당 후보 10,000행, 단일 판정 출처 128개, SQL 15초·잠금 3초를 제한합니다. 기업 후보 한도 초과는 보류하고 다음 기업으로 갑니다. SQL/인프라/미분류 실패는 롤백 후 프로세스 종료하며 무조건 재시작하지 않습니다. 운영 컨테이너의 restart 정책은 no입니다.
- SIGTERM은 신규 기업 처리를 멈추고 현재 트랜잭션 종료 후 연결을 닫습니다. 기존 서버 시작·정상 종료 순서에 포함했으며 운영 시간표는 바꾸지 않았습니다. 워커 메모리 128MiB, CPU 0.25 제한입니다.

실행 예: `python -m workers.financial_mapping_worker --loop --interval 2`. 기존 운영에는 별도 소비자가 이미 있으므로 중복 실행하지 마세요. 확인 대상은 `finance_mapping667.queue/approved/held/receipt`입니다. 승인 근거는 저장 fact와 SEC 출처 메타데이터에 대한 검증이며 공식 원문을 새로 내려받아 바이트를 재검증한 것은 아닙니다. 승인하는 것은 태그의 의미·기간 범위이며 공시 수치를 고정하지 않습니다. 실제 조회는 기존 최신 공시/동일일 충돌 규칙을 그대로 적용합니다.

원래 metric_id가 없는 8,908종 태그나 IFRS·새 지표를 여섯 지표에 강제로 배정하지 않습니다. 별도 의미 검토가 필요합니다. 중복 실행 검증과 실제 ETL 큐 롤백 검증, 여섯 지표 운영 조회 대사는 `output/mapper667/`에 보관합니다. 운영 백업의 선택 스키마에 수동/자동 매핑·큐·영수증도 포함했으며 원본 시장 전체를 일일 운영 백업에 새로 포함하지는 않습니다.


667 후속: 운영의 실제 SEC 수집 쓰기 대상은 `control649`이고 기존 공개 재무 뷰는 해당 스테이징을 포함하지 않는 것을 읽기 확인했다. 미래 수집 큐는 공개 보유 자료와 수집 테이블을 함께 읽도록 보완했다. 수집 기업 ID·CIK가 공개 기업과 정확히 맞아야 하며 물리 지표 ID는 공개 지표 코드로 다시 해석한다. 기업 식별 불일치는 회사 단위 보류로 남긴다. 스테이징 사실 행을 공개 뷰에 붙이거나 자동 게시하지 않는다. 따라서 이후 수집의 **매핑 검증**은 자동화되지만 새로운 사실 자체가 서비스에 공개되는 것은 별도의 기존 게시 절차를 따라야 한다. `collection-writer-capability.json`, `collection-visibility.json`, `collection-staging-identity.json`, `deployed-staging.json`에 구조 및 배포 근거를 보관한다.


668 업그레이드: Go 관리자 수동·예약 실행은 같은 큐를 사용한다. 큐의 `refresh_requested`와 영수증 `queue_generation` 컬럼이 필요하므로 668 SQL을 먼저 적용한다. 원래 SEC 자동 큐 등록은 처리 중인 수동 세대·범위를 유지하고 후속 전체 검증을 표시한다. 현재 워커는 회사별 완료 영수증을 세대별로 보존한다. 원본 수집과 매핑은 별개이고 매핑은 자료를 새로 수집/공개 게시하지 않는다. 자세한 API·운영 절차는 trader-controller/README.md의 668 절을 참조한다.

### 674: 원본 XBRL을 대조한 역사 범위 자동 보완

기존 수집·매핑 워커 이미지 `trader-data:history674c`에는 `FINANCIAL_HISTORY_RAW_ENABLED=true`를 적용했다. `pipeline/financial_history674.py`가 원본 Company Facts의 CIK·canonical JSON 해시와 개별 관측값 해시/값/단위/기간/접수번호/공시일을 저장행과 대조한다. 원 보고서와 비교 공시의 해당 연결 총액이 모두 같고 지원되는 제공 태그가 있을 때만 회사·지표·종료일별 제안을 기존 receipt에 추가한다. 금액 변경은 주석·조정표 수동 검토 대상으로 보류하고 원본을 덮어쓰지 않는다. `SalesRevenueNet`을 전체 시장 별칭으로 활성화하지 않는다.

Go의 `annual_only` 실행은 해당 종료일을 실제 보고기간으로 하는 원 10-K와 연간 길이(또는 자산·부채의 시점값)가 있어야 한다. 분기 10-Q만 있는 자산·부채 시점을 연간으로 승인하지 않는다. 같은 회계연도 종료일의 검증된 Q4 시작기간은 연간과 함께 대조한다. 실제 저장 자료의 분기 전용 시점과 연간 시점으로 이 경계를 READ ONLY 재검증했다.

직접 SEC 백필과 Kafka ETL이 원본 payload를 적재 트랜잭션의 확인 훅까지 전달한다. 큐 행→회사 advisory lock 순서를 지키고 큐와 확인 근거를 함께 커밋한다. 기존 Go 668 수동/스케줄링 경로도 같은 소비자를 사용한다. 원본 hash 불일치는 근거를 기록하지 않으며 일반 후보 한도·SQL/잠금 제한·커밋 보호를 유지한다. 새 스테이징 관측값을 공개 뷰에 붙이는 절차는 별개다.

대량 감사용 `SEC_RAW_REPAIR674` 큐는 준비된 원본 대조 제안만 적용하는 제한 모드다. 일반 667 규칙만으로 다른 범위를 함께 승인하지 않고 기존 활성/비활성 승인 이력을 보존한다. 이후 원본이 바뀌면 674 자동 승인을 새 원본과 다시 대조하고, 근거가 불충분한 경우 해당 자동 범위를 검토 상태로 두며 원 관측값·최초 proof·새 확인 영수증은 남긴다. 승인하는 것은 검토된 태그/기간이며 최초 발표판 선택이나 모든 회계정책의 동일성 판단이 아니다.

확인 자료는 `output/finance-mapping674/`와 `trader-ai/docs/implementation_650_launch_followup.md`의 674절이다. 기존 운영 소비자는 계속 실행하므로 일반 운영에서 중복 소비자를 추가하지 않는다. 이번 일회성 보완에서는 같은 CPU 제한 안에서만 최대 1,000기업의 유한한 대기열 소비를 별도 실행했고 정상 종료 영수증을 남겼다. 원래 서버·스케줄·SQL 스키마·공개 포트는 유지한다.

### 675: 미국 회계기준 본 재무제표를 확인한 외국 연차

현재 이미지 `trader-data:history675`는 검토된 20-F·40-F도 정확한 CIK·접수번호·원문 URL·문서 SHA256·감사의견 근거가 있을 때만 처리한다. `foreign_annual=True`만으로 허용되지 않으며 `foreignAnnualReview`가 각 적격 접수번호를 포함해야 한다. 원/비교 공시 금액 불변과 원 보고서·USD/us-gaap·무차원 조건은 동일하다. 새 proof의 `annualFormContract=SEC_FOREIGN_ANNUAL_US_GAAP675_V1`으로 기존 승인과 경계를 구분한다.

재수집은 활성 승인에 묶인 같은 접수번호의 검토 기록만 재사용한다. 새 외국 연차보고서는 별도의 회계기준 문서 검토가 필요하다. 검토되지 않은 신규 보고서, IFRS·비 USD·차원 혼재·6-K 분기 추정·금액 변경은 자동 승인하지 않는다. Go 연간 모드도 검토된 원 연차보고서와 실제 기간을 요구한다. 17기업·41개 추가 매핑과 이전 승인 752,443개 불변을 검증했다. 상세 근거·원인별 잔여 집계·실제 챗봇 검증은 675절과 `output/finance-mapping675/`에 보관했다.


### Reviewed reconciliation 677 (local code, deployment required)

`python -m workers.prepare_reconciliation677 --manifest /operator/review.json --destination /operator/new-bundle [--download]` verifies operator-reviewed official SEC documents and immutable expected hashes. Run preparation on an approved SEC-accessible host; no outbound network is required by the mapping consumer. Mount the completed bundle read-only, set `FINANCIAL_RECONCILIATION_MANIFEST=/reviewed/manifest.json`, then use the existing mapping worker. Queue reason `REVIEWED_RECONCILIATION677` invokes the strict annual revenue reconciliation path, not the ordinary mapper. It never overwrites existing approvals or facts. New/unreviewed filings are held; corrupted documents/configuration/SQL errors stop processing. Deploy the worker before enabling the new Go mode. Human registration of a new accounting reconciliation remains required; this is not an LLM approval pipeline.


### 678 운영 반영 완료 (2026-10-31)

매핑 소비자 현재 운영 이미지 `trader-data:reconcile678`, 원 수집 컨테이너 `trader-data:history675`. 검토 bundle 3범위를 read-only로 연결하고 INTU 신규1/기존2의 실제 Go 큐 소비와 독립 감사를 통과했다. 상세 영수증은 `output/reconciliation678/`와 구현 문서678절을 참조한다.


### 679 잔여 후보 발견·검토 자료 자동화 (2026-10-31)

현재 매핑 이미지 `trader-data:reconcile679b`. reviewedReconciliation 실행 시 미등록/변경된 연간 매출 후보를 receipt.reviewCandidates에 자동 기록한다. 별도 준비 CLI: `python -m workers.review_reconciliation679 --inventory frozen.jsonl.gz --output new-review-folder --end 2026-10-31 --limit 10 [--company-id ID] [--download]`. 원문·표의 수치 등식은 검토 자료이며 자동 승인이 아니다. 검토/등록된 bundle만 반영한다. 구현 문서679절과 output/reconciliation679/ 영수증을 참조한다.

### 680 오류 정정 근거 재검증 (2026-10-31)

현재 매핑 이미지는 `trader-data:reconcile680b`이며 등록 bundle은 11개 범위다. 기존 Go `mode: reviewedReconciliation` 수동/예약 경로를 그대로 쓴다. 새 예약은 등록하지 않았다.

`pipeline/error_correction680.py`의 `EXPLICIT_ERROR_CORRECTION`은 기존 Topic606 정책과 별도인 `sec-reviewed-error-correction680-v1` 승인 증거를 만든다. 등록된 원문 SHA·사유 문단·주석 참조·정확한 연도/지표/단위·원값+조정액=정정값·직전 공시 및 저장 행 해시를 재검증한다. 등록되지 않은 후보, 새 공시로 범위가 바뀐 후보, 기존 다른 승인은 덮어쓰지 않는다. 현재 지원 범위는 검토된 연간 US-GAAP/USD 매출이며 순이익/EPS/다른 회계 원인에 자동 확장하지 않는다.

BAX FY2017 매출 10,561+23=10,584백만 USD는 외화 환산 오류 정정으로 등록했다. Target FY2017 매출 71,879+835=72,714백만 USD는 기존 수익 인식 전환 정책으로 등록했다. Target FY2016 조정표의 1백만 차이와 Grainger FY2017 표시 정밀도 변경은 보류한다. 원본 fact/filing 쓰기 0, 이전 승인 752,493개 불변, 재실행 신규 삽입 0을 확인했다. 근거와 실제 Go/챗봇 검증은 `output/reconciliation680/` 및 구현 문서680절에 보존한다.

### 681 대규모 원문 검토 자료 자동화

현재 매핑 이미지 `trader-data:reconcile681`에 `workers.bulk_reconciliation681`을 포함했다. 기존 11범위 승인 번들과 Go 수동/예약 API는 유지한다. 다음 준비 명령은 DB를 조회하거나 승인/반영하지 않는다.

```powershell
python -m workers.bulk_reconciliation681 --plan candidate-index.json --output new-bulk-folder --cache-root existing-review-folder --download
```

입력 최대500범위/800문서, 같은 회사 SEC Archives URL, 문서32MiB/전체 원문2GiB, 순차 GET 1초 간격·redirect 금지다. 기존 원문은 SHA 대사 후 재사용한다. 404는 해당 문서의 불완전 결과로 남기고 다음 문서를 읽으며, 403/429·미분류 오류·해시 충돌·예산 초과는 공통 중단 사유로 기록한다. 새 출력폴더만 사용해 원본/이전 보고서를 덮어쓰지 않는다.

`screened-scopes.jsonl`은 조정표 수치 등식 후보, 표시 정밀도 후보, 정확한 조정표 미확인, 원문 미완료를 구분한다. `accountingLanguageHints`는 문서에 해당 용어가 있다는 뜻이며 그 회사/기간/금액의 회계 원인을 확정한 결과가 아니다. 반올림 등식만으로 승인하지 않는다. 모든 결과는 `requiresHumanReview=true`, `approvalGenerated=false`다. 검토 후 등록된 근거만 기존677/680 경로에서 재검증/반영한다.

`go-batches.json`은 50기업 이하의 기존 `mode: reviewedReconciliation` 요청으로 분할한다. 입력 해시 기반의 안정적인 requestKey를 사용하고 allCompanies=false/annualOnly=true를 유지한다. 생성은 제출/예약 활성화와 구분한다. SEC 원문 준비는 인터넷이 허용된 PC에서 하고 사설 워커에 NAT/공개 경로를 추가하지 않는다. 작업 상태·원문 SHA·중단 사유·분류 집계는 새 폴더의 JSON 영수증으로 확인한다.


### 682 공시 표시 정밀도 검증

현재 매핑 이미지 `trader-data:reconcile682`, 등록 근거31범위. `pipeline/precision_evidence682.py`는 원래 공시에도 동일한 표시값이 있는 경우만 문서/표 SHA, 연도 열과 값 정렬, 단위·실제 소수자리·반올림 quantum, 저장행 집합 해시로 재검증한다. 별도 `sec-reviewed-presentation-precision682-v1` 정책이며 경제적 오류 정정으로 분류하지 않는다. 기존677 Go 수동/예약 경로로 처리하고 기존 승인은 보존한다. 숫자상 반올림 관계만 있는 후보는 승인하지 않는다. 이번31범위 중15건 반영/16건 보류, 원본 fact/filing 쓰기0. 구현 문서682절과 `output/reconciliation682/completion.json` 참조.


### 683 비율 열과 복합 오류 정정

현재 매핑 이미지 `trader-data:reconcile683`, 등록 근거38범위. `pipeline/reviewed_evidence683.py`는 비율 열이 있는 공시의 재무 연도 prefix/비율 개수/표SHA/단위·소수자리와 원래 공시 동일 표시값을 검증하는 별도 정밀도 정책, 그리고 오류 성분별 사유/금액/연도/표·원문SHA와 합계를 검증하는 복합 오류 정책을 제공한다. 기존677 Go 수동/예약 경로로 등록된 근거만 재검증한다. 기존 정책·승인·원본은 유지한다. SMCI 복합 오류를 ASC606 전환으로 분류하지 않는다. 신규7/재생신규0, Python205·Go10·실제챗봇114 검사 통과. 문서683절 및 `output/reconciliation683/completion.json` 참조.


### 684 여러 기간의 공시 표시 정밀도 연결

현재 매핑 이미지 `trader-data:reconcile684`, 등록 근거47범위. `pipeline/precision_series684.py`는 원래/후속 공시(선택적 중간 공시 포함2~4개)의 단위·소수자리·공시일·문서/표SHA와 연도/실제 결산일을 결속하고 모든 인접 노드에서 최소2개 공통 기간의 표시값 일치를 검증한다. 하나라도 비교값이 충돌하면 승인하지 않는다. `workers/prepare_precision684.bindings()`는 검토용 후보를 생성하며 자동 승인하지 않는다. 새 정책은 공시 표시 정밀도 호환성만 뜻하고 정밀도 아래 경제적 불변/오류 정정을 뜻하지 않는다. 원래의 더 정밀한 raw와 이전 proof는 유지한다. 기존 Go mode 경로로 검토 등록 근거만 재검증한다. 이번 신규9/재생신규0, Python223·Go10·새9범위 실제챗봇29 검사 통과. BKNG 순이익2개 미보유는 명시적으로 검증했으며 임의 대체하지 않았다. 문서684절과 `output/reconciliation684/completion.json` 참조.


### 685~690 원문 연결·frame 이력·재표시 검증 (2026-10-31)

현재 매퍼 trader-data:reconcile690, 원 수집기 trader-data:history686, Go trader-controller:frame686. financial_history674의 raw hook은 FINANCIAL_HISTORY_RAW_ENABLED/FINANCIAL_FRAME_METADATA_ENABLED 플래그 아래 정확한 frame 전체 해시 복원만 허용한다. Go rawVerifiedRepair는 FINANCIAL_RAW_VERIFIED_REPAIR_ENABLED 아래 명시 기업<=50/endDate필수/allCompanies=false의 유한 수동·예약 코드이며 새 예약은 만들지 않았다. reviewedReconciliation은 읽기 전용 등록 근거98범위의 완전한 저장행/문서/표/기간/단위/원인/등식을 재검증한다. As Revised 후보와 명시 em dash 0열을 지원하되 새 원인은 자동 승인하지 않는다. 후보 requiresHumanReview=true/approvalGenerated=false. 기존 원문·재무행·승인 불변, 새3,948범위 및 실제 대화198검사·Python331통과. 상세 계약과 잔여181,115범위는 trader-ai/docs/implementation_685_690_financial_review_20261031.md 및 output/reconciliation685~690/ 참조.


### 691 NEOG 연간 분류 오류 근거 추가

코드 이미지는 trader-data:reconcile690 그대로이며 bundle691(99범위)을 읽기 전용으로 연결했다. As Revised 후보 발견이 NEOG의 리베이트 비용→매출 차감 재분류를 발견했고, 독립 검토/등록 후 기존 Go mode로 신규1/재생신규0을 검증했다. 새 원인은 자동 승인하지 않는다. 최종 누적추가3949/잔여181114·실제대화203·원본보존은 trader-ai/docs/implementation_691_financial_review_final_20261031.md 및 output/reconciliation691/ 참조.

### 692~697 추가 검증·구형 태그·매출 범위 구분 (2026-10-31)

최신 매퍼 `trader-data:coverage696`, 원 수집기 `trader-data:history693`, Go `trader-controller:frame686`, CPU API `trader-ai:cpu697`이다. 등록된 읽기 전용 근거는103범위이며 원문·원 재무/가격·기존 승인을 유지한다. 이번11,723개 매핑 범위를 추가 검증했고 동결 잔여169,391개를 유지한다. 전체 매핑 완료를 뜻하지 않는다.

`financial_history674.legacy_revenue_approval`은 모든 지원 시작일의 원/후속 값과 실제 SEC 관측을 확인한 회사·종료일 한정 구형 `SalesRevenueNet` 선택이다. `FINANCIAL_HISTORY_RAW_ENABLED` 및 `FINANCIAL_LEGACY_REVENUE693_ENABLED` 아래 미래 ETL hook에서 근거를 준비한다. 현대 태그가 가능하면 기존 정책을 쓰며 금액 차이·최초 공시 부재·외국 공시 미검토 등을 우회하지 않는다. Kafka가 중지된 현재 원 수집기 수명은 정상으로 보장하지 않는다. 검증된 유한 준비 작업 및 Go `rawVerifiedRepair` 순차 회차는 별도로 완료했다. 새 예약은 만들지 않았다.

`error_scope692`는 원문의 명시 연간 범위/정정 표 그룹/0을 포함한 성분별 사유를 결속한다. `revenue_coverage694`는 원 손익계산서 부분매출과 동일 총매출을 구분하고, 별도696 계약은 보험료·서비스 합계와 투자수익을 분리한다. `revenue_scope_review695` 및 `bulk_reconciliation681`은 회사 식별된 원/선택 URL·문서/표SHA·연도/단위를 포함한 검토 카드만 생성한다. `approvalGenerated=false`, 실제 기간·회계 원인은 별도 검토가 필요하다. SHA 기준 파싱 캐시는 최대2문서다.

기존 Go `reviewedReconciliation`의 명시 기업<=50·연간·등록 근거 재검증 계약을 재사용한다. 전체 검사408·AI 재무/비교916·실제 HTTPS140검사, 원본/승인 독립 감사와 재생 신규0을 확인했다. 기업 비교의 선택 달력연도 경로는 매핑/값을 회사별로 조회하며 동일 기간·태그·증권 검증·총20,000행 제한을 보존한다. 상세 근거·전후 대화·잔여 원인은 `trader-ai/docs/implementation_692_697_financial_review_20261031.md`, 영수증은 `output/reconciliation692~696/`에 있다.

### 698 원 공시 소계 계층·반올림에서 상세값으로의 연결

최신 매퍼는 `trader-data:bridge698`, 등록 읽기 전용 근거는105범위다. `pipeline/revenue_bridge698.py`는 HGBL의 상세→소계→기타매출→총매출 계층을 한 번씩만 합산하고, MTEX의 같은 원 공시 요약·상세 및 후속 상세표를 2개 연도 반올림/단위/실제 결산일/전체 소스 해시로 연결한다. 기존 정밀도 정책을 반대로 완화하지 않는다. 모두 `restatementClaimed=false`, 원 관측값 보존이며 등록되지 않은 새 기간/공시/회계 원인은 자동 승인하지 않는다. 기존 Go `reviewedReconciliation` 및 Python 재검증 경로로 신규2/재생신규0, 워커448·운영 Go10·실제 HTTPS12검사를 통과했다. 새 스케줄·다운로드0. 상세 근거와 전후 대화는 `trader-ai/docs/implementation_698_revenue_bridges_20261031.md`, 최종 감사와 정확 잔여는 `output/reconciliation698/`을 참조한다.
# RSS / GDELT 뉴스 후보 수집

### 2026-11-02 뉴스 영속 복구·실제 Kafka 중단/재개 운영 반영

기존 KIS/SEC 복구를 NEWS로 확장했다. **새 관리자 뉴스 실행/새 예약에서 생성되는 작업부터**
Go의 `NEWS_RECOVERY_ENABLED=true`가 `recoveryVersion: news-coordinated-v1`을 고정한다.
기존 job/체크포인트/예약은 수정·자동 재생하지 않는다. 아래의 이전 KIS/SEC 한정 검증 기록은
그 당시 범위이며, 이번 확장·실제 전송 결과는 `output/news-recovery-20261102/`가 정본이다.

연결 경로:

```text
관리자 news/runs → 기존 JOB_ITEM_QUEUED Kafka → 기존 공유 NEWS 수집 워커
 → 원문 + RAW_READY 영수증 atomic commit
 → 전용 복구 소비자의 raw intent 인계 → 격리 outbox → 실제 Kafka ACK
 → NEWS 후보 생성 → 원문 재생 검증 → append-only 저장 → 새 DB 연결 검증
 → COMPLETE → Kafka offset commit
```

- `pipeline/coordinated_news.py`가 RSS/GDELT 원문 바이트·SHA·수집 범위를 JSON envelope에
  결속하고 기존 파서·종목 선택·관련성 분류를 재사용한다. 원문은 마운트된 영속 EBS에 저장한다.
  기존 수집기의 S3 환경을 전역 변경하지 않는다. 원문이 확보된 뒤에는 재개 시 외부 재요청하지 않는다.
- `workers/coordinated_news_collector.py`는 원문 체크포인트까지만 맡는다. 원문 인계와 후속 단계는
  `workers/coordinated_news_consumer.py`가 담당해 두 프로세스의 단계 선점 경쟁을 피한다.
- 요청 전 영속 예산 예약과 기존 `news_collection_pacing`의 공유 5.1초 간격을 모두 적용한다.
  전송 중 종료되어 결과가 불명확한 요청은 PENDING으로 남기며 자동 재전송하지 않는다.
  기본 최대 요청3/재시도2/lease300초/전체24시간 계약은 같은 작업에서 초기화되지 않는다.
- 뉴스 전용 토픽 `trader.raw.coordinated.news.ready649`와 별도 그룹
  `coordinated-news-production649`만 사용한다. 기존 Kafka 토픽·그룹 offset을 리셋하지 않았다.
  `trader-news-recovery649`는 기존 색인/Python EC2의 추가 컨테이너이며 새 EC2가 아니다.
  원문 마운트는 이 소비자에서 읽기 전용이다. 예기치 않은 프로세스 실패 재시작은 최대3회이며,
  인증·원문 해시·불명확한 커밋/요청·검토 상태를 자동 우회하지 않는다.
- 이미 COMPLETE인 메시지가 재전달돼도 새 읽기 전용 DB 연결로 실제 관측값·원문 SHA를
  다시 확인한 뒤 offset을 확정한다. 원장 상태만 보고 무조건 ACK하지 않는다.
- 원문 누락 제목은 검토 사유를 보존한다. 기사 URL의 추가 제목 수집은 별도 예산을 가진
  기존 append-only 보완 작업에서 수행한다. 이번 경로는 해당 외부 사이트를 몰래 재요청하지 않는다.
- 파싱된 GKG 유효 행을 저장해도 원문의 잘못된 행이 있으면 결과 `needsReview=true`를 유지한다.
  기사 본문 확보·기사 발행일/종목 식별 검증·public/RAG 승격을 했다는 뜻이 아니다.

실제 검증 결과:

| 검증 | 결과 |
|---|---|
| RSS 수집기: RAW_READY 직후 SIGKILL | 새 프로세스로 원문 재요청 없이 인계 |
| RSS 소비자: DB COMPLETE 후 offset 확정 전 SIGKILL | 동일 그룹 재전달, DB 중복0, offset0→다음1 확정 |
| GDELT 15분1구간 실제 수집·전송 | 요청1, NVDA 관측2, Kafka offset1. 잘못된 원문6행은 검토 보존 |
| 일반 관리자 API→기존 Kafka 명령→공유 워커→복구 소비자 | 수동 수집기 호출 없이 RSS 관측3, offset2→다음3 확정 |
| 전체 신규 뉴스 작업3개 | 외부 요청 합계3, 재시도0, 재개 추가 요청0, 각6단계 영수증 |
| 결과 분류 | 정상 성공2, 품질 검토1, 미완료0·커밋 불명0 |
| 기존 데이터 | 기존 뉴스 후보2,776/관측2,780 및 기존 금융/작업 행의 정확 해시 불변 |

기존 DB·origin·Redis·색인·Go 서버는 유지했고, 실제 전송에 필요한 기존 Kafka 한 대를 추가 기동했다.
GPU는 중지 상태다. 모든 검증용 프로세스는 종료됐으며 운영 공유 워커·전용 뉴스 소비자는 실행 중이다.
신규 금융/주가 수집·기존 백필 재개·정기 뉴스 수집 예약 활성화·데이터 삭제는 하지 않았다.

검증 영수증: `final-audit.json`, `runtime-test.json`, `proof-replay-rss.json`,
`proof-consume-gdelt.json`, `completion.json`. 독립 DB 감사는 원문 SHA/실제 저장값/관리자 후보 조회와
Kafka partition·offset·일반 명령 processed_event를 대사했다. 로컬 회귀152개와 추가 소유권 검사,
Go 복구/뉴스/API/앱/outbox 검사도 통과했다. 원격 적용은 `control649`의 NEWS 후보 유형·격리 토픽
허용 목록만 추가했다. V053은 전체 마이그레이션 순서에서 FINANCIAL_REVISION을 유지하도록 작성했고,
V043~45만 적용된 현 서버에서는 기존 PRICE/FINANCIAL에 NEWS만 추가한 범위로 적용했다.

### 2026-11-02 영속 복구 운영 활성화·실제 SEC 중단 검증

운영 Go의 `RECOVERY_COORDINATOR_ENABLED=true`와 전용 `control649.pipeline_recovery_*`
8개 테이블을 활성화했다. 기존 public 금융 데이터와 기존 작업을 새 경로로 자동 편입하지 않는다.
관리자 영속 복구 화면은 읽기 전용이며, 여는 것만으로 수집이나 재개가 시작되지 않는다.

실제 SEC AAL 한 기업의 submissions/companyfacts를 2회 요청해 원문 2개를 영속 디스크에 저장했다.
`RAW_READY`에서 일시정지·첫 프로세스 종료 후 다른 컨테이너가 이어받았다. 검증 중 발견한
인계 오류와 최종 새 연결 오류도 영수증을 먼저 대사한 뒤 보완했다. 최종적으로
`run_cycle`이 종료된 이전 프로세스의 만료된 lease를 확인하고 `PUBLISHED`부터 이어받아
새 DB 연결 독립 검증과 `COMPLETE`까지 마쳤다. 추가 SEC 요청 0, 요청 수 전후 2,
재시도 0, 완료 재실행은 NO_OP, 6개 단계 영수증과 미확정 요청 0을 확인했다.

이 검증은 격리된 `control649`에 새 Assets fact·매핑 각 1건만 추가했다.
공식 원문은 기존 보관 공시의 SHA 검증본을 사용했다. 기존 12개 테이블의 원래 행 집합
해시가 동일하며 기존 값 UPDATE/DELETE는 하지 않았다. 증거는
`output/recovery-activation-20261102/`와 서버의 `/var/lib/trader-recovery/proof-20261102/`에 보존한다.

운영 보완:

- `RECOVERY_RAW_TOPIC=trader.raw.coordinated.ready649`를 Go와 Python에 명시한다.
  기존 격리 토픽 차단 함수에는 이 격리 토픽 하나만 추가했다. 기본/다른 토픽을 우회 허용하지 않는다.
- SEC XBRL의 공식 GAAP namespace는 연도형과 실제 연월일형을 지원한다.
  정확한 fasb.org 호스트·유효 날짜·원문 SHA·CIK·기간·단위·값 일치를 유지한다.
- 최종 독립 DB 연결은 마스킹된 `connection.dsn`을 재사용하지 않는다.
  `db.info.dsn_parameters`와 `db.info.password`를 메모리에서만 사용해 동일 DB에 새로 연결하고,
  기존 `confirm_publication`의 연결 대상 대사·읽기 전용 검증을 그대로 수행한다.

적용 범위와 미검증 범위:

- 현재 coordinator 공급자는 KIS/SEC다. 이번 실제 공급자 검증은 SEC 1기업이며 KIS 실제 재수집을 하지 않았다.
- 재개 스케줄러는 명시적으로 승인·해시 고정한 plan/registry만 처리한다.
  정기 스케줄이나 모든 기존 큐를 자동 복구하는 서비스는 새로 시작하지 않았다.
  PAUSED/REVIEW·미확정 HTTP/커밋은 자동 우회하지 않는다. 예산·절대시한도 초기화하지 않는다.
- Kafka는 기존 중지 상태다. outbox 한 건은 실제 `PENDING`으로 보존했으며 Kafka 전송/offset 검증은
  이번 범위에 포함하지 않았다. DB 단계 완료와 Kafka 전달 완료는 별도 상태다.
- 뉴스 RSS/GDELT는 아래의 자체 checkpoint 복구 계약을 사용한다. 이 활성화로 뉴스 전체가
  KIS/SEC coordinator에 자동 편입된다고 해석하지 않는다.
- 서버·DB/Redis/색인/Go는 실행 상태를 유지했다. GPU/Kafka를 추가 기동하지 않았다.

### 제목 보완·종목별 관련성 자동 분류 (2026-11-02, 배포 전)

새 Go 종목 선택 계획은 `enrichmentPolicy: news-title-relevance-v1`을 고정한다.
Python `news_collector`가 제공자 원문 검증·종목 후보 선택 후 `pipeline/news_enrichment.py`를 실행한다.
기존 계획의 해시·체크포인트는 바꾸지 않으며 정책 없는 과거 작업은 기존 방식으로 재생된다.

- 제공자 제목이 있으면 유지한다. 누락 제목만 기사 URL의 `og:title`, `twitter:title`, `<title>` 순서로 보완한다.
- `providerTitle`, `titleEnrichment`(방법·현재 조회 시각·최종 URL·응답 SHA·robots 결과), `assetRelevance`(종목별 분류·사유·제목 근거·정책 버전)를 관측 JSON에 저장한다.
- 분류는 `DIRECT` 직접 관련, `INDIRECT` 경쟁 제품·업계 관련, `MENTION` 단순 언급·시장 목록, `REVIEW` 판단 보류다. 원문 전체를 확인한 사실 판정이나 투자 영향 판단이 아니다. 조직 목록/검색 조건만으로 직접 관련으로 승격하지 않는다.
- HTML은 메모리에서 최대512KiB만 처리하며 본문은 저장하지 않는다. `originalArticleVerified/tickerIdentityVerified/ragEligible=false`를 유지한다. 현재 페이지의 제목을 얻어도 과거 시점의 제목·기사 본문이 검증됐다는 뜻은 아니다.
- HTTP 링크는 HTTPS로 승격하며 실패해도 평문 HTTP로 재시도하지 않는다. DNS의 모든 주소가 공개 주소인지 검사하고 연결은 검증한 IP에 고정한다. 각 리다이렉트도 검사하며 TLS 호스트 검증을 유지한다.
- robots 허용·호스트 간격2초를 지킨다. 긴 crawl-delay, robots 접근 실패/금지, 로그인·오류 페이지,404/타임아웃은 검토 사유로 남기고 다음 후보를 처리한다. 우회 조회하지 않는다.
- 한 수집 구간에서 누락 URL 최대10개, 제목 단계60초, URL당12초, HTTP 최대40회, 리다이렉트2회다. 한도 밖 제목은 검토 대상으로 남긴다. 이 제한은 제공자 RSS/GDELT 요청 한도와 별도다.
- URL 조회 전 예약과 결과를 체크포인트에 저장한다. 중단 후 결과가 불명확한 요청을 자동 재전송하지 않는다. SQL·소유권·미분류 오류는 성공으로 바꾸지 않는다.

기존 저장 관측을 보완하려면 종료된 작업을 지정해 유한 배치로 실행한다. **기존 원문·관측은 수정하지 않고**, `originalObservationHash`로 연결한 파생 관측을 추가한다. 관리자 후보 조회는 같은 원 관측의 최신 보완본을 보여준다. 마이그레이션·새 테이블·UPDATE/DELETE 권한은 필요하지 않다.

```powershell
# trader-data 디렉터리, 기존 DATABASE_URL/DB search_path를 설정한 환경
# 미리보기: DB 읽기만, 기사 사이트 요청/DB 쓰기 없음
python -m workers.news_enrichment_backfill --job-id 8 --limit 50 --output ../output/news-enrichment-job8
# 위 선택을 재사용해 조회·추가 저장. 서버를 자동 기동하지 않는다.
python -m workers.news_enrichment_backfill --job-id 8 --output ../output/news-enrichment-job8 --resume --apply
```

출력 디렉터리에 `STOP`을 만들면 다음 요청/저장 전 중단한다. 재개 시 같은 출력 디렉터리와 `--resume`을 사용한다. 새 디렉터리는 새로운 실행이므로 알 수 없는 이전 요청을 다시 보내는 용도로 사용하지 않는다. 실패·접근 제한 제목의 재검토와 한도 밖 후보 처리는 별도 범위를 검토한 뒤 실행한다. 이 코드 추가만으로 예약·기존 DB 보완·운영 배포를 실행하지 않는다.

검증: `output/news-enrichment-20261102/`. Python111검사 통과, Go71검사·프론트 빌드/lint 통과. 기존7개 예시 재생에서 제공자 제목4개는 직접2/간접2로 분류했다. 누락 제목3개는 실제 조회 결과 전송 실패1·긴 crawl-delay1·robots 금지1로 판단 보류다. 제목 보완 성공 경로와 시장 목록 단순 언급은 별도 고정 입력 검사로 검증했다. 운영 DB 저장·관리자 브라우저 클릭 검증은 아직 하지 않았다. EC2/GPU 기동0, DB 쓰기0.

2026-11-02 확장: `pipeline/news_targets.py`와 collector가 종목 선택 `news-v2`를 지원한다.
Go의 활성 종목 스냅샷/자동 RSS queryKey를 검증한 뒤 GKG의 회사명·티커 문맥 언급 후보를
필터링한다. RSS는 해당 검색어의 대상 종목에 결속하고 제목 미언급 결과는 검토 표시를 보존한다.
본문/직접 관련성/역사적 코드 검증으로 승격하지 않는다. 선택 대상 밖의 GKG 행도 원본 ZIP에는
보존하고 excludedByTargetMetadata에 기록한다. 새 관측의 collectionContractHash는 작업 범위와
뉴스 후보 조회를 결속하며 원문 재사용 시 다른 선택 범위가 섞이는 것을 방지한다.
기존 v1 작업·체크포인트를 지원하며 기존 데이터 수정이나 DDL은 없다. 운영 보완판
`trader-data:newsstocks20261102b`는 한글 종목 표시명과 연결된 영문 company_name을 구분해 사용한다.
표시명을 변경하거나 DB 연결을 새로 매핑하지 않았다. 실제 RSS 후보4 / GKG NVDA2·AAPL1 및 잘못된6행의
검토 상태를 확인하고 새 연결에서 원문 SHA·정확 관측값·DB 후보 조회·Kafka 영수증을 대사했다.
기존 v1 후보 조회도 유지하며 정기 예약은 미활성이다. 최초 0건 진단과 보완 기록은
`output/news-stock-production-20261102/`에 별도로 보존한다.

2026-11-01 운영 공유 워커를 `trader-data:news20261101`로 교체했다. 기존 dispatcher/consumer는
뉴스 변경을 제외한 소스 해시가 일치했고, 동일 Kafka topic/group·512MiB 한도·인증·신뢰 마운트를 유지했다.
실제 RSS/GKG 두 item의 요청·원문 SHA·후보 집합·Kafka 영수증을 독립 확인했다. GKG의 유효 2,772행은
보관했지만 잘못된 6행은 `NEWS_PARTIAL_SCHEMA_REVIEW_REQUIRED`로 남겼다. 뉴스는 public/RAG로 승격하지 않았다.
원격 스키마·배포·검증 영수증은 `output/news-collection-production-20261101/`을 참조한다.

`workers.kafka_pipeline_consumer` → `kafka_dispatcher` → `news_collector`로 연결한다.
`NEWS_RSS_COLLECT` / `NEWS_GDELT_COLLECT`, source `NEWS`, pipelineMode `COLLECT_ONLY`,
contract `news-v1`만 지원한다. 활성화 기본값은 `NEWS_COLLECTION_ENABLED=false`다.
새 EC2나 독립 Python 서버는 필요하지 않으며, 기존 색인/Python 호스트를 사용한다.

- RSS는 날짜별 검색 결과 전체(최대 200개)를 읽으며 실험용 첫 20개 표본 제한을 쓰지 않는다.
  UTC 날짜를 다시 검증한다. 결과 100개 이상은 포화 가능성을 표시한다. 전체 과거 기사
  수집이나 원문 발행일 검증을 의미하지 않는다. 연도 경계/키워드 결과 밖 누락을 복구했다고 주장하지 않는다.
- GDELT는 고정 HTTPS GKG 아카이브 한 개/15분을 읽고 27열·ZIP 멤버·크기를 검증한다.
  시도당 압축 32MiB, 해제 128MiB, 100,000행을 넘으면 성공으로 자르지 않고 실패한다.
  조직/주제 목록은 단순 언급 후보이며 `publishedAt`·사건 날짜·검증된 티커로 변환하지 않는다.
- 연결/읽기 timeout 5/60초, 응답 수신이 120초를 넘으면 다음 chunk에서 중단한다(읽기 대기는 최대 60초).
  요청은 자동 HTTP 재시도 없이 최대 3회.
  408/429/500/502/503/504와 timeout/connection 오류만 2/4초 backoff 후 재시도한다.
  숫자 Retry-After는 최대 60초이며 헤더/원문 오류 본문을 로그에 출력하지 않는다.
  요청 전 시도 횟수·대기 시각을 DB에 기록하고 공급자별 공유 5.1초 요청 간격을 예약한다.
  중단 중에도 이미 예약한 시도는 소비된다. 소진하면 FAILED이며 빈 뉴스 성공으로 처리하지 않는다.
- 원문은 기존 LocalPipelineStorage(LOCAL/S3)와 source_object에 해시로 보관하고 체크포인트에 결속한다.
  체크포인트가 보존된 같은 item의 복구는 원문을 재읽고 해시를 검증해 외부 재요청 없이 정규화한다.
  저장과 체크포인트 사이의 프로세스 종료는 제한된 재요청을 일으킬 수 있다.
  실패 job의 새 retry job은 새 요청 예산을 갖는다. 성공 item은 복사하지 않는다.
- URL(호스트 대소문자·fragment만 정규화)로 candidate를 중복 제거하되 제목 유사도에 의한 병합이나
  Google 링크 디코딩/기사 본문 크롤링은 하지 않는다. 공급자별/원문별/검색어별 observation을 append-only로 보존한다.
- V051의 후보/관측 테이블에 저장하며 기존 news_article·금융 데이터는 수정하지 않는다.
  모든 metadata는 ragEligible=false, tickerIdentityVerified=false, originalArticleVerified=false다.
  원문의 잘못된 행은 invalid 수와 raw로 보존하고 유효 행을 저장해도 item은 FAILED/검토 필요로 남긴다.
- 예상 수집 오류는 해당 item을 FAILED로 종료해 다음 큐 item을 허용한다. SQL/원문 해시/미분류 오류는
  Kafka offset을 커밋하지 않고 워커를 중단한다. Go 관리자 취소는 요청 사이·backoff 중 관찰한다.
  진행 중 요청은 유한 timeout까지 기다린다. 숨겨진 자동 재시작/예약 활성화는 없다.

Go/관리자 UI의 구체적 요청 예시는 trader-controller README의 news-v1 절을 참조한다.
이 구현의 로컬 테스트는 mock HTTP/DB 경계 및 기존 다운로드 ZIP 재생이며 실제 운영 DB/Kafka/원격 수집
통합 검증은 서버를 켜지 않은 상태에서는 수행하지 않는다.

