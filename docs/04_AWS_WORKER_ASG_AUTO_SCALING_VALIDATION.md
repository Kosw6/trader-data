# AWS Worker ASG 자동 확장 검증

> 검증일: 2026-07-08  
> 목적: Kafka lag와 worker heartbeat를 기준으로 Python worker ASG가 자동 scale-out/in 되는지 확인한다.

---

## 1. 검증 목적

Trader Data Platform은 DB, Kafka, Go controller를 source of truth/control plane으로 두고, Python worker node는 필요할 때만 실행되는 stateless 처리 노드로 둔다.

이번 검증의 목적은 다음 운영 흐름이 실제 AWS 환경에서 닫히는지 확인하는 것이다.

```text
Kafka lag 발생
-> Go controller scale planner가 policy 판단
-> AWS ASG desired capacity 증가
-> Python worker node 기동
-> job/raw 처리
-> lag 0 및 heartbeat idle 확인
-> AWS ASG desired capacity 감소
```

---

## 2. 검증 전제

Python worker target은 AWS ASG driver로 설정했다.

```text
target_key=python-worker-node
environment=AWS
driver=AWS_ASG
min_capacity=0
max_capacity=1
desired_capacity=0
scale_in_idle_seconds=120
autoScalingGroupName=trader-python-worker-asg
region=ap-northeast-2
```

job control-plane topic도 scale policy 대상으로 추가했다.

```text
policy_key=job-event-worker-policy
source=JOB
worker_role=COMBINED
worker_name=kafka_pipeline_consumer
topic=trader.jobs.events
consumer_group=trader-data-workers
scale_out_lag_threshold=1
scale_in_idle_seconds=120
```

이 정책은 source별 raw ETL topic이 아니라, 모든 수집 job의 최초 진입점인 `trader.jobs.events`를 감시한다.

---

## 3. 검증 흐름

관리자 페이지에서 BLS 단건 job을 생성했다.

초기 상태:

```text
Python worker node 없음
ASG desired capacity=0
```

Kafka lag:

```text
topic=trader.jobs.events
consumer_group=trader-data-workers
total_lag=1
threshold_lag=1
status=WARN
commitOffsets={"0":3}
latestOffsets={"0":4}
partitionLags={"0":1}
```

이 lag를 기준으로 `worker-scale-planner`가 scale-out command를 생성했다.

```text
id=14
command_type=SCALE_OUT
requested_capacity=1
previous_capacity=0
reason=KAFKA_LAG_THRESHOLD_EXCEEDED
triggered_by=LAG_MONITOR
status=SUCCEEDED
details.topic=trader.jobs.events
details.source=JOB
details.policyKey=job-event-worker-policy
details.totalLag=1
details.scaleOutLagThreshold=1
```

ASG desired capacity가 1로 변경되고 Python worker node가 기동했다.

---

## 4. Worker 처리 결과

Python worker가 `JOB_ITEM_QUEUED`를 소비했고 BLS raw 수집이 완료되었다.

```text
pipeline_job_item:
id=42
item_key=BLS:CES0500000003
status=COLLECTED
started_at=2026-07-08 11:15:01+09
completed_at=2026-07-08 11:15:02+09
```

worker heartbeat:

```text
node_name=python-worker-node-i-005e4560531a011c6
status=IDLE
idle=true
running_job_items=0
processing_source_objects=0
idleSince=2026-07-08 11:15:02+09
```

---

## 5. 자동 Scale-In 결과

작업 완료 후 모든 관련 Kafka lag가 0이 되었고, worker heartbeat가 120초 이상 idle 상태를 유지했다.

scale-in command:

```text
id=15
command_type=SCALE_IN
requested_capacity=0
previous_capacity=1
reason=IDLE_TIMEOUT
triggered_by=SYSTEM
status=SUCCEEDED
requested_at=2026-07-08 11:17:05+09
completed_at=2026-07-08 11:17:11+09
```

scale-in 판단 근거:

```text
trader.jobs.events lag=0
trader.raw.kis.ready lag=0
trader.raw.bls.ready lag=0
trader.raw.sec.ready lag=0
trader.raw.news.ready lag=0
oldestIdleSince=2026-07-08 11:15:02+09
scaleInIdleSeconds=120
```

ASG 결과:

```text
AutoScalingGroup=trader-python-worker-asg
Desired=0
Min=0
Max=1
Instance i-005e4560531a011c6=Terminating
```

---

## 6. 코드 보완 사항

검증 중 `job-event-worker-policy`를 `worker_role=COMBINED`로 두었을 때 scale planner가 감지하지 못하는 문제가 있었다.

원인:

```text
scale planner가 worker_role='ETL' 정책만 후보로 조회하고 있었음
```

수정:

```text
worker_role IN ('ETL', 'COMBINED')
```

수정 후 `trader.jobs.events` lag 1을 기준으로 `SCALE_OUT` command가 정상 생성되었다.

또한 worker heartbeat에 `idleSince` metadata를 추가했다. 단순히 `last_heartbeat_at`만 있으면 worker가 정상적으로 heartbeat를 계속 보내는 동안 idle 지속 시간을 판단할 수 없기 때문이다.

---

## 7. 운영 판단

이번 검증으로 다음 운영 시나리오가 닫혔다.

```text
job lag 기반 scale-out
-> AWS ASG worker node 기동
-> Python worker가 job event 소비
-> raw 수집 완료
-> idle heartbeat 및 전체 topic lag 0 확인
-> idle timeout 기반 scale-in
-> AWS ASG worker node 종료
```

따라서 현재 구조에서 DB, Kafka, Go controller는 On-Demand로 유지하고, Python worker node는 ASG/Spot 후보로 운영하는 설계가 타당함을 확인했다.

