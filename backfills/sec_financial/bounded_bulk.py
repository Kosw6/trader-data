"""승인된 미보유 회사 snapshot만 순차 처리하는 한시 백필 실행기.

컨트롤러 API로 100개 이하 Job을 만들고 지정 item ID만 선점한다.
원본/진행 상태를 보존하며 자동 재시도나 매핑 승인은 하지 않는다.
"""

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.request import Request, urlopen

ROOT = Path("/app/pipeline_output/sec344")
MIN_FREE = 8 * 1024**3
BATCH_SIZE = 100


def validate_targets(targets):
    if not targets or len(targets) > 10000:
        raise ValueError("Target count outside approved safety bound")
    ids = [t["company_id"] for t in targets]
    ciks = [t["cik"] for t in targets]
    if len(ids) != len(set(ids)) or len(ciks) != len(set(ciks)):
        raise ValueError("Duplicate company/CIK in immutable plan")
    if any(len(c) != 10 or not c.isdigit() for c in ciks):
        raise ValueError("Invalid SEC identity")


def pause_reason(free_bytes, consecutive_failures, stop_requested):
    if stop_requested:
        return "PAUSED_STOP_REQUEST"
    if free_bytes < MIN_FREE:
        return "PAUSED_LOW_DISK"
    if consecutive_failures >= 3:
        return "PAUSED_CONSECUTIVE_FAILURES"
    return None


def save(path, data):
    tmp = path.with_suffix(".pending")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def main():
    from workers.db import connect_database
    from workers.job_repository import claim_queued_item_by_id, complete_item, fail_item
    from workers.sec_financial_worker import process_item

    ROOT.mkdir(parents=True, exist_ok=True)
    plan = json.loads(Path("/control/plan.json").read_text())
    targets = plan["targets"]
    validate_targets(targets)
    digest = hashlib.sha256(json.dumps(targets, sort_keys=True).encode()).hexdigest()
    assert digest == plan["sha256"], "Plan changed"
    progress_path = ROOT / "progress.json"
    progress = (
        json.loads(progress_path.read_text())
        if progress_path.exists()
        else {
            "status": "RUNNING",
            "planHash": digest,
            "targetCount": len(targets),
            "items": {},
            "batches": {},
            "consecutiveFailures": 0,
        }
    )
    assert progress["planHash"] == digest

    def update(status="RUNNING"):
        progress["status"] = status
        progress["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save(progress_path, progress)

    def api(path, payload=None):
        request = Request(
            "http://127.0.0.1:8090/api/v1/admin/pipeline/" + path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={
                "X-Admin-API-Key": os.environ["ADMIN_API_KEY"],
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=30) as response:
            return json.load(response)

    args = SimpleNamespace(
        include_history=True, all_facts=False, request_interval=1.0, output_dir=str(ROOT / "data")
    )
    conn = connect_database()
    try:
        for offset in range(0, len(targets), BATCH_SIZE):
            group = targets[offset : offset + BATCH_SIZE]
            batch = str(offset // BATCH_SIZE)
            reason = pause_reason(
                shutil.disk_usage(ROOT).free,
                progress["consecutiveFailures"],
                (ROOT / "STOP").exists(),
            )
            if reason:
                update(reason)
                return
            key = f"sec344-{digest[:20]}-{batch}"
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM pipeline_job WHERE idempotency_key=%s", (key,))
                existing = cur.fetchone()
            conn.rollback()
            if existing:
                detail = api("jobs/" + str(existing[0]))
            else:
                detail = api(
                    "jobs",
                    {
                        "jobType": "SEC_FINANCIAL_BACKFILL",
                        "source": "SEC",
                        "priority": 5,
                        "requestedBy": "USER_APPROVED_MISSING_ALL_344",
                        "idempotencyKey": key,
                        "parameters": {
                            "mode": "BACKFILL",
                            "includeHistory": True,
                            "allFacts": False,
                            "requestInterval": 1.0,
                        },
                        "itemKeys": ["SEC:" + t["cik"] for t in group],
                    },
                )
            allowed = {"SEC:" + t["cik"]: t for t in group}
            assert len(detail["items"]) == len(group)
            assert {i["itemKey"] for i in detail["items"]} == set(allowed)
            progress["batches"][batch] = detail["job"]["id"]
            update()
            for planned in detail["items"]:
                target = allowed[planned["itemKey"]]
                cik = target["cik"]
                if planned["status"] in {"SUCCESS", "SKIPPED", "FAILED", "CANCELLED"}:
                    progress["items"].setdefault(
                        cik, {"status": planned["status"], "itemId": planned["id"]}
                    )
                    update()
                    continue
                reason = pause_reason(
                    shutil.disk_usage(ROOT).free,
                    progress["consecutiveFailures"],
                    (ROOT / "STOP").exists(),
                )
                if reason:
                    update(reason)
                    return
                # 중단 후 RUNNING item을 임의 재선점하지 않는다. 사람의 검토가 필요하다.
                if planned["status"] != "QUEUED":
                    update("PAUSED_UNRESOLVED_ITEM")
                    return
                item = claim_queued_item_by_id(
                    conn, planned["id"], source="SEC", job_type="SEC_FINANCIAL_BACKFILL"
                )
                if not item or item.job_id != detail["job"]["id"]:
                    update("PAUSED_CLAIM_MISMATCH")
                    return
                progress["current"] = {
                    "cik": cik,
                    "ticker": target["ticker"],
                    "jobId": item.job_id,
                    "itemId": item.item_id,
                }
                update()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT EXISTS(SELECT 1 FROM financial_fact WHERE company_id=%s)",
                            (target["company_id"],),
                        )
                        now_present = cur.fetchone()[0]
                    conn.rollback()
                    if now_present:
                        complete_item(
                            conn,
                            item.item_id,
                            "SKIPPED",
                            error_code="SEC344_ALREADY_PRESENT",
                            error_message="계획 이후 다른 작업에서 적재되어 건너뜁니다.",
                        )
                    else:
                        process_item(conn, item, args)
                    progress["consecutiveFailures"] = 0
                except Exception as exc:
                    conn.rollback()
                    fail_item(conn, item.item_id, "SEC344_COLLECTION_FAILED", type(exc).__name__)
                    progress["consecutiveFailures"] += 1
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status,error_code,checkpoint FROM pipeline_job_item WHERE id=%s",
                        (item.item_id,),
                    )
                    status, error, checkpoint = cur.fetchone()
                conn.rollback()
                progress["items"][cik] = {
                    "ticker": target["ticker"],
                    "status": status,
                    "errorCode": error,
                    "jobId": item.job_id,
                    "itemId": item.item_id,
                    "stats": (checkpoint or {}).get("stats", {}),
                }
                update()
                print(json.dumps(progress["items"][cik]), flush=True)
        update(
            "COMPLETE_WITH_REVIEW"
            if any(i["status"] != "SUCCESS" for i in progress["items"].values())
            else "COMPLETE"
        )
    except Exception as exc:
        conn.rollback()
        progress["errorType"] = type(exc).__name__
        update("PAUSED_REVIEW_REQUIRED")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
