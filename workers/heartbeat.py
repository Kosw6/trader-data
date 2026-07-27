import json
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone

import requests


LOGGER = logging.getLogger("worker-heartbeat")


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _bool_env(key, default=True):
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(key, default):
    value = os.getenv(key)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


class WorkerHeartbeatReporter:
    """Python worker node의 생존 상태를 trader-controller에 주기적으로 보고한다.

    controller는 이 값을 `pipeline_worker_node_heartbeat`에 upsert하고,
    관리자 페이지는 이 테이블을 조회해 외부 worker node가 살아있는지 보여준다.
    """

    def __init__(
        self,
        controller_url,
        admin_api_key,
        node_name,
        target_key,
        running_workers,
        interval_seconds=30,
        metadata=None,
        enabled=True,
    ):
        self.controller_url = (controller_url or "").rstrip("/")
        self.admin_api_key = admin_api_key or ""
        self.node_name = node_name
        self.target_key = target_key
        self.interval_seconds = max(5, interval_seconds)
        self.enabled = enabled and bool(self.controller_url) and bool(self.admin_api_key)
        self.stop_event = threading.Event()
        self.thread = None
        self.lock = threading.Lock()
        idle_since = _utc_now_iso()
        base_metadata = dict(metadata or {})
        base_metadata.setdefault("idleSince", idle_since)
        self.state = {
            "status": "IDLE",
            "idle": True,
            "acceptingNewWork": True,
            "runningWorkers": running_workers or [],
            "runningJobItems": 0,
            "processingSourceObjects": 0,
            "metadata": base_metadata,
        }

    @classmethod
    def from_env(cls, worker_name, metadata=None):
        """환경 변수 기반으로 heartbeat reporter를 생성한다."""
        hostname = socket.gethostname()
        node_name = os.getenv("WORKER_NODE_NAME") or f"{hostname}:{os.getpid()}"
        target_key = os.getenv("WORKER_TARGET_KEY", "python-worker-node")
        controller_url = os.getenv("TRADER_CONTROLLER_URL") or os.getenv("CONTROLLER_URL", "")
        admin_api_key = os.getenv("ADMIN_API_KEY", "")
        interval_seconds = _int_env("WORKER_HEARTBEAT_INTERVAL_SECONDS", 30)
        enabled = _bool_env("WORKER_HEARTBEAT_ENABLED", True)
        base_metadata = {
            "hostname": hostname,
            "pid": os.getpid(),
            "workerName": worker_name,
            "startedAt": _utc_now_iso(),
        }
        if metadata:
            base_metadata.update(metadata)
        return cls(
            controller_url=controller_url,
            admin_api_key=admin_api_key,
            node_name=node_name,
            target_key=target_key,
            running_workers=[worker_name],
            interval_seconds=interval_seconds,
            metadata=base_metadata,
            enabled=enabled,
        )

    def start(self):
        """background heartbeat 전송을 시작한다."""
        if not self.enabled:
            LOGGER.warning(
                "worker heartbeat disabled or missing controller credentials controller_url=%s node=%s",
                self.controller_url,
                self.node_name,
            )
            return
        self._send_once()
        self.thread = threading.Thread(target=self._run, name="worker-heartbeat", daemon=True)
        self.thread.start()

    def mark_busy(self, running_job_items=1, processing_source_objects=0, metadata=None):
        """worker가 메시지나 raw object를 처리하기 시작했음을 표시한다."""
        self.update(
            status="RUNNING",
            idle=False,
            accepting_new_work=True,
            running_job_items=running_job_items,
            processing_source_objects=processing_source_objects,
            metadata=metadata,
        )

    def mark_idle(self, metadata=None):
        """현재 처리 중인 일이 없음을 표시한다."""
        self.update(
            status="IDLE",
            idle=True,
            accepting_new_work=True,
            running_job_items=0,
            processing_source_objects=0,
            metadata=metadata,
        )

    def mark_error(self, metadata=None):
        """worker가 치명적인 예외를 만났음을 표시한다."""
        self.update(
            status="ERROR",
            idle=False,
            accepting_new_work=False,
            running_job_items=0,
            processing_source_objects=0,
            metadata=metadata,
        )

    def stop(self, status="STOPPED"):
        """worker 종료 직전에 마지막 heartbeat를 전송한다."""
        self.update(
            status=status,
            idle=True,
            accepting_new_work=False,
            running_job_items=0,
            processing_source_objects=0,
        )
        self.stop_event.set()
        self._send_once()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def update(
        self,
        status=None,
        idle=None,
        accepting_new_work=None,
        running_job_items=None,
        processing_source_objects=None,
        metadata=None,
    ):
        """heartbeat payload에 들어갈 상태 값을 갱신한다."""
        with self.lock:
            previous_idle = self.state["idle"]
            if status is not None:
                self.state["status"] = status
            if idle is not None:
                self.state["idle"] = idle
            if accepting_new_work is not None:
                self.state["acceptingNewWork"] = accepting_new_work
            if running_job_items is not None:
                self.state["runningJobItems"] = running_job_items
            if processing_source_objects is not None:
                self.state["processingSourceObjects"] = processing_source_objects
            next_metadata = dict(self.state["metadata"])
            if metadata:
                next_metadata.update(metadata)
            if idle is not None and idle != previous_idle:
                if idle:
                    next_metadata["idleSince"] = _utc_now_iso()
                else:
                    next_metadata.pop("idleSince", None)
            self.state["metadata"] = next_metadata

    def _run(self):
        while not self.stop_event.wait(self.interval_seconds):
            self._send_once()

    def _send_once(self):
        if not self.enabled:
            return
        payload = self._payload()
        try:
            response = requests.post(
                f"{self.controller_url}/api/v1/worker-agent/heartbeat",
                headers={
                    "Content-Type": "application/json",
                    "X-Admin-API-Key": self.admin_api_key,
                },
                data=json.dumps(payload, ensure_ascii=False),
                timeout=5,
            )
            response.raise_for_status()
        except Exception as exc:
            LOGGER.warning("worker heartbeat report failed node=%s error=%s", self.node_name, exc)

    def _payload(self):
        with self.lock:
            state = dict(self.state)
            metadata = dict(state.get("metadata") or {})
        return {
            "nodeName": self.node_name,
            "targetKey": self.target_key,
            "status": state["status"],
            "idle": state["idle"],
            "acceptingNewWork": state["acceptingNewWork"],
            "runningWorkers": state["runningWorkers"],
            "runningJobItems": state["runningJobItems"],
            "processingSourceObjects": state["processingSourceObjects"],
            "metadata": metadata,
        }
