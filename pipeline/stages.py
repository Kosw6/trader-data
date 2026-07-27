"""Small runtime helpers that make worker responsibility boundaries explicit.

The current MVP workers still run collection and database loading in one
process. These helpers record the same flow as named stages so the code can be
split later into:

1. collect-only worker: external API -> raw object
2. ETL worker: raw object -> normalized database rows
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter


class PipelineMode:
    """Worker execution modes used in checkpoints and later controller policy."""

    COMBINED = "COMBINED_COLLECT_TRANSFORM_LOAD"
    COLLECT_ONLY = "COLLECT_ONLY"
    ETL_ONLY = "ETL_ONLY"


class PipelineStage:
    """Stable stage names shared by BLS, SEC, and KIS workers."""

    RESOLVE_TARGET = "RESOLVE_TARGET"
    COLLECT_RAW = "COLLECT_RAW"
    WRITE_RAW_PREVIEW = "WRITE_RAW_PREVIEW"
    TRANSFORM = "TRANSFORM"
    LOAD_DB = "LOAD_DB"
    COMPLETE_ITEM = "COMPLETE_ITEM"


@dataclass
class StageRecord:
    """One measured stage execution stored in a job checkpoint."""

    name: str
    status: str
    duration_ms: int
    metadata: dict = field(default_factory=dict)
    error: str | None = None


class PipelineRun:
    """Collects stage timing and status for one pipeline_job_item."""

    def __init__(self, worker_name, mode=PipelineMode.COMBINED):
        self.worker_name = worker_name
        self.mode = mode
        self.records = []

    @contextmanager
    def stage(self, name, **metadata):
        """Measure a logical pipeline stage and store success/failure metadata."""
        started = perf_counter()
        try:
            yield
        except Exception as error:
            self.records.append(
                StageRecord(
                    name=name,
                    status="FAILED",
                    duration_ms=int((perf_counter() - started) * 1000),
                    metadata=metadata,
                    error=str(error),
                )
            )
            raise
        else:
            self.records.append(
                StageRecord(
                    name=name,
                    status="SUCCESS",
                    duration_ms=int((perf_counter() - started) * 1000),
                    metadata=metadata,
                )
            )

    def checkpoint(self):
        """Return JSON-serializable pipeline runtime metadata."""
        return {
            "worker": self.worker_name,
            "mode": self.mode,
            "stages": [
                {
                    "name": record.name,
                    "status": record.status,
                    "duration_ms": record.duration_ms,
                    "metadata": record.metadata,
                    "error": record.error,
                }
                for record in self.records
            ],
        }
