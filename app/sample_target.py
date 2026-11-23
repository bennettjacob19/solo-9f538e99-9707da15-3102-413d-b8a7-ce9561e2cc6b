"""Local sample target exposing a Prometheus latency histogram.

Two instances of this app are started with different ``SAMPLE_BUCKETS``
values so the verifier has to cope with differing bucket layouts.

Runtime controls (used by tests and demos):

* ``POST /inject``  -- add one or more request observations
                       body: ``{"duration_seconds": 0.23, "count": 1}``
* ``POST /restart`` -- simulate a process restart: new run id, new process
                       start time, all counters reset to zero
* ``GET  /state``   -- inspect the in-memory counters
"""

from __future__ import annotations

import os
import time
import uuid

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

METRIC_NAME = "http_request_duration_seconds"
DEFAULT_BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
DEFAULT_BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class InjectBody(BaseModel):
    duration_seconds: float = Field(..., ge=0)
    count: int = Field(1, ge=1, le=100_000)


class TargetState:
    def __init__(self, instance_id: str, boundaries: list[float]) -> None:
        self.instance_id = instance_id
        self.boundaries = boundaries
        self.reset()

    def reset(self) -> None:
        """Restart the (simulated) process."""
        self.run_id = uuid.uuid4().hex[:12]
        self.process_start = time.time()
        # cumulative counts per finite boundary + +Inf
        self.bucket_counts: dict[float, int] = {b: 0 for b in self.boundaries}
        self.total_count = 0
        self.total_sum = 0.0
        self.scrape_seq = 0
        self.restart_count = getattr(self, "restart_count", 0) + 1

    def inject(self, duration: float, count: int) -> None:
        for boundary in self.boundaries:
            if duration <= boundary:
                self.bucket_counts[boundary] += count
        self.total_count += count
        self.total_sum += duration * count


def _read_buckets(instance_id: str) -> list[float]:
    raw = os.environ.get("SAMPLE_BUCKETS")
    if raw is None:
        raw = DEFAULT_BUCKETS_A if instance_id.endswith("a") else DEFAULT_BUCKETS_B
    values = [float(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise ValueError("SAMPLE_BUCKETS must contain at least one boundary")
    return sorted(values)


def create_app() -> FastAPI:
    instance_id = os.environ.get("INSTANCE_ID", "sample-a")
    state = TargetState(instance_id, _read_buckets(instance_id))
    app = FastAPI(title=f"sample-target-{instance_id}", version="1.0.0")
    app.state.target = state

    @app.post("/inject")
    def inject(body: InjectBody) -> dict:
        state.inject(body.duration_seconds, body.count)
        return {"ok": True, "total_count": state.total_count}

    @app.post("/restart")
    def restart() -> dict:
        state.reset()
        return {"ok": True, "run_id": state.run_id}

    @app.get("/state")
    def get_state() -> dict:
        return {
            "instance_id": state.instance_id,
            "run_id": state.run_id,
            "process_start_time_seconds": state.process_start,
            "boundaries": state.boundaries,
            "bucket_counts": state.bucket_counts,
            "count": state.total_count,
            "sum": state.total_sum,
            "restart_count": state.restart_count,
        }

    # Plain-text exposition, returned with the Prometheus content type.
    @app.get("/metrics", response_class=PlainTextResponse)
    def render_metrics() -> PlainTextResponse:
        lines = [
            f"# HELP {METRIC_NAME} request latency in seconds",
            f"# TYPE {METRIC_NAME} histogram",
        ]
        for boundary, value in state.bucket_counts.items():
            lines.append(
                f'{METRIC_NAME}_bucket{{le="{boundary:g}",run_id="{state.run_id}"}} {value}'
            )
        lines.append(
            f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{state.run_id}"}} {state.total_count}'
        )
        lines.append(f'{METRIC_NAME}_count{{run_id="{state.run_id}"}} {state.total_count}')
        lines.append(f'{METRIC_NAME}_sum{{run_id="{state.run_id}"}} {state.total_sum}')
        lines.extend(
            [
                "# HELP process_start_time_seconds start time of the process since unix epoch",
                "# TYPE process_start_time_seconds gauge",
                f"process_start_time_seconds {state.process_start:.6f}",
                "# HELP sample_scrape_seq number of scrapes served since process start",
                "# TYPE sample_scrape_seq gauge",
                f"sample_scrape_seq {state.scrape_seq}",
            ]
        )
        state.scrape_seq += 1
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app


app = create_app()
