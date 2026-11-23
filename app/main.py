"""FastAPI application for the canary rollout SLO verification."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from pydantic import BaseModel

from .decisions import DEFAULT_TARGET, Decision, decide
from .histogram import combine_instances
from .scraper import ScrapeStore, store_lifespan


def _instance_urls() -> dict[str, str]:
    raw = os.environ.get(
        "SCRAPE_TARGETS",
        "sample-a=http://sample-a:9101/metrics,sample-b=http://sample-b:9102/metrics",
    )
    instances: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(f"invalid SCRAPE_TARGETS entry: {part!r}")
        name, url = part.split("=", 1)
        instances[name.strip()] = url.strip()
    if len(instances) < 2:
        raise ValueError("at least two scrape instances are required")
    return instances


def create_app() -> FastAPI:
    store = ScrapeStore(_instance_urls())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with store_lifespan(store):
            yield

    app = FastAPI(
        title="灰度放量核查 API",
        description=(
            "抓取两个本地 Prometheus 延迟直方图实例，每实例取最近两次抓取"
            "（均须在近 3 秒内、来自同一进程、间隔至少 1 秒），用桶计数增量"
            "计算两实例合计达标比例的上下界并给出放量建议。"
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.store = store

    class InstanceSample(BaseModel):
        earlier_scrape_at: float
        later_scrape_at: float
        process_id: str | None
        gap_seconds: float
        requests_in_window: int
        compliant_lower: int
        compliant_upper: int

    class CheckResponse(BaseModel):
        decision: Decision
        reason: str
        threshold_seconds: float
        target_ratio: float
        waited_seconds: float
        timed_out: bool
        total_requests: int | None = None
        compliant_lower: int | None = None
        compliant_upper: int | None = None
        lower_ratio: float | None = None
        upper_ratio: float | None = None
        zero_traffic: bool | None = None
        instances: dict[str, InstanceSample] = {}
        missing_instances: list[str] = []
        missing_reasons: dict[str, str] = {}

    @app.get("/check", response_model=CheckResponse, tags=["核查"])
    async def check(
        threshold_seconds: float = Query(
            ...,
            gt=0,
            alias="threshold",
            description="延迟阈值（秒），超过该时长视为不达标",
        ),
        max_wait_seconds: float = Query(
            ...,
            gt=0,
            le=60,
            alias="max-wait",
            description="最长等待样本时间（秒），超时即按缺证据返回",
        ),
        target: float = Query(
            DEFAULT_TARGET, gt=0, le=1, description="达标比例目标，默认 0.99"
        ),
    ) -> CheckResponse:
        started = time.monotonic()
        deadline = started + max_wait_seconds
        # Each request owns its deadline; cancellation of another request
        # never reaches the shared store.
        ready, missing = await store.wait_for_samples(deadline)
        waited = time.monotonic() - started
        timed_out = bool(missing)

        if missing:
            reasons = {name: _missing_reason(store, name) for name in missing}
            return CheckResponse(
                decision=Decision.INCONCLUSIVE,
                reason=(
                    "等待超时，以下实例缺少合格证据："
                    + "、".join(f"{n}（{reasons[n]}）" for n in missing)
                    + "，证据不足。"
                ),
                threshold_seconds=threshold_seconds,
                target_ratio=target,
                waited_seconds=round(waited, 4),
                timed_out=True,
                missing_instances=missing,
                missing_reasons=reasons,
            )

        deltas = {name: sample.delta for name, sample in ready.items()}
        combined = combine_instances(deltas, threshold_seconds)
        decision, reason = decide(combined, target=target)

        instance_payload: dict[str, InstanceSample] = {}
        for name, sample in ready.items():
            bounds = combined.per_instance[name]
            earlier_at = sample.earlier.at
            later_at = sample.later.at
            instance_payload[name] = InstanceSample(
                earlier_scrape_at=earlier_at,
                later_scrape_at=later_at,
                process_id=sample.later.snapshot.process_id,
                gap_seconds=round(later_at - earlier_at, 4),
                requests_in_window=sample.delta.total_requests,
                compliant_lower=int(bounds.lower),
                compliant_upper=int(bounds.upper),
            )

        return CheckResponse(
            decision=decision,
            reason=reason,
            threshold_seconds=threshold_seconds,
            target_ratio=target,
            waited_seconds=round(waited, 4),
            timed_out=False,
            total_requests=combined.total_requests,
            compliant_lower=combined.compliant_lower,
            compliant_upper=combined.compliant_upper,
            lower_ratio=combined.lower_ratio,
            upper_ratio=combined.upper_ratio,
            zero_traffic=combined.is_zero_traffic,
            instances=instance_payload,
        )

    @app.get("/stats", tags=["核查"])
    async def stats() -> dict:
        """Actual scrape counts and the timestamps of samples in use."""
        data = store.stats()
        now = time.time()
        data["checked_at"] = now
        data["config"] = {
            "min_sample_gap_seconds": store.min_gap,
            "freshness_window_seconds": store.freshness,
            "scrape_interval_seconds": store.scrape_interval,
        }
        data["current_ready_samples"] = {}
        for name in store.instances:
            outcome = store.sample_status(name, now)
            if hasattr(outcome, "earlier"):
                data["current_ready_samples"][name] = {
                    "earlier_scrape_at": outcome.earlier.at,
                    "later_scrape_at": outcome.later.at,
                    "process_id": outcome.later.snapshot.process_id,
                    "gap_seconds": round(outcome.later.at - outcome.earlier.at, 4),
                    "requests_in_window": outcome.delta.total_requests,
                }
            else:
                data["current_ready_samples"][name] = {"not_ready_reason": outcome}
        return data

    @app.get("/health", tags=["运维"])
    async def health() -> dict:
        return {"ok": True}

    return app


def _missing_reason(store: ScrapeStore, name: str) -> str:
    outcome = store.sample_status(name)
    if isinstance(outcome, str):
        return _REASON_TEXT.get(outcome, outcome)
    return "ready"


_REASON_TEXT = {
    "insufficient_scrapes": "成功抓取不足两次",
    "stale_scrape": "最近抓取超出近 3 秒新鲜窗口",
    "samples_too_close": "两次抓取间隔不足 1 秒",
    "process_restarted": "两次抓取来自不同进程（疑似重启）",
}


app = create_app()
