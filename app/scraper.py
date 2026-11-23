"""Shared scrape store and the verification orchestration.

A single background task scrapes every configured instance on a fixed
cadence (>= 1s apart per instance).  All concurrent ``/check`` requests read
from the same in-memory store, so checks share samples and one request
timing out never disturbs another.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx

from .histogram import (
    HistogramDelta,
    HistogramSnapshot,
    MetricsParseError,
    compute_delta,
    parse_metrics_text,
)

logger = logging.getLogger(__name__)

METRIC_NAME = "http_request_duration_seconds"
# Same-instance scrapes must be at least this far apart (seconds).
MIN_SAMPLE_GAP = 1.0
# A scrape is only usable for this long after it happened.
FRESHNESS_WINDOW = 3.0
# How often the background loop scrapes each instance.
SCRAPE_INTERVAL = 1.05
# Keep the last N scrapes per instance (only the newest two are used).
HISTORY_LIMIT = 8
HTTP_TIMEOUT = 2.0


@dataclass
class ScrapeRecord:
    ok: bool
    at: float
    snapshot: HistogramSnapshot | None = None
    error: str | None = None


@dataclass
class InstanceState:
    name: str
    url: str
    history: list[ScrapeRecord] = field(default_factory=list)
    total_attempts: int = 0
    total_successes: int = 0
    total_failures: int = 0

    @property
    def latest(self) -> ScrapeRecord | None:
        return self.history[-1] if self.history else None


@dataclass
class ReadySample:
    """The two validated scrapes for one instance."""

    earlier: ScrapeRecord
    later: ScrapeRecord
    delta: HistogramDelta

    @property
    def sample_times(self) -> tuple[float, float]:
        return self.earlier.at, self.later.at


class ScrapeStore:
    def __init__(
        self,
        instances: dict[str, str],
        metric_name: str = METRIC_NAME,
        scrape_interval: float = SCRAPE_INTERVAL,
        min_gap: float = MIN_SAMPLE_GAP,
        freshness: float = FRESHNESS_WINDOW,
    ) -> None:
        self.instances = {
            name: InstanceState(name=name, url=url) for name, url in instances.items()
        }
        self.metric_name = metric_name
        self.scrape_interval = scrape_interval
        self.min_gap = min_gap
        self.freshness = freshness
        self._cond = asyncio.Condition()
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._task is None:
            self._client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
            self._task = asyncio.create_task(self._scrape_loop(), name="scrape-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _scrape_once(self, state: InstanceState) -> None:
        assert self._client is not None
        state.total_attempts += 1
        try:
            response = await self._client.get(state.url)
            response.raise_for_status()
            snapshot = parse_metrics_text(
                response.text, self.metric_name, time.time()
            )
            record = ScrapeRecord(ok=True, at=snapshot.scraped_at, snapshot=snapshot)
            state.total_successes += 1
        except (httpx.HTTPError, MetricsParseError, ValueError) as exc:
            record = ScrapeRecord(ok=False, at=time.time(), error=f"{type(exc).__name__}: {exc}")
            state.total_failures += 1
        except Exception as exc:  # defensive: one bad scrape must not kill the loop
            logger.exception("unexpected scrape failure for %s", state.name)
            record = ScrapeRecord(ok=False, at=time.time(), error=f"{type(exc).__name__}: {exc}")
            state.total_failures += 1

        state.history.append(record)
        if len(state.history) > HISTORY_LIMIT:
            del state.history[: len(state.history) - HISTORY_LIMIT]

    async def _scrape_loop(self) -> None:
        while True:
            await asyncio.gather(
                *(self._scrape_once(state) for state in self.instances.values()),
                return_exceptions=True,
            )
            async with self._cond:
                self._cond.notify_all()
            await asyncio.sleep(self.scrape_interval)

    # ------------------------------------------------------------------ #
    # Read side
    # ------------------------------------------------------------------ #

    def sample_status(self, name: str, now: float | None = None) -> ReadySample | str:
        """Public wrapper around :meth:`_select_sample` for observability."""
        return self._select_sample(
            self.instances[name], time.time() if now is None else now
        )

    def _select_sample(self, state: InstanceState, now: float) -> ReadySample | str:
        """Validate the two most recent scrapes for one instance.

        Returns a :class:`ReadySample`, or a machine-readable reason string.
        """

        records = [r for r in state.history if r.ok]
        if len(records) < 2:
            return "insufficient_scrapes"
        earlier, later = records[-2], records[-1]
        assert earlier.snapshot is not None and later.snapshot is not None

        if now - later.at > self.freshness:
            return "stale_scrape"
        if now - earlier.at > self.freshness:
            return "stale_scrape"
        if later.at - earlier.at < self.min_gap:
            return "samples_too_close"
        if (
            earlier.snapshot.process_id is None
            or later.snapshot.process_id is None
            or earlier.snapshot.process_id != later.snapshot.process_id
        ):
            return "process_restarted"
        try:
            delta = compute_delta(earlier.snapshot, later.snapshot)
        except MetricsParseError:
            # A counter reset means a restart slipped past process labels.
            return "process_restarted"
        return ReadySample(earlier=earlier, later=later, delta=delta)

    async def wait_for_samples(
        self, deadline: float
    ) -> tuple[dict[str, ReadySample], list[str]]:
        """Wait until every instance has a valid two-scrape sample or timeout.

        Concurrent calls block on the same condition variable and therefore
        share every scrape the background task produces.
        """

        while True:
            ready: dict[str, ReadySample] = {}
            missing: list[str] = []
            wall_now = time.time()
            for name, state in self.instances.items():
                outcome = self._select_sample(state, wall_now)
                if isinstance(outcome, ReadySample):
                    ready[name] = outcome
                else:
                    missing.append(name)

            if not missing:
                return ready, []
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ready, missing

            async with self._cond:
                try:
                    await asyncio.wait_for(
                        self._cond.wait(),
                        timeout=max(0.0, deadline - time.monotonic()),
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

    # ------------------------------------------------------------------ #
    # Observability
    # ------------------------------------------------------------------ #

    def stats(self) -> dict:
        result: dict = {"instances": {}}
        for name, state in self.instances.items():
            last_ok = next(
                (r for r in reversed(state.history) if r.ok), None
            )
            result["instances"][name] = {
                "url": state.url,
                "scrape_attempts": state.total_attempts,
                "scrape_successes": state.total_successes,
                "scrape_failures": state.total_failures,
                "history_len": len(state.history),
                "latest_scrape_at": state.latest.at if state.latest else None,
                "latest_ok": state.latest.ok if state.latest else None,
                "latest_error": state.latest.error if state.latest else None,
                "last_success_at": last_ok.at if last_ok else None,
                "last_process_id": (
                    last_ok.snapshot.process_id if last_ok and last_ok.snapshot else None
                ),
            }
        return result


@asynccontextmanager
async def store_lifespan(store: ScrapeStore):
    await store.start()
    try:
        yield
    finally:
        await store.stop()
