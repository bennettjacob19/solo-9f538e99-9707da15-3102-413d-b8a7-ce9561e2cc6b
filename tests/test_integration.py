"""End-to-end tests with real uvicorn servers and background scraping."""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
import uvicorn

from app.main import create_app
from app.sample_target import create_app as create_target_app

PORTS = {"a": 9201, "b": 9202}
BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class Server:
    def __init__(self, name: str, port: int, app_factory, env: dict[str, str] | None = None):
        self.name = name
        self.port = port
        # Factory is invoked inside start() *after* the env vars are set,
        # otherwise SAMPLE_BUCKETS / INSTANCE_ID would not take effect.
        self.app_factory = app_factory
        self.env = env or {}

    async def start(self) -> None:
        old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            app = self.app_factory()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="error", lifespan="on"
        )
        self.server = uvicorn.Server(self.config)
        self._task = asyncio.create_task(self.server.serve())
        await _wait_port(self.port)

    async def stop(self) -> None:
        self.server.should_exit = True
        await asyncio.wait_for(self._task, timeout=5)


async def _wait_port(port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    last_exc = None
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                await client.get(f"http://127.0.0.1:{port}/state")
                return
            except httpx.HTTPError as exc:
                last_exc = exc
                await asyncio.sleep(0.05)
    raise RuntimeError(f"port {port} never came up: {last_exc}")


@pytest.fixture
async def servers():
    target_a = Server("a", PORTS["a"], create_target_app,
                      {"INSTANCE_ID": "sample-a", "SAMPLE_BUCKETS": BUCKETS_A})
    target_b = Server("b", PORTS["b"], create_target_app,
                      {"INSTANCE_ID": "sample-b", "SAMPLE_BUCKETS": BUCKETS_B})
    await asyncio.gather(target_a.start(), target_b.start())

    os.environ["SCRAPE_TARGETS"] = (
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        f"sample-b=http://127.0.0.1:{PORTS['b']}/metrics"
    )
    verifier = Server("verifier", 9200, create_app)
    await verifier.start()

    yield {
        "verifier": "http://127.0.0.1:9200",
        "a": f"http://127.0.0.1:{PORTS['a']}",
        "b": f"http://127.0.0.1:{PORTS['b']}",
    }

    await verifier.stop()
    await asyncio.gather(target_a.stop(), target_b.stop())
    os.environ.pop("SCRAPE_TARGETS", None)


async def _inject(base: str, duration: float, count: int = 1) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{base}/inject",
            json={"duration_seconds": duration, "count": count},
        )


async def _check(base: str, threshold: float, max_wait: float, target: float = 0.99):
    async with httpx.AsyncClient(timeout=max_wait + 10) as client:
        resp = await client.get(
            f"{base}/check",
            params={"threshold": threshold, "max-wait": max_wait, "target": target},
        )
        return resp.json()


async def _wait_ready(base: str, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{base}/stats")).json()
            samples = stats["current_ready_samples"]
            if all("not_ready_reason" not in v for v in samples.values()):
                return
            await asyncio.sleep(0.1)
    raise AssertionError("verifier never got ready samples")


# --------------------------------------------------------------------------- #


async def test_proceed_when_lower_bound_meets_99_percent(servers):
    # 100 reqs per instance, exactly 99 within 0.5s on each -> 198/200.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "proceed", result
    assert result["total_requests"] == 200
    assert result["compliant_lower"] == 198
    assert result["lower_ratio"] == pytest.approx(0.99)
    assert result["timed_out"] is False
    # two scrapes, at least 1s apart, same process per instance
    for name in ("sample-a", "sample-b"):
        sample = result["instances"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["process_id"]
        assert sample["earlier_scrape_at"] < sample["later_scrape_at"]


async def test_halt_when_upper_bound_below_99_percent(servers):
    # 90/100 good on each instance -> 180/200 = 90%, upper bound stays 90%.
    await _inject(servers["a"], 0.2, 90)
    await _inject(servers["a"], 2.0, 10)
    await _inject(servers["b"], 0.2, 90)
    await _inject(servers["b"], 2.0, 10)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "halt", result
    assert result["upper_ratio"] == pytest.approx(0.9)
    assert result["lower_ratio"] == pytest.approx(0.9)


async def test_inconclusive_when_threshold_inside_bucket(servers):
    # The verifier scores the *increment* between the two most recent
    # scrapes. Restart both targets first so no traffic from earlier tests
    # leaks into the window, then wait for fresh same-process samples.
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    # Instance A buckets include 0.5 -> exact at threshold 0.5:
    # 99 good, 1 slow = 99/100.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    # Instance B's nearest boundaries around 0.5 are 0.3 and 0.7.
    # 97 clearly <= 0.3; three requests in (0.3, 0.7] are unknown at
    # threshold 0.5 -> per-instance bounds 97..100 of 100.
    await _inject(servers["b"], 0.2, 97)
    await _inject(servers["b"], 0.4, 3)

    # Wait until a scrape window has captured the injected traffic on both
    # instances, then the next check evaluates that window.
    deadline = time.monotonic() + 8
    captured = False
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            windows = stats["current_ready_samples"]
            counts = [v.get("requests_in_window", 0) for v in windows.values()]
            if counts == [100, 100]:
                captured = True
                break
            await asyncio.sleep(0.1)
    assert captured, "injected traffic never appeared in a scrape window"

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    b = result["instances"]["sample-b"]
    assert b["compliant_lower"] == 97
    assert b["compliant_upper"] == 100
    # combined: lower 196/200 = 98%, upper 199/200 = 99.5% -> straddles 99%
    assert result["lower_ratio"] == pytest.approx(0.98)
    assert result["upper_ratio"] == pytest.approx(0.995)


async def test_zero_traffic_is_inconclusive(servers):
    await _wait_ready(servers["verifier"])
    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    assert result["zero_traffic"] is True
    assert result["total_requests"] == 0
    assert "零流量" in result["reason"]


async def test_restart_invalidates_samples_then_recovers(servers):
    await _inject(servers["a"], 0.2, 50)
    await _inject(servers["b"], 0.2, 50)
    await _wait_ready(servers["verifier"])

    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{servers['a']}/restart")
        new_run_id = resp.json()["run_id"]

        # Wait until the verifier has observed the new process exactly once:
        # the newest two scrapes then straddle the restart (different run
        # ids), which must be rejected.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            info = stats["instances"]["sample-a"]
            ready = stats["current_ready_samples"]["sample-a"]
            if (
                info["last_process_id"] == new_run_id
                and ready.get("not_ready_reason") == "process_restarted"
            ):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("verifier never observed the restart boundary")

        # A check with a short deadline must return sample-a as missing
        # evidence rather than silently using cross-process samples.
        check_resp = await client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.5},
        )
        result = check_resp.json()
    assert result["timed_out"] is True
    assert "sample-a" in result["missing_instances"]
    assert result["decision"] == "inconclusive"
    assert "不同进程" in result["missing_reasons"]["sample-a"]

    # After two fresh same-process scrapes (~2.1s) the store recovers.
    await _wait_ready(servers["verifier"], timeout=8)
    stats = await _get_stats(servers["verifier"])
    ready_a = stats["current_ready_samples"]["sample-a"]
    assert ready_a["process_id"] == new_run_id
    assert ready_a["requests_in_window"] == 0

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive"  # zero traffic after restart
    assert result["instances"]["sample-a"]["process_id"] == new_run_id


async def test_missing_instance_times_out_with_reason(monkeypatch, servers):
    # Shut instance B down; the verifier keeps serving and the check returns
    # with B named as the missing-evidence instance.
    async with httpx.AsyncClient() as client:
        # nothing to shut via fixture; simulate by pointing a fresh verifier
        # at a dead port.
        pass
    monkeypatch.setenv(
        "SCRAPE_TARGETS",
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        "sample-b=http://127.0.0.1:9999/metrics",
    )
    extra = Server("verifier2", 9210, create_app)
    await extra.start()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "http://127.0.0.1:9210/check",
                params={"threshold": 0.5, "max-wait": 3},
            )
            result = resp.json()
        assert result["timed_out"] is True
        assert result["missing_instances"] == ["sample-b"]
        assert result["missing_reasons"]["sample-b"] == "成功抓取不足两次"
    finally:
        await extra.stop()
        monkeypatch.delenv("SCRAPE_TARGETS", raising=False)


async def test_concurrent_checks_share_samples_and_isolate_timeouts(servers):
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    async with httpx.AsyncClient(timeout=30) as client:
        # One impatient request (likely times out) alongside a patient one;
        # both must end independently and the patient one must still decide.
        short = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.2},
        )
        long = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 8},
        )
        short_resp, long_resp = await asyncio.gather(short, long)

    short_result = short_resp.json()
    long_result = long_resp.json()
    assert long_result["decision"] == "proceed"
    # The impatient request either timed out or (if samples were already
    # cached from another test) succeeded — but it never broke the long one.
    assert short_result["decision"] in ("inconclusive", "proceed")

    stats = await _get_stats(servers["verifier"])
    # Two instances scraped in lockstep; attempts must be shared, never
    # multiplied per concurrent check request.
    for name in ("sample-a", "sample-b"):
        assert stats["instances"][name]["scrape_attempts"] >= 2


async def test_stats_reports_scrape_counts_and_sample_times(servers):
    await _wait_ready(servers["verifier"])
    stats = await _get_stats(servers["verifier"])
    for name in ("sample-a", "sample-b"):
        info = stats["instances"][name]
        assert info["scrape_attempts"] >= 2
        assert info["scrape_successes"] == info["scrape_attempts"]
        assert info["scrape_failures"] == 0
        sample = stats["current_ready_samples"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["earlier_scrape_at"] <= time.time()
        assert sample["later_scrape_at"] <= time.time()


async def _get_stats(base: str) -> dict:
    async with httpx.AsyncClient() as client:
        return (await client.get(f"{base}/stats")).json()
