"""Deterministic tests for the two-scrape selection rules."""

import time

from app.histogram import HistogramSnapshot
from app.scraper import FRESHNESS_WINDOW, MIN_SAMPLE_GAP, ScrapeRecord, ScrapeStore


def _record(at: float, pid: str, buckets=(0.1, 0.5, 1.0), counts=(1, 2, 3),
            total=4, ok=True, error=None) -> ScrapeRecord:
    if not ok:
        return ScrapeRecord(ok=False, at=at, error=error)
    snap = HistogramSnapshot(
        metric_name="m",
        buckets=dict(zip(buckets, counts)),
        count=total,
        sum_value=1.0,
        process_id=pid,
        scrape_seq=None,
        scraped_at=at,
    )
    return ScrapeRecord(ok=True, at=at, snapshot=snap)


def _store(records):
    store = ScrapeStore({"i1": "http://x/metrics"})
    store.instances["i1"].history = records
    return store


def test_select_happy_path():
    now = 100.0
    store = _store([_record(now - 2.0, "p1"), _record(now - 0.9, "p1",
                                                       counts=(2, 4, 6), total=8)])
    outcome = store._select_sample(store.instances["i1"], now)
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4
    assert outcome.sample_times == (now - 2.0, now - 0.9)


def test_select_insufficient_scrapes():
    store = _store([_record(1.0, "p1")])
    assert store._select_sample(store.instances["i1"], 2.0) == "insufficient_scrapes"


def test_select_ignores_failed_scrapes():
    now = 10.0
    store = _store([
        _record(now - 2.1, "p1"),
        _record(now - 1.5, "x", ok=False, error="boom"),
        _record(now - 1.0, "p1", counts=(2, 4, 6), total=8),
    ])
    outcome = store._select_sample(store.instances["i1"], now)
    # The two successful records are 1.1s apart and both fresh.
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4


def test_select_stale_scrape():
    now = time.time()
    store = _store([
        _record(now - FRESHNESS_WINDOW - 0.1, "p1"),
        _record(now - FRESHNESS_WINDOW - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "stale_scrape"


def test_select_samples_too_close():
    now = time.time()
    store = _store([
        _record(now - 0.9, "p1"),
        _record(now - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "samples_too_close"


def test_select_process_restarted():
    now = time.time()
    store = _store([
        _record(now - 2.0, "p1"),
        _record(now - 0.9, "p2"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_select_counter_reset_treated_as_restart():
    now = time.time()
    # Same process label but the newer counters are lower -> reset.
    store = _store([
        _record(now - 2.0, "p1", counts=(5, 9, 10), total=10),
        _record(now - 0.9, "p1", counts=(1, 2, 3), total=4),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_gap_constant_matches_requirement():
    assert MIN_SAMPLE_GAP >= 1.0
    assert FRESHNESS_WINDOW <= 3.0
