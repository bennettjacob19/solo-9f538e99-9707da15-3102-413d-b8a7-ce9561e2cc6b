"""Unit tests for histogram parsing, deltas and SLO bounds."""

import math

import pytest

from app.histogram import (
    INF,
    HistogramSnapshot,
    MetricsParseError,
    combine_instances,
    compliant_count_bounds,
    compute_delta,
    parse_metrics_text,
)

SAMPLE_A = """# HELP http_request_duration_seconds request latency
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05",run_id="r1"} 10
http_request_duration_seconds_bucket{le="0.1",run_id="r1"} 30
http_request_duration_seconds_bucket{le="0.25",run_id="r1"} 70
http_request_duration_seconds_bucket{le="0.5",run_id="r1"} 90
http_request_duration_seconds_bucket{le="1",run_id="r1"} 98
http_request_duration_seconds_bucket{le="2.5",run_id="r1"} 99
http_request_duration_seconds_bucket{le="5",run_id="r1"} 100
http_request_duration_seconds_bucket{le="+Inf",run_id="r1"} 100
http_request_duration_seconds_count{run_id="r1"} 100
http_request_duration_seconds_sum{run_id="r1"} 12.5
process_start_time_seconds 1.7e9
"""


def test_parse_metrics_text_basic():
    snap = parse_metrics_text(SAMPLE_A, "http_request_duration_seconds", 1000.0)
    assert snap.count == 100
    assert snap.sum_value == 12.5
    assert snap.process_id == "r1"
    assert snap.buckets[INF] == 100
    assert snap.buckets[0.25] == 70
    assert list(snap.buckets) == [0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, INF]


def test_parse_missing_metric_raises():
    with pytest.raises(MetricsParseError):
        parse_metrics_text("up 1\n", "http_request_duration_seconds", 0.0)


def _snap(boundaries, cumulative, count, sum_value, pid="p1", at=0.0):
    buckets = dict(zip(boundaries, cumulative))
    return HistogramSnapshot(
        metric_name="m",
        buckets=buckets,
        count=count,
        sum_value=sum_value,
        process_id=pid,
        scrape_seq=None,
        scraped_at=at,
    )


def test_compute_delta_increments():
    old = _snap([0.1, 0.5, 1.0], [10, 30, 45], 50, 10.0)
    new = _snap([0.1, 0.5, 1.0], [15, 40, 60], 70, 16.0, at=1.1)
    delta = compute_delta(old, new)
    assert delta.delta_buckets == [5, 10, 15]
    assert delta.delta_inf == 20
    assert delta.total_requests == 20


def test_compute_delta_detects_counter_reset():
    old = _snap([0.1, 0.5], [10, 30], 30, 1.0)
    new = _snap([0.1, 0.5], [0, 5], 5, 0.2, pid="p2")
    with pytest.raises(MetricsParseError):
        compute_delta(old, new)


def test_compute_delta_rejects_different_bucket_layouts():
    old = _snap([0.1, 0.5], [10, 30], 30, 1.0)
    new = _snap([0.2, 0.5, 1.0], [5, 15, 20], 20, 1.0)
    with pytest.raises(MetricsParseError):
        compute_delta(old, new)


def test_bounds_threshold_on_boundary_is_exact():
    # buckets 0.1 / 0.5 / 1.0; window adds: 5 in (0.1,0.5], 10 in (0.5,1.0],
    # 5 in (1.0, +Inf) -> cumulative 5, 15, 20
    old = _snap([0.1, 0.5, 1.0], [0, 0, 0], 0, 0.0)
    new = _snap([0.1, 0.5, 1.0], [0, 5, 15], 20, 4.0, at=1.1)
    delta = compute_delta(old, new)

    bounds = compliant_count_bounds(delta, 0.5)
    assert bounds.lower == bounds.upper == 5

    bounds = compliant_count_bounds(delta, 1.0)
    assert bounds.lower == bounds.upper == 15


def test_bounds_threshold_inside_bucket_keeps_uncertainty():
    old = _snap([0.1, 0.5, 1.0], [0, 0, 0], 0, 0.0)
    new = _snap([0.1, 0.5, 1.0], [0, 5, 15], 20, 4.0, at=1.1)
    delta = compute_delta(old, new)

    # threshold 0.3 cuts bucket (0.1, 0.5]: definitely 0, possibly 5
    bounds = compliant_count_bounds(delta, 0.3)
    assert bounds.lower == 0
    assert bounds.upper == 5

    # threshold 0.7 cuts bucket (0.5, 1.0]: definitely 5, possibly 15
    bounds = compliant_count_bounds(delta, 0.7)
    assert bounds.lower == 5
    assert bounds.upper == 15

    # threshold above the largest boundary: tail (1.0, +Inf) uncertain
    bounds = compliant_count_bounds(delta, 2.0)
    assert bounds.lower == 15
    assert bounds.upper == 20


def test_bounds_layout_b_with_threshold_between_03_and_07():
    # Sample-B style buckets: 0.1 / 0.3 / 0.7 / 1.5 / 3 / 6
    boundaries = [0.1, 0.3, 0.7, 1.5, 3.0, 6.0]
    # 97 reqs <= 0.3, then 3 more reqs inside (0.3, 0.7], 100 total
    old = _snap(boundaries, [0] * 6, 0, 0.0)
    new = _snap(boundaries, [0, 97, 100, 100, 100, 100], 100, 30.0, at=1.1)
    delta = compute_delta(old, new)

    # threshold 0.5 cuts bucket (0.3, 0.7]: 97 known good, up to 100
    bounds = compliant_count_bounds(delta, 0.5)
    assert bounds.lower == 97
    assert bounds.upper == 100

    # threshold exactly 0.3: the boundary is known exactly -> 97 / 97
    bounds = compliant_count_bounds(delta, 0.3)
    assert bounds.lower == bounds.upper == 97

    # threshold below the smallest boundary: nothing proven good
    bounds = compliant_count_bounds(delta, 0.05)
    assert bounds.lower == 0
    assert bounds.upper == 0


def test_bounds_zero_traffic_window():
    old = _snap([0.1, 0.5], [10, 30], 30, 1.0)
    new = _snap([0.1, 0.5], [10, 30], 30, 1.0, at=1.1)
    delta = compute_delta(old, new)
    bounds = compliant_count_bounds(delta, 0.3)
    assert bounds.lower == bounds.upper == 0
    assert delta.total_requests == 0


def test_combine_two_instances_different_bucket_layouts():
    # instance A: boundaries 0.1/0.5/1.0, 100 requests, 98 <= 1.0
    old_a = _snap([0.1, 0.5, 1.0], [0, 0, 0], 0, 0.0)
    new_a = _snap([0.1, 0.5, 1.0], [50, 90, 98], 100, 20.0, at=1.1)
    delta_a = compute_delta(old_a, new_a)

    # instance B: different boundaries 0.3/0.7/1.5, 100 requests,
    # 97 <= 0.7 and all 100 <= 1.5
    old_b = _snap([0.3, 0.7, 1.5], [0, 0, 0], 0, 0.0)
    new_b = _snap([0.3, 0.7, 1.5], [40, 97, 100], 100, 30.0, at=1.1)
    delta_b = compute_delta(old_b, new_b)

    combined = combine_instances({"a": delta_a, "b": delta_b}, threshold_s=1.0)
    # A exact 98; B threshold inside bucket (0.7, 1.5]: 97..100
    assert combined.per_instance["a"].lower == 98
    assert combined.per_instance["a"].upper == 98
    assert combined.per_instance["b"].lower == 97
    assert combined.per_instance["b"].upper == 100
    assert combined.total_requests == 200
    assert combined.compliant_lower == 195
    assert combined.compliant_upper == 198
    assert math.isclose(combined.lower_ratio, 0.975)
    assert math.isclose(combined.upper_ratio, 0.99)


def test_combine_zero_traffic():
    old = _snap([0.1, 0.5], [5, 10], 10, 1.0)
    new = _snap([0.1, 0.5], [5, 10], 10, 1.0, at=1.1)
    delta = compute_delta(old, new)
    combined = combine_instances({"a": delta, "b": delta}, threshold_s=0.3)
    assert combined.is_zero_traffic
    assert combined.lower_ratio == combined.upper_ratio == 0.0
