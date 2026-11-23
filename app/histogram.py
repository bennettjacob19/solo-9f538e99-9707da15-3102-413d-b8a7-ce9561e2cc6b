"""Core logic for the canary rollout SLO check.

This module only contains pure functions / data classes so it can be unit
tested without any HTTP or async machinery.

Prometheus histograms are *cumulative*: every ``_bucket{le="x"}`` counts the
observations whose value is <= x, and ``+Inf`` equals ``_count``.  Between two
scrapes of the same process, the number of requests that landed in a bucket
range is therefore the difference of two cumulative counts.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

INF = float("inf")
# Internal label used by the bundled sample targets to identify a process.
PROCESS_LABEL = "run_id"


class MetricsParseError(ValueError):
    """Raised when exposition text cannot be interpreted as expected."""


@dataclass(frozen=True)
class HistogramSnapshot:
    """One scraped histogram (cumulative bucket counters)."""

    metric_name: str
    buckets: dict[float, int]  # ordered upper bound -> cumulative count
    count: int
    sum_value: float
    process_id: str | None
    scrape_seq: int | None
    scraped_at: float  # wall-clock seconds (time.time())

    def __post_init__(self) -> None:
        if INF not in self.buckets:
            object.__setattr__(self, "buckets", {**self.buckets, INF: self.count})


@dataclass
class HistogramDelta:
    """Incremental bucket counts between two snapshots of one process."""

    metric_name: str
    boundaries: list[float]  # finite upper bounds, sorted ascending
    # delta_buckets[i] = requests with value <= boundaries[i] (incremental)
    delta_buckets: list[int]
    delta_inf: int  # total requests in the window
    delta_sum: float
    earlier: HistogramSnapshot
    later: HistogramSnapshot

    @property
    def total_requests(self) -> int:
        return self.delta_inf


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

_LINE_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{.*\})?\s+(?P<value>\S+)\s*$")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"')


def _parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    return {m.group(1): _unescape(m.group(2)) for m in _LABEL_RE.finditer(raw)}


def _unescape(value: str) -> str:
    return re.sub(r"\\(.)", r"\1", value)


def _parse_le(raw: str) -> float:
    if raw in ("+Inf", "-Inf", "Inf"):
        return INF
    return float(raw)


def parse_metrics_text(
    text: str,
    metric_name: str,
    scraped_at: float,
) -> HistogramSnapshot:
    """Parse Prometheus exposition text for one histogram metric.

    Expects ``<metric_name>_bucket{...}``, ``<metric_name>_count`` and
    ``<metric_name>_sum`` samples plus the process identity labels
    ``process_start_time_seconds`` / ``sample_run_id``.
    """

    buckets: dict[float, int] = {}
    label_sets: list[dict[str, str]] = []
    count: int | None = None
    sum_value = 0.0
    process_start: float | None = None
    scrape_seq: int | None = None

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE_RE.match(line)
        if not match:
            continue
        name = match.group("name")
        labels = _parse_labels(match.group("labels"))
        value_raw = match.group("value")

        if name == "process_start_time_seconds":
            process_start = float(value_raw)
            continue
        if name == "sample_scrape_seq":
            scrape_seq = int(float(value_raw))
            continue

        if name == f"{metric_name}_bucket":
            if "le" not in labels:
                continue
            buckets[_parse_le(labels["le"])] = int(float(value_raw))
            label_sets.append(labels)
        elif name == f"{metric_name}_count":
            count = int(float(value_raw))
        elif name == f"{metric_name}_sum":
            sum_value = float(value_raw)

    if not buckets:
        raise MetricsParseError(f"no buckets found for metric {metric_name!r}")

    if count is None:
        count = buckets.get(INF)
    if count is None:
        raise MetricsParseError(f"no {metric_name}_count sample and no +Inf bucket")

    # Process identity: prefer the run_id label on histogram samples; fall
    # back to the process_start_time_seconds gauge when the label is absent.
    process_id = None
    for labels in label_sets:
        if PROCESS_LABEL in labels:
            process_id = labels[PROCESS_LABEL]
            break
    if process_id is None and process_start is not None:
        process_id = f"start-{process_start:f}"

    ordered = dict(sorted(buckets.items(), key=lambda item: item[0]))
    return HistogramSnapshot(
        metric_name=metric_name,
        buckets=ordered,
        count=count,
        sum_value=sum_value,
        process_id=process_id,
        scrape_seq=scrape_seq,
        scraped_at=scraped_at,
    )


# --------------------------------------------------------------------------- #
# Bucket increments
# --------------------------------------------------------------------------- #

def compute_delta(earlier: HistogramSnapshot, later: HistogramSnapshot) -> HistogramDelta:
    """Incremental counts between two cumulative snapshots of one process.

    Bucket layouts must match (the two scrapes come from the same process,
    where bucket boundaries do not change).  Counter resets are guarded
    against defensively even though same-process selection already excludes
    restart windows.
    """

    if earlier.metric_name != later.metric_name:
        raise MetricsParseError("snapshots describe different metrics")
    if list(earlier.buckets.keys()) != list(later.buckets.keys()):
        raise MetricsParseError(
            "bucket boundaries differ between the two scrapes; "
            "they must come from the same process"
        )

    boundaries = [b for b in later.buckets if math.isfinite(b)]
    # Difference of two cumulative series at the same bound = cumulative
    # count of window requests whose value is <= that boundary.
    delta_buckets: list[int] = []
    for boundary in boundaries:
        diff = later.buckets[boundary] - earlier.buckets[boundary]
        if diff < 0:
            raise MetricsParseError(
                f"counter reset detected at le={boundary}; "
                "the two scrapes are not from the same process lifetime"
            )
        delta_buckets.append(diff)

    delta_inf = later.count - earlier.count
    if delta_inf < 0:
        raise MetricsParseError("counter reset detected on _count")
    delta_sum = max(0.0, later.sum_value - earlier.sum_value)

    return HistogramDelta(
        metric_name=later.metric_name,
        boundaries=boundaries,
        delta_buckets=delta_buckets,
        delta_inf=delta_inf,
        delta_sum=delta_sum,
        earlier=earlier,
        later=later,
    )


# --------------------------------------------------------------------------- #
# SLO bounds
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Interval:
    """Closed-ish interval [lower, upper] of request counts/ratios."""

    lower: float
    upper: float

    def __post_init__(self) -> None:
        if self.lower < -1e-12 or self.upper < self.lower - 1e-12:
            raise ValueError(f"invalid interval: {self.lower}..{self.upper}")


def compliant_count_bounds(delta: HistogramDelta, threshold_s: float) -> Interval:
    """Bounds on the number of requests completed within ``threshold_s``.

    Prometheus only tells us which *bucket* each request fell into, so when
    the threshold cuts through a bucket the exact answer is unknown and an
    uncertainty range is returned.

    Given cumulative bucket counts ``C(b)`` (# requests <= b):

    * requests definitely <= threshold:  C(largest boundary <= threshold)
    * requests possibly   <= threshold:  C(smallest boundary >= threshold)
      (a threshold equal to a boundary makes that bucket fully known, so the
      two coincide)
    """

    if threshold_s < 0:
        raise ValueError("threshold must be non-negative")

    # delta_buckets are per-upper-bound *cumulative* counts of the window;
    # the +Inf equivalent is delta_inf.
    definitely = 0
    possibly: int | None = None

    for boundary, cumulative in zip(delta.boundaries, delta.delta_buckets):
        if boundary < threshold_s:
            definitely = cumulative
        elif boundary == threshold_s:
            # Threshold on a boundary: everything up to it is known.
            definitely = cumulative
            possibly = cumulative
        else:  # boundary > threshold_s: first such bucket straddles it
            if possibly is None:
                possibly = cumulative

    if possibly is None:
        # Threshold above the largest finite boundary: the tail bucket is
        # (largest_bound, +Inf), every request there may or may not qualify.
        possibly = delta.delta_inf

    return Interval(lower=definitely, upper=possibly)


@dataclass
class CombinedResult:
    total_requests: int
    compliant_lower: int
    compliant_upper: int
    lower_ratio: float
    upper_ratio: float
    per_instance: dict[str, Interval] = field(default_factory=dict)

    @property
    def is_zero_traffic(self) -> bool:
        return self.total_requests == 0


def combine_instances(
    deltas: dict[str, HistogramDelta], threshold_s: float
) -> CombinedResult:
    """Sum SLO bounds across instances with potentially different buckets."""

    per_instance: dict[str, Interval] = {}
    total = 0
    lower = 0
    upper = 0
    for instance, delta in deltas.items():
        bounds = compliant_count_bounds(delta, threshold_s)
        per_instance[instance] = bounds
        total += delta.total_requests
        lower += int(bounds.lower)
        upper += int(bounds.upper)

    if total == 0:
        lower_ratio = upper_ratio = 0.0
    else:
        lower_ratio = lower / total
        upper_ratio = upper / total

    return CombinedResult(
        total_requests=total,
        compliant_lower=lower,
        compliant_upper=upper,
        lower_ratio=lower_ratio,
        upper_ratio=upper_ratio,
        per_instance=per_instance,
    )
