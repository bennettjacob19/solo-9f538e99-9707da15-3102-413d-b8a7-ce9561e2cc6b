"""Rollout decision rules.

Three outcomes, derived only from the lower/upper SLO ratios that the
histogram buckets can prove:

* ``proceed``  -- lower bound >= target (at least 99% can be proven good)
* ``halt``     -- upper bound <  target (fewer than 99% can possibly be good)
* ``inconclusive`` -- anything in between, including zero traffic
"""

from __future__ import annotations

from enum import Enum

from .histogram import CombinedResult

DEFAULT_TARGET = 0.99


class Decision(str, Enum):
    PROCEED = "proceed"
    HALT = "halt"
    INCONCLUSIVE = "inconclusive"


def decide(
    result: CombinedResult,
    target: float = DEFAULT_TARGET,
    zero_traffic: bool = False,
) -> tuple[Decision, str]:
    """Return the decision and a human readable (Chinese) reason."""

    if zero_traffic or result.is_zero_traffic:
        return (
            Decision.INCONCLUSIVE,
            "两实例在采样窗口内合计零流量，没有可用于判定的请求，证据不足。",
        )

    lo_pct = f"{result.lower_ratio * 100:.4f}%"
    hi_pct = f"{result.upper_ratio * 100:.4f}%"

    if result.lower_ratio + 1e-12 >= target:
        return (
            Decision.PROCEED,
            f"达标比例下界 {lo_pct} 已达到目标 {target * 100:.0f}%，可以放量。",
        )
    if result.upper_ratio + 1e-12 < target:
        return (
            Decision.HALT,
            f"达标比例上界 {hi_pct} 仍低于目标 {target * 100:.0f}%，必须停止放量。",
        )
    return (
        Decision.INCONCLUSIVE,
        f"达标比例位于 [{lo_pct}, {hi_pct}]，阈值落在直方图桶内导致范围不确定，"
        f"区间跨越目标 {target * 100:.0f}%，证据不足。",
    )
