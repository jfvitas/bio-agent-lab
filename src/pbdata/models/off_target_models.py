"""Off-target baseline ranking helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OffTargetModelPlan:
    status: str = "baseline_heuristic_available"
    notes: str = (
        "Off-target ranking uses baseline affinity proxies plus graph/pathway "
        "context. It is intended as a first operational substrate, not a final model."
    )


def plan_off_target_models() -> OffTargetModelPlan:
    return OffTargetModelPlan()
