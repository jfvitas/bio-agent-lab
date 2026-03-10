"""Severity-scoring scaffold."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SeverityScoringPlan:
    status: str = "stub"
    levels: tuple[str, ...] = ("low", "moderate", "high", "critical")


def plan_severity_scoring() -> SeverityScoringPlan:
    return SeverityScoringPlan()
