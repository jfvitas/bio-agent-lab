"""Pathway-reasoning scaffold."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PathwayReasoningPlan:
    status: str = "stub"
    notes: str = "Pathway reasoning should combine target hits, overlap, and conflict scoring."


def plan_pathway_reasoning() -> PathwayReasoningPlan:
    return PathwayReasoningPlan()
