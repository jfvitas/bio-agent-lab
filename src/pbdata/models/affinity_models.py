"""Lightweight baseline affinity helpers.

Assumptions:
- These are heuristic ranking/affinity proxies, not trained models.
- Lower standardized log10 affinity values imply stronger binding.
- Similarity is deliberately simple and auditable so it can run without
  external cheminformatics dependencies.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

_TOKEN_RE = re.compile(r"Cl|Br|[A-Z][a-z]?|[cnops]|[0-9]+|[@+\-\[\]\(\)=#$:/\\.%,]")


@dataclass(frozen=True)
class AffinityModelPlan:
    status: str = "baseline_heuristic_available"
    notes: str = (
        "Baseline affinity modeling uses ligand-string similarity, assay strength, "
        "and data-quality penalties. It is auditable but not a trained model."
    )


def plan_affinity_models() -> AffinityModelPlan:
    return AffinityModelPlan()


def tokenize_ligand_string(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return _TOKEN_RE.findall(text)


def ligand_similarity_score(query: str | None, reference: str | None) -> float:
    query_tokens = set(tokenize_ligand_string(query))
    ref_tokens = set(tokenize_ligand_string(reference))
    if not query_tokens or not ref_tokens:
        return 0.0
    overlap = len(query_tokens & ref_tokens)
    union = len(query_tokens | ref_tokens)
    return overlap / union if union else 0.0


def affinity_strength_from_log10(value: float | None) -> float:
    if value is None:
        return 0.0
    # Standardized values are in nM-like log10 space in current exports.
    # Lower is stronger; cap into a 0..1 score for heuristic ranking.
    return max(0.0, min(1.0, 1.0 - (value / 6.0)))


def confidence_bonus(agreement_band: str | None, conflict_flag: bool) -> float:
    score = {
        "high": 0.20,
        "medium": 0.10,
        "low": -0.05,
    }.get(str(agreement_band or "").strip().lower(), 0.0)
    if conflict_flag:
        score -= 0.10
    return score


def logistic_confidence(raw_score: float) -> float:
    return 1.0 / (1.0 + math.exp(-raw_score))
