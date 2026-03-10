"""Variant-effect workflow scaffold."""

from dataclasses import dataclass


@dataclass(frozen=True)
class VariantEffectsPlan:
    status: str = "stub"
    notes: str = "Variant-effect prediction should remain mutation-specific and never collapse WT/mutant contexts."


def plan_variant_effects() -> VariantEffectsPlan:
    return VariantEffectsPlan()
