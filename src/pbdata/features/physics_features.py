"""Local physics-style descriptors derived from microstate records.

Assumptions:
- These are compact local electrostatic proxies, not full MM energies.
- Outputs remain pair-level and dense to avoid sparse one-hot explosions.
- When microstates or structures are absent, the stage leaves the pair unscored.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    return []


def summarize_microstates_to_physics_features(
    microstates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Summarize microstate assignments into dense pair-style physics features."""
    if not microstates:
        return None
    charges = [float(item.get("adjusted_charge_estimate") or 0.0) for item in microstates]
    same_charge = 0
    opposite_charge = 0
    metal_contacts = 0
    acidic_cluster_penalty = 0.0
    for item in microstates:
        same_d = item.get("nearest_same_charge_distance")
        opp_d = item.get("nearest_opposite_charge_distance")
        metal_d = item.get("nearest_metal_distance")
        if same_d is not None and float(same_d) < 4.5:
            same_charge += 1
        if opp_d is not None and float(opp_d) < 4.5:
            opposite_charge += 1
        if metal_d is not None and float(metal_d) < 3.2:
            metal_contacts += 1
        if item.get("residue_name") in {"ASP", "GLU"} and same_d is not None and float(same_d) < 4.0:
            acidic_cluster_penalty += 1.0 / max(float(same_d), 0.1)

    return {
        "microstate_record_count": len(microstates),
        "estimated_net_charge": round(sum(charges), 4),
        "mean_abs_residue_charge": round(sum(abs(charge) for charge in charges) / len(charges), 4),
        "positive_residue_count": sum(1 for charge in charges if charge > 0.2),
        "negative_residue_count": sum(1 for charge in charges if charge < -0.2),
        "same_charge_contact_count": same_charge,
        "opposite_charge_contact_count": opposite_charge,
        "metal_contact_count": metal_contacts,
        "acidic_cluster_penalty": round(acidic_cluster_penalty, 4),
        "local_electrostatic_balance": round(opposite_charge - same_charge - acidic_cluster_penalty, 4),
    }


def build_local_physics_features(
    microstate_path: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Build pair-level dense local-physics descriptors from microstates."""
    rows = _load_json_rows(microstate_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    feature_rows: list[dict[str, Any]] = []
    for row in rows:
        microstates = row.get("microstates") or []
        summary = summarize_microstates_to_physics_features(microstates)
        if summary is None:
            continue
        feature_rows.append({
            "pdb_id": row.get("pdb_id"),
            "pair_identity_key": row.get("pair_identity_key"),
            "binding_affinity_type": row.get("binding_affinity_type"),
            **summary,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "method": "microstate_proxy_electrostatics_v1",
        })

    out_path = output_dir / "physics_feature_records.json"
    out_path.write_text(json.dumps(feature_rows, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "materialized_from_microstates",
        "record_count": len(feature_rows),
        "notes": (
            "Pair-level local electrostatic proxy features derived from heuristic "
            "microstate assignments. These are not full MM energies."
        ),
    }
    manifest_path = output_dir / "physics_feature_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path, manifest_path
