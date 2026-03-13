"""Planning artifacts for microstate refinement and local MM jobs.

Assumptions:
- These builders do not execute Amber/CHARMM/OpenMM/QM.
- They produce explicit, reviewable planning records for local refinement.
- The scope is intentionally local to the pair-specific binding/interface region.
"""

from __future__ import annotations

import json
import importlib.util
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import gemmi

from pbdata.pairing import parse_pair_identity_key
from pbdata.table_io import load_json_rows, load_table_json


TierName = Literal[
    "structure_proxy",
    "microstate_assignment",
    "classical_mm_refinement",
    "local_qm_refinement",
]


@dataclass(frozen=True)
class MolecularMechanicsFeaturePlan:
    structure_id: str
    status: str = "stub"
    tier: TierName = "structure_proxy"
    recommended_engine: str = "gemmi"
    notes: str = ""


def plan_mm_features(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        notes=(
            "Start with structure-derived continuous descriptors already available "
            "from the local mmCIF. Add external MM/QM only after caching, "
            "protonation-state handling, and local-region extraction are finalized."
        ),
    )


def plan_microstate_assignment(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="microstate_assignment",
        recommended_engine="AmberTools_or_CHARMM_with_explicit_protonation_workflow",
        notes=(
            "Assign context-sensitive residue and ligand states before MM scoring. "
            "This is the stage where Asp/Glu/Lys/His/terminal states should become "
            "environment-aware rather than residue-name defaults."
        ),
    )


def plan_classical_mm_refinement(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="classical_mm_refinement",
        recommended_engine="OpenMM_plus_AMBER_or_CHARMM_force_fields",
        notes=(
            "Refine only the local binding environment, not the full structure. "
            "Use this stage for Coulombic potentials, GB/SA-like terms, local "
            "strain, and environment-dependent atomic descriptors."
        ),
    )


def plan_local_qm_refinement(structure_id: str) -> MolecularMechanicsFeaturePlan:
    return MolecularMechanicsFeaturePlan(
        structure_id=structure_id,
        tier="local_qm_refinement",
        recommended_engine="semiempirical_or_QM_MM_local_cluster",
        notes=(
            "Reserve for a small local region: binding-site residues, ligand, metals, "
            "and catalytic waters. Full-structure ab initio is not operationally "
            "realistic for dataset-scale feature generation."
        ),
    )

def _matching_interface_residue_ids(
    interfaces_by_pdb: dict[str, list[dict[str, Any]]],
    *,
    pdb_id: str,
    pair_key: str,
) -> list[str]:
    parsed = parse_pair_identity_key(pair_key)
    if parsed is None:
        return []
    receptor_chain_ids = set(parsed.receptor_chain_ids)
    residue_ids: list[str] = []
    for interface in interfaces_by_pdb.get(pdb_id, []):
        binding_chains = set(interface.get("binding_site_chain_ids") or [])
        if receptor_chain_ids and binding_chains and not receptor_chain_ids.intersection(binding_chains):
            continue
        if (
            parsed.task_type == "protein_ligand"
            and parsed.ligand_key
            and str(interface.get("entity_name_b") or "")
            and str(interface.get("entity_name_b") or "") != parsed.ligand_key
        ):
            continue
        for residue_id in interface.get("binding_site_residue_ids") or []:
            residue_text = str(residue_id)
            if residue_text and residue_text not in residue_ids:
                residue_ids.append(residue_text)
    return residue_ids


def _parse_focus_residue_token(token: str) -> tuple[str | None, int | None]:
    token = str(token or "").strip()
    if ":" not in token:
        return None, None
    chain_id, residue_part = token.split(":", 1)
    digits = "".join(ch for ch in residue_part if ch.isdigit() or ch == "-")
    try:
        residue_number = int(digits)
    except ValueError:
        return chain_id or None, None
    return chain_id or None, residue_number


def _enumerate_protonation_candidates(microstate: dict[str, Any]) -> list[str]:
    residue_name = str(microstate.get("residue_name") or "").upper()
    if residue_name in {"ASP", "GLU"}:
        return ["deprotonated", "protonated"]
    if residue_name == "HIS":
        return ["neutral_delta", "neutral_epsilon", "diprotonated"]
    if residue_name in {"LYS", "ARG"}:
        return ["protonated", "neutral"]
    if residue_name in {"CYS", "TYR"}:
        return ["neutral", "deprotonated"]
    return ["unknown"]


def _residue_centroids(cif_path: str, receptor_chain_ids: list[str]) -> dict[tuple[str, int], tuple[float, float, float]]:
    structure = gemmi.read_structure(cif_path)
    centroids: dict[tuple[str, int], tuple[float, float, float]] = {}
    receptor_set = set(receptor_chain_ids)
    for model in structure:
        for chain in model:
            if receptor_set and str(chain.name) not in receptor_set:
                continue
            for residue in chain:
                coords = [(float(atom.pos.x), float(atom.pos.y), float(atom.pos.z)) for atom in residue]
                if not coords:
                    continue
                centroids[(str(chain.name), int(residue.seqid.num))] = (
                    sum(coord[0] for coord in coords) / len(coords),
                    sum(coord[1] for coord in coords) / len(coords),
                    sum(coord[2] for coord in coords) / len(coords),
                )
    return centroids


def _distance(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2) ** 0.5


def _extract_local_shell_residue_ids(
    cif_path: str,
    receptor_chain_ids: list[str],
    focus_residue_ids: list[str],
    radius_angstrom: float = 8.0,
) -> list[str]:
    centroids = _residue_centroids(cif_path, receptor_chain_ids)
    focus_centroids: list[tuple[float, float, float]] = []
    for token in focus_residue_ids:
        chain_id, residue_number = _parse_focus_residue_token(token)
        if chain_id is None or residue_number is None:
            continue
        centroid = centroids.get((chain_id, residue_number))
        if centroid is not None:
            focus_centroids.append(centroid)
    if not focus_centroids:
        return []

    shell_ids: list[str] = []
    for (chain_id, residue_number), centroid in centroids.items():
        if any(_distance(centroid, focus) <= radius_angstrom for focus in focus_centroids):
            token = f"{chain_id}:{residue_number}"
            if token not in shell_ids:
                shell_ids.append(token)
    return shell_ids


def _sanitize_job_id(job_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(job_id))


def _write_openmm_runner_template(job_dir: Path) -> Path:
    script = """\
from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    job_dir = Path(__file__).resolve().parent
    config = json.loads((job_dir / "openmm_job_config.json").read_text(encoding="utf-8"))
    raise SystemExit(
        "OpenMM execution is not bundled with this repo. "
        "Use the generated config, structure subset, and protonation assignments "
        "to run the local refinement in an environment with OpenMM and the chosen force field."
    )


if __name__ == "__main__":
    main()
"""
    path = job_dir / "run_openmm_local.py"
    path.write_text(script, encoding="utf-8")
    return path


def _detect_openmm_backend() -> dict[str, Any]:
    """Return conservative backend readiness information."""
    spec = importlib.util.find_spec("openmm")
    return {
        "backend_name": "openmm",
        "available": spec is not None,
        "notes": (
            "OpenMM Python package importable."
            if spec is not None
            else "OpenMM Python package not available in the current environment."
        ),
    }


def _materialize_openmm_job_bundle(job: dict[str, Any], bundle: dict[str, Any], output_dir: Path) -> Path:
    job_dir = output_dir / "openmm_jobs" / _sanitize_job_id(str(job.get("job_id") or "job"))
    job_dir.mkdir(parents=True, exist_ok=True)

    input_structure = Path(str(job.get("input_structure_file") or ""))
    copied_structure = None
    if input_structure.exists():
        copied_structure = job_dir / input_structure.name
        if copied_structure.resolve() != input_structure.resolve():
            shutil.copy2(input_structure, copied_structure)

    protonation_assignments = [
        {
            "chain_id": plan.get("chain_id"),
            "residue_number": plan.get("residue_number"),
            "residue_name": plan.get("residue_name"),
            "preferred_state": plan.get("preferred_state"),
            "candidate_states": plan.get("candidate_states") or [],
            "confidence": plan.get("confidence"),
        }
        for plan in (bundle.get("protonation_site_plans") or [])
        if isinstance(plan, dict)
    ]

    config = {
        "job_id": job.get("job_id"),
        "pair_identity_key": job.get("pair_identity_key"),
        "binding_affinity_type": job.get("binding_affinity_type"),
        "backend": "openmm_local",
        "force_field_family": "amber",
        "input_structure_file": str(copied_structure or input_structure),
        "region_residue_ids": bundle.get("region_residue_ids") or [],
        "region_strategy": bundle.get("region_strategy"),
        "requires_metal_parameterization": bool(job.get("requires_metal_parameterization")),
        "requires_mutation_specific_setup": bool(job.get("requires_mutation_specific_setup")),
        "protonation_assignments": protonation_assignments,
        "recommended_policies": job.get("recommended_policies") or [],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "notes": (
            "Prepared local OpenMM input bundle. Execution still requires an external "
            "OpenMM environment and explicit force-field parameterization."
        ),
    }
    (job_dir / "openmm_job_config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    (job_dir / "job_summary.json").write_text(json.dumps(job, indent=2), encoding="utf-8")
    _write_openmm_runner_template(job_dir)
    return job_dir


def build_microstate_refinement_plan(
    extracted_dir: Path,
    microstate_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Build explicit protonation-policy records for local refinement."""
    entries = load_table_json(extracted_dir / "entry")
    interfaces = load_table_json(extracted_dir / "interfaces")
    microstate_rows = load_json_rows(microstate_dir / "microstate_records.json")
    entry_by_pdb = {str(entry.get("pdb_id") or ""): entry for entry in entries if entry.get("pdb_id")}
    interfaces_by_pdb: dict[str, list[dict[str, Any]]] = {}
    for interface in interfaces:
        pdb_id = str(interface.get("pdb_id") or "")
        if pdb_id:
            interfaces_by_pdb.setdefault(pdb_id, []).append(interface)

    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for row in microstate_rows:
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        entry = entry_by_pdb.get(pdb_id, {})
        cif_path = str(entry.get("structure_file_cif_path") or "")
        parsed = parse_pair_identity_key(pair_key)
        microstates = [item for item in (row.get("microstates") or []) if isinstance(item, dict)]
        if not pdb_id or not pair_key or parsed is None or not cif_path:
            continue

        focus_residue_ids = _matching_interface_residue_ids(interfaces_by_pdb, pdb_id=pdb_id, pair_key=pair_key)
        shell_residue_ids = _extract_local_shell_residue_ids(cif_path, list(parsed.receptor_chain_ids), focus_residue_ids)
        low_confidence_count = sum(1 for item in microstates if str(item.get("confidence") or "") == "low")
        histidine_count = sum(1 for item in microstates if str(item.get("residue_name") or "") == "HIS")
        metal_contact_count = sum(1 for item in microstates if item.get("nearest_metal_distance") is not None)
        acidic_cluster_count = sum(
            1
            for item in microstates
            if str(item.get("residue_name") or "") in {"ASP", "GLU"}
            and item.get("nearest_same_charge_distance") is not None
            and float(item["nearest_same_charge_distance"]) < 4.0
        )
        recommended_policies: list[str] = []
        if low_confidence_count:
            recommended_policies.append("review_low_confidence_ionizable_residues")
        if histidine_count:
            recommended_policies.append("enumerate_histidine_tautomers")
        if metal_contact_count:
            recommended_policies.append("preserve_metal_coordination_state")
        if acidic_cluster_count:
            recommended_policies.append("review_acidic_cluster_protonation")
        if parsed.mutation_key and parsed.mutation_key not in {"wt", "-", "wildtype"}:
            recommended_policies.append("keep_mutation_specific_microstates_separate")

        protonation_site_plans = []
        for item in microstates:
            preferred_state = str(item.get("state_label") or "") or "unknown"
            protonation_site_plans.append({
                "chain_id": item.get("chain_id"),
                "residue_number": item.get("residue_number"),
                "residue_name": item.get("residue_name"),
                "confidence": item.get("confidence"),
                "preferred_state": preferred_state,
                "candidate_states": _enumerate_protonation_candidates(item),
            })

        rows.append({
            "pdb_id": pdb_id,
            "pair_identity_key": pair_key,
            "binding_affinity_type": affinity_type,
            "structure_file_cif_path": cif_path,
            "receptor_chain_ids": list(parsed.receptor_chain_ids),
            "partner_chain_ids": list(parsed.partner_chain_ids),
            "ligand_key": parsed.ligand_key,
            "mutation_key": parsed.mutation_key,
            "focus_residue_ids": focus_residue_ids,
            "focus_residue_count": len(focus_residue_ids),
            "shell_residue_ids": shell_residue_ids,
            "shell_residue_count": len(shell_residue_ids),
            "microstate_record_count": len(microstates),
            "low_confidence_microstate_count": low_confidence_count,
            "histidine_microstate_count": histidine_count,
            "metal_contact_microstate_count": metal_contact_count,
            "acidic_cluster_count": acidic_cluster_count,
            "protonation_site_plans": protonation_site_plans,
            "recommended_policies": recommended_policies,
            "recommended_engine": "AmberTools_or_CHARMM_with_explicit_protonation_workflow",
            "status": "planned",
            "method": "microstate_refinement_policy_v1",
            "notes": (
                "Local microstate refinement plan derived from extracted interface context "
                "and heuristic ionizable-residue states. This is a planning artifact, not "
                "an executed protonation workflow."
            ),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        })

    records_path = output_dir / "microstate_refinement_records.json"
    records_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "planned_from_microstate_and_interface_context",
        "record_count": len(rows),
        "notes": (
            "Pair-level protonation-policy planning records for later MM refinement. "
            "No external MM backend was executed."
        ),
    }
    manifest_path = output_dir / "microstate_refinement_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return records_path, manifest_path


def build_mm_job_manifests(
    refinement_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Build backend-ready local MM job manifests from refinement plans."""
    refinement_rows = load_json_rows(refinement_dir / "microstate_refinement_records.json")
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    bundles: list[dict[str, Any]] = []
    materialized_openmm_dirs: list[str] = []
    for row in refinement_rows:
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        cif_path = str(row.get("structure_file_cif_path") or "")
        if not pdb_id or not pair_key or not cif_path:
            continue

        focus_residue_ids = [str(item) for item in (row.get("focus_residue_ids") or []) if item]
        shell_residue_ids = [str(item) for item in (row.get("shell_residue_ids") or []) if item]
        recommended_policies = [str(item) for item in (row.get("recommended_policies") or []) if item]
        protonation_site_plans = [item for item in (row.get("protonation_site_plans") or []) if isinstance(item, dict)]
        prefer_qm = any(
            policy in {"preserve_metal_coordination_state", "review_acidic_cluster_protonation"}
            for policy in recommended_policies
        )
        backend_candidates = ["openmm_amber_local", "openmm_charmm_local"]
        if prefer_qm:
            backend_candidates.append("semiempirical_local_cluster")

        job_id = f"mm_job:{pdb_id}:{len(rows)}"
        rows.append({
            "job_id": job_id,
            "pdb_id": pdb_id,
            "pair_identity_key": pair_key,
            "binding_affinity_type": affinity_type,
            "input_structure_file": cif_path,
            "focus_residue_ids": focus_residue_ids,
            "focus_residue_count": len(focus_residue_ids),
            "shell_residue_ids": shell_residue_ids,
            "shell_residue_count": len(shell_residue_ids),
            "recommended_policies": recommended_policies,
            "backend_candidates": backend_candidates,
            "recommended_backend": backend_candidates[0],
            "requires_metal_parameterization": "preserve_metal_coordination_state" in recommended_policies,
            "requires_mutation_specific_setup": str(row.get("mutation_key") or "") not in {"", "-", "wt", "wildtype"},
            "local_region_strategy": "pair_interface_shell",
            "status": "planned_not_executed",
            "notes": (
                "Backend-ready local MM job manifest. Intended for cached local-region refinement "
                "around the pair-specific interface rather than whole-structure minimization."
            ),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        })
        bundles.append({
            "job_id": job_id,
            "input_structure_file": cif_path,
            "region_residue_ids": shell_residue_ids or focus_residue_ids,
            "region_strategy": "pair_interface_shell",
            "protonation_site_plans": protonation_site_plans,
            "backend_inputs": {
                "openmm_amber_local": {
                    "force_field_family": "amber",
                    "requires_explicit_protonation_assignment": True,
                },
                "openmm_charmm_local": {
                    "force_field_family": "charmm",
                    "requires_explicit_protonation_assignment": True,
                },
                "semiempirical_local_cluster": {
                    "enabled": prefer_qm,
                    "reason": "metal_or_acidic_cluster_context" if prefer_qm else None,
                },
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        })
        materialized_openmm_dirs.append(
            str(_materialize_openmm_job_bundle(rows[-1], bundles[-1], output_dir))
        )

    records_path = output_dir / "mm_job_records.json"
    records_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    bundles_path = output_dir / "mm_backend_input_bundles.json"
    bundles_path.write_text(json.dumps(bundles, indent=2), encoding="utf-8")
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "planned_from_refinement_records",
        "record_count": len(rows),
        "bundle_count": len(bundles),
        "materialized_openmm_job_count": len(materialized_openmm_dirs),
        "materialized_openmm_job_dirs": materialized_openmm_dirs,
        "notes": (
            "Local MM job manifests and OpenMM-ready input bundles for later execution. "
            "No MM engine was run in this stage."
        ),
    }
    manifest_path = output_dir / "mm_job_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return records_path, manifest_path


def run_mm_job_bundles(
    mm_jobs_dir: Path,
    *,
    execute: bool = False,
) -> tuple[Path, Path]:
    """Validate and dispatch local MM job bundles when a backend is present.

    Assumptions:
    - This stage does not silently emulate molecular mechanics.
    - If OpenMM is unavailable, jobs are reported as blocked rather than marked
      completed.
    - Even when OpenMM is available, execution remains conservative until a
      full force-field/runtime pipeline is implemented.
    """
    job_rows = load_json_rows(mm_jobs_dir / "mm_job_records.json")
    backend = _detect_openmm_backend()
    results: list[dict[str, Any]] = []

    for row in job_rows:
        job_id = str(row.get("job_id") or "")
        job_dir = mm_jobs_dir / "openmm_jobs" / _sanitize_job_id(job_id)
        config_path = job_dir / "openmm_job_config.json"
        runner_path = job_dir / "run_openmm_local.py"
        structure_path = Path(str(row.get("input_structure_file") or ""))
        copied_structure_path = job_dir / structure_path.name if structure_path.name else None
        bundle_valid = config_path.exists() and runner_path.exists() and bool(copied_structure_path and copied_structure_path.exists())

        if not bundle_valid:
            status = "invalid_bundle"
            notes = "Required OpenMM bundle files are missing."
        elif not backend["available"]:
            status = "backend_unavailable"
            notes = "OpenMM is not installed in the current environment."
        elif execute:
            status = "backend_ready_execution_deferred"
            notes = (
                "OpenMM is available and the bundle is valid, but full execution remains "
                "deferred until the runtime/refinement pipeline is finalized."
            )
        else:
            status = "backend_ready_not_executed"
            notes = "OpenMM is available and the bundle is valid; execution was not requested."

        result = {
            "job_id": job_id,
            "pdb_id": row.get("pdb_id"),
            "pair_identity_key": row.get("pair_identity_key"),
            "recommended_backend": row.get("recommended_backend"),
            "job_dir": str(job_dir),
            "bundle_valid": bundle_valid,
            "backend_available": backend["available"],
            "status": status,
            "notes": notes,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        results.append(result)
        (job_dir / "execution_result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "completed" if results else "no_jobs_found",
        "job_count": len(results),
        "backend": backend,
        "executed": execute,
        "notes": (
            "MM job runner validates bundle completeness and backend availability. "
            "It does not fabricate refinement outputs when the execution backend is absent or not yet finalized."
        ),
    }
    results_path = mm_jobs_dir / "mm_job_execution_results.json"
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    manifest_path = mm_jobs_dir / "mm_job_execution_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return results_path, manifest_path
