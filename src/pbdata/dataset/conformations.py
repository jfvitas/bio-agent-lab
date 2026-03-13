"""Conformational-state catalog builder."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pbdata.schemas.conformational_state import ConformationalStateRecord
from pbdata.sources.alphafold import plan_alphafold_state
from pbdata.table_io import load_table_json


def build_conformation_states(extracted_dir: Path, output_dir: Path) -> tuple[Path, Path]:
    entries = load_table_json(extracted_dir / "entry")
    chains = load_table_json(extracted_dir / "chains")
    output_dir.mkdir(parents=True, exist_ok=True)

    uniprot_by_pdb: dict[str, str] = {}
    for chain in chains:
        pdb_id = str(chain.get("pdb_id") or "")
        uniprot_id = str(chain.get("uniprot_id") or "")
        if pdb_id and uniprot_id and pdb_id not in uniprot_by_pdb:
            uniprot_by_pdb[pdb_id] = uniprot_id

    states: list[ConformationalStateRecord] = []
    for entry in entries:
        pdb_id = str(entry.get("pdb_id") or "")
        if not pdb_id:
            continue
        cif_path = str(entry.get("structure_file_cif_path") or "") or None
        task_hint = str(entry.get("task_hint") or "")
        states.append(ConformationalStateRecord(
            target_id=uniprot_by_pdb.get(pdb_id, pdb_id),
            state_id=f"{pdb_id}:experimental",
            pdb_id=pdb_id,
            structure_source="RCSB",
            apo_or_holo="holo" if task_hint in {"protein_ligand", "protein_protein"} else None,
            active_inactive_unknown="unknown",
            open_closed_unknown="unknown",
            ligand_class_in_state=task_hint or None,
            conformation_cluster="experimental_observed",
            provenance={
                "source": "RCSB",
                "retrieved_at": str(entry.get("downloaded_at") or datetime.now(timezone.utc).isoformat()),
                "confidence": "high",
                "structure_path": cif_path,
            },
        ))
        uniprot_id = uniprot_by_pdb.get(pdb_id)
        if uniprot_id:
            af_plan = plan_alphafold_state(uniprot_id)
            states.append(ConformationalStateRecord(
                target_id=uniprot_id,
                state_id=f"{uniprot_id}:alphafold_planned",
                pdb_id=None,
                structure_source=af_plan.structure_source,
                apo_or_holo="apo_like",
                active_inactive_unknown="unknown",
                open_closed_unknown="unknown",
                ligand_class_in_state=None,
                conformation_cluster="predicted_pending",
                provenance={
                    "source": "AlphaFold",
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "confidence": "planned",
                },
            ))

    states_path = output_dir / "conformation_states.json"
    states_path.write_text(
        json.dumps([state.model_dump(mode="json") for state in states], indent=2),
        encoding="utf-8",
    )
    manifest_path = output_dir / "conformation_state_manifest.json"
    manifest_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "materialized_with_experimental_and_planned_predicted_states",
        "record_count": len(states),
    }, indent=2), encoding="utf-8")
    return states_path, manifest_path
