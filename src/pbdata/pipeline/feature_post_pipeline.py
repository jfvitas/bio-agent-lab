from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pbdata.storage import StorageLayout
from pbdata.table_io import read_dataframe

_ARCHETYPE_CLUSTER_VERSION = "site_archetype_clusters_v1"
_DESCRIPTOR_COLUMNS = [
    "neighbor_atom_count",
    "sum_partial_charge",
    "electric_field_magnitude",
    "donor_count",
    "acceptor_count",
]


def _safe_file_stem(value: str) -> str:
    return value.replace(":", "_").replace("|", "_").replace("/", "_").replace("\\", "_")


def _site_descriptor_vector(site_df: pd.DataFrame) -> list[float]:
    vector: list[float] = []
    for shell_name in ("shell_1", "shell_2", "shell_3"):
        shell_df = site_df[site_df["shell_name"] == shell_name]
        row = shell_df.iloc[0].to_dict() if not shell_df.empty else {}
        for column in _DESCRIPTOR_COLUMNS:
            vector.append(float(row.get(column) or 0.0))
    return vector


def _normalized_descriptor_distance(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 1.0
    deltas = [
        abs(lval - rval) / max(abs(lval), abs(rval), 1.0)
        for lval, rval in zip(left, right)
    ]
    return sum(deltas) / len(deltas)


def _cluster_archetype_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clustered_rows: list[dict[str, Any]] = []
    for motif_class in sorted({str(row["motif_class"]) for row in rows}):
        motif_rows = sorted(
            [row for row in rows if str(row["motif_class"]) == motif_class],
            key=lambda row: (str(row["site_id"]), str(row["archetype_id"])),
        )
        clusters: list[dict[str, Any]] = []
        for row in motif_rows:
            descriptor = list(row["descriptor_vector"])
            assigned_cluster = None
            for cluster in clusters:
                if _normalized_descriptor_distance(descriptor, list(cluster["centroid"])) <= 0.15:
                    assigned_cluster = cluster
                    break
            if assigned_cluster is None:
                assigned_cluster = {
                    "cluster_index": len(clusters),
                    "centroid": descriptor,
                    "members": [],
                }
                clusters.append(assigned_cluster)
            assigned_cluster["members"].append(row)
        for cluster in clusters:
            representative = min(
                cluster["members"],
                key=lambda row: (str(row["archetype_id"]), str(row["site_id"])),
            )
            cluster_id = f"{motif_class}:cluster_{cluster['cluster_index']:03d}"
            cluster_size = len(cluster["members"])
            for row in cluster["members"]:
                enriched = dict(row)
                enriched["cluster_id"] = cluster_id
                enriched["cluster_size"] = cluster_size
                enriched["cluster_representative_archetype_id"] = representative["archetype_id"]
                enriched["cluster_representative_site_id"] = representative["site_id"]
                enriched["is_cluster_representative"] = (
                    row["archetype_id"] == representative["archetype_id"]
                    and row["site_id"] == representative["site_id"]
                )
                enriched["archetype_cluster_version"] = _ARCHETYPE_CLUSTER_VERSION
                enriched["descriptor_features_json"] = json.dumps(row["descriptor_vector"])
                clustered_rows.append(enriched)
    return clustered_rows


def build_archetype_rows(layout: StorageLayout, *, run_id: str) -> list[dict[str, Any]]:
    env_dir = layout.base_features_artifacts_dir / run_id
    archetype_rows: list[dict[str, Any]] = []
    for env_path in sorted(env_dir.glob("*.env_vectors.parquet")):
        env_df = read_dataframe(env_path)
        if env_df.empty:
            continue
        for site_id, site_df in env_df.groupby("site_id", sort=True):
            motif_class = str(site_df["motif_class"].iloc[0])
            descriptor_vector = _site_descriptor_vector(site_df)
            descriptor_hash = hashlib.sha256(
                json.dumps(descriptor_vector, sort_keys=True).encode("utf-8")
            ).hexdigest()
            archetype_rows.append(
                {
                    "run_id": run_id,
                    "site_id": site_id,
                    "motif_class": motif_class,
                    "archetype_id": f"{motif_class}:{descriptor_hash[:12]}",
                    "descriptor_hash": descriptor_hash,
                    "descriptor_vector": descriptor_vector,
                }
            )
    return _cluster_archetype_rows(archetype_rows)


def build_analysis_queue_batches(archetype_df: pd.DataFrame) -> list[dict[str, Any]]:
    queue_rows: list[dict[str, Any]] = []
    if archetype_df.empty:
        return queue_rows
    if "is_cluster_representative" in archetype_df.columns:
        representative_df = archetype_df[archetype_df["is_cluster_representative"] == True]  # noqa: E712
    else:
        representative_df = pd.DataFrame()
    if representative_df.empty:
        representative_df = archetype_df.drop_duplicates(subset=["archetype_id"])
    for motif_class, motif_df in representative_df.groupby("motif_class", sort=True):
        queue_rows.append(
            {
                "motif_class": motif_class,
                "archetype_ids": motif_df.head(20)["archetype_id"].tolist(),
                "cluster_ids": motif_df.head(20).get("cluster_id", pd.Series(dtype=str)).tolist(),
                "analysis_types": ["orca", "apbs", "openmm"],
            }
        )
    return queue_rows


def write_analysis_queue_yaml(path, *, run_id: str, queue_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"run_id": run_id, "batches": queue_rows}, handle, sort_keys=False)


def build_cluster_summary_rows(archetype_df: pd.DataFrame) -> list[dict[str, Any]]:
    if archetype_df.empty or "cluster_id" not in archetype_df.columns:
        return []
    summary_rows: list[dict[str, Any]] = []
    for cluster_id, cluster_df in archetype_df.groupby("cluster_id", sort=True):
        representative_df = cluster_df[cluster_df["is_cluster_representative"] == True]  # noqa: E712
        representative_row = representative_df.iloc[0].to_dict() if not representative_df.empty else cluster_df.iloc[0].to_dict()
        summary_rows.append(
            {
                "cluster_id": cluster_id,
                "motif_class": representative_row.get("motif_class"),
                "cluster_size": int(len(cluster_df.index)),
                "representative_archetype_id": representative_row.get("archetype_id"),
                "representative_site_id": representative_row.get("site_id"),
                "archetype_cluster_version": representative_row.get("archetype_cluster_version"),
            }
        )
    return summary_rows


def build_representative_archetype_rows(archetype_df: pd.DataFrame) -> list[dict[str, Any]]:
    if archetype_df.empty or "is_cluster_representative" not in archetype_df.columns:
        return archetype_df.drop_duplicates(subset=["archetype_id"]).to_dict(orient="records") if not archetype_df.empty else []
    representative_df = archetype_df[archetype_df["is_cluster_representative"] == True]  # noqa: E712
    if representative_df.empty:
        representative_df = archetype_df.drop_duplicates(subset=["archetype_id"])
    return representative_df.to_dict(orient="records")


def export_representative_fragment_inputs(
    layout: StorageLayout,
    *,
    run_id: str,
    representative_rows: list[dict[str, Any]],
    radius_angstrom: float = 6.0,
) -> list[dict[str, Any]]:
    fragment_rows: list[dict[str, Any]] = []
    if not representative_rows:
        return fragment_rows
    input_dir = layout.external_analysis_artifacts_dir / "orca" / run_id / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    atom_cache: dict[str, pd.DataFrame] = {}
    site_cache: dict[str, pd.DataFrame] = {}
    for row in representative_rows:
        site_id = str(row.get("site_id") or "")
        archetype_id = str(row.get("archetype_id") or "")
        motif_class = str(row.get("motif_class") or "")
        cluster_id = str(row.get("cluster_id") or "")
        pdb_id = str(row.get("pdb_id") or site_id.split("|", 1)[0] if site_id else "")
        if not pdb_id or not site_id or not archetype_id:
            continue
        if pdb_id not in atom_cache:
            atom_path = layout.site_envs_artifacts_dir / run_id / f"{pdb_id}.atoms.parquet"
            atom_cache[pdb_id] = read_dataframe(atom_path) if atom_path.exists() else pd.DataFrame()
        if pdb_id not in site_cache:
            site_path = layout.prepared_structures_artifacts_dir / run_id / f"{pdb_id}.sites.parquet"
            site_cache[pdb_id] = read_dataframe(site_path) if site_path.exists() else pd.DataFrame()
        atoms_df = atom_cache[pdb_id]
        sites_df = site_cache[pdb_id]
        if atoms_df.empty or sites_df.empty:
            continue
        site_matches = sites_df[sites_df["site_id"] == site_id]
        if site_matches.empty:
            continue
        site_row = site_matches.iloc[0]
        center = (
            float(site_row["x"]),
            float(site_row["y"]),
            float(site_row["z"]),
        )
        fragment_atoms: list[dict[str, Any]] = []
        for atom in atoms_df.to_dict(orient="records"):
            if not bool(atom.get("is_heavy", True)):
                continue
            dx = float(atom.get("x") or 0.0) - center[0]
            dy = float(atom.get("y") or 0.0) - center[1]
            dz = float(atom.get("z") or 0.0) - center[2]
            distance = (dx * dx + dy * dy + dz * dz) ** 0.5
            if distance <= radius_angstrom:
                fragment_atoms.append(
                    {
                        "element": str(atom.get("element") or "X"),
                        "x": float(atom.get("x") or 0.0),
                        "y": float(atom.get("y") or 0.0),
                        "z": float(atom.get("z") or 0.0),
                        "chain_id": str(atom.get("chain_id") or ""),
                        "residue_name": str(atom.get("residue_name") or ""),
                        "residue_number": int(atom.get("residue_number") or 0),
                        "atom_name": str(atom.get("atom_name") or ""),
                        "distance_to_site": round(distance, 6),
                    }
                )
        if not fragment_atoms:
            continue
        fragment_id = f"{cluster_id or archetype_id}:fragment"
        file_stem = _safe_file_stem(fragment_id)
        xyz_path = input_dir / f"{file_stem}.xyz"
        metadata_path = input_dir / f"{file_stem}.metadata.json"
        xyz_lines = [str(len(fragment_atoms)), fragment_id]
        for atom in fragment_atoms:
            xyz_lines.append(
                f"{atom['element']} {atom['x']:.6f} {atom['y']:.6f} {atom['z']:.6f}"
            )
        xyz_path.write_text("\n".join(xyz_lines) + "\n", encoding="utf-8")
        metadata = {
            "fragment_id": fragment_id,
            "run_id": run_id,
            "pdb_id": pdb_id,
            "site_id": site_id,
            "archetype_id": archetype_id,
            "cluster_id": cluster_id,
            "motif_class": motif_class,
            "radius_angstrom": radius_angstrom,
            "center_coordinates": {"x": center[0], "y": center[1], "z": center[2]},
            "fragment_atom_count": len(fragment_atoms),
            "fragment_atom_map": fragment_atoms,
            "assumptions": [
                "Fragment export is a site-centered heavy-atom neighborhood from prepared atom tables.",
                "Coordinates are exported directly from the current structure artifact and are not protonated QM-ready inputs.",
            ],
        }
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        fragment_rows.append(
            {
                "fragment_id": fragment_id,
                "archetype_id": archetype_id,
                "cluster_id": cluster_id,
                "motif_class": motif_class,
                "site_id": site_id,
                "pdb_id": pdb_id,
                "fragment_file": str(xyz_path),
                "metadata_file": str(metadata_path),
                "fragment_atom_count": len(fragment_atoms),
            }
        )
    return fragment_rows
