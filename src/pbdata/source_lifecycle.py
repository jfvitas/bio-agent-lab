"""Source asset discovery and lifecycle policy reporting.

This module makes local-source packaging status explicit so the platform can
differentiate between:
- sources that are enabled conceptually
- sources whose bulk/local assets are actually present
- sources that are only partially staged and still need follow-up work

The goal is to support a local-first workflow where large source drops are
managed intentionally rather than accumulating as opaque folders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pbdata.config import AppConfig
from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class SourceLifecyclePolicy:
    source_name: str
    packaging_mode: str
    freshness_check: str
    targeted_refresh: str
    stale_retention: str
    deletion_policy: str


_POLICIES: dict[str, SourceLifecyclePolicy] = {
    "bindingdb": SourceLifecyclePolicy(
        source_name="bindingdb",
        packaging_mode="bulk_dump_preferred_with_optional_per_pdb_cache",
        freshness_check="check staged bulk dump release when preparing a new local bootstrap build",
        targeted_refresh="use per-PDB live API only for selected training-set IDs or gap repair",
        stale_retention="retain the newest validated bulk zip and small per-PDB cache payloads used by active workspaces",
        deletion_policy="delete superseded per-PDB cache payloads after a newer bootstrap build; keep bulk dumps until a newer validated release is staged",
    ),
    "chembl": SourceLifecyclePolicy(
        source_name="chembl",
        packaging_mode="bulk_sqlite_snapshot",
        freshness_check="check staged ChEMBL release during periodic workspace refresh or before release prep",
        targeted_refresh="prefer snapshot replacement over per-record refresh",
        stale_retention="retain the active SQLite snapshot until the next one is validated",
        deletion_policy="delete superseded snapshots after migration/validation completes",
    ),
    "pdbbind": SourceLifecyclePolicy(
        source_name="pdbbind",
        packaging_mode="licensed_local_dataset",
        freshness_check="manual release check when the local licensed package is updated",
        targeted_refresh="none; replace or augment with a newer licensed release",
        stale_retention="retain the active licensed release used for dataset provenance",
        deletion_policy="do not delete without an explicit replacement and provenance update",
    ),
    "biolip": SourceLifecyclePolicy(
        source_name="biolip",
        packaging_mode="bulk_text_snapshot",
        freshness_check="check the staged BioLiP snapshot during bootstrap refresh planning",
        targeted_refresh="none; refresh by replacing the staged snapshot",
        stale_retention="retain the active snapshot used by the current workspace",
        deletion_policy="delete only after a newer validated snapshot is in place",
    ),
    "skempi": SourceLifecyclePolicy(
        source_name="skempi",
        packaging_mode="single_csv_snapshot",
        freshness_check="manual version check when mutation-effect coverage is refreshed",
        targeted_refresh="none; refresh by replacing the staged CSV",
        stale_retention="retain the active CSV used for reproducible mutation-effect runs",
        deletion_policy="delete only after a newer validated CSV is staged",
    ),
    "uniprot": SourceLifecyclePolicy(
        source_name="uniprot",
        packaging_mode="bulk_reviewed_snapshot_with_optional_unreviewed_expansion",
        freshness_check="check Swiss-Prot and optional TrEMBL staged releases during bootstrap rebuilds",
        targeted_refresh="use API lookup only for targeted repairs or missing accessions",
        stale_retention="retain the current reviewed snapshot; optional unreviewed snapshot may be pruned if storage pressure is high",
        deletion_policy="replace older snapshots after validating accession coverage against the bootstrap store",
    ),
    "alphafold_db": SourceLifecyclePolicy(
        source_name="alphafold_db",
        packaging_mode="bulk_metadata_snapshot_with_optional structure payloads",
        freshness_check="check Swiss-Prot metadata release before predicted-structure enrichment runs",
        targeted_refresh="refresh selected accessions when the training set requires structure fallback coverage",
        stale_retention="retain metadata indexes and only keep structure payloads referenced by active workspaces",
        deletion_policy="delete unreferenced predicted-structure payloads after manifest verification",
    ),
    "interpro": SourceLifecyclePolicy(
        source_name="interpro",
        packaging_mode="bulk_xml_snapshot",
        freshness_check="check staged InterPro release when annotation refresh is requested",
        targeted_refresh="prefer snapshot replacement over per-record refresh",
        stale_retention="retain the active annotation snapshot until a newer one is validated",
        deletion_policy="delete superseded snapshots after mapping validation completes",
    ),
    "reactome": SourceLifecyclePolicy(
        source_name="reactome",
        packaging_mode="bulk_mapping_snapshot",
        freshness_check="check staged mapping files during pathway-context refresh",
        targeted_refresh="prefer snapshot replacement",
        stale_retention="retain the active mapping set used by the workspace",
        deletion_policy="delete older mappings after validating UniProt coverage",
    ),
    "cath": SourceLifecyclePolicy(
        source_name="cath",
        packaging_mode="bulk_classification_snapshot",
        freshness_check="check staged classification files during split-governance refresh",
        targeted_refresh="prefer snapshot replacement",
        stale_retention="retain the active classification release used by split audits",
        deletion_policy="delete superseded releases after fold-group validation",
    ),
    "pfam": SourceLifecyclePolicy(
        source_name="pfam",
        packaging_mode="bulk_domain_snapshot",
        freshness_check="check staged Pfam files during domain-annotation refresh",
        targeted_refresh="prefer snapshot replacement",
        stale_retention="retain the active domain snapshot used by the workspace",
        deletion_policy="delete superseded snapshots after accession mapping validation",
    ),
    "scop": SourceLifecyclePolicy(
        source_name="scop",
        packaging_mode="bulk_classification_snapshot",
        freshness_check="check staged SCOPe files during split-governance refresh",
        targeted_refresh="prefer snapshot replacement",
        stale_retention="retain the active classification release used by split audits",
        deletion_policy="delete superseded releases after classification validation",
    ),
}


def build_source_lifecycle_report(
    layout: StorageLayout,
    config: AppConfig,
) -> dict[str, Any]:
    assets = [_source_asset_payload(name, layout, config) for name in _ordered_sources()]
    present_count = sum(1 for item in assets if item["status"] in {"ready", "partial"})
    ready_count = sum(1 for item in assets if item["status"] == "ready")
    blocked_count = sum(1 for item in assets if item["status"] == "missing")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "storage_root": str(layout.root),
        "summary": {
            "tracked_sources": len(assets),
            "ready_sources": ready_count,
            "partially_staged_sources": present_count - ready_count,
            "missing_sources": blocked_count,
        },
        "sources": assets,
        "next_action": _next_action(assets),
    }


def export_source_lifecycle_report(
    layout: StorageLayout,
    config: AppConfig,
) -> tuple[Path, Path, dict[str, Any]]:
    report = build_source_lifecycle_report(layout, config)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "source_lifecycle_report.json"
    md_path = layout.reports_dir / "source_lifecycle_report.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Source Lifecycle Report",
        "",
        f"- Storage root: {layout.root}",
        f"- Tracked sources: {report['summary']['tracked_sources']}",
        f"- Ready sources: {report['summary']['ready_sources']}",
        f"- Partially staged sources: {report['summary']['partially_staged_sources']}",
        f"- Missing sources: {report['summary']['missing_sources']}",
        f"- Next action: {report['next_action']}",
        "",
        "## Sources",
    ]
    for source in report["sources"]:
        lines.extend(
            [
                f"### {source['label']}",
                "",
                f"- Status: {source['status']}",
                f"- Enabled: {source['enabled']}",
                f"- Asset count: {source['asset_count']}",
                f"- Total bytes: {source['total_bytes']}",
                f"- Packaging mode: {source['policy']['packaging_mode']}",
                f"- Freshness check: {source['policy']['freshness_check']}",
                f"- Targeted refresh: {source['policy']['targeted_refresh']}",
                f"- Stale retention: {source['policy']['stale_retention']}",
                f"- Deletion policy: {source['policy']['deletion_policy']}",
                "",
            ]
        )
        for asset in source["assets"]:
            lines.append(
                f"- Asset `{asset['label']}`: {asset['status']} at `{asset['path']}`"
            )
        lines.append("")
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return json_path, md_path, report


def _ordered_sources() -> list[str]:
    preferred = [
        "bindingdb",
        "chembl",
        "pdbbind",
        "biolip",
        "skempi",
        "uniprot",
        "alphafold_db",
        "interpro",
        "reactome",
        "cath",
        "pfam",
        "scop",
    ]
    return preferred


def _source_asset_payload(name: str, layout: StorageLayout, config: AppConfig) -> dict[str, Any]:
    assets = _discover_assets(name, layout, config)
    present_assets = [asset for asset in assets if asset["exists"]]
    required_assets = [asset for asset in assets if asset["required"]]
    present_required_assets = [asset for asset in required_assets if asset["exists"]]
    status = "missing"
    if required_assets and len(present_required_assets) == len(required_assets):
        status = "ready"
    elif present_assets:
        status = "partial"
    policy = _POLICIES[name]
    source_cfg = getattr(config.sources, name)
    return {
        "name": name,
        "label": _label_for(name),
        "enabled": bool(source_cfg.enabled),
        "status": status,
        "asset_count": len(present_assets),
        "required_asset_count": len(required_assets),
        "total_bytes": sum(int(asset["size_bytes"]) for asset in present_assets),
        "assets": [
            {
                "label": asset["label"],
                "path": asset["path"],
                "exists": asset["exists"],
                "required": asset["required"],
                "status": "ready" if asset["exists"] else "missing",
                "size_bytes": asset["size_bytes"],
            }
            for asset in assets
        ],
        "policy": {
            "packaging_mode": policy.packaging_mode,
            "freshness_check": policy.freshness_check,
            "targeted_refresh": policy.targeted_refresh,
            "stale_retention": policy.stale_retention,
            "deletion_policy": policy.deletion_policy,
        },
    }


def _discover_assets(name: str, layout: StorageLayout, config: AppConfig) -> list[dict[str, Any]]:
    data_sources = layout.root / "data_sources"
    if name == "bindingdb":
        bulk_zip = Path(str(config.sources.bindingdb.extra.get("bulk_zip") or data_sources / "bindingdb" / "BDB-mySQL_All_202603_dmp.zip"))
        cache_dir = Path(str(config.sources.bindingdb.extra.get("local_dir") or layout.raw_bindingdb_dir))
        return [
            _asset("BindingDB bulk dump zip", bulk_zip),
            _asset("BindingDB local cache dir", cache_dir, directory=True, required=False),
        ]
    if name == "chembl":
        chembl_root = data_sources / "chembl"
        sqlite_candidates = list(chembl_root.rglob("*.db")) if chembl_root.exists() else []
        primary = sqlite_candidates[0] if sqlite_candidates else chembl_root / "chembl_latest.db"
        return [_asset("ChEMBL SQLite snapshot", primary)]
    if name == "pdbbind":
        base = Path(str(config.sources.pdbbind.extra.get("local_dir") or data_sources / "pdbbind"))
        return [
            _asset("PDBbind index directory", base / "index", directory=True),
            _asset("PDBbind general protein-ligand index", _first_existing(base / "index", "INDEX_general_PL*.lst")),
        ]
    if name == "biolip":
        base = Path(str(config.sources.biolip.extra.get("local_dir") or data_sources / "biolip"))
        return [_asset("BioLiP text snapshot", _first_existing(base, "BioLiP.txt"))]
    if name == "skempi":
        path = Path(str(config.sources.skempi.extra.get("local_path") or layout.raw_skempi_dir / "skempi_v2.csv"))
        return [_asset("SKEMPI v2 CSV", path)]
    if name == "uniprot":
        base = data_sources / "uniprot"
        return [
            _asset("UniProt Swiss-Prot snapshot", _first_existing(base, "uniprot_sprot*.gz")),
            _asset("UniProt TrEMBL snapshot", _first_existing(base, "uniprot_trembl*.gz"), required=False),
        ]
    if name == "alphafold_db":
        configured_archive = str(config.sources.alphafold_db.extra.get("local_archive") or "").strip()
        configured_dir = str(config.sources.alphafold_db.extra.get("local_dir") or "").strip()
        base = Path(configured_dir) if configured_dir else data_sources / "alphafold"
        archive_path = Path(configured_archive) if configured_archive else _first_existing(base, "swissprot*.tar*")
        return [
            _asset("AlphaFold bulk archive", archive_path),
            _asset("AlphaFold extracted metadata root", base, directory=True, required=False),
        ]
    if name == "interpro":
        base = data_sources / "interpro"
        return [_asset("InterPro XML snapshot", _first_existing(base, "interpro*.xml.gz"))]
    if name == "reactome":
        base = data_sources / "reactome"
        return [
            _asset("Reactome UniProt mapping", _first_existing(base, "UniProt2Reactome*.txt")),
            _asset("Reactome pathways", _first_existing(base, "ReactomePathways*.txt")),
        ]
    if name == "cath":
        base = data_sources / "cath"
        return [
            _asset("CATH domain list", _first_existing(base, "cath-domain-list*.txt")),
            _asset("CATH names", _first_existing(base, "cath-names*.txt")),
        ]
    if name == "pfam":
        base = data_sources / "pfam"
        return [_asset("Pfam clans table", _first_existing(base, "Pfam-A.clans.tsv*"))]
    if name == "scop":
        base = data_sources / "scope"
        return [
            _asset("SCOPe classification table", _first_existing(base, "dir.cla.scope*.txt")),
            _asset("SCOPe description table", _first_existing(base, "dir.des.scope*.txt")),
        ]
    raise KeyError(f"Unsupported source lifecycle name: {name}")


def _asset(label: str, path: Path, *, directory: bool = False, required: bool = True) -> dict[str, Any]:
    exists = path.exists()
    size_bytes = 0
    if exists:
        try:
            if directory:
                size_bytes = sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())
            else:
                size_bytes = path.stat().st_size
        except OSError:
            size_bytes = 0
    return {
        "label": label,
        "path": str(path),
        "exists": exists,
        "size_bytes": size_bytes,
        "required": required,
    }


def _first_existing(base: Path, pattern: str) -> Path:
    if base.exists():
        matches = sorted(base.glob(pattern))
        file_matches = [match for match in matches if match.is_file()]
        if file_matches:
            return file_matches[0]
        if matches:
            return matches[0]
    return base / pattern.replace("*", "_latest")


def _label_for(name: str) -> str:
    return {
        "bindingdb": "BindingDB",
        "chembl": "ChEMBL",
        "pdbbind": "PDBbind",
        "biolip": "BioLiP",
        "skempi": "SKEMPI v2",
        "uniprot": "UniProt",
        "alphafold_db": "AlphaFold DB",
        "interpro": "InterPro",
        "reactome": "Reactome",
        "cath": "CATH",
        "pfam": "Pfam",
        "scop": "SCOPe",
    }[name]


def _next_action(assets: list[dict[str, Any]]) -> str:
    missing_critical = [
        item["label"]
        for item in assets
        if item["name"] in {"bindingdb", "chembl", "uniprot", "alphafold_db"} and item["status"] != "ready"
    ]
    if missing_critical:
        return (
            "Finish staging the highest-value local-first assets: "
            + ", ".join(missing_critical)
            + "."
        )
    return "Bulk/local source assets are staged well enough to keep wiring ingestion and refresh logic."
