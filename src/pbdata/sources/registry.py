"""Source capability registry and reporting helpers.

This module makes the current source surface explicit without forcing a large
rewrite of the existing adapters. The biological/data-engineering assumption is
conservative: every source keeps its own native identifiers and access mode,
and the registry describes capabilities and requirements rather than pretending
all sources are equivalent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pbdata.config import AppConfig, SourceConfig
from pbdata.storage import StorageLayout


@dataclass(frozen=True)
class SourceDescriptor:
    name: str
    label: str
    category: str
    access_mode: str
    implementation_state: str
    enabled_by_default: bool
    requires_local_path: bool
    local_path_field: str | None
    identifiers: tuple[str, ...]
    dataset_roles: tuple[str, ...]
    description: str
    notes: str


_SOURCE_DESCRIPTORS: tuple[SourceDescriptor, ...] = (
    SourceDescriptor(
        name="rcsb",
        label="RCSB PDB",
        category="experimental_structure",
        access_mode="live_api",
        implementation_state="implemented",
        enabled_by_default=True,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("pdb_id", "entity_id", "chain_id"),
        dataset_roles=("structure_metadata", "coordinates", "bound_state_context"),
        description="Experimental structures, metadata, and structure downloads.",
        notes="Primary ingest source. Used for PDB search, metadata, and mmCIF/PDB downloads.",
    ),
    SourceDescriptor(
        name="bindingdb",
        label="BindingDB",
        category="affinity_bioactivity",
        access_mode="live_api_with_optional_cache",
        implementation_state="implemented",
        enabled_by_default=True,
        requires_local_path=False,
        local_path_field="local_dir",
        identifiers=("pdb_id", "ligand_name", "ligand_smiles"),
        dataset_roles=("affinity_enrichment", "bioactivity_context"),
        description="Binding affinity lookups keyed primarily by PDB ID during extract.",
        notes="Optional local cache supported. Sparse coverage by PDB ID; 404 often means no match, not pipeline failure.",
    ),
    SourceDescriptor(
        name="chembl",
        label="ChEMBL",
        category="affinity_bioactivity",
        access_mode="live_api",
        implementation_state="implemented",
        enabled_by_default=True,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "ligand_smiles", "inchikey"),
        dataset_roles=("affinity_enrichment", "bioactivity_context"),
        description="Bioactivity enrichment for ligand-target evidence during extract.",
        notes="Queried from extract-time ligand and protein identifiers when available.",
    ),
    SourceDescriptor(
        name="pdbbind",
        label="PDBbind",
        category="affinity_curated_dataset",
        access_mode="local_dataset",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=True,
        local_path_field="local_dir",
        identifiers=("pdb_id", "ligand_component_id"),
        dataset_roles=("curated_affinity", "benchmark_corpus"),
        description="Locally licensed curated protein-ligand affinity dataset.",
        notes="Requires a local dataset directory. Not enabled by default because licensing/local availability varies.",
    ),
    SourceDescriptor(
        name="biolip",
        label="BioLiP",
        category="ligand_annotation",
        access_mode="local_dataset",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=True,
        local_path_field="local_dir",
        identifiers=("pdb_id", "chain_id", "ligand_component_id"),
        dataset_roles=("binding_site_annotation", "biological_relevance_filtering"),
        description="Local BioLiP binding-site annotations.",
        notes="Requires local files. Useful for biologically relevant ligand context and binding-site review.",
    ),
    SourceDescriptor(
        name="skempi",
        label="SKEMPI v2",
        category="mutation_effect_dataset",
        access_mode="bulk_download_or_local_override",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field="local_path",
        identifiers=("pdb_id", "mutation_token", "partner_chain"),
        dataset_roles=("mutation_effects", "protein_protein_binding"),
        description="Protein-protein mutation/ddG dataset.",
        notes="Can be downloaded as a bulk CSV or pointed at a local override path.",
    ),
    SourceDescriptor(
        name="alphafold_db",
        label="AlphaFold DB",
        category="predicted_structure",
        access_mode="bulk_download_or_api",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id",),
        dataset_roles=("predicted_structures", "structure_fallback"),
        description="Predicted single-chain structure coverage keyed by UniProt accession.",
        notes="Adapter support is available for metadata harvest by UniProt accession. Full bulk-ingest/workspace orchestration remains future work.",
    ),
    SourceDescriptor(
        name="uniprot",
        label="UniProt",
        category="sequence_annotation",
        access_mode="live_api_or_bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "gene_name", "taxonomy_id"),
        dataset_roles=("canonical_protein_identity", "sequence_annotation", "functional_metadata"),
        description="Canonical protein identity, sequence, and annotation surface.",
        notes="Adapter support is available for metadata harvest and cross-source annotation enrichment.",
    ),
    SourceDescriptor(
        name="interpro",
        label="InterPro",
        category="domain_annotation",
        access_mode="live_api_or_bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "interpro_id"),
        dataset_roles=("domain_annotation", "family_annotation"),
        description="Protein family, domain, and site annotation aggregation.",
        notes="Metadata-harvest adapter is available via PDBe/SIFTS structure mappings and UniProt cross-references.",
    ),
    SourceDescriptor(
        name="pfam",
        label="Pfam",
        category="domain_annotation",
        access_mode="bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "pfam_id"),
        dataset_roles=("domain_annotation", "family_annotation"),
        description="Protein domain families for grouping and feature context.",
        notes="Metadata-harvest adapter is available via PDBe/SIFTS structure mappings and UniProt cross-references.",
    ),
    SourceDescriptor(
        name="cath",
        label="CATH",
        category="structural_classification",
        access_mode="bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("pdb_id", "chain_id", "cath_id"),
        dataset_roles=("fold_classification", "split_grouping"),
        description="Structural fold / architecture classification for leakage-aware evaluation.",
        notes="Metadata-harvest adapter is available via PDBe/SIFTS chain-aware fold mappings.",
    ),
    SourceDescriptor(
        name="scop",
        label="SCOP",
        category="structural_classification",
        access_mode="bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("pdb_id", "chain_id", "scop_id"),
        dataset_roles=("fold_classification", "split_grouping"),
        description="Alternative structural classification surface for split governance.",
        notes="Metadata-harvest adapter is available via PDBe/SIFTS chain-aware fold mappings.",
    ),
    SourceDescriptor(
        name="string",
        label="STRING",
        category="interaction_network",
        access_mode="live_api_or_bulk_download",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "string_id"),
        dataset_roles=("interaction_context", "graph_enrichment"),
        description="Protein-protein interaction network context.",
        notes="Planned graph-context connector; current repo only has connector stubs.",
    ),
    SourceDescriptor(
        name="biogrid",
        label="BioGRID",
        category="interaction_network",
        access_mode="bulk_download",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("gene_name", "uniprot_id", "biogrid_id"),
        dataset_roles=("interaction_context", "graph_enrichment"),
        description="Interaction evidence surface complementary to STRING.",
        notes="Planned adapter for broader biological context integration.",
    ),
    SourceDescriptor(
        name="intact",
        label="IntAct",
        category="interaction_network",
        access_mode="live_api_or_bulk_download",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "intact_id"),
        dataset_roles=("interaction_context", "interaction_evidence"),
        description="Curated interaction evidence with experiment context.",
        notes="Planned; useful for higher-provenance interaction edges.",
    ),
    SourceDescriptor(
        name="reactome",
        label="Reactome",
        category="pathway_context",
        access_mode="live_api_or_bulk_download",
        implementation_state="implemented",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "reactome_id"),
        dataset_roles=("pathway_context", "graph_enrichment"),
        description="Pathway membership and systems-biology context.",
        notes="Graph connector and metadata-harvest adapter support are available; broader ingestion/orchestration remains future work.",
    ),
    SourceDescriptor(
        name="sabio_rk",
        label="SABIO-RK",
        category="kinetics_assay_context",
        access_mode="live_api_or_bulk_download",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "enzyme_id", "sabio_rk_id"),
        dataset_roles=("kinetics_context", "assay_context"),
        description="Reaction-kinetics and assay-condition context.",
        notes="Planned kinetics source for richer experimental metadata.",
    ),
    SourceDescriptor(
        name="prosite",
        label="PROSITE",
        category="motif_site_annotation",
        access_mode="bulk_download",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "prosite_id"),
        dataset_roles=("motif_annotation", "site_annotation"),
        description="Motif and site annotation surface.",
        notes="Planned motif/site enrichment adapter.",
    ),
    SourceDescriptor(
        name="elm",
        label="ELM",
        category="motif_site_annotation",
        access_mode="bulk_download_or_live_site",
        implementation_state="planned",
        enabled_by_default=False,
        requires_local_path=False,
        local_path_field=None,
        identifiers=("uniprot_id", "motif_id"),
        dataset_roles=("motif_annotation", "interaction_motif_context"),
        description="Eukaryotic linear motif annotations.",
        notes="Planned motif-context connector for sequence and interaction features.",
    ),
)


def list_source_descriptors() -> list[SourceDescriptor]:
    return list(_SOURCE_DESCRIPTORS)


def get_source_descriptor(name: str) -> SourceDescriptor | None:
    lookup = name.strip().lower()
    for descriptor in _SOURCE_DESCRIPTORS:
        if descriptor.name == lookup:
            return descriptor
    return None


def _source_config_for(app_config: AppConfig, name: str) -> SourceConfig:
    return getattr(app_config.sources, name, SourceConfig())


def build_source_capability_report(
    layout: StorageLayout,
    app_config: AppConfig,
) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    enabled_count = 0
    live_enabled_count = 0
    local_enabled_count = 0
    misconfigured_count = 0
    implemented_count = 0
    planned_count = 0

    for descriptor in _SOURCE_DESCRIPTORS:
        if descriptor.implementation_state == "implemented":
            implemented_count += 1
        else:
            planned_count += 1
        source_cfg = _source_config_for(app_config, descriptor.name)
        enabled = bool(source_cfg.enabled)
        configured_path = ""
        if descriptor.local_path_field:
            configured_path = str(source_cfg.extra.get(descriptor.local_path_field) or "").strip()
        has_required_path = (not descriptor.requires_local_path) or bool(configured_path)

        status = "disabled"
        if enabled and descriptor.requires_local_path and not has_required_path:
            status = "misconfigured_missing_local_path"
        elif enabled:
            status = "enabled"

        if enabled:
            enabled_count += 1
            if descriptor.access_mode.startswith("local"):
                local_enabled_count += 1
            else:
                live_enabled_count += 1
        if status == "misconfigured_missing_local_path":
            misconfigured_count += 1

        rows.append(
            {
                "name": descriptor.name,
                "label": descriptor.label,
                "category": descriptor.category,
                "access_mode": descriptor.access_mode,
                "implementation_state": descriptor.implementation_state,
                "status": status,
                "enabled": enabled,
                "requires_local_path": descriptor.requires_local_path,
                "configured_path": configured_path,
                "identifiers": list(descriptor.identifiers),
                "dataset_roles": list(descriptor.dataset_roles),
                "description": descriptor.description,
                "notes": descriptor.notes,
            }
        )

    if misconfigured_count:
        readiness = "needs_configuration"
        next_action = "Configure required local dataset paths before relying on every enabled source."
    elif enabled_count == 0:
        readiness = "no_sources_enabled"
        next_action = "Enable at least one source before running ingest or extract."
    else:
        readiness = "ready"
        next_action = "Run Ingest for raw acquisition, then Extract to materialize source-aware assay enrichment."

    summary = (
        f"{enabled_count} enabled sources "
        f"({live_enabled_count} live-capable, {local_enabled_count} local-dataset)"
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": readiness,
        "summary": summary,
        "next_action": next_action,
        "counts": {
            "total_sources": len(_SOURCE_DESCRIPTORS),
            "implemented_sources": implemented_count,
            "planned_sources": planned_count,
            "enabled_sources": enabled_count,
            "enabled_live_sources": live_enabled_count,
            "enabled_local_sources": local_enabled_count,
            "misconfigured_sources": misconfigured_count,
        },
        "sources": rows,
    }


def export_source_capability_report(
    layout: StorageLayout,
    app_config: AppConfig,
) -> tuple[Path, Path, dict[str, object]]:
    report = build_source_capability_report(layout, app_config)
    layout.reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = layout.reports_dir / "source_capabilities.json"
    md_path = layout.reports_dir / "source_capabilities.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = [
        "# Source Capabilities",
        "",
        f"- Status: {report['status']}",
        f"- Summary: {report['summary']}",
        f"- Next action: {report['next_action']}",
        "",
        "## Sources",
    ]
    for row in report["sources"]:
        payload = dict(row)
        lines.append(
            f"- {payload['label']} ({payload['name']}): {payload['status']}; "
            f"{payload['implementation_state']}; {payload['access_mode']}; "
            f"roles={', '.join(payload['dataset_roles'])}"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, report
