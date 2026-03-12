"""Extract-time enrichment loading kept out of the CLI layer."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from pbdata.catalog import summarize_bulk_file, update_download_manifest
from pbdata.config import AppConfig
from pbdata.pairing import chain_group_key
from pbdata.source_state import write_source_state
from pbdata.storage import StorageLayout, reuse_existing_file, validate_bindingdb_raw_json

logger = logging.getLogger(__name__)


def load_external_assay_samples(
    config: AppConfig,
    *,
    layout: StorageLayout,
) -> dict[str, list]:
    grouped: dict[str, list] = defaultdict(list)

    if config.sources.skempi.enabled:
        from pbdata.sources.skempi import load_skempi_csv

        raw_path = config.sources.skempi.extra.get("local_path") or str(layout.raw_skempi_dir / "skempi_v2.csv")
        path = Path(raw_path)
        if path.exists():
            for sample in load_skempi_csv(path, download=False):
                if sample.pdb_id:
                    grouped[sample.pdb_id].append(sample)
        else:
            logger.warning("SKEMPI enabled but file not found: %s", path)

    if config.sources.pdbbind.enabled:
        from pbdata.sources.pdbbind import PDBbindAdapter

        local_dir_raw = config.sources.pdbbind.extra.get("local_dir")
        if local_dir_raw:
            local_dir = Path(str(local_dir_raw))
            if local_dir.exists():
                for sample in PDBbindAdapter(local_dir=local_dir).fetch_all():
                    if sample.pdb_id:
                        grouped[sample.pdb_id].append(sample)
            else:
                logger.warning("PDBbind enabled but local_dir not found: %s", local_dir)

    if config.sources.biolip.enabled:
        from pbdata.sources.biolip import BioLiPAdapter

        local_dir_raw = config.sources.biolip.extra.get("local_dir")
        if local_dir_raw:
            local_dir = Path(str(local_dir_raw))
            if local_dir.exists():
                for sample in BioLiPAdapter(local_dir=local_dir).fetch_all():
                    if sample.pdb_id:
                        grouped[sample.pdb_id].append(sample)
            else:
                logger.warning("BioLiP enabled but local_dir not found: %s", local_dir)

    return dict(grouped)


def _raw_uniprot_ids(raw: dict) -> list[str]:
    seen: dict[str, None] = {}
    for ent in raw.get("polymer_entities") or []:
        ids = ((ent.get("rcsb_polymer_entity_container_identifiers") or {}).get("uniprot_ids") or [])
        for uniprot_id in ids:
            if uniprot_id:
                seen[str(uniprot_id)] = None
    return list(seen)


def _raw_chain_ids_by_uniprot(raw: dict) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for ent in raw.get("polymer_entities") or []:
        ids = ((ent.get("rcsb_polymer_entity_container_identifiers") or {}).get("uniprot_ids") or [])
        chains = ((ent.get("rcsb_polymer_entity_container_identifiers") or {}).get("auth_asym_ids") or [])
        if not chains:
            continue
        for uniprot_id in ids:
            if not uniprot_id:
                continue
            mapping.setdefault(str(uniprot_id), [])
            for chain_id in chains:
                if chain_id and chain_id not in mapping[str(uniprot_id)]:
                    mapping[str(uniprot_id)].append(str(chain_id))
    return mapping


def _raw_ligand_inchikeys(raw: dict, chem_descriptors: dict[str, dict[str, str]]) -> list[str]:
    seen: dict[str, None] = {}
    for ent in raw.get("nonpolymer_entities") or []:
        comp_id = (((ent.get("nonpolymer_comp") or {}).get("chem_comp") or {}).get("id", ""))
        if not comp_id:
            continue
        desc = chem_descriptors.get(str(comp_id), {})
        inchikey = desc.get("InChIKey")
        if inchikey:
            seen[str(inchikey)] = None
    return list(seen)


def fetch_chembl_samples_for_raw(
    raw: dict,
    chem_descriptors: dict[str, dict[str, str]],
    config: AppConfig,
    *,
    layout: StorageLayout | None = None,
) -> list:
    if not config.sources.chembl.enabled:
        return []

    from pbdata.sources.chembl import ChEMBLAdapter

    accession_ids = _raw_uniprot_ids(raw)
    inchikeys = _raw_ligand_inchikeys(raw, chem_descriptors)
    if not accession_ids or not inchikeys:
        if layout is not None:
            write_source_state(
                layout,
                source_name="ChEMBL",
                status="missing_identifiers",
                mode="live_api",
                record_id=str(raw.get("rcsb_id") or "").upper() or None,
                notes="No UniProt accession or ligand InChIKey was available for ChEMBL lookup.",
                extra={
                    "accession_count": len(accession_ids),
                    "inchikey_count": len(inchikeys),
                },
            )
        return []

    adapter = ChEMBLAdapter()
    results: list = []
    seen: set[str] = set()
    failed_lookup_count = 0
    raw_pdb_id = str(raw.get("rcsb_id") or "").upper()
    chain_ids_by_uniprot = _raw_chain_ids_by_uniprot(raw)
    for accession in accession_ids:
        for inchikey in inchikeys:
            try:
                samples = adapter.fetch_by_uniprot_and_inchikey(accession, inchikey)
            except Exception as exc:
                logger.warning(
                    "ChEMBL lookup failed for accession=%s inchikey=%s: %s",
                    accession,
                    inchikey,
                    exc,
                )
                failed_lookup_count += 1
                if layout is not None:
                    write_source_state(
                        layout,
                        source_name="ChEMBL",
                        status="error",
                        mode="live_api",
                        record_id=f"{accession}|{inchikey}",
                        notes=str(exc),
                        extra={"pdb_id": raw_pdb_id or None},
                    )
                continue
            for sample in samples:
                if sample.sample_id in seen:
                    continue
                seen.add(sample.sample_id)
                chain_ids = chain_ids_by_uniprot.get(accession, sample.chain_ids_receptor or [])
                provenance = dict(sample.provenance or {})
                ligand_key = sample.ligand_inchi_key or sample.ligand_id or sample.ligand_smiles or "unknown_ligand"
                mutation_key = sample.mutation_string or sample.wildtype_or_mutant or f"mutation_unknown:{sample.source_record_id}"
                provenance["pair_grouping_override"] = "|".join([
                    "protein_ligand",
                    raw_pdb_id or "-",
                    chain_group_key(chain_ids),
                    ligand_key,
                    mutation_key,
                ])
                results.append(sample.model_copy(update={
                    "pdb_id": raw_pdb_id or sample.pdb_id,
                    "chain_ids_receptor": chain_ids or sample.chain_ids_receptor,
                    "provenance": provenance,
                }))
    if layout is not None:
        write_source_state(
            layout,
            source_name="ChEMBL",
            status="ready" if results else ("lookup_failed" if failed_lookup_count else "no_matches"),
            mode="live_api",
            record_id=raw_pdb_id or None,
            record_count=len(results),
            notes="ChEMBL enrichment lookup completed.",
            extra={
                "accession_count": len(accession_ids),
                "inchikey_count": len(inchikeys),
                "failed_lookup_count": failed_lookup_count,
            },
        )
    return results


def fetch_bindingdb_samples_for_pdb(
    pdb_id: str,
    config: AppConfig,
    *,
    layout: StorageLayout,
) -> list:
    if not config.sources.bindingdb.enabled or not pdb_id:
        return []

    from pbdata.sources.bindingdb import BindingDBAdapter

    local_dir = str(config.sources.bindingdb.extra.get("local_dir") or "").strip()
    local_cache_path = Path(local_dir) / f"{pdb_id.upper()}.json" if local_dir else None
    managed_cache_path = layout.raw_bindingdb_dir / f"{pdb_id.upper()}.json"
    cache_path = managed_cache_path
    cache_mode = "managed_cache"
    raw: dict | None = None

    if local_cache_path and reuse_existing_file(
        local_cache_path,
        validator=lambda path, expected=pdb_id: validate_bindingdb_raw_json(path, expected_pdb_id=expected),
    ):
        cache_path = local_cache_path
        cache_mode = "local_cache"
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    elif reuse_existing_file(
        managed_cache_path,
        validator=lambda path, expected=pdb_id: validate_bindingdb_raw_json(path, expected_pdb_id=expected),
    ):
        cache_path = managed_cache_path
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        managed_cache_path.parent.mkdir(parents=True, exist_ok=True)
        adapter = BindingDBAdapter()
        try:
            raw = adapter.fetch_metadata(pdb_id)
        except Exception as exc:
            logger.warning("BindingDB lookup failed for %s: %s", pdb_id, exc)
            write_source_state(
                layout,
                source_name="BindingDB",
                status="error",
                mode="live_api",
                record_id=pdb_id.upper(),
                notes=str(exc),
                extra={"configured_local_dir": local_dir or None},
            )
            return []
        managed_cache_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        cache_path = managed_cache_path
        cache_mode = "live_api"
        if not validate_bindingdb_raw_json(cache_path, expected_pdb_id=pdb_id):
            managed_cache_path.unlink(missing_ok=True)
            logger.warning("BindingDB payload for %s was invalid and was removed.", pdb_id)
            write_source_state(
                layout,
                source_name="BindingDB",
                status="invalid_payload",
                mode="live_api",
                cache_path=managed_cache_path,
                record_id=pdb_id.upper(),
                extra={"configured_local_dir": local_dir or None},
            )
            return []
        update_download_manifest([
            summarize_bulk_file(
                source_database="BindingDB",
                source_record_id=pdb_id.upper(),
                pdb_id=pdb_id.upper(),
                raw_file_path=managed_cache_path,
                raw_format="json",
                downloaded_at=datetime.now(timezone.utc).isoformat(),
                title="BindingDB cache payload",
                task_hint="protein_ligand",
                notes="BindingDB enrichment payload cached by PDB ID.",
                status="cached",
            )
        ], layout.catalog_path)

    try:
        samples = BindingDBAdapter().normalize_all(raw or {})
    except Exception as exc:
        logger.warning("BindingDB normalization failed for %s: %s", pdb_id, exc)
        write_source_state(
            layout,
            source_name="BindingDB",
            status="normalization_error",
            mode=cache_mode,
            cache_path=cache_path,
            record_id=pdb_id.upper(),
            notes=str(exc),
            extra={"configured_local_dir": local_dir or None},
        )
        return []

    samples = [
        sample.model_copy(update={
            "provenance": {
                **(sample.provenance or {}),
                "cache_mode": cache_mode,
                "cache_path": str(cache_path),
                "configured_local_dir": local_dir or None,
            },
        })
        for sample in samples
    ]
    write_source_state(
        layout,
        source_name="BindingDB",
        status="ready",
        mode=cache_mode,
        cache_path=cache_path,
        record_id=pdb_id.upper(),
        record_count=len(samples),
        notes="BindingDB enrichment loaded and normalized.",
        extra={"configured_local_dir": local_dir or None},
    )
    return samples
