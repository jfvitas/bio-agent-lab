"""RCSB PDB Search and Data API client."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from pbdata.catalog import DEFAULT_MANIFEST_PATH, summarize_rcsb_entry, update_download_manifest
from pbdata.criteria import SearchCriteria

_SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"
_GRAPHQL_URL = "https://data.rcsb.org/graphql"
_TIMEOUT = 60
_BATCH_SIZE = 100
_CC_BATCH_SIZE = 200
_PAGE_SIZE = 10_000

_ENTRY_GQL = """
query BatchEntries($ids: [String!]!) {
  entries(entry_ids: $ids) {
    rcsb_id
    exptl { method }
    rcsb_entry_info {
      resolution_combined
      polymer_entity_count_protein
      nonpolymer_entity_count
      deposited_atom_count
      assembly_count
    }
    rcsb_accession_info {
      initial_release_date
      deposit_date
    }
    struct { title }
    struct_keywords {
      pdbx_keywords
      text
    }
    assemblies {
      rcsb_id
      pdbx_struct_assembly {
        oligomeric_details
        oligomeric_count
      }
      rcsb_assembly_info {
        polymer_entity_count
        polymer_entity_count_protein
      }
    }
    polymer_entities {
      rcsb_id
      entity_poly {
        pdbx_seq_one_letter_code_can
        type
      }
      rcsb_polymer_entity_container_identifiers {
        auth_asym_ids
        uniprot_ids
      }
      rcsb_entity_source_organism {
        ncbi_taxonomy_id
      }
    }
    nonpolymer_entities {
      rcsb_id
      nonpolymer_comp {
        chem_comp {
          id
          name
        }
      }
      rcsb_nonpolymer_entity_container_identifiers {
        auth_asym_ids
      }
    }
  }
}
"""

_CHEMCOMP_GQL = """
query BatchChemComps($ids: [String!]!) {
  chem_comps(comp_ids: $ids) {
    rcsb_id
    rcsb_chem_comp_descriptor {
      type
      descriptor
    }
  }
}
"""


def _build_query(criteria: SearchCriteria) -> dict[str, Any]:
    """Translate SearchCriteria into an RCSB Search API request body."""
    nodes: list[dict[str, Any]] = []

    if criteria.keyword_query:
        nodes.append({
            "type": "terminal",
            "service": "full_text",
            "parameters": {"value": criteria.keyword_query},
        })

    methods = criteria.rcsb_method_labels()
    if methods:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "exptl.method",
                "operator": "in",
                "negation": False,
                "value": methods,
            },
        })

    if criteria.max_resolution_angstrom is not None:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.resolution_combined",
                "operator": "less_or_equal",
                "value": criteria.max_resolution_angstrom,
            },
        })

    if criteria.min_protein_entities is not None:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                "operator": "greater_or_equal",
                "value": criteria.min_protein_entities,
            },
        })
    elif criteria.require_protein:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                "operator": "greater",
                "value": 0,
            },
        })

    if criteria.require_ligand:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.nonpolymer_entity_count",
                "operator": "greater",
                "value": 0,
            },
        })

    if criteria.max_deposited_atom_count is not None:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.deposited_atom_count",
                "operator": "less_or_equal",
                "value": criteria.max_deposited_atom_count,
            },
        })

    task_node = _build_task_type_node(criteria.task_types)
    if task_node is not None:
        nodes.append(task_node)

    if criteria.min_release_year is not None:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_accession_info.initial_release_date",
                "operator": "greater_or_equal",
                "value": f"{criteria.min_release_year}-01-01T00:00:00Z",
            },
        })

    if criteria.max_release_year is not None:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_accession_info.initial_release_date",
                "operator": "less_or_equal",
                "value": f"{criteria.max_release_year}-12-31T23:59:59Z",
            },
        })

    if not nodes:
        query: dict[str, Any] = {
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                "operator": "greater_or_equal",
                "value": 0,
            },
        }
    elif len(nodes) == 1:
        query = nodes[0]
    else:
        query = {"type": "group", "logical_operator": "and", "nodes": nodes}

    return {"query": query, "return_type": "entry"}


def _build_task_type_node(task_types: list[str]) -> dict[str, Any] | None:
    """Return an OR group for task-type constraints, or None if unconstrained."""
    all_types = {"protein_ligand", "protein_protein", "mutation_ddg"}
    if not task_types or set(task_types) >= all_types:
        return None

    type_nodes: list[dict[str, Any]] = []

    if "protein_ligand" in task_types:
        type_nodes.append({
            "type": "group",
            "logical_operator": "and",
            "nodes": [
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                        "operator": "greater_or_equal",
                        "value": 1,
                    },
                },
                {
                    "type": "terminal",
                    "service": "text",
                    "parameters": {
                        "attribute": "rcsb_entry_info.nonpolymer_entity_count",
                        "operator": "greater",
                        "value": 0,
                    },
                },
            ],
        })

    if "protein_protein" in task_types:
        type_nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                "operator": "greater_or_equal",
                "value": 2,
            },
        })

    if not type_nodes:
        return None
    if len(type_nodes) == 1:
        return type_nodes[0]
    return {"type": "group", "logical_operator": "or", "nodes": type_nodes}


def count_entries(criteria: SearchCriteria) -> int:
    """Return the number of RCSB entries matching criteria."""
    payload = _build_query(criteria)
    payload["request_options"] = {"paginate": {"start": 0, "rows": 1}}
    resp = requests.post(_SEARCH_URL, json=payload, timeout=_TIMEOUT)
    resp.raise_for_status()
    return int(resp.json().get("total_count", 0))


def search_entries(criteria: SearchCriteria) -> list[str]:
    """Return all PDB IDs matching criteria via paginated search."""
    payload = _build_query(criteria)
    ids: list[str] = []
    start = 0
    total: int | None = None

    while True:
        payload["request_options"] = {"paginate": {"start": start, "rows": _PAGE_SIZE}}
        resp = requests.post(_SEARCH_URL, json=payload, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if total is None:
            total = int(data.get("total_count", 0))

        results: list[dict[str, Any]] = data.get("result_set") or []
        if not results:
            break

        ids.extend(str(result["identifier"]) for result in results)
        start += len(results)
        if start >= total:
            break

    return ids


def fetch_entries_batch(pdb_ids: list[str]) -> list[dict[str, Any]]:
    """Fetch entry metadata for a batch of PDB IDs via RCSB GraphQL."""
    resp = requests.post(
        _GRAPHQL_URL,
        json={"query": _ENTRY_GQL, "variables": {"ids": pdb_ids}},
        headers={"Content-Type": "application/json"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if errors := body.get("errors"):
        raise RuntimeError(f"GraphQL errors: {errors}")
    return body.get("data", {}).get("entries") or []


def search_and_download(
    criteria: SearchCriteria,
    output_dir: Path,
    log_fn: Callable[[str], None] = print,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
) -> list[str]:
    """Search RCSB, save raw JSON files, and update the download manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)

    log_fn("Collecting entry IDs from RCSB Search API...")
    pdb_ids = search_entries(criteria)
    total = len(pdb_ids)
    log_fn(f"{total:,} entries to download. Fetching metadata in batches of {_BATCH_SIZE}...")

    downloaded: list[str] = []
    failed: list[str] = []
    manifest_rows: list[dict[str, str]] = []

    for batch_start in range(0, total, _BATCH_SIZE):
        batch = pdb_ids[batch_start : batch_start + _BATCH_SIZE]
        existing_ids = [pid for pid in batch if (output_dir / f"{pid}.json").exists()]
        to_fetch = [pid for pid in batch if pid not in existing_ids]
        already_have = len(existing_ids)
        downloaded.extend(existing_ids)

        for pid in existing_ids:
            raw_path = output_dir / f"{pid}.json"
            try:
                entry = json.loads(raw_path.read_text(encoding="utf-8"))
                manifest_rows.append(
                    summarize_rcsb_entry(
                        entry,
                        raw_path,
                        downloaded_at=datetime.now(timezone.utc).isoformat(),
                        status="cached",
                    )
                )
            except Exception as exc:
                log_fn(f"  WARN could not summarize cached file {raw_path.name}: {exc}")

        if to_fetch:
            try:
                entries = fetch_entries_batch(to_fetch)
                for entry in entries:
                    pid = str(entry.get("rcsb_id") or "")
                    if not pid:
                        continue
                    raw_path = output_dir / f"{pid}.json"
                    raw_path.write_text(json.dumps(entry, indent=2), encoding="utf-8")
                    downloaded.append(pid)
                    manifest_rows.append(
                        summarize_rcsb_entry(
                            entry,
                            raw_path,
                            downloaded_at=datetime.now(timezone.utc).isoformat(),
                            status="downloaded",
                        )
                    )
            except Exception as exc:
                log_fn(f"  WARN batch starting at {batch_start}: {exc}")
                failed.extend(to_fetch)
                time.sleep(2)

        done_so_far = batch_start + len(batch)
        if batch_start % (10 * _BATCH_SIZE) == 0 or done_so_far >= total:
            msg = f"  {done_so_far:,}/{total:,} processed"
            if already_have:
                msg += f"  ({already_have} skipped, already on disk)"
            if failed:
                msg += f"  ({len(failed)} failed)"
            log_fn(msg)

    summary = (
        f"Download complete: {len(downloaded):,} saved, {len(failed):,} failed."
        + (f"\nFailed IDs: {failed[:20]}" if failed else "")
    )
    log_fn(summary)
    if manifest_rows:
        update_download_manifest(manifest_rows, manifest_path)
        log_fn(f"Download manifest updated at {manifest_path}.")
    return downloaded


def fetch_chemcomp_descriptors(comp_ids: list[str]) -> dict[str, dict[str, str]]:
    """Fetch SMILES and InChIKey for a list of chemical component IDs."""
    result: dict[str, dict[str, str]] = {}
    unique_ids = list(dict.fromkeys(comp_ids))

    for batch_start in range(0, len(unique_ids), _CC_BATCH_SIZE):
        batch = unique_ids[batch_start : batch_start + _CC_BATCH_SIZE]
        try:
            resp = requests.post(
                _GRAPHQL_URL,
                json={"query": _CHEMCOMP_GQL, "variables": {"ids": batch}},
                headers={"Content-Type": "application/json"},
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            body = resp.json()
            if errors := body.get("errors"):
                raise RuntimeError(f"GraphQL errors: {errors}")
            for entry in body.get("data", {}).get("chem_comps") or []:
                cid = str(entry.get("rcsb_id") or "")
                descriptors: dict[str, str] = {}
                for descriptor in entry.get("rcsb_chem_comp_descriptor") or []:
                    dtype = descriptor.get("type", "")
                    dval = descriptor.get("descriptor", "")
                    if dtype and dval:
                        descriptors[str(dtype)] = str(dval)
                if cid:
                    result[cid] = descriptors
        except Exception:
            pass

    return result
