"""RCSB PDB Search and Data API client.

Uses:
- RCSB Search API v1  (https://search.rcsb.org) for querying by criteria
- RCSB GraphQL API    (https://data.rcsb.org/graphql) for batch metadata
- RCSB Data REST API  (https://data.rcsb.org/rest/v1/core/chemcomp) for
  chemical component descriptors (SMILES, InChIKey)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

import requests

from pbdata.criteria import SearchCriteria

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

_SEARCH_URL    = "https://search.rcsb.org/rcsbsearch/v1/query"
_GRAPHQL_URL   = "https://data.rcsb.org/graphql"
_CHEMCOMP_URL  = "https://data.rcsb.org/rest/v1/core/chemcomp"
_TIMEOUT       = 60       # seconds per request
_BATCH_SIZE    = 100      # entries per GraphQL request
_CC_BATCH_SIZE = 200      # chem-comp IDs per GraphQL request
_PAGE_SIZE     = 10_000   # IDs per search-API page (RCSB max)

# ---------------------------------------------------------------------------
# GraphQL query — entry-level fields only (no per-chain calls)
# TODO: add polymer_entities / nonpolymer_entities sub-queries for
#       sequences, chain IDs, and ligand SMILES.
# ---------------------------------------------------------------------------

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
    }
    rcsb_accession_info {
      initial_release_date
      deposit_date
    }
    struct { title }
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
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

def _build_query(criteria: SearchCriteria) -> dict[str, Any]:
    """Translate SearchCriteria into an RCSB Search API request body."""
    nodes: list[dict[str, Any]] = []

    # Experimental method
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

    # Resolution
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

    # Require protein
    if criteria.require_protein:
        nodes.append({
            "type": "terminal",
            "service": "text",
            "parameters": {
                "attribute": "rcsb_entry_info.polymer_entity_count_protein",
                "operator": "greater",
                "value": 0,
            },
        })

    # Task-type filter (OR across selected types)
    task_node = _build_task_type_node(criteria.task_types)
    if task_node is not None:
        nodes.append(task_node)

    # Minimum release year
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

    # Combine with AND
    if not nodes:
        # Fallback: return every deposited entry
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
        return None  # no restriction

    type_nodes: list[dict[str, Any]] = []

    if "protein_ligand" in task_types:
        # protein + at least one non-polymer entity
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
        # 2+ protein entities
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def count_entries(criteria: SearchCriteria) -> int:
    """Return the number of RCSB entries matching criteria (no data fetched)."""
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

        results: list[dict] = data.get("result_set") or []
        if not results:
            break

        ids.extend(r["identifier"] for r in results)
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
) -> list[str]:
    """Search RCSB and save raw entry metadata to output_dir as JSON files.

    Skips entries that already exist on disk (resumable).
    Uses batched GraphQL requests for efficiency.

    Args:
        criteria:   Search criteria.
        output_dir: Destination directory; one {PDB_ID}.json per entry.
        log_fn:     Callable for progress messages (thread-safe in GUI usage).

    Returns:
        List of PDB IDs that were successfully saved.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    log_fn("Collecting entry IDs from RCSB Search API...")
    pdb_ids = search_entries(criteria)
    total = len(pdb_ids)
    log_fn(f"{total:,} entries to download. Fetching metadata in batches of {_BATCH_SIZE}...")

    downloaded: list[str] = []
    failed: list[str] = []

    for batch_start in range(0, total, _BATCH_SIZE):
        batch = pdb_ids[batch_start : batch_start + _BATCH_SIZE]

        # Skip already-present files (allows resuming interrupted downloads)
        to_fetch = [pid for pid in batch if not (output_dir / f"{pid}.json").exists()]
        already_have = len(batch) - len(to_fetch)
        downloaded.extend(
            pid for pid in batch if (output_dir / f"{pid}.json").exists()
        )

        if to_fetch:
            try:
                entries = fetch_entries_batch(to_fetch)
                for entry in entries:
                    pid = entry.get("rcsb_id", "")
                    if pid:
                        (output_dir / f"{pid}.json").write_text(
                            json.dumps(entry, indent=2)
                        )
                        downloaded.append(pid)
            except Exception as exc:
                log_fn(f"  WARN batch starting at {batch_start}: {exc}")
                failed.extend(to_fetch)
                time.sleep(2)  # back off on error

        # Progress every 10 batches (~1000 entries)
        done_so_far = batch_start + len(batch)
        if batch_start % (10 * _BATCH_SIZE) == 0 or done_so_far >= total:
            msg = f"  {done_so_far:,}/{total:,} processed"
            if already_have:
                msg += f"  ({already_have} skipped, already on disk)"
            if failed:
                msg += f"  ({len(failed)} failed)"
            log_fn(msg)

    log_fn(
        f"Download complete — {len(downloaded):,} saved, {len(failed):,} failed."
        + (f"\nFailed IDs: {failed[:20]}" if failed else "")
    )
    return downloaded


# ---------------------------------------------------------------------------
# Chemical component (ligand) descriptor fetching
# ---------------------------------------------------------------------------

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


def fetch_chemcomp_descriptors(comp_ids: list[str]) -> dict[str, dict[str, str]]:
    """Fetch SMILES and InChIKey for a list of chemical component IDs.

    Returns a dict mapping comp_id → {descriptor_type: descriptor_string}.
    Descriptor types include 'SMILES', 'SMILES_CANONICAL', 'InChI', 'InChIKey'.
    """
    result: dict[str, dict[str, str]] = {}
    unique_ids = list(dict.fromkeys(comp_ids))  # deduplicate, preserve order

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
                cid = entry.get("rcsb_id", "")
                descriptors: dict[str, str] = {}
                for d in entry.get("rcsb_chem_comp_descriptor") or []:
                    dtype = d.get("type", "")
                    dval  = d.get("descriptor", "")
                    if dtype and dval:
                        descriptors[dtype] = dval
                if cid:
                    result[cid] = descriptors
        except Exception:
            # Non-fatal: SMILES enrichment is best-effort
            pass

    return result
