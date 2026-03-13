"""Sequence-identity-aware dataset splitting for CanonicalBindingSample records.

Primary strategy  (cluster_aware_split)
---------------------------------------
Groups records by receptor-sequence similarity using k-mer Jaccard clustering,
then assigns whole clusters to train / val / test.  This prevents proteins with
high sequence identity from appearing in both train and test sets (the main
source of leakage in ML benchmarks).

Algorithm:
1. Compute 5-gram k-mer sets for each receptor sequence.
2. Build an inverted index (k-mer → list of record indices).
3. Greedy single-linkage clustering: for each unassigned record, find
   candidates with enough shared k-mers, compute exact Jaccard, and assign
   similar records to the same cluster (Jaccard ≥ threshold).
4. Sort clusters largest-first, then greedily fill train → val → test.

Complexity: O(n × avg_candidates × k-mer_set_size).  For n ≤ 200 k records
of average length 300 aa with k=5 and max_candidates=500, this runs in a
few minutes on a single CPU.  For larger datasets, use MMseqs2 (see TODO).

Fallback strategy  (hash_based_split)
--------------------------------------
Records whose sequence_receptor is None are split by a deterministic MD5 hash
of their sample_id.  Records without a sequence always use this fallback.

TODO: add MMseqs2-based clustering as an optional fast path for very large
      datasets (>500k sequences).
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from pbdata.pairing import ParsedPairKey, parse_pair_identity_key

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SplitResult:
    train: list[str] = field(default_factory=list)
    val:   list[str] = field(default_factory=list)
    test:  list[str] = field(default_factory=list)

    def sizes(self) -> dict[str, int]:
        return {"train": len(self.train), "val": len(self.val), "test": len(self.test)}


@dataclass(frozen=True)
class PairSplitItem:
    item_id: str
    pair_identity_key: str
    affinity_type: str | None
    receptor_sequence: str | None
    receptor_identity: str
    representation_key: str
    hard_group_key: str
    scaffold_key: str = ""
    family_key: str = ""
    domain_group_key: str = ""
    pathway_group_key: str = ""
    fold_group_key: str = ""
    mutation_group_key: str = ""
    source_group_key: str = ""
    release_date: str | None = None


def _validate_split_fractions(train_frac: float, val_frac: float) -> None:
    if not 0.0 <= train_frac <= 1.0:
        raise ValueError(f"train_frac must be in [0.0, 1.0], got {train_frac}")
    if not 0.0 <= val_frac <= 1.0:
        raise ValueError(f"val_frac must be in [0.0, 1.0], got {val_frac}")
    if train_frac + val_frac > 1.0:
        raise ValueError(
            f"train_frac + val_frac must be <= 1.0, got {train_frac + val_frac}"
        )


def _normalize_mutation_key(parsed_pair: ParsedPairKey | None) -> str:
    raw = (parsed_pair.mutation_key if parsed_pair is not None else None) or "wt_or_unspecified"
    value = raw.strip().lower()
    if value in {"wt", "wildtype", "wt_or_unspecified"}:
        return "wildtype_family"
    if value.startswith("mutation_unknown"):
        return "mutation_unknown_family"
    return "mutant_family"


def _bucket_sequence_length(seq: str | None) -> str:
    if not seq:
        return "seq_unknown"
    n = len(seq)
    if n < 150:
        return "seq_short"
    if n < 400:
        return "seq_medium"
    return "seq_long"


# ---------------------------------------------------------------------------
# Hash-based fallback
# ---------------------------------------------------------------------------

def assign_split(
    sample_id: str,
    *,
    train_frac: float = 0.70,
    val_frac:   float = 0.15,
    seed:       int   = 42,
) -> str:
    """Deterministic hash-based split assignment for a single sample_id."""
    _validate_split_fractions(train_frac, val_frac)
    digest = hashlib.md5(f"{seed}:{sample_id}".encode()).hexdigest()
    val = int(digest[:8], 16) / 0xFFFF_FFFF
    if val < train_frac:
        return "train"
    if val < train_frac + val_frac:
        return "val"
    return "test"


def build_splits(
    sample_ids: Sequence[str],
    *,
    train_frac: float = 0.70,
    val_frac:   float = 0.15,
    seed:       int   = 42,
) -> SplitResult:
    """Pure hash-based split — fast but not sequence-identity-aware."""
    _validate_split_fractions(train_frac, val_frac)
    result = SplitResult()
    for sid in sample_ids:
        split = assign_split(sid, train_frac=train_frac, val_frac=val_frac, seed=seed)
        getattr(result, split).append(sid)
    return result


# ---------------------------------------------------------------------------
# K-mer Jaccard clustering
# ---------------------------------------------------------------------------

def _kmer_set(seq: str, k: int) -> frozenset[str]:
    return frozenset(seq[i : i + k] for i in range(len(seq) - k + 1))


def _cluster_sequences(
    sample_ids: list[str],
    sequences:  list[str | None],
    k:                int   = 5,
    threshold:        float = 0.30,
    max_candidates:   int   = 500,
) -> dict[str, int]:
    """Greedy single-linkage clustering by k-mer Jaccard similarity.

    Records with sequence=None get singleton clusters.

    Args:
        sample_ids:     One ID per record.
        sequences:      Receptor amino-acid sequences (or None).
        k:              K-mer length.
        threshold:      Minimum Jaccard similarity to merge into same cluster.
        max_candidates: Maximum similar candidates to evaluate per record
                        (caps worst-case runtime on redundant datasets).

    Returns:
        {sample_id: cluster_id} mapping.
    """
    n = len(sample_ids)

    kmer_sets: list[frozenset[str] | None] = [
        _kmer_set(seq, k) if seq and len(seq) >= k else None
        for seq in sequences
    ]

    # Inverted index: k-mer string → list of record indices
    inverted: dict[str, list[int]] = defaultdict(list)
    for i, kset in enumerate(kmer_sets):
        if kset is not None:
            for km in kset:
                inverted[km].append(i)

    cluster_of = [-1] * n
    next_cluster = 0

    for i in range(n):
        if cluster_of[i] != -1:
            continue

        cluster_of[i] = next_cluster

        if kmer_sets[i] is None:
            next_cluster += 1
            continue

        # Count shared k-mers with unassigned later records
        shared_counts: Counter[int] = Counter()
        for km in kmer_sets[i]:
            for j in inverted[km]:
                if j > i and cluster_of[j] == -1:
                    shared_counts[j] += 1

        # Evaluate the most-shared candidates up to max_candidates
        min_shared = max(1, int(threshold * len(kmer_sets[i]) * 0.4))
        for j, cnt in shared_counts.most_common(max_candidates):
            if cnt < min_shared or kmer_sets[j] is None:
                continue
            union = len(kmer_sets[i] | kmer_sets[j])
            if union and (len(kmer_sets[i] & kmer_sets[j]) / union) >= threshold:
                cluster_of[j] = next_cluster

        next_cluster += 1

    return {sample_ids[i]: cluster_of[i] for i in range(n)}


def cluster_aware_split(
    sample_ids: list[str],
    sequences:  list[str | None],
    *,
    train_frac:     float = 0.70,
    val_frac:       float = 0.15,
    seed:           int   = 42,
    k:              int   = 5,
    threshold:      float = 0.30,
    max_candidates: int   = 500,
    log_fn: object = print,
) -> SplitResult:
    """Split records such that similar sequences stay in the same partition.

    Args:
        sample_ids:     One ID per record.
        sequences:      receptor sequences (or None for no-sequence records).
        train_frac:     Target training-set fraction.
        val_frac:       Target validation-set fraction.
        seed:           Tie-break seed for deterministic ordering.
        k:              K-mer length for similarity (default 5 amino acids).
        threshold:      Jaccard threshold for merging records (default 0.30,
                        roughly equivalent to ~30-40% sequence identity).
        max_candidates: Candidate cap per record (limits runtime).
        log_fn:         Progress callback.
    """
    _validate_split_fractions(train_frac, val_frac)
    log_fn(
        f"Clustering {len(sample_ids):,} sequences "
        f"(k={k}, threshold={threshold})..."
    )
    cluster_map = _cluster_sequences(
        sample_ids, sequences, k=k, threshold=threshold,
        max_candidates=max_candidates,
    )
    n_clusters = len(set(cluster_map.values()))
    log_fn(f"Found {n_clusters:,} sequence clusters.")

    # Group by cluster, sort deterministically: (−size, first_sample_id)
    cluster_to_ids: dict[int, list[str]] = defaultdict(list)
    for sid, cid in cluster_map.items():
        cluster_to_ids[cid].append(sid)

    sorted_clusters = sorted(
        cluster_to_ids.values(),
        key=lambda ids: (
            -len(ids),
            hashlib.md5(f"{seed}:{ids[0]}".encode()).hexdigest(),
            ids[0],
        ),
    )

    total = len(sample_ids)
    train_target = int(total * train_frac)
    val_target   = int(total * val_frac)

    result = SplitResult()
    train_count = val_count = 0

    for ids in sorted_clusters:
        if train_count < train_target:
            result.train.extend(ids)
            train_count += len(ids)
        elif val_count < val_target:
            result.val.extend(ids)
            val_count += len(ids)
        else:
            result.test.extend(ids)

    return result


def build_pair_aware_splits(
    items: list[PairSplitItem],
    *,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
    seed: int = 42,
    k: int = 5,
    threshold: float = 0.30,
    max_candidates: int = 500,
    log_fn: object = print,
) -> tuple[SplitResult, dict[str, object]]:
    """Hierarchical split with hard leakage groups first, sequence clusters second.

    Method:
    1. Collapse rows into hard groups that keep the same biological pair family
       together. Mutation variants of the same pair stay in the same hard group.
    2. Cluster hard groups by receptor-sequence similarity.
    3. Assign whole sequence clusters to train/val/test while roughly balancing
       representation buckets.
    """
    _validate_split_fractions(train_frac, val_frac)
    if not items:
        return SplitResult(), {
            "mode": "pair_aware",
            "hard_group_count": 0,
            "sequence_cluster_count": 0,
            "representation_counts": {},
        }

    hard_groups: dict[str, list[PairSplitItem]] = defaultdict(list)
    for item in items:
        hard_groups[item.hard_group_key].append(item)

    group_ids = sorted(hard_groups)
    group_sequences: list[str | None] = []
    group_representation: dict[str, str] = {}
    group_sizes: dict[str, int] = {}
    for group_id in group_ids:
        rows = hard_groups[group_id]
        representative = max(rows, key=lambda row: len(row.receptor_sequence or ""))
        group_sequences.append(representative.receptor_sequence)
        group_representation[group_id] = representative.representation_key
        group_sizes[group_id] = len(rows)

    log_fn(
        f"Pair-aware grouping: {len(items):,} rows -> {len(group_ids):,} hard groups "
        f"(threshold={threshold}, k={k})."
    )
    group_cluster_map = _cluster_sequences(
        group_ids,
        group_sequences,
        k=k,
        threshold=threshold,
        max_candidates=max_candidates,
    )

    cluster_to_group_ids: dict[int, list[str]] = defaultdict(list)
    for group_id, cluster_id in group_cluster_map.items():
        cluster_to_group_ids[cluster_id].append(group_id)

    total_items = len(items)
    train_target = int(total_items * train_frac)
    val_target = int(total_items * val_frac)
    target_sizes = {"train": train_target, "val": val_target, "test": total_items - train_target - val_target}

    representation_counts_total: Counter[str] = Counter(item.representation_key for item in items)
    split_repr_counts: dict[str, Counter[str]] = {
        "train": Counter(),
        "val": Counter(),
        "test": Counter(),
    }
    split_sizes = {"train": 0, "val": 0, "test": 0}
    result = SplitResult()

    ordered_clusters = sorted(
        cluster_to_group_ids.values(),
        key=lambda ids: (
            -sum(group_sizes[group_id] for group_id in ids),
            hashlib.md5(f"{seed}:{ids[0]}".encode()).hexdigest(),
            ids[0],
        ),
    )

    def _cluster_items(group_id_list: list[str]) -> list[PairSplitItem]:
        rows: list[PairSplitItem] = []
        for group_id in group_id_list:
            rows.extend(hard_groups[group_id])
        return rows

    def _cluster_score(split_name: str, cluster_rows: list[PairSplitItem]) -> tuple[float, float, str]:
        size_after = split_sizes[split_name] + len(cluster_rows)
        size_target = max(target_sizes[split_name], 1)
        size_penalty = size_after / size_target

        repr_penalty = 0.0
        cluster_repr = Counter(row.representation_key for row in cluster_rows)
        for rep_key, count in cluster_repr.items():
            total_for_key = max(representation_counts_total.get(rep_key, 1), 1)
            after = split_repr_counts[split_name][rep_key] + count
            target = max(int(round(total_for_key * (target_sizes[split_name] / max(total_items, 1)))), 1)
            repr_penalty += after / target
        return (size_penalty, repr_penalty, split_name)

    for cluster_group_ids in ordered_clusters:
        cluster_rows = _cluster_items(cluster_group_ids)
        split_name = min(
            ("train", "val", "test"),
            key=lambda name: _cluster_score(name, cluster_rows),
        )
        getattr(result, split_name).extend(row.item_id for row in cluster_rows)
        split_sizes[split_name] += len(cluster_rows)
        split_repr_counts[split_name].update(row.representation_key for row in cluster_rows)

    metadata = {
        "mode": "pair_aware",
        "hard_group_count": len(group_ids),
        "sequence_cluster_count": len(cluster_to_group_ids),
        "representation_counts": dict(representation_counts_total),
        "targets": target_sizes,
    }
    return result, metadata


def build_grouped_pair_splits(
    items: list[PairSplitItem],
    *,
    grouping: str,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
    seed: int = 42,
    log_fn: object = print,
) -> tuple[SplitResult, dict[str, object]]:
    """Split pair-level rows by an explicit proxy grouping key.

    Assumptions:
    - ``scaffold`` uses ligand InChIKey/SMILES/ligand-id proxies already present
      in training examples. It is a conservative ligand-identity proxy, not a
      Murcko-scaffold implementation.
    - ``family`` uses exact receptor identity proxies (UniProt IDs or extracted
      receptor-sequence composites), not curated protein family annotations.
    - ``mutation`` uses the exact parsed mutation token within a receptor/partner
      family so wildtype and specific mutation series can be isolated together.
    """
    _validate_split_fractions(train_frac, val_frac)
    grouping_map = {
        "scaffold": "scaffold_key",
        "family": "family_key",
        "mutation": "mutation_group_key",
        "source": "source_group_key",
    }
    key_name = grouping_map.get(grouping)
    if key_name is None:
        raise ValueError(f"Unsupported grouping mode: {grouping}")
    if not items:
        return SplitResult(), {
            "mode": f"{grouping}_grouped",
            "group_count": 0,
            "representation_counts": {},
            "biological_assumptions": [],
        }

    groups: dict[str, list[PairSplitItem]] = defaultdict(list)
    for item in items:
        group_key = str(getattr(item, key_name, "") or "").strip() or item.hard_group_key or item.item_id
        groups[group_key].append(item)

    log_fn(f"Grouped split mode '{grouping}': {len(items):,} rows -> {len(groups):,} groups.")

    total_items = len(items)
    train_target = int(total_items * train_frac)
    val_target = int(total_items * val_frac)
    target_sizes = {"train": train_target, "val": val_target, "test": total_items - train_target - val_target}
    representation_counts_total: Counter[str] = Counter(item.representation_key for item in items)
    split_repr_counts: dict[str, Counter[str]] = {
        "train": Counter(),
        "val": Counter(),
        "test": Counter(),
    }
    split_sizes = {"train": 0, "val": 0, "test": 0}
    result = SplitResult()

    ordered_groups = sorted(
        groups.items(),
        key=lambda entry: (
            -len(entry[1]),
            hashlib.md5(f"{seed}:{entry[0]}".encode()).hexdigest(),
            entry[0],
        ),
    )

    def _group_score(split_name: str, rows: list[PairSplitItem]) -> tuple[float, float, str]:
        size_after = split_sizes[split_name] + len(rows)
        size_target = max(target_sizes[split_name], 1)
        size_penalty = size_after / size_target

        repr_penalty = 0.0
        group_repr = Counter(row.representation_key for row in rows)
        for rep_key, count in group_repr.items():
            total_for_key = max(representation_counts_total.get(rep_key, 1), 1)
            after = split_repr_counts[split_name][rep_key] + count
            target = max(int(round(total_for_key * (target_sizes[split_name] / max(total_items, 1)))), 1)
            repr_penalty += after / target
        return (size_penalty, repr_penalty, split_name)

    for _, rows in ordered_groups:
        split_name = min(
            ("train", "val", "test"),
            key=lambda name: _group_score(name, rows),
        )
        getattr(result, split_name).extend(row.item_id for row in rows)
        split_sizes[split_name] += len(rows)
        split_repr_counts[split_name].update(row.representation_key for row in rows)

    assumptions = {
        "scaffold": [
            "Ligand grouping uses InChIKey, SMILES, or ligand-id proxies already present in the training corpus.",
            "This is a conservative ligand-identity split, not a chemistry-toolkit Murcko scaffold split.",
        ],
        "family": [
            "Family grouping uses exact receptor identity proxies from UniProt IDs or extracted receptor sequences.",
            "It reduces receptor-identity leakage but does not replace curated homology-family annotations.",
        ],
        "mutation": [
            "Mutation grouping uses the exact parsed mutation token within a receptor/partner family.",
            "It is intended to keep wildtype and specific mutation-series leakage under control.",
        ],
        "source": [
            "Source grouping uses the preferred assay/source database assigned during merge.",
            "It reduces source-specific leakage but does not normalize away all protocol differences within a source.",
        ],
    }
    metadata = {
        "mode": f"{grouping}_grouped",
        "group_count": len(groups),
        "representation_counts": dict(representation_counts_total),
        "targets": target_sizes,
        "biological_assumptions": assumptions[grouping],
    }
    return result, metadata


def build_temporal_pair_splits(
    items: list[PairSplitItem],
    *,
    train_frac: float = 0.70,
    val_frac: float = 0.15,
    seed: int = 42,
    log_fn: object = print,
) -> tuple[SplitResult, dict[str, object]]:
    """Chronological split by entry release date.

    Assumptions:
    - Uses extracted RCSB entry release dates already present in the workspace.
    - Older released structures go to train first, then validation, then test.
    - Missing dates are treated as latest/unknown and pushed to the back so they
      do not silently contaminate earlier chronological partitions.
    """
    _validate_split_fractions(train_frac, val_frac)
    if not items:
        return SplitResult(), {
            "mode": "time_ordered",
            "dated_item_count": 0,
            "undated_item_count": 0,
            "biological_assumptions": [],
        }

    total = len(items)
    train_target = int(total * train_frac)
    val_target = int(total * val_frac)
    sorted_items = sorted(
        items,
        key=lambda item: (
            str(item.release_date or "9999-12-31"),
            hashlib.md5(f"{seed}:{item.item_id}".encode()).hexdigest(),
            item.item_id,
        ),
    )
    dated_count = sum(1 for item in sorted_items if item.release_date)
    undated_count = total - dated_count
    log_fn(
        f"Temporal split: {dated_count:,} dated items, {undated_count:,} undated items "
        f"(older releases assigned to train first)."
    )

    result = SplitResult()
    result.train.extend(item.item_id for item in sorted_items[:train_target])
    result.val.extend(item.item_id for item in sorted_items[train_target:train_target + val_target])
    result.test.extend(item.item_id for item in sorted_items[train_target + val_target:])
    metadata = {
        "mode": "time_ordered",
        "dated_item_count": dated_count,
        "undated_item_count": undated_count,
        "biological_assumptions": [
            "Temporal grouping uses extracted RCSB release dates already present in the workspace.",
            "Undated items are treated conservatively as latest/unknown and pushed to later partitions.",
        ],
    }
    return result, metadata


# ---------------------------------------------------------------------------
# Split diagnostics
# ---------------------------------------------------------------------------

def _split_item_map(items: list[PairSplitItem]) -> dict[str, PairSplitItem]:
    mapped: dict[str, PairSplitItem] = {}
    for item in items:
        if item.item_id not in mapped:
            mapped[item.item_id] = item
    return mapped


def _counter_summary(values: list[str]) -> dict[str, object]:
    counts = Counter(value for value in values if value)
    total = sum(counts.values())
    top_key, top_count = counts.most_common(1)[0] if counts else ("", 0)
    return {
        "unique_count": len(counts),
        "largest_group_key": top_key,
        "largest_group_count": top_count,
        "largest_group_fraction": round((top_count / total), 6) if total else 0.0,
    }


def build_split_diagnostics(
    items: list[PairSplitItem],
    result: SplitResult,
    *,
    strategy: str,
    extra_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build an audit-friendly diagnostics payload for pair-aware split outputs."""
    if not items:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "no_pair_items",
            "strategy": strategy,
            "summary": "No pair-aware split items were available for diagnostics.",
            "next_action": "Run extract and build training examples before expecting split diagnostics.",
            "counts": {
                "item_count": 0,
                "train_count": 0,
                "val_count": 0,
                "test_count": 0,
            },
            "split_breakdown": {},
            "overlap": {},
            "dominance": {},
            "metadata": extra_metadata or {},
        }

    item_by_id = _split_item_map(items)
    group_fields = {
        "pair_identity_key": lambda item: item.pair_identity_key,
        "hard_group_key": lambda item: item.hard_group_key,
        "family_key": lambda item: item.family_key,
        "domain_group_key": lambda item: item.domain_group_key,
        "pathway_group_key": lambda item: item.pathway_group_key,
        "fold_group_key": lambda item: item.fold_group_key,
        "scaffold_key": lambda item: item.scaffold_key,
        "mutation_group_key": lambda item: item.mutation_group_key,
        "source_group_key": lambda item: item.source_group_key,
        "representation_key": lambda item: item.representation_key,
        "receptor_identity": lambda item: item.receptor_identity,
    }

    split_to_ids = {
        "train": list(result.train),
        "val": list(result.val),
        "test": list(result.test),
    }
    split_breakdown: dict[str, dict[str, object]] = {}
    dominance: dict[str, dict[str, object]] = {}
    groups_to_splits: dict[str, dict[str, set[str]]] = {
        field: defaultdict(set)
        for field in group_fields
    }

    for split_name, item_ids in split_to_ids.items():
        split_items = [item_by_id[item_id] for item_id in item_ids if item_id in item_by_id]
        split_breakdown[split_name] = {
            "item_count": len(split_items),
            "supervision_item_count": len(split_items),
        }
        dominance[split_name] = {}
        for field_name, getter in group_fields.items():
            values = [str(getter(item) or "") for item in split_items]
            split_breakdown[split_name][field_name] = _counter_summary(values)
            if field_name in {
                "representation_key",
                "family_key",
                "domain_group_key",
                "pathway_group_key",
                "fold_group_key",
                "scaffold_key",
                "source_group_key",
            }:
                dominance[split_name][field_name] = split_breakdown[split_name][field_name]
            for value in values:
                if value:
                    groups_to_splits[field_name][value].add(split_name)

    overlap: dict[str, dict[str, object]] = {}
    for field_name, mapping in groups_to_splits.items():
        overlapping = {
            key: sorted(split_names)
            for key, split_names in mapping.items()
            if len(split_names) > 1
        }
        overlap[field_name] = {
            "overlap_count": len(overlapping),
            "sample_keys": sorted(overlapping)[:5],
        }

    largest_representation_fraction = max(
        (
            float(
                ((dominance.get(split_name) or {}).get("representation_key") or {}).get(
                    "largest_group_fraction",
                    0.0,
                )
            )
            for split_name in split_to_ids
        ),
        default=0.0,
    )
    largest_family_fraction = max(
        (
            float(
                ((dominance.get(split_name) or {}).get("family_key") or {}).get(
                    "largest_group_fraction",
                    0.0,
                )
            )
            for split_name in split_to_ids
        ),
        default=0.0,
    )
    hard_group_overlap = int((overlap.get("hard_group_key") or {}).get("overlap_count") or 0)
    family_overlap = int((overlap.get("family_key") or {}).get("overlap_count") or 0)
    domain_overlap = int((overlap.get("domain_group_key") or {}).get("overlap_count") or 0)
    pathway_overlap = int((overlap.get("pathway_group_key") or {}).get("overlap_count") or 0)
    fold_overlap = int((overlap.get("fold_group_key") or {}).get("overlap_count") or 0)
    scaffold_overlap = int((overlap.get("scaffold_key") or {}).get("overlap_count") or 0)
    largest_domain_fraction = max(
        (
            float(
                ((dominance.get(split_name) or {}).get("domain_group_key") or {}).get(
                    "largest_group_fraction",
                    0.0,
                )
            )
            for split_name in split_to_ids
        ),
        default=0.0,
    )
    largest_pathway_fraction = max(
        (
            float(
                ((dominance.get(split_name) or {}).get("pathway_group_key") or {}).get(
                    "largest_group_fraction",
                    0.0,
                )
            )
            for split_name in split_to_ids
        ),
        default=0.0,
    )

    status = "ready"
    if not result.val and not result.test:
        status = "no_held_out_split"
    elif hard_group_overlap > 0:
        status = "leakage_risk"
    elif (
        largest_family_fraction > 0.6
        or largest_domain_fraction > 0.6
        or largest_pathway_fraction > 0.6
        or largest_representation_fraction > 0.6
    ):
        status = "dominance_risk"
    elif family_overlap > 0 or domain_overlap > 0 or pathway_overlap > 0 or fold_overlap > 0 or scaffold_overlap > 0:
        status = "attention_needed"

    summary = (
        f"{strategy} split with {len(items):,} items; hard-group overlap={hard_group_overlap}, "
        f"family overlap={family_overlap}, domain overlap={domain_overlap}, "
        f"pathway overlap={pathway_overlap}, fold overlap={fold_overlap}, scaffold overlap={scaffold_overlap}, "
        f"largest family share={largest_family_fraction:.1%}."
    )
    if status == "leakage_risk":
        next_action = "Regenerate splits with a stricter grouping strategy before trusting held-out metrics."
    elif status == "dominance_risk":
        next_action = "Reduce dominant family or representation concentration before presenting benchmark results."
    elif status == "attention_needed":
        next_action = "Review family and scaffold overlap before treating the split as leakage-resistant."
    elif status == "no_held_out_split":
        next_action = "Create validation or test partitions before using the split for benchmark claims."
    else:
        next_action = "Inspect the diagnostics artifact alongside training-quality and release reports."

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "strategy": strategy,
        "summary": summary,
        "next_action": next_action,
        "counts": {
            "item_count": len(items),
            "train_count": len(result.train),
            "val_count": len(result.val),
            "test_count": len(result.test),
            "hard_group_overlap_count": hard_group_overlap,
            "family_overlap_count": family_overlap,
            "domain_overlap_count": domain_overlap,
            "pathway_overlap_count": pathway_overlap,
            "fold_overlap_count": fold_overlap,
            "scaffold_overlap_count": scaffold_overlap,
        },
        "split_breakdown": split_breakdown,
        "overlap": overlap,
        "dominance": dominance,
        "metadata": extra_metadata or {},
    }


def export_split_diagnostics(
    items: list[PairSplitItem],
    result: SplitResult,
    output_dir: Path,
    *,
    strategy: str,
    extra_metadata: dict[str, object] | None = None,
) -> tuple[Path, Path, dict[str, object]]:
    """Write machine-readable and markdown split diagnostics artifacts."""
    diagnostics = build_split_diagnostics(
        items,
        result,
        strategy=strategy,
        extra_metadata=extra_metadata,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "split_diagnostics.json"
    md_path = output_dir / "split_diagnostics.md"
    json_path.write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")

    lines = [
        "# Split Diagnostics",
        "",
        f"- Status: {diagnostics['status']}",
        f"- Strategy: {diagnostics['strategy']}",
        f"- Summary: {diagnostics['summary']}",
        f"- Next action: {diagnostics['next_action']}",
        "",
        "## Counts",
    ]
    for key, value in (diagnostics.get("counts") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend([
        "",
        "## Dominance",
    ])
    for split_name, payload in (diagnostics.get("dominance") or {}).items():
        family = (payload or {}).get("family_key") or {}
        domain = (payload or {}).get("domain_group_key") or {}
        pathway = (payload or {}).get("pathway_group_key") or {}
        scaffold = (payload or {}).get("scaffold_key") or {}
        representation = (payload or {}).get("representation_key") or {}
        lines.append(
            f"- {split_name}: family={family.get('largest_group_fraction', 0.0):.1%}, "
            f"domain={domain.get('largest_group_fraction', 0.0):.1%}, "
            f"pathway={pathway.get('largest_group_fraction', 0.0):.1%}, "
            f"scaffold={scaffold.get('largest_group_fraction', 0.0):.1%}, "
            f"representation={representation.get('largest_group_fraction', 0.0):.1%}"
        )
    lines.extend([
        "",
        "## Overlap",
    ])
    for field_name, payload in (diagnostics.get("overlap") or {}).items():
        lines.append(
            f"- {field_name}: {payload.get('overlap_count', 0)} overlapping key(s); "
            f"samples={', '.join(payload.get('sample_keys') or []) or 'none'}"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path, diagnostics


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_splits(
    result:     SplitResult,
    output_dir: Path,
    *,
    seed:     int  = 42,
    strategy: str  = "cluster_aware",
    extra_metadata: dict | None = None,
) -> None:
    """Write train.txt / val.txt / test.txt + metadata.json to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for split_name in ("train", "val", "test"):
        ids = getattr(result, split_name)
        (output_dir / f"{split_name}.txt").write_text("\n".join(ids) + "\n")

    sizes = result.sizes()
    total = sum(sizes.values())
    metadata: dict = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "seed":     seed,
        "strategy": strategy,
        "total":    total,
        "sizes":    sizes,
        "fractions": {k: round(v / total, 4) if total else 0 for k, v in sizes.items()},
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))
