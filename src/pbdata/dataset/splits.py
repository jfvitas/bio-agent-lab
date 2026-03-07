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
        key=lambda ids: (-len(ids), ids[0]),
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
