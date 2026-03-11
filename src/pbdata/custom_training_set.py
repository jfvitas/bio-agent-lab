"""Custom training-set builder focused on diversity, trust, and anti-redundancy.

Assumptions:
- Selection starts from `model_ready_pairs.csv`, not the full extracted pool.
- The selector does not infer missing biology; it uses currently exported fields.
- One candidate is chosen per `(pair_identity_key, binding_affinity_type)` pool entry.
- Diversity is optimized conservatively over categorical coverage plus receptor
  sequence-family diversity, not over arbitrary dense embeddings.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from pbdata.dataset.splits import _normalize_mutation_key
from pbdata.pairing import parse_pair_identity_key
from pbdata.storage import StorageLayout

_CUSTOM_TRAINING_SET_CSV = "custom_training_set.csv"
_CUSTOM_TRAINING_EXCLUSIONS_CSV = "custom_training_exclusions.csv"
_CUSTOM_TRAINING_SUMMARY_JSON = "custom_training_summary.json"
_CUSTOM_TRAINING_MANIFEST_JSON = "custom_training_manifest.json"
_CUSTOM_TRAINING_SCORECARD_JSON = "custom_training_scorecard.json"
_CUSTOM_TRAINING_SPLIT_BENCHMARK_CSV = "custom_training_split_benchmark.csv"

_MODE_OPTIONS = {
    "generalist",
    "protein_ligand",
    "protein_protein",
    "mutation_effect",
    "high_trust",
}


@dataclass(frozen=True)
class SelectionCandidate:
    candidate_id: str
    pdb_id: str
    pair_identity_key: str
    binding_affinity_type: str
    source_database: str
    selected_preferred_source: str
    source_agreement_band: str
    receptor_uniprot_ids: str
    receptor_chain_ids: str
    ligand_key: str
    ligand_types: str
    matching_interface_types: str
    membrane_vs_soluble: str
    oligomeric_state: str
    homomer_or_heteromer: str
    experimental_method: str
    taxonomy_ids: str
    quality_score: float
    reported_measurement_count: int
    release_split: str
    mutation_family: str
    task_type: str
    pair_family_key: str
    receptor_identity: str
    receptor_sequence: str | None
    receptor_cluster_key: str
    mode_eligible: bool
    confidence_penalty: float
    base_score: float
    row: dict[str, str]


def _repo_path(name: str, repo_root: Path | None = None) -> Path:
    return (repo_root or Path.cwd()) / name


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)
    return path


def _safe_float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(str(value or "").strip())
    except ValueError:
        return default


def _safe_int(value: str | None, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except ValueError:
        return default


def _semicolon_values(raw: str | None) -> list[str]:
    return [item.strip() for item in str(raw or "").split(";") if item.strip()]


def _confidence_penalty(raw_json: str | None) -> float:
    if not raw_json:
        return 0.0
    try:
        parsed = json.loads(str(raw_json))
    except json.JSONDecodeError:
        return 0.2
    if not isinstance(parsed, dict):
        return 0.0
    penalty = 0.0
    for value in parsed.values():
        level = str(value or "").lower()
        if level == "medium":
            penalty += 0.08
        elif level == "low":
            penalty += 0.16
    return penalty


def _agreement_bonus(value: str) -> float:
    label = value.strip().lower()
    if label == "high":
        return 0.18
    if label == "medium":
        return 0.08
    if label == "low":
        return -0.08
    return 0.0


def _normalize_quality_band(score: float) -> str:
    if score >= 0.8:
        return "quality_high"
    if score >= 0.55:
        return "quality_medium"
    return "quality_low"


def _mode_filter(mode: str, task_type: str, mutation_family: str, confidence_penalty: float) -> bool:
    if mode == "protein_ligand":
        return task_type == "protein_ligand"
    if mode == "protein_protein":
        return task_type == "protein_protein"
    if mode == "mutation_effect":
        return mutation_family != "wildtype_family"
    if mode == "high_trust":
        return confidence_penalty <= 0.0
    return True


def _pair_family_key(task_type: str, receptor_identity: str, ligand_key: str, partner_key: str) -> str:
    if task_type == "protein_ligand":
        return f"{task_type}|{receptor_identity}|{ligand_key or '-'}"
    return f"{task_type}|{receptor_identity}|{partner_key or '-'}"


def _receptor_sequence_token(row: dict[str, str]) -> str | None:
    uniprot_ids = _semicolon_values(row.get("receptor_uniprot_ids"))
    if uniprot_ids:
        return "|".join(uniprot_ids)
    chain_ids = _semicolon_values(str(row.get("receptor_chain_ids") or "").replace(",", ";"))
    pdb_id = str(row.get("pdb_id") or "")
    if chain_ids and pdb_id:
        return "|".join(f"{pdb_id}:{chain_id}" for chain_id in chain_ids)
    return None


def _dedupe_model_ready_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    winners: list[dict[str, str]] = []
    excluded: list[dict[str, str]] = []
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row.get("pair_identity_key") or ""), str(row.get("binding_affinity_type") or ""))].append(row)

    def _rank(row: dict[str, str]) -> tuple[float, float, float, str]:
        preferred_match = 1.0 if str(row.get("source_database") or "") == str(row.get("selected_preferred_source") or "") else 0.0
        measurement_count = float(_safe_int(row.get("reported_measurement_count")))
        agreement = {"high": 3.0, "medium": 2.0, "low": 1.0}.get(str(row.get("source_agreement_band") or "").lower(), 0.0)
        return (preferred_match, agreement, measurement_count, str(row.get("source_database") or ""))

    for rows_for_key in grouped.values():
        ranked = sorted(rows_for_key, key=_rank, reverse=True)
        winners.append(ranked[0])
        for row in ranked[1:]:
            excluded.append({
                "pdb_id": str(row.get("pdb_id") or ""),
                "pair_identity_key": str(row.get("pair_identity_key") or ""),
                "binding_affinity_type": str(row.get("binding_affinity_type") or ""),
                "reason": "duplicate_pair_affinity_not_selected",
                "details": "Another source row ranked higher for the same pair and affinity type.",
            })
    return winners, excluded


def _build_candidates(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
    mode: str,
) -> tuple[list[SelectionCandidate], list[dict[str, str]]]:
    root = repo_root or Path.cwd()
    model_ready_rows = _read_csv(_repo_path("model_ready_pairs.csv", root))
    entry_rows = _read_csv(_repo_path("master_pdb_repository.csv", root))
    entry_by_pdb = {str(row.get("pdb_id") or ""): row for row in entry_rows if row.get("pdb_id")}
    deduped_rows, dedupe_exclusions = _dedupe_model_ready_rows(model_ready_rows)
    candidates: list[SelectionCandidate] = []
    for index, row in enumerate(deduped_rows):
        pdb_id = str(row.get("pdb_id") or "")
        pair_key = str(row.get("pair_identity_key") or "")
        affinity_type = str(row.get("binding_affinity_type") or "")
        parsed = parse_pair_identity_key(pair_key)
        task_type = parsed.task_type if parsed is not None else "unknown"
        mutation_family = _normalize_mutation_key(parsed)
        receptor_identity = str(row.get("receptor_uniprot_ids") or row.get("receptor_chain_ids") or pdb_id or "unknown")
        ligand_key = str(row.get("ligand_key") or "")
        partner_key = ",".join(parsed.partner_chain_ids) if parsed is not None and parsed.partner_chain_ids else ""
        pair_family = _pair_family_key(task_type, receptor_identity, ligand_key, partner_key)
        entry = entry_by_pdb.get(pdb_id, {})
        receptor_sequence = _receptor_sequence_token(row)
        confidence_penalty = _confidence_penalty(row.get("assay_field_confidence_json")) + _confidence_penalty(entry.get("field_confidence_json"))
        quality_score = _safe_float(entry.get("quality_score"), 0.0)
        base_score = (
            quality_score
            + min(math.log10(max(_safe_int(row.get("reported_measurement_count"), 1), 1) + 1.0), 0.6)
            + _agreement_bonus(str(row.get("source_agreement_band") or ""))
            + (0.15 if str(row.get("source_database") or "") == str(row.get("selected_preferred_source") or "") else 0.0)
            - confidence_penalty
        )
        candidate_id = f"{pdb_id}:{index}"
        candidates.append(SelectionCandidate(
            candidate_id=candidate_id,
            pdb_id=pdb_id,
            pair_identity_key=pair_key,
            binding_affinity_type=affinity_type,
            source_database=str(row.get("source_database") or ""),
            selected_preferred_source=str(row.get("selected_preferred_source") or ""),
            source_agreement_band=str(row.get("source_agreement_band") or ""),
            receptor_uniprot_ids=str(row.get("receptor_uniprot_ids") or ""),
            receptor_chain_ids=str(row.get("receptor_chain_ids") or ""),
            ligand_key=ligand_key,
            ligand_types=str(row.get("ligand_types") or ""),
            matching_interface_types=str(row.get("matching_interface_types") or ""),
            membrane_vs_soluble=str(entry.get("membrane_vs_soluble") or ""),
            oligomeric_state=str(entry.get("oligomeric_state") or ""),
            homomer_or_heteromer=str(entry.get("homomer_or_heteromer") or ""),
            experimental_method=str(entry.get("experimental_method") or ""),
            taxonomy_ids=str(entry.get("taxonomy_ids") or ""),
            quality_score=quality_score,
            reported_measurement_count=_safe_int(row.get("reported_measurement_count"), 1),
            release_split=str(row.get("release_split") or ""),
            mutation_family=mutation_family,
            task_type=task_type,
            pair_family_key=pair_family,
            receptor_identity=receptor_identity,
            receptor_sequence=receptor_sequence,
            receptor_cluster_key="",
            mode_eligible=_mode_filter(mode, task_type, mutation_family, confidence_penalty),
            confidence_penalty=confidence_penalty,
            base_score=base_score,
            row=row,
        ))

    if not candidates:
        return [], dedupe_exclusions

    cluster_map: dict[str, int] = {}
    token_to_cluster: dict[str, int] = {}
    for candidate in candidates:
        token = candidate.receptor_sequence or candidate.receptor_identity or candidate.candidate_id
        cluster_id = token_to_cluster.setdefault(token, len(token_to_cluster))
        cluster_map[candidate.candidate_id] = cluster_id
    normalized: list[SelectionCandidate] = []
    for candidate in candidates:
        normalized.append(SelectionCandidate(
            **{**candidate.__dict__, "receptor_cluster_key": f"cluster_{cluster_map.get(candidate.candidate_id, -1)}"}
        ))
    return normalized, dedupe_exclusions


def _candidate_tokens(candidate: SelectionCandidate) -> set[str]:
    tokens = {
        f"task:{candidate.task_type}",
        f"assay:{candidate.binding_affinity_type}",
        f"source:{candidate.selected_preferred_source or candidate.source_database}",
        f"agreement:{candidate.source_agreement_band or 'unknown'}",
        f"method:{candidate.experimental_method or 'unknown'}",
        f"membrane:{candidate.membrane_vs_soluble or 'unknown'}",
        f"oligomer:{candidate.oligomeric_state or 'unknown'}",
        f"assembly:{candidate.homomer_or_heteromer or 'unknown'}",
        f"mutation:{candidate.mutation_family}",
        f"quality:{_normalize_quality_band(candidate.quality_score)}",
        f"split:{candidate.release_split or 'unsplit'}",
    }
    for ligand_type in _semicolon_values(candidate.ligand_types):
        tokens.add(f"ligand:{ligand_type}")
    for interface_type in _semicolon_values(candidate.matching_interface_types):
        tokens.add(f"interface:{interface_type}")
    for taxonomy_id in _semicolon_values(candidate.taxonomy_ids):
        tokens.add(f"taxonomy:{taxonomy_id}")
    return tokens


def _benchmark_rows(selected: list[SelectionCandidate]) -> list[dict[str, str]]:
    """Summarize leakage-sensitive grouping modes over the selected set.

    Assumption:
    - This is a curation benchmark, not an ML performance benchmark. It reports
      how concentrated the selected set is under several biologically relevant
      grouping keys so the user can judge likely leakage pressure.
    """
    group_modes = {
        "release_split": lambda candidate: candidate.release_split or "unsplit",
        "receptor_cluster": lambda candidate: candidate.receptor_cluster_key,
        "pair_family": lambda candidate: candidate.pair_family_key,
        "mutation_family": lambda candidate: candidate.mutation_family,
        "task_type": lambda candidate: candidate.task_type,
        "taxonomy": lambda candidate: candidate.taxonomy_ids or "taxonomy_unknown",
    }
    rows: list[dict[str, str]] = []
    total = max(len(selected), 1)
    for mode_name, key_fn in group_modes.items():
        counts = Counter(key_fn(candidate) or "unknown" for candidate in selected)
        largest_group = max(counts.values(), default=0)
        singleton_groups = sum(1 for value in counts.values() if value == 1)
        rows.append({
            "benchmark_mode": mode_name,
            "group_count": str(len(counts)),
            "largest_group_size": str(largest_group),
            "largest_group_fraction": f"{(largest_group / total):.4f}" if total else "0.0000",
            "singleton_group_count": str(singleton_groups),
            "mean_group_size": f"{(sum(counts.values()) / len(counts)):.4f}" if counts else "0.0000",
            "dominant_groups": "; ".join(
                f"{group}={count}"
                for group, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:5]
            ),
        })
    return rows


def _build_scorecard(
    *,
    selected: list[SelectionCandidate],
    pool: list[SelectionCandidate],
    exclusions: list[dict[str, str]],
    mode: str,
    target_size: int,
    per_receptor_cluster_cap: int,
) -> dict[str, object]:
    selected_task_counts = Counter(candidate.task_type for candidate in selected)
    selected_source_counts = Counter(candidate.selected_preferred_source or candidate.source_database for candidate in selected)
    selected_ligand_types = Counter(
        ligand_type
        for candidate in selected
        for ligand_type in _semicolon_values(candidate.ligand_types)
    )
    selected_interface_types = Counter(
        interface_type
        for candidate in selected
        for interface_type in _semicolon_values(candidate.matching_interface_types)
    )
    release_split_counts = Counter(candidate.release_split or "unsplit" for candidate in selected)
    membrane_counts = Counter(candidate.membrane_vs_soluble or "unknown" for candidate in selected)
    method_counts = Counter(candidate.experimental_method or "unknown" for candidate in selected)
    mutation_counts = Counter(candidate.mutation_family for candidate in selected)
    exclusion_counts = Counter(str(row.get("reason") or "unknown") for row in exclusions)
    mean_quality = round(sum(candidate.quality_score for candidate in selected) / len(selected), 4) if selected else 0.0
    mean_measurements = round(
        sum(candidate.reported_measurement_count for candidate in selected) / len(selected), 4
    ) if selected else 0.0

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selection_mode": mode,
        "target_size": target_size,
        "selected_count": len(selected),
        "candidate_pool_count": len(pool),
        "selection_fraction": round((len(selected) / len(pool)), 4) if pool else 0.0,
        "per_receptor_cluster_cap": per_receptor_cluster_cap,
        "quality": {
            "mean_quality_score": mean_quality,
            "mean_reported_measurement_count": mean_measurements,
            "high_agreement_count": sum(1 for candidate in selected if candidate.source_agreement_band.lower() == "high"),
            "medium_agreement_count": sum(1 for candidate in selected if candidate.source_agreement_band.lower() == "medium"),
            "low_agreement_count": sum(1 for candidate in selected if candidate.source_agreement_band.lower() == "low"),
        },
        "diversity": {
            "selected_receptor_clusters": len({candidate.receptor_cluster_key for candidate in selected}),
            "selected_pair_families": len({candidate.pair_family_key for candidate in selected}),
            "selected_receptor_identities": len({candidate.receptor_identity for candidate in selected}),
            "release_splits": dict(release_split_counts),
            "task_types": dict(selected_task_counts),
            "sources": dict(selected_source_counts),
            "ligand_types": dict(selected_ligand_types),
            "interface_types": dict(selected_interface_types),
            "membrane_vs_soluble": dict(membrane_counts),
            "experimental_methods": dict(method_counts),
            "mutation_families": dict(mutation_counts),
        },
        "exclusions": {
            "count": len(exclusions),
            "reasons": dict(exclusion_counts),
        },
        "benchmark_modes": _benchmark_rows(selected),
        "recommended_next_step": (
            "Inspect the split benchmark and exclusion reasons, then rerun with a different mode "
            "or receptor-cluster cap if one receptor family dominates."
        ),
    }


def build_custom_training_set(
    layout: StorageLayout,
    *,
    repo_root: Path | None = None,
    mode: str = "generalist",
    target_size: int = 500,
    seed: int = 42,
    per_receptor_cluster_cap: int = 1,
    release_tag: str | None = None,
) -> dict[str, str]:
    if mode not in _MODE_OPTIONS:
        raise ValueError(f"mode must be one of: {', '.join(sorted(_MODE_OPTIONS))}")
    if target_size <= 0:
        raise ValueError("target_size must be > 0")
    if per_receptor_cluster_cap <= 0:
        raise ValueError("per_receptor_cluster_cap must be > 0")

    from pbdata.release_export import export_release_artifacts

    root = repo_root or Path.cwd()
    export_release_artifacts(layout, repo_root=root)
    candidates, exclusions = _build_candidates(layout, repo_root=root, mode=mode)

    pool = [candidate for candidate in candidates if candidate.mode_eligible]
    for candidate in candidates:
        if not candidate.mode_eligible:
            exclusions.append({
                "pdb_id": candidate.pdb_id,
                "pair_identity_key": candidate.pair_identity_key,
                "binding_affinity_type": candidate.binding_affinity_type,
                "reason": "filtered_by_mode",
                "details": f"Excluded by mode={mode}.",
            })

    token_freq = Counter(token for candidate in pool for token in _candidate_tokens(candidate))
    token_weights = {
        token: 1.0 / math.sqrt(max(freq, 1))
        for token, freq in token_freq.items()
    }

    selected: list[SelectionCandidate] = []
    covered_tokens: Counter[str] = Counter()
    receptor_cluster_counts: Counter[str] = Counter()
    pair_family_counts: Counter[str] = Counter()
    receptor_identity_counts: Counter[str] = Counter()

    remaining = pool[:]
    max_selection = min(target_size, len(remaining))

    def _score(candidate: SelectionCandidate) -> tuple[float, str]:
        novelty = 0.0
        diversity = 0.0
        for token in _candidate_tokens(candidate):
            weight = token_weights.get(token, 0.0)
            if covered_tokens[token] == 0:
                novelty += weight
            diversity += weight / (covered_tokens[token] + 1.0)
        redundancy_penalty = (
            0.55 * receptor_cluster_counts[candidate.receptor_cluster_key]
            + 0.45 * pair_family_counts[candidate.pair_family_key]
            + 0.20 * receptor_identity_counts[candidate.receptor_identity]
        )
        fresh_cluster_bonus = 0.50 if receptor_cluster_counts[candidate.receptor_cluster_key] == 0 else 0.0
        mode_bonus = 0.18 if mode == candidate.task_type else 0.0
        if mode == "mutation_effect" and candidate.mutation_family != "wildtype_family":
            mode_bonus += 0.30
        if mode == "high_trust":
            mode_bonus += max(0.0, 0.12 - candidate.confidence_penalty)
        score = candidate.base_score + novelty + 0.45 * diversity + fresh_cluster_bonus + mode_bonus - redundancy_penalty
        tie = hashlib.md5(f"{seed}:{candidate.candidate_id}".encode()).hexdigest()
        return (score, tie)

    while remaining and len(selected) < max_selection:
        eligible = [
            candidate
            for candidate in remaining
            if receptor_cluster_counts[candidate.receptor_cluster_key] < per_receptor_cluster_cap
            and pair_family_counts[candidate.pair_family_key] < 1
        ]
        if not eligible:
            eligible = [
                candidate
                for candidate in remaining
                if pair_family_counts[candidate.pair_family_key] < 1
            ]
        if not eligible:
            break
        winner = max(eligible, key=_score)
        selected.append(winner)
        for token in _candidate_tokens(winner):
            covered_tokens[token] += 1
        receptor_cluster_counts[winner.receptor_cluster_key] += 1
        pair_family_counts[winner.pair_family_key] += 1
        receptor_identity_counts[winner.receptor_identity] += 1
        remaining = [candidate for candidate in remaining if candidate.candidate_id != winner.candidate_id]

    selected_ids = {candidate.candidate_id for candidate in selected}
    for candidate in pool:
        if candidate.candidate_id not in selected_ids:
            reason = "target_limit_after_diversity_ranking"
            if pair_family_counts[candidate.pair_family_key] > 0:
                reason = "redundant_pair_family"
            elif receptor_cluster_counts[candidate.receptor_cluster_key] >= per_receptor_cluster_cap:
                reason = "receptor_cluster_cap_reached"
            exclusions.append({
                "pdb_id": candidate.pdb_id,
                "pair_identity_key": candidate.pair_identity_key,
                "binding_affinity_type": candidate.binding_affinity_type,
                "reason": reason,
                "details": "Not selected into the final diversity-optimized set.",
            })

    selection_columns = [
        "selection_rank",
        "selection_mode",
        "pdb_id",
        "pair_identity_key",
        "binding_affinity_type",
        "source_database",
        "selected_preferred_source",
        "source_agreement_band",
        "binding_affinity_value",
        "binding_affinity_unit",
        "binding_affinity_log10_standardized",
        "reported_measurement_count",
        "receptor_uniprot_ids",
        "receptor_chain_ids",
        "ligand_key",
        "ligand_types",
        "matching_interface_types",
        "release_split",
        "receptor_cluster_key",
        "pair_family_key",
        "mutation_family",
        "base_score",
        "quality_score",
    ]
    selected_rows: list[dict[str, str]] = []
    for rank, candidate in enumerate(selected, start=1):
        row = {
            "selection_rank": str(rank),
            "selection_mode": mode,
            "pdb_id": candidate.pdb_id,
            "pair_identity_key": candidate.pair_identity_key,
            "binding_affinity_type": candidate.binding_affinity_type,
            "source_database": candidate.source_database,
            "selected_preferred_source": candidate.selected_preferred_source,
            "source_agreement_band": candidate.source_agreement_band,
            "binding_affinity_value": str(candidate.row.get("binding_affinity_value") or ""),
            "binding_affinity_unit": str(candidate.row.get("binding_affinity_unit") or ""),
            "binding_affinity_log10_standardized": str(candidate.row.get("binding_affinity_log10_standardized") or ""),
            "reported_measurement_count": str(candidate.row.get("reported_measurement_count") or ""),
            "receptor_uniprot_ids": candidate.receptor_uniprot_ids,
            "receptor_chain_ids": candidate.receptor_chain_ids,
            "ligand_key": candidate.ligand_key,
            "ligand_types": candidate.ligand_types,
            "matching_interface_types": candidate.matching_interface_types,
            "release_split": candidate.release_split,
            "receptor_cluster_key": candidate.receptor_cluster_key,
            "pair_family_key": candidate.pair_family_key,
            "mutation_family": candidate.mutation_family,
            "base_score": f"{candidate.base_score:.4f}",
            "quality_score": f"{candidate.quality_score:.4f}",
        }
        selected_rows.append(row)
    selection_path = _write_csv(_repo_path(_CUSTOM_TRAINING_SET_CSV, root), selection_columns, selected_rows)

    exclusion_columns = ["pdb_id", "pair_identity_key", "binding_affinity_type", "reason", "details"]
    exclusion_path = _write_csv(
        _repo_path(_CUSTOM_TRAINING_EXCLUSIONS_CSV, root),
        exclusion_columns,
        sorted(exclusions, key=lambda row: (row["reason"], row["pdb_id"], row["pair_identity_key"])),
    )

    scorecard = _build_scorecard(
        selected=selected,
        pool=pool,
        exclusions=exclusions,
        mode=mode,
        target_size=target_size,
        per_receptor_cluster_cap=per_receptor_cluster_cap,
    )
    summary = {
        "generated_at": str(scorecard["generated_at"]),
        "selection_mode": mode,
        "target_size": target_size,
        "selected_count": len(selected),
        "candidate_pool_count": len(pool),
        "per_receptor_cluster_cap": per_receptor_cluster_cap,
        "selected_task_types": dict(scorecard["diversity"]["task_types"]),  # type: ignore[index]
        "selected_sources": dict(scorecard["diversity"]["sources"]),  # type: ignore[index]
        "selected_ligand_types": dict(scorecard["diversity"]["ligand_types"]),  # type: ignore[index]
        "selected_interface_types": dict(scorecard["diversity"]["interface_types"]),  # type: ignore[index]
        "selected_receptor_clusters": int(scorecard["diversity"]["selected_receptor_clusters"]),  # type: ignore[index]
        "selected_pair_families": int(scorecard["diversity"]["selected_pair_families"]),  # type: ignore[index]
        "mean_quality_score": float(scorecard["quality"]["mean_quality_score"]),  # type: ignore[index]
    }
    summary_path = _repo_path(_CUSTOM_TRAINING_SUMMARY_JSON, root)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    scorecard_path = _repo_path(_CUSTOM_TRAINING_SCORECARD_JSON, root)
    scorecard_path.write_text(json.dumps(scorecard, indent=2, sort_keys=True), encoding="utf-8")

    benchmark_rows = _benchmark_rows(selected)
    benchmark_path = _write_csv(
        _repo_path(_CUSTOM_TRAINING_SPLIT_BENCHMARK_CSV, root),
        [
            "benchmark_mode",
            "group_count",
            "largest_group_size",
            "largest_group_fraction",
            "singleton_group_count",
            "mean_group_size",
            "dominant_groups",
        ],
        benchmark_rows,
    )

    tag = release_tag or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = layout.data_dir / "custom_training_sets" / tag
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_paths: dict[str, str] = {}
    for path in (selection_path, exclusion_path, summary_path, scorecard_path, benchmark_path):
        copied = snapshot_dir / path.name
        shutil.copy2(path, copied)
        snapshot_paths[path.name] = str(copied)

    manifest = {
        "generated_at": summary["generated_at"],
        "selection_mode": mode,
        "seed": seed,
        "target_size": target_size,
        "per_receptor_cluster_cap": per_receptor_cluster_cap,
        "candidate_pool_count": len(pool),
        "selected_count": len(selected),
        "paths": {
            "custom_training_set_csv": str(selection_path),
            "custom_training_exclusions_csv": str(exclusion_path),
            "custom_training_summary_json": str(summary_path),
            "custom_training_scorecard_json": str(scorecard_path),
            "custom_training_split_benchmark_csv": str(benchmark_path),
            "snapshot_dir": str(snapshot_dir),
        },
        "coverage_objectives": [
            "maximize categorical coverage across task, source, assay, receptor family, ligand type, interface type, membrane context, and taxonomy",
            "prefer high-quality, high-agreement, low-penalty candidates",
            "penalize redundant pair families and overused receptor clusters",
        ],
        "snapshot_paths": snapshot_paths,
    }
    manifest_path = _repo_path(_CUSTOM_TRAINING_MANIFEST_JSON, root)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    snapshot_manifest_path = snapshot_dir / _CUSTOM_TRAINING_MANIFEST_JSON
    snapshot_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "custom_training_set_csv": str(selection_path),
        "custom_training_exclusions_csv": str(exclusion_path),
        "custom_training_summary_json": str(summary_path),
        "custom_training_manifest_json": str(manifest_path),
        "custom_training_scorecard_json": str(scorecard_path),
        "custom_training_split_benchmark_csv": str(benchmark_path),
        "custom_training_snapshot_dir": str(snapshot_dir),
    }
