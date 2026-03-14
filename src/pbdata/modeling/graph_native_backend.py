"""Native graph backend preparation helpers.

These helpers normalize PyG-ready JSON samples into dense matrix-style payloads
and optionally convert them into torch_geometric Data objects when the required
libraries are installed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class NativeGraphSample:
    example_id: str
    pdb_id: str
    pair_identity_key: str
    node_feature_keys: tuple[str, ...]
    edge_feature_keys: tuple[str, ...]
    node_features: tuple[tuple[float, ...], ...]
    edge_features: tuple[tuple[float, ...], ...]
    edge_index: tuple[tuple[int, int], ...]
    node_count: int
    edge_count: int


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _sorted_feature_keys(rows: list[dict[str, Any]], key_name: str) -> tuple[str, ...]:
    declared: list[str] = []
    for row in rows:
        keys = row.get(key_name) or []
        if isinstance(keys, list):
            for value in keys:
                text = str(value).strip()
                if text:
                    declared.append(text)
    if declared:
        return tuple(sorted(dict.fromkeys(declared)))
    discovered: set[str] = set()
    source_key = "node_features" if key_name.startswith("node") else "edge_features"
    for row in rows:
        feature_rows = row.get(source_key) or []
        if not isinstance(feature_rows, list):
            continue
        for feature_row in feature_rows:
            if isinstance(feature_row, dict):
                discovered.update(str(key).strip() for key in feature_row.keys() if str(key).strip())
    return tuple(sorted(discovered))


def _dense_feature_rows(feature_rows: list[dict[str, Any]], feature_keys: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
    dense_rows: list[tuple[float, ...]] = []
    for feature_row in feature_rows:
        if not isinstance(feature_row, dict):
            continue
        dense_rows.append(tuple(float(feature_row.get(key, 0.0) or 0.0) for key in feature_keys))
    return tuple(dense_rows)


def load_native_graph_samples(samples_path: str | Path) -> list[NativeGraphSample]:
    payload = _read_json(Path(samples_path))
    if not isinstance(payload, list):
        return []
    node_feature_keys = _sorted_feature_keys(payload, "node_feature_keys")
    edge_feature_keys = _sorted_feature_keys(payload, "edge_feature_keys")
    samples: list[NativeGraphSample] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        edge_index_rows = row.get("edge_index") or []
        edge_index: list[tuple[int, int]] = []
        if isinstance(edge_index_rows, list):
            for pair in edge_index_rows:
                if isinstance(pair, list) and len(pair) == 2:
                    try:
                        edge_index.append((int(pair[0]), int(pair[1])))
                    except (TypeError, ValueError):
                        continue
        node_features = _dense_feature_rows(list(row.get("node_features") or []), node_feature_keys)
        edge_features = _dense_feature_rows(list(row.get("edge_features") or []), edge_feature_keys)
        samples.append(NativeGraphSample(
            example_id=str(row.get("example_id") or ""),
            pdb_id=str(row.get("pdb_id") or ""),
            pair_identity_key=str(row.get("pair_identity_key") or ""),
            node_feature_keys=node_feature_keys,
            edge_feature_keys=edge_feature_keys,
            node_features=node_features,
            edge_features=edge_features,
            edge_index=tuple(edge_index),
            node_count=int(row.get("node_count") or len(node_features)),
            edge_count=int(row.get("edge_count") or len(edge_index)),
        ))
    return samples


def build_torch_geometric_data(samples_path: str | Path) -> list[Any]:
    samples = load_native_graph_samples(samples_path)
    try:
        import torch  # type: ignore
        from torch_geometric.data import Data  # type: ignore
    except Exception as exc:  # pragma: no cover - only used when optional deps installed
        raise RuntimeError("torch and torch_geometric are required to build native graph Data objects.") from exc

    items: list[Any] = []
    for sample in samples:
        x = torch.tensor(sample.node_features, dtype=torch.float32)
        edge_index = torch.tensor(sample.edge_index, dtype=torch.long).t().contiguous() if sample.edge_index else torch.empty((2, 0), dtype=torch.long)
        edge_attr = torch.tensor(sample.edge_features, dtype=torch.float32) if sample.edge_features else torch.empty((0, len(sample.edge_feature_keys)), dtype=torch.float32)
        data = Data(x=x, edge_index=edge_index, edge_attr=edge_attr)
        data.example_id = sample.example_id
        data.pdb_id = sample.pdb_id
        data.pair_identity_key = sample.pair_identity_key
        items.append(data)
    return items
