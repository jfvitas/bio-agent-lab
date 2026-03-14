"""Portable package helpers for exported Model Studio runs."""

from __future__ import annotations


def portable_training_backend_source() -> str:
    return '''"""Portable tabular trainer for exported Model Studio packages."""

from __future__ import annotations

import hashlib
import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path

try:
    import torch
    from torch_geometric.nn import GCNConv, global_mean_pool
except Exception:
    torch = None
    GCNConv = None
    global_mean_pool = None


NUMERIC_SECTIONS = ("structure", "protein", "ligand", "interaction", "experiment", "graph_features")
CATEGORICAL_FIELDS = (
    ("ligand", "ligand_id", 16),
    ("ligand", "ligand_type", 8),
    ("protein", "uniprot_id", 16),
    ("protein", "gene", 16),
    ("experiment", "affinity_type", 8),
    ("experiment", "source_database", 8),
    ("experiment", "preferred_source_database", 8),
)


if torch is not None and GCNConv is not None and global_mean_pool is not None:
    class SimpleGCNModel(torch.nn.Module):
        def __init__(self, in_dim: int, hidden_dim: int, out_dim: int) -> None:
            super().__init__()
            self.conv1 = GCNConv(in_dim, hidden_dim)
            self.conv2 = GCNConv(hidden_dim, hidden_dim)
            self.head = torch.nn.Linear(hidden_dim, out_dim)

        def forward(self, data):
            x, edge_index, batch = data.x, data.edge_index, data.batch
            x = self.conv1(x, edge_index).relu()
            x = self.conv2(x, edge_index).relu()
            x = global_mean_pool(x, batch)
            return self.head(x)


    class HybridGCNModel(torch.nn.Module):
        def __init__(self, graph_in_dim: int, attr_dim: int, hidden_dim: int, out_dim: int) -> None:
            super().__init__()
            self.conv1 = GCNConv(graph_in_dim, hidden_dim)
            self.conv2 = GCNConv(hidden_dim, hidden_dim)
            self.attr_mlp = torch.nn.Sequential(
                torch.nn.Linear(attr_dim, hidden_dim),
                torch.nn.ReLU(),
                torch.nn.Linear(hidden_dim, hidden_dim),
                torch.nn.ReLU(),
            )
            self.head = torch.nn.Linear(hidden_dim * 2, out_dim)

        def forward(self, data):
            x, edge_index, batch = data.x, data.edge_index, data.batch
            gx = self.conv1(x, edge_index).relu()
            gx = self.conv2(gx, edge_index).relu()
            gx = global_mean_pool(gx, batch)
            ax = self.attr_mlp(data.attr_x)
            return self.head(torch.cat([gx, ax], dim=1))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def safe_float(value):
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def feature_bucket(prefix: str, token: str, bins: int) -> str:
    digest = hashlib.md5(f"{prefix}:{token}".encode("utf-8")).hexdigest()
    return f"{prefix}.hash_{int(digest[:8], 16) % bins:02d}"


def flatten_numeric(prefix, value, out):
    if isinstance(value, dict):
        for key, inner in value.items():
            flatten_numeric(f"{prefix}.{key}" if prefix else str(key), inner, out)
        return
    if isinstance(value, bool):
        out[prefix] = 1.0 if value else 0.0
        return
    numeric = safe_float(value)
    if numeric is not None:
        out[prefix] = numeric


def example_features(example):
    features = {}
    for section_name in NUMERIC_SECTIONS:
        section = example.get(section_name)
        if isinstance(section, dict):
            flatten_numeric(section_name, section, features)
    for section_name, field_name, bins in CATEGORICAL_FIELDS:
        section = example.get(section_name) or {}
        if not isinstance(section, dict):
            continue
        token = str(section.get(field_name) or "").strip()
        if token:
            features[feature_bucket(f"{section_name}.{field_name}", token, bins)] = 1.0
    return features


def read_split_ids(path: Path):
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def canonical_ids(example):
    identifiers = set()
    example_id = str(example.get("example_id") or "").strip()
    if example_id:
        identifiers.add(example_id)
    structure = example.get("structure") or {}
    provenance = example.get("provenance") or {}
    pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
    if pdb_id:
        identifiers.add(pdb_id)
        identifiers.add(f"RCSB_{pdb_id}")
    pair_key = str(provenance.get("pair_identity_key") or "").strip()
    if pair_key:
        identifiers.add(pair_key)
    return identifiers


def fallback_split(example_id: str) -> str:
    digest = hashlib.md5(example_id.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) % 100
    if bucket < 70:
        return "train"
    if bucket < 85:
        return "val"
    return "test"


def assign_splits(package_root: Path, examples):
    split_dir = package_root / "package_data" / "splits"
    explicit = {name: read_split_ids(split_dir / f"{name}.txt") for name in ("train", "val", "test")}
    assigned = {"train": [], "val": [], "test": []}
    matched = 0
    for example in examples:
        identifiers = canonical_ids(example)
        split_name = None
        for candidate_name in ("train", "val", "test"):
            if identifiers & explicit[candidate_name]:
                split_name = candidate_name
                matched += 1
                break
        if split_name is None:
            split_name = fallback_split(str(example.get("example_id") or "unknown"))
        assigned[split_name].append(example)
    if not assigned["val"] and len(assigned["train"]) > 3:
        assigned["val"].append(assigned["train"].pop())
    if not assigned["test"] and len(assigned["train"]) > 4:
        assigned["test"].append(assigned["train"].pop())
    strategy = "explicit+fallback" if matched and matched < len(examples) else ("explicit" if matched else "hash_fallback")
    return assigned, {"strategy": strategy, "explicit_matches": matched, "counts": {name: len(rows) for name, rows in assigned.items()}}


def graph_context_by_pdb(package_root: Path):
    nodes = read_json(package_root / "package_data" / "graph" / "graph_nodes.json")
    edges = read_json(package_root / "package_data" / "graph" / "graph_edges.json")
    context = {}
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            metadata = node.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or node.get("primary_id") or "").strip().upper()
            if not pdb_id:
                continue
            row = context.setdefault(pdb_id, {})
            row["graph_context.node_count"] = float(row.get("graph_context.node_count", 0.0)) + 1.0
            node_type = str(node.get("node_type") or "").strip().lower()
            if node_type:
                row[f"graph_context.node_type.{node_type}"] = float(row.get(f"graph_context.node_type.{node_type}", 0.0)) + 1.0
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            metadata = edge.get("metadata") or {}
            pdb_id = str(metadata.get("pdb_id") or "").strip().upper()
            if not pdb_id:
                continue
            row = context.setdefault(pdb_id, {})
            row["graph_context.edge_count"] = float(row.get("graph_context.edge_count", 0.0)) + 1.0
            edge_type = str(edge.get("edge_type") or "").strip().lower()
            if edge_type:
                row[f"graph_context.edge_type.{edge_type}"] = float(row.get(f"graph_context.edge_type.{edge_type}", 0.0)) + 1.0
    return context


def augment_with_graph_context(examples, context):
    for example in examples:
        structure = example.get("structure") or {}
        provenance = example.get("provenance") or {}
        pdb_id = str(structure.get("pdb_id") or provenance.get("pdb_id") or "").strip().upper()
        if pdb_id in context:
            example.setdefault("_features", {}).update(context[pdb_id])


def resolve_target(examples, task):
    labeled = []
    metadata = {}
    if task == "classification":
        binary_rows = []
        for example in examples:
            labels = example.get("labels") or {}
            if "is_mutant" in labels and labels.get("is_mutant") is not None:
                row = dict(example)
                row["_target"] = 1 if bool(labels.get("is_mutant")) else 0
                binary_rows.append(row)
        if binary_rows:
            return "labels.is_mutant", metadata, binary_rows
        values = sorted(
            value for value in [safe_float((example.get("labels") or {}).get("binding_affinity_log10")) for example in examples]
            if value is not None
        )
        if not values:
            raise ValueError("No supervised classification label is available.")
        threshold = values[len(values) // 2]
        metadata["derived_threshold"] = threshold
        for example in examples:
            value = safe_float((example.get("labels") or {}).get("binding_affinity_log10"))
            if value is None:
                continue
            row = dict(example)
            row["_target"] = 1 if value >= threshold else 0
            labeled.append(row)
        return "labels.binding_affinity_log10_median_bin", metadata, labeled
    for example in examples:
        value = safe_float((example.get("labels") or {}).get("binding_affinity_log10"))
        if value is None:
            continue
        row = dict(example)
        row["_target"] = value
        labeled.append(row)
    if not labeled:
        raise ValueError("No regression label is available.")
    return "labels.binding_affinity_log10", metadata, labeled


def regression_metrics(y_true, y_pred):
    if not y_true:
        return {}
    errors = [pred - true for true, pred in zip(y_true, y_pred)]
    mse = sum(error * error for error in errors) / len(errors)
    mae = sum(abs(error) for error in errors) / len(errors)
    mean_true = sum(y_true) / len(y_true)
    total_ss = sum((value - mean_true) ** 2 for value in y_true)
    residual_ss = sum((pred - true) ** 2 for true, pred in zip(y_true, y_pred))
    r2 = 1.0 - (residual_ss / total_ss) if total_ss > 1e-9 else 0.0
    return {"rmse": round(math.sqrt(mse), 6), "mae": round(mae, 6), "r2": round(r2, 6)}


def classification_metrics(y_true, y_pred):
    if not y_true:
        return {}
    total = len(y_true)
    correct = sum(1 for truth, pred in zip(y_true, y_pred) if truth == pred)
    tp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 1)
    fp = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 0 and pred == 1)
    fn = sum(1 for truth, pred in zip(y_true, y_pred) if truth == 1 and pred == 0)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {"accuracy": round(correct / total, 6), "precision": round(precision, 6), "recall": round(recall, 6), "f1": round(f1, 6)}


def write_predictions(path: Path, rows, predictions, split_name: str):
    payload = []
    for row, prediction in zip(rows, predictions):
        payload.append({
            "split": split_name,
            "example_id": str(row.get("example_id") or ""),
            "pdb_id": str(((row.get("structure") or {}).get("pdb_id")) or ((row.get("provenance") or {}).get("pdb_id")) or ""),
            "target": row.get("_target"),
            "prediction": float(prediction) if not isinstance(prediction, (int, str)) else prediction,
        })
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_curve(path: Path, history):
    if not history:
        return
    width = 720
    height = 320
    margin = 40
    train_points = [float(row["train_metric"]) for row in history]
    val_points = [float(row["val_metric"]) for row in history]
    values = train_points + val_points
    lo = min(values)
    hi = max(values)
    if abs(hi - lo) < 1e-9:
        hi = lo + 1.0
    def polyline(values):
        coords = []
        span = max(len(values) - 1, 1)
        for idx, value in enumerate(values):
            x = margin + (idx / span) * (width - 2 * margin)
            y = height - margin - ((value - lo) / (hi - lo)) * (height - 2 * margin)
            coords.append(f"{x:.1f},{y:.1f}")
        return " ".join(coords)
    path.write_text("\\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="40" y="24" font-size="16" font-family="Segoe UI" fill="#222222">Training vs validation progress</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<polyline fill="none" stroke="#1f77b4" stroke-width="2" points="{polyline(train_points)}"/>',
        f'<polyline fill="none" stroke="#d62728" stroke-width="2" points="{polyline(val_points)}"/>',
        '</svg>',
    ]), encoding="utf-8")


def write_embedding_plot(path: Path, points, colors):
    width = 720
    height = 320
    margin = 40
    if not points:
        points = [(0.0, 0.0)]
        colors = [0]
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    lo_x = min(xs)
    hi_x = max(xs)
    lo_y = min(ys)
    hi_y = max(ys)
    if abs(hi_x - lo_x) < 1e-9:
        hi_x = lo_x + 1.0
    if abs(hi_y - lo_y) < 1e-9:
        hi_y = lo_y + 1.0
    palette = ("#1f77b4", "#d62728", "#2ca02c", "#ff7f0e", "#9467bd", "#8c564b")
    circles = []
    for idx, point in enumerate(points):
        x_value = float(point[0])
        y_value = float(point[1])
        x = margin + ((x_value - lo_x) / (hi_x - lo_x)) * (width - 2 * margin)
        y = height - margin - ((y_value - lo_y) / (hi_y - lo_y)) * (height - 2 * margin)
        color = palette[int(colors[idx]) % len(palette)] if idx < len(colors) else palette[0]
        circles.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" fill-opacity="0.75"/>')
    path.write_text("\\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="40" y="24" font-size="16" font-family="Segoe UI" fill="#222222">Embedding projection</text>',
        f'<line x1="{margin}" y1="{height - margin}" x2="{width - margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height - margin}" stroke="#888888" stroke-width="1"/>',
        *circles,
        '</svg>',
    ]), encoding="utf-8")


def build_native_graph_items(package_root: Path, payload_name: str, *, hybrid: bool):
    if torch is None:
        raise RuntimeError("torch is required for native portable graph training.")
    try:
        from torch_geometric.data import Data
    except Exception as exc:
        raise RuntimeError("torch_geometric is required for native portable graph training.") from exc
    payload = read_json(package_root / "package_data" / "graph" / payload_name)
    samples = read_json(package_root / "package_data" / "graph" / "pyg_ready" / "pyg_ready_graph_samples.json")
    if not isinstance(payload, list) or not isinstance(samples, list):
        raise ValueError("Portable graph payload artifacts are missing.")
    sample_by_example = {
        str(row.get("example_id") or ""): row
        for row in samples
        if isinstance(row, dict)
    }
    items = {"train": [], "val": [], "test": []}
    example_ids = {"train": [], "val": [], "test": []}
    truths = {"train": [], "val": [], "test": []}
    for row in payload:
        if not isinstance(row, dict):
            continue
        split = str(row.get("split") or "")
        if split not in items:
            continue
        target = row.get("target_value")
        if target is None:
            continue
        sample = sample_by_example.get(str(row.get("example_id") or ""))
        if not isinstance(sample, dict):
            continue
        node_keys = [str(key) for key in (sample.get("node_feature_keys") or [])]
        edge_keys = [str(key) for key in (sample.get("edge_feature_keys") or [])]
        node_rows = []
        for feature_row in sample.get("node_features") or []:
            if isinstance(feature_row, dict):
                node_rows.append([float(feature_row.get(key, 0.0) or 0.0) for key in node_keys])
        edge_rows = []
        for feature_row in sample.get("edge_features") or []:
            if isinstance(feature_row, dict):
                edge_rows.append([float(feature_row.get(key, 0.0) or 0.0) for key in edge_keys])
        x = torch.tensor(node_rows, dtype=torch.float32)
        edge_pairs = sample.get("edge_index") or []
        edge_index = torch.tensor(edge_pairs, dtype=torch.long).t().contiguous() if edge_pairs else torch.empty((2, 0), dtype=torch.long)
        edge_attr = torch.tensor(edge_rows, dtype=torch.float32) if edge_rows else torch.empty((0, len(edge_keys)), dtype=torch.float32)
        data = Data(x=x, edge_index=edge_index, edge_attr=edge_attr)
        if str(row.get("task") or "regression") == "classification":
            data.y = torch.tensor([int(target)], dtype=torch.long)
        else:
            data.y = torch.tensor([float(target)], dtype=torch.float32)
        if hybrid:
            attr_features = row.get("attribute_features") or {}
            ordered_keys = sorted(attr_features.keys())
            data.attr_x = torch.tensor([[float(attr_features.get(key, 0.0) or 0.0) for key in ordered_keys]], dtype=torch.float32)
        items[split].append(data)
        example_ids[split].append(str(row.get("example_id") or ""))
        truths[split].append(float(target) if str(row.get("task") or "regression") != "classification" else int(target))
    return items, example_ids, truths


def train_native_graph_family(package_root: Path, family: str, task: str, runtime_target: str, training_cfg):
    if torch is None:
        raise RuntimeError("torch is required for native portable graph training.")
    try:
        from torch_geometric.loader import DataLoader
    except Exception as exc:
        raise RuntimeError("torch_geometric is required for native portable graph training.") from exc

    hybrid = family == "hybrid_fusion"
    payload_name = "hybrid_training_payload/hybrid_training_payload.json" if hybrid else "training_payload/graph_training_payload.json"
    items, example_ids, truths = build_native_graph_items(package_root, payload_name, hybrid=hybrid)
    if not items["train"]:
        raise ValueError("No train split graph items are available in the exported package.")
    device = "cpu"
    if runtime_target == "local_gpu" and bool(getattr(torch, "cuda", None)) and bool(torch.cuda.is_available()):
        device = "cuda"
    in_dim = int(items["train"][0].x.shape[1])
    hidden_dim = int(training_cfg.get("hidden_dim", 64))
    epochs = int(training_cfg.get("epochs", 40))
    learning_rate = float(training_cfg.get("learning_rate", 5e-4))
    out_dim = 2 if task == "classification" else 1
    if hybrid:
        attr_dim = int(items["train"][0].attr_x.shape[1])
        model = HybridGCNModel(in_dim, attr_dim, hidden_dim, out_dim).to(device)
    else:
        model = SimpleGCNModel(in_dim, hidden_dim, out_dim).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    loss_fn = torch.nn.CrossEntropyLoss() if task == "classification" else torch.nn.MSELoss()
    train_loader = DataLoader(items["train"], batch_size=max(1, min(16, len(items["train"]))), shuffle=True)
    val_loader = DataLoader(items["val"], batch_size=max(1, min(16, len(items["val"]))), shuffle=False) if items["val"] else None
    history = []
    best_score = -1.0 if task == "classification" else float("inf")
    best_state = None
    for epoch in range(1, epochs + 1):
        model.train()
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad(set_to_none=True)
            output = model(batch)
            target = batch.y.view(-1)
            if task == "classification":
                loss = loss_fn(output, target)
            else:
                loss = loss_fn(output.view(-1), target.float())
            loss.backward()
            optimizer.step()
        def evaluate(loader, split_name):
            if loader is None:
                return None, []
            model.eval()
            preds = []
            with torch.no_grad():
                for batch in loader:
                    batch = batch.to(device)
                    output = model(batch)
                    if task == "classification":
                        preds.extend(int(value) for value in output.argmax(dim=1).detach().cpu().tolist())
                    else:
                        preds.extend(float(value) for value in output.view(-1).detach().cpu().tolist())
            truth = truths[split_name]
            if task == "classification":
                metric = classification_metrics(list(truth), preds)
                return float(metric.get("f1", 0.0)), preds
            metric = regression_metrics([float(value) for value in truth], [float(value) for value in preds])
            return float(metric.get("rmse", 0.0)), preds
        train_metric, _train_preds = evaluate(DataLoader(items["train"], batch_size=max(1, min(16, len(items["train"]))), shuffle=False), "train")
        val_metric, _val_preds = evaluate(val_loader, "val") if val_loader is not None else (train_metric, [])
        history.append({"epoch": float(epoch), "train_metric": float(train_metric or 0.0), "val_metric": float(val_metric or 0.0)})
        current = float(val_metric or 0.0)
        better = current >= best_score if task == "classification" else current <= best_score
        if better:
            best_score = current
            best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
    if best_state is not None:
        model.load_state_dict(best_state)
    split_predictions = {}
    for split_name in ("train", "val", "test"):
        if not items[split_name]:
            split_predictions[split_name] = {"example_ids": [], "truth": [], "pred": []}
            continue
        loader = DataLoader(items[split_name], batch_size=max(1, min(16, len(items[split_name]))), shuffle=False)
        model.eval()
        preds = []
        with torch.no_grad():
            for batch in loader:
                batch = batch.to(device)
                output = model(batch)
                if task == "classification":
                    preds.extend(int(value) for value in output.argmax(dim=1).detach().cpu().tolist())
                else:
                    preds.extend(float(value) for value in output.view(-1).detach().cpu().tolist())
        split_predictions[split_name] = {
            "example_ids": list(example_ids[split_name]),
            "truth": list(truths[split_name]),
            "pred": preds,
        }
    return model, {"backend": "pyg_hybrid_fusion" if hybrid else "pyg_gnn", "device": device, "split_predictions": split_predictions}, history, [
        "Portable native graph training is an MVP baseline; compare it against local and tabular baselines."
    ]


def train_family(family, task, x_train, y_train, x_val, y_val, model_cfg, training_cfg):
    warnings = []
    history = []
    seed = int(training_cfg.get("seed", 42))
    if family == "random_forest":
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

        model = RandomForestClassifier(random_state=seed, n_estimators=int(model_cfg.get("n_estimators", 400))) if task == "classification" else RandomForestRegressor(random_state=seed, n_estimators=int(model_cfg.get("n_estimators", 400)))
        model.fit(x_train, y_train)
        return model, "sklearn_random_forest", history, warnings
    if family == "xgboost":
        try:
            import xgboost as xgb
            model = xgb.XGBClassifier(random_state=seed, eval_metric="logloss") if task == "classification" else xgb.XGBRegressor(random_state=seed)
            model.fit(x_train, y_train)
            return model, "xgboost", history, warnings
        except Exception:
            from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

            warnings.append("xgboost unavailable; using sklearn gradient boosting fallback.")
            model = HistGradientBoostingClassifier(random_state=seed) if task == "classification" else HistGradientBoostingRegressor(random_state=seed)
            model.fit(x_train, y_train)
            return model, "sklearn_hist_gradient_boosting", history, warnings
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    hidden_dims = tuple(int(value) for value in (model_cfg.get("hidden_dims") or [256, 128]))
    epochs = int(training_cfg.get("epochs", 60))
    learning_rate = float(training_cfg.get("learning_rate", 1e-3))
    estimator = MLPClassifier(hidden_layer_sizes=hidden_dims, random_state=seed, max_iter=1, warm_start=True, learning_rate_init=learning_rate) if task == "classification" else MLPRegressor(hidden_layer_sizes=hidden_dims, random_state=seed, max_iter=1, warm_start=True, learning_rate_init=learning_rate)
    pipeline = Pipeline([("scaler", StandardScaler()), ("mlp", estimator)])
    best_snapshot = None
    best_score = -1.0 if task == "classification" else float("inf")
    classes = sorted({int(value) for value in y_train}) if task == "classification" else None
    for epoch in range(1, epochs + 1):
        pipeline.named_steps["scaler"].fit(x_train)
        x_train_scaled = pipeline.named_steps["scaler"].transform(x_train)
        x_val_scaled = pipeline.named_steps["scaler"].transform(x_val) if x_val else x_train_scaled
        if task == "classification":
            estimator.partial_fit(x_train_scaled, y_train, classes=classes)
            train_metrics = classification_metrics(list(y_train), list(estimator.predict(x_train_scaled)))
            val_metrics = classification_metrics(list(y_val), list(estimator.predict(x_val_scaled))) if y_val else train_metrics
            current = float(val_metrics.get("f1", 0.0))
            if current >= best_score:
                best_score = current
                best_snapshot = pickle.dumps(pipeline)
            history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("f1", 0.0)), "val_metric": current})
        else:
            estimator.partial_fit(x_train_scaled, y_train)
            train_metrics = regression_metrics(list(y_train), list(estimator.predict(x_train_scaled)))
            val_metrics = regression_metrics(list(y_val), list(estimator.predict(x_val_scaled))) if y_val else train_metrics
            current = float(val_metrics.get("rmse", 0.0))
            if current <= best_score:
                best_score = current
                best_snapshot = pickle.dumps(pipeline)
            history.append({"epoch": float(epoch), "train_metric": float(train_metrics.get("rmse", 0.0)), "val_metric": current})
    return pickle.loads(best_snapshot) if best_snapshot is not None else pipeline, "sklearn_mlp", history, warnings


def train_unsupervised_family(family, x_rows, training_cfg):
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x_rows)
    latent_dim = max(2, min(int(training_cfg.get("latent_dim", 8)), len(x_rows), len(x_rows[0]) if x_rows else 2))
    embedder = PCA(n_components=latent_dim, random_state=int(training_cfg.get("seed", 42)))
    embeddings = embedder.fit_transform(x_scaled)
    projection = embeddings[:, :2] if embeddings.shape[1] >= 2 else [[float(row[0]), 0.0] for row in embeddings]
    if family == "clustering":
        cluster_count = max(2, min(int(training_cfg.get("cluster_count", 4)), len(x_rows)))
        clusterer = KMeans(n_clusters=cluster_count, random_state=int(training_cfg.get("seed", 42)), n_init="auto")
        assignments = clusterer.fit_predict(embeddings)
        silhouette = None
        try:
            if len(set(int(value) for value in assignments)) > 1 and len(assignments) > cluster_count:
                silhouette = float(silhouette_score(embeddings, assignments))
        except Exception:
            silhouette = None
        return (
            {"scaler": scaler, "embedder": embedder, "clusterer": clusterer},
            "sklearn_kmeans",
            [],
            [],
            {
                "silhouette": None if silhouette is None else round(silhouette, 6),
                "cluster_count": cluster_count,
                "embedding_dim": int(embeddings.shape[1]),
            },
            [[float(value) for value in row[:2]] for row in projection],
            [int(value) for value in assignments],
        )
    reconstructed = embedder.inverse_transform(embeddings)
    errors = [
        math.sqrt(sum((float(source) - float(recon)) ** 2 for source, recon in zip(source_row, recon_row)) / max(len(source_row), 1))
        for source_row, recon_row in zip(x_scaled, reconstructed)
    ]
    return (
        {"scaler": scaler, "embedder": embedder},
        "pca_autoencoder",
        [],
        ["Autoencoder export currently uses a PCA reconstruction baseline for unsupervised exploration."],
        {
            "reconstruction_rmse": round(sum(errors) / max(len(errors), 1), 6),
            "embedding_dim": int(embeddings.shape[1]),
        },
        [[float(value) for value in row[:2]] for row in projection],
        [0 for _ in range(len(x_rows))],
    )


def run_training(package_root: Path, output_dir: Path):
    config = json.loads((package_root / "config.json").read_text(encoding="utf-8"))
    family = str(config.get("family") or ((config.get("model") or {}).get("type")) or "unknown")
    task = str(config.get("task") or "regression")
    if task == "ranking":
        task = "regression"
    if family not in {"random_forest", "xgboost", "dense_nn", "gnn", "hybrid_fusion", "clustering", "autoencoder"}:
        raise ValueError(f"Portable training currently supports random_forest, xgboost, dense_nn, gnn, hybrid_fusion, clustering, and autoencoder. Received: {family}")
    examples = read_json(package_root / "package_data" / "training_examples.json")
    if not isinstance(examples, list) or not examples:
        raise ValueError("package_data/training_examples.json is missing or empty.")
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_targets = read_json(package_root / "runtime_targets.json") or {}
    trainer_backend = config.get("trainer_backend") or {}
    if task == "unsupervised" or family in {"clustering", "autoencoder"}:
        enriched_examples = [dict(example) for example in examples if isinstance(example, dict)]
        for example in enriched_examples:
            example["_features"] = example_features(example)
        if family in {"gnn", "hybrid_fusion"}:
            augment_with_graph_context(enriched_examples, graph_context_by_pdb(package_root))
        feature_names = sorted({name for row in enriched_examples for name in row.get("_features", {}).keys()})
        x_rows = [[float(row["_features"].get(name, 0.0)) for name in feature_names] for row in enriched_examples]
        model, backend, history, warnings, metrics, embeddings, assignments = train_unsupervised_family(
            family, x_rows, dict(config.get("training") or {})
        )
        (output_dir / "metrics.json").write_text(json.dumps({"unsupervised": metrics}, indent=2), encoding="utf-8")
        (output_dir / "run_metrics.json").write_text(json.dumps({
            "family": family,
            "task": "unsupervised",
            "metrics": {"unsupervised": metrics},
            "backend": backend,
            "warnings": warnings,
        }, indent=2), encoding="utf-8")
        (output_dir / "split_summary.json").write_text(json.dumps({"all": {"count": len(enriched_examples)}}, indent=2), encoding="utf-8")
        (output_dir / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
        (output_dir / "feature_schema.json").write_text(json.dumps({"target_name": "", "target_metadata": {}, "feature_names": feature_names}, indent=2), encoding="utf-8")
        (output_dir / "embedding_records.json").write_text(json.dumps([
            {
                "example_id": str(example.get("example_id") or ""),
                "pdb_id": str(((example.get("structure") or {}).get("pdb_id")) or ((example.get("provenance") or {}).get("pdb_id")) or ""),
                "embedding": embeddings[idx] if idx < len(embeddings) else [],
                "cluster": assignments[idx] if idx < len(assignments) else 0,
            }
            for idx, example in enumerate(enriched_examples)
        ], indent=2), encoding="utf-8")
        with (output_dir / "model.pkl").open("wb") as handle:
            pickle.dump({"model": model, "feature_names": feature_names, "task": "unsupervised", "family": family, "backend_id": backend}, handle)
        write_embedding_plot(output_dir / "test_performance.svg", embeddings, assignments)
        (output_dir / "run_manifest.json").write_text(json.dumps({
            "generated_at": utc_now(),
            "family": family,
            "task": "unsupervised",
            "backend": backend,
            "trainer_backend": trainer_backend,
            "runtime_targets": runtime_targets,
            "warnings": warnings,
        }, indent=2), encoding="utf-8")
        return
    target_name, target_metadata, labeled_examples = resolve_target(examples, task)
    for example in labeled_examples:
        example["_features"] = example_features(example)
    if family in {"gnn", "hybrid_fusion"}:
        augment_with_graph_context(labeled_examples, graph_context_by_pdb(package_root))
    backend_id = str((trainer_backend or {}).get("backend_id") or "")
    runtime_target = str((runtime_targets or {}).get("selected_target") or "local_cpu")
    if family in {"gnn", "hybrid_fusion"} and backend_id in {"pyg_gnn", "pyg_hybrid_fusion"}:
        try:
            model, backend, history, warnings = train_native_graph_family(
                package_root,
                family=family,
                task=task,
                runtime_target=runtime_target,
                training_cfg=dict(config.get("training") or {}),
            )
            split_predictions = backend.get("split_predictions") or {}
            metrics = {}
            for split_name in ("train", "val", "test"):
                truth = (split_predictions.get(split_name) or {}).get("truth") or []
                pred = (split_predictions.get(split_name) or {}).get("pred") or []
                if task == "classification":
                    metrics[split_name] = classification_metrics([int(value) for value in truth], [int(value) for value in pred]) if truth else {}
                else:
                    metrics[split_name] = regression_metrics([float(value) for value in truth], [float(value) for value in pred]) if truth else {}
            (output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
            (output_dir / "run_metrics.json").write_text(json.dumps({"family": family, "task": task, "metrics": metrics, "backend": backend["backend"], "warnings": warnings}, indent=2), encoding="utf-8")
            (output_dir / "split_summary.json").write_text(json.dumps({
                split_name: {"count": len((split_predictions.get(split_name) or {}).get("example_ids") or [])}
                for split_name in ("train", "val", "test")
            }, indent=2), encoding="utf-8")
            (output_dir / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
            (output_dir / "feature_schema.json").write_text(json.dumps({"target_name": target_name, "target_metadata": target_metadata, "feature_names": []}, indent=2), encoding="utf-8")
            with (output_dir / "model.pkl").open("wb") as handle:
                pickle.dump({"model": model, "feature_names": [], "task": task, "family": family, "backend_id": backend["backend"]}, handle)
            write_curve(output_dir / "training_curve.svg", history)
            for split_name in ("train", "val", "test"):
                payload = split_predictions.get(split_name) or {}
                (output_dir / f"{split_name}_predictions.json").write_text(json.dumps([
                    {
                        "split": split_name,
                        "example_id": payload["example_ids"][idx] if idx < len(payload.get("example_ids") or []) else "",
                        "target": payload["truth"][idx] if idx < len(payload.get("truth") or []) else None,
                        "prediction": payload["pred"][idx] if idx < len(payload.get("pred") or []) else None,
                    }
                    for idx in range(len(payload.get("pred") or []))
                ], indent=2), encoding="utf-8")
            if task == "classification":
                write_embedding_plot(
                    output_dir / "test_performance.svg",
                    [(float(idx), float((split_predictions.get("test") or {}).get("pred", [])[idx])) for idx in range(len((split_predictions.get("test") or {}).get("pred") or []))],
                    [int(value) for value in ((split_predictions.get("test") or {}).get("pred") or [])],
                )
            else:
                write_embedding_plot(
                    output_dir / "test_performance.svg",
                    [
                        (float(truth), float(pred))
                        for truth, pred in zip(
                            (split_predictions.get("test") or {}).get("truth") or [],
                            (split_predictions.get("test") or {}).get("pred") or [],
                        )
                    ],
                    [0 for _ in range(len((split_predictions.get("test") or {}).get("pred") or []))],
                )
            (output_dir / "run_manifest.json").write_text(json.dumps({
                "generated_at": utc_now(),
                "family": family,
                "task": task,
                "backend": backend["backend"],
                "trainer_backend": trainer_backend,
                "runtime_targets": runtime_targets,
                "warnings": warnings,
            }, indent=2), encoding="utf-8")
            return
        except Exception as exc:
            native_warning = f"Portable native graph training was requested but fell back to the surrogate trainer: {exc}"
        else:
            native_warning = ""
    else:
        native_warning = ""
    split_rows, split_info = assign_splits(package_root, labeled_examples)
    feature_names = sorted({name for row in split_rows["train"] for name in row["_features"].keys()})
    x_train = [[float(row["_features"].get(name, 0.0)) for name in feature_names] for row in split_rows["train"]]
    y_train = [row["_target"] for row in split_rows["train"]]
    x_val = [[float(row["_features"].get(name, 0.0)) for name in feature_names] for row in split_rows["val"]]
    y_val = [row["_target"] for row in split_rows["val"]]
    x_test = [[float(row["_features"].get(name, 0.0)) for name in feature_names] for row in split_rows["test"]]
    y_test = [row["_target"] for row in split_rows["test"]]
    effective_family = family
    warnings = []
    if native_warning:
        warnings.append(native_warning)
    if family == "gnn":
        warnings.append("Graph-native training is not implemented yet; using a graph-aware surrogate dense model over graph-derived context features.")
        effective_family = "dense_nn"
    elif family == "hybrid_fusion":
        warnings.append("Hybrid graph+attribute fusion is currently executed as a multimodal surrogate dense model using attribute and graph-context features together.")
        effective_family = "dense_nn"
    model, backend, history, trainer_warnings = train_family(effective_family, task, x_train, y_train, x_val, y_val, dict(config.get("model") or {}), dict(config.get("training") or {}))
    warnings.extend(trainer_warnings)
    train_pred = list(model.predict(x_train))
    val_pred = list(model.predict(x_val)) if x_val else []
    test_pred = list(model.predict(x_test)) if x_test else []
    metrics = {
        "train": classification_metrics(list(y_train), [int(round(value)) for value in train_pred]) if task == "classification" else regression_metrics([float(value) for value in y_train], [float(value) for value in train_pred]),
        "val": classification_metrics(list(y_val), [int(round(value)) for value in val_pred]) if task == "classification" else regression_metrics([float(value) for value in y_val], [float(value) for value in val_pred]),
        "test": classification_metrics(list(y_test), [int(round(value)) for value in test_pred]) if task == "classification" else regression_metrics([float(value) for value in y_test], [float(value) for value in test_pred]),
    }
    (output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (output_dir / "run_metrics.json").write_text(json.dumps({"family": family, "task": task, "metrics": metrics, "backend": backend, "warnings": warnings}, indent=2), encoding="utf-8")
    (output_dir / "split_summary.json").write_text(json.dumps(split_info, indent=2), encoding="utf-8")
    (output_dir / "history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
    (output_dir / "feature_schema.json").write_text(json.dumps({"target_name": target_name, "target_metadata": target_metadata, "feature_names": feature_names}, indent=2), encoding="utf-8")
    with (output_dir / "model.pkl").open("wb") as handle:
        pickle.dump({"model": model, "feature_names": feature_names, "task": task, "family": family}, handle)
    write_curve(output_dir / "training_curve.svg", history)
    write_predictions(output_dir / "train_predictions.json", split_rows["train"], train_pred, "train")
    write_predictions(output_dir / "val_predictions.json", split_rows["val"], val_pred, "val")
    write_predictions(output_dir / "test_predictions.json", split_rows["test"], test_pred, "test")
    (output_dir / "run_manifest.json").write_text(json.dumps({
        "generated_at": utc_now(),
        "family": family,
        "task": task,
        "backend": backend,
        "trainer_backend": trainer_backend,
        "runtime_targets": runtime_targets,
        "warnings": warnings,
    }, indent=2), encoding="utf-8")
'''


def portable_train_entrypoint_source(target: str) -> str:
    return f'''"""Portable Model Studio training entrypoint."""

from __future__ import annotations

from pathlib import Path

from trainer_backend import run_training


def main() -> None:
    package_root = Path(__file__).parent
    output_dir = package_root / "model_outputs"
    run_training(package_root, output_dir)
    print("Portable training completed for target: {target}")
    print(f"Artifacts written to {{output_dir}}")


if __name__ == "__main__":
    main()
'''
