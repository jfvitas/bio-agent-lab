"""Native PyG training helpers for Model Studio graph models."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


try:  # pragma: no cover - optional dependency shim
    import torch  # type: ignore
    from torch_geometric.nn import GCNConv, global_mean_pool  # type: ignore
except Exception:  # pragma: no cover
    torch = None  # type: ignore
    GCNConv = None  # type: ignore
    global_mean_pool = None  # type: ignore


@dataclass(frozen=True)
class GraphTrainingRecord:
    example_id: str
    pdb_id: str
    pair_identity_key: str
    split: str
    task: str
    target_name: str
    target_value: float | int | None
    graph_sample: dict[str, Any]


@dataclass(frozen=True)
class HybridTrainingRecord:
    example_id: str
    pdb_id: str
    pair_identity_key: str
    split: str
    task: str
    target_name: str
    target_value: float | int | None
    attribute_features: dict[str, float]
    graph_sample: dict[str, Any]


if torch is not None and GCNConv is not None and global_mean_pool is not None:  # pragma: no branch
    class SimpleGCNModel(torch.nn.Module):  # type: ignore[misc]
        def __init__(self, in_dim: int, hidden_dim: int, out_dim: int) -> None:
            super().__init__()
            self.conv1 = GCNConv(in_dim, hidden_dim)
            self.conv2 = GCNConv(hidden_dim, hidden_dim)
            self.head = torch.nn.Linear(hidden_dim, out_dim)

        def forward(self, data: Any) -> Any:
            x, edge_index, batch = data.x, data.edge_index, data.batch
            x = self.conv1(x, edge_index).relu()
            x = self.conv2(x, edge_index).relu()
            x = global_mean_pool(x, batch)
            return self.head(x)


    class HybridGCNModel(torch.nn.Module):  # type: ignore[misc]
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

        def forward(self, data: Any) -> Any:
            x, edge_index, batch = data.x, data.edge_index, data.batch
            gx = self.conv1(x, edge_index).relu()
            gx = self.conv2(gx, edge_index).relu()
            gx = global_mean_pool(gx, batch)
            ax = self.attr_mlp(data.attr_x)
            fused = torch.cat([gx, ax], dim=1)
            return self.head(fused)


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_graph_training_records(payload_path: str | Path) -> list[GraphTrainingRecord]:
    payload = _read_json(Path(payload_path))
    if not isinstance(payload, list):
        return []
    records: list[GraphTrainingRecord] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        target_value = row.get("target_value")
        if target_value is not None:
            try:
                if str(row.get("task") or "regression") == "classification":
                    target_value = int(target_value)
                else:
                    target_value = float(target_value)
            except (TypeError, ValueError):
                target_value = None
        records.append(GraphTrainingRecord(
            example_id=str(row.get("example_id") or ""),
            pdb_id=str(row.get("pdb_id") or ""),
            pair_identity_key=str(row.get("pair_identity_key") or ""),
            split=str(row.get("split") or "unspecified"),
            task=str(row.get("task") or "regression"),
            target_name=str(row.get("target_name") or ""),
            target_value=target_value,
            graph_sample=dict(row.get("graph_sample") or {}),
        ))
    return records


def load_hybrid_training_records(payload_path: str | Path) -> list[HybridTrainingRecord]:
    payload = _read_json(Path(payload_path))
    if not isinstance(payload, list):
        return []
    records: list[HybridTrainingRecord] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        target_value = row.get("target_value")
        if target_value is not None:
            try:
                if str(row.get("task") or "regression") == "classification":
                    target_value = int(target_value)
                else:
                    target_value = float(target_value)
            except (TypeError, ValueError):
                target_value = None
        records.append(HybridTrainingRecord(
            example_id=str(row.get("example_id") or ""),
            pdb_id=str(row.get("pdb_id") or ""),
            pair_identity_key=str(row.get("pair_identity_key") or ""),
            split=str(row.get("split") or "unspecified"),
            task=str(row.get("task") or "regression"),
            target_name=str(row.get("target_name") or ""),
            target_value=target_value,
            attribute_features=dict(row.get("attribute_features") or {}),
            graph_sample=dict(row.get("graph_sample") or {}),
        ))
    return records


def train_pyg_gnn(
    payload_path: str | Path,
    *,
    runtime_target: str,
    training_cfg: dict[str, Any],
) -> tuple[Any, dict[str, Any], list[dict[str, float]], list[str]]:
    records = load_graph_training_records(payload_path)
    labeled_records = [record for record in records if record.target_value is not None and record.split in {"train", "val", "test"}]
    if not labeled_records:
        raise ValueError(f"No labeled graph training records are available in {payload_path}")

    try:
        import torch  # type: ignore
        from torch_geometric.loader import DataLoader  # type: ignore
    except Exception as exc:  # pragma: no cover - only reachable when optional deps installed
        raise RuntimeError("Native graph training requires torch and torch_geometric.") from exc

    from pbdata.modeling.graph_native_backend import build_torch_geometric_data

    data_items = build_torch_geometric_data(
        Path(payload_path).parent.parent / "pyg_ready_graphs" / "pyg_ready_graph_samples.json"
    )
    data_by_example_id = {str(getattr(item, "example_id", "")): item for item in data_items}

    task = labeled_records[0].task
    train_items = []
    val_items = []
    test_items = []
    train_example_ids: list[str] = []
    val_example_ids: list[str] = []
    test_example_ids: list[str] = []
    for record in labeled_records:
        item = data_by_example_id.get(record.example_id)
        if item is None:
            continue
        if task == "classification":
            item.y = torch.tensor([int(record.target_value)], dtype=torch.long)
        else:
            item.y = torch.tensor([float(record.target_value)], dtype=torch.float32)
        if record.split == "train":
            train_items.append(item)
            train_example_ids.append(record.example_id)
        elif record.split == "val":
            val_items.append(item)
            val_example_ids.append(record.example_id)
        elif record.split == "test":
            test_items.append(item)
            test_example_ids.append(record.example_id)

    if not train_items:
        raise ValueError("No train split graph samples are available for native graph training.")

    device = "cpu"
    if runtime_target == "local_gpu" and bool(getattr(torch, "cuda", None)) and bool(torch.cuda.is_available()):
        device = "cuda"

    in_dim = int(train_items[0].x.shape[1])
    hidden_dim = int(training_cfg.get("hidden_dim", 64))
    epochs = int(training_cfg.get("epochs", 40))
    learning_rate = float(training_cfg.get("learning_rate", 5e-4))
    out_dim = 2 if task == "classification" else 1

    model = SimpleGCNModel(in_dim, hidden_dim, out_dim).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    loss_fn = torch.nn.CrossEntropyLoss() if task == "classification" else torch.nn.MSELoss()
    train_loader = DataLoader(train_items, batch_size=max(1, min(16, len(train_items))), shuffle=True)
    val_loader = DataLoader(val_items, batch_size=max(1, min(16, len(val_items))), shuffle=False) if val_items else None
    history: list[dict[str, float]] = []
    best_score = -1.0 if task == "classification" else float("inf")
    best_state: dict[str, Any] | None = None

    def _evaluate(loader: Any) -> tuple[list[float], list[float]]:
        predictions: list[float] = []
        truths: list[float] = []
        model.eval()
        with torch.no_grad():
            for batch in loader:
                batch = batch.to(device)
                output = model(batch)
                if task == "classification":
                    preds = torch.argmax(output, dim=1).detach().cpu().numpy().tolist()
                    labels = batch.y.detach().cpu().numpy().tolist()
                else:
                    preds = output.reshape(-1).detach().cpu().numpy().tolist()
                    labels = batch.y.reshape(-1).detach().cpu().numpy().tolist()
                predictions.extend(float(value) for value in preds)
                truths.extend(float(value) for value in labels)
        return truths, predictions

    def _classification_f1(y_true: list[float], y_pred: list[float]) -> float:
        truth = [int(value) for value in y_true]
        pred = [int(value) for value in y_pred]
        tp = sum(1 for t, p in zip(truth, pred) if t == 1 and p == 1)
        fp = sum(1 for t, p in zip(truth, pred) if t == 0 and p == 1)
        fn = sum(1 for t, p in zip(truth, pred) if t == 1 and p == 0)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        return (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    def _rmse(y_true: list[float], y_pred: list[float]) -> float:
        if not y_true:
            return 0.0
        total = sum((pred - truth) ** 2 for truth, pred in zip(y_true, y_pred)) / len(y_true)
        return float(total ** 0.5)

    for epoch in range(1, epochs + 1):
        model.train()
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            target = batch.y if task == "classification" else batch.y.reshape(-1, 1)
            loss = loss_fn(output, target)
            loss.backward()
            optimizer.step()

        train_truth, train_pred = _evaluate(train_loader)
        if val_loader is not None:
            val_truth, val_pred = _evaluate(val_loader)
        else:
            val_truth, val_pred = train_truth, train_pred

        if task == "classification":
            train_metric = _classification_f1(train_truth, train_pred)
            val_metric = _classification_f1(val_truth, val_pred)
            history.append({"epoch": float(epoch), "train_metric": float(train_metric), "val_metric": float(val_metric)})
            if val_metric >= best_score:
                best_score = val_metric
                best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
        else:
            train_metric = _rmse(train_truth, train_pred)
            val_metric = _rmse(val_truth, val_pred)
            history.append({"epoch": float(epoch), "train_metric": float(train_metric), "val_metric": float(val_metric)})
            if val_metric <= best_score:
                best_score = val_metric
                best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)
    train_truth, train_pred = _evaluate(train_loader)
    val_truth, val_pred = _evaluate(val_loader) if val_loader is not None else ([], [])
    test_loader = DataLoader(test_items, batch_size=max(1, min(16, len(test_items))), shuffle=False) if test_items else None
    test_truth, test_pred = _evaluate(test_loader) if test_loader is not None else ([], [])
    warnings = ["Native PyG training path is an MVP baseline and should be compared against the established tabular baselines."]
    return model, {
        "backend": "pyg_gnn",
        "device": device,
        "split_predictions": {
            "train": {"example_ids": train_example_ids, "truth": train_truth, "pred": train_pred},
            "val": {"example_ids": val_example_ids, "truth": val_truth, "pred": val_pred},
            "test": {"example_ids": test_example_ids, "truth": test_truth, "pred": test_pred},
        },
    }, history, warnings


def train_pyg_hybrid_fusion(
    payload_path: str | Path,
    *,
    runtime_target: str,
    training_cfg: dict[str, Any],
) -> tuple[Any, dict[str, Any], list[dict[str, float]], list[str]]:
    records = load_hybrid_training_records(payload_path)
    labeled_records = [record for record in records if record.target_value is not None and record.split in {"train", "val", "test"}]
    if not labeled_records:
        raise ValueError(f"No labeled hybrid training records are available in {payload_path}")

    try:
        import torch  # type: ignore
        from torch_geometric.loader import DataLoader  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Native hybrid graph training requires torch and torch_geometric.") from exc

    from pbdata.modeling.graph_native_backend import build_torch_geometric_data

    pyg_samples_path = Path(payload_path).parent.parent / "pyg_ready_graphs" / "pyg_ready_graph_samples.json"
    data_items = build_torch_geometric_data(pyg_samples_path)
    data_by_example_id = {str(getattr(item, "example_id", "")): item for item in data_items}

    attribute_keys = sorted({key for record in labeled_records for key in record.attribute_features.keys()})
    task = labeled_records[0].task
    train_items = []
    val_items = []
    test_items = []
    train_example_ids: list[str] = []
    val_example_ids: list[str] = []
    test_example_ids: list[str] = []

    def _attr_vector(features: dict[str, float]) -> Any:
        return [float(features.get(key, 0.0)) for key in attribute_keys]

    for record in labeled_records:
        item = data_by_example_id.get(record.example_id)
        if item is None:
            continue
        item.attr_x = torch.tensor([_attr_vector(record.attribute_features)], dtype=torch.float32)
        if task == "classification":
            item.y = torch.tensor([int(record.target_value)], dtype=torch.long)
        else:
            item.y = torch.tensor([float(record.target_value)], dtype=torch.float32)
        if record.split == "train":
            train_items.append(item)
            train_example_ids.append(record.example_id)
        elif record.split == "val":
            val_items.append(item)
            val_example_ids.append(record.example_id)
        elif record.split == "test":
            test_items.append(item)
            test_example_ids.append(record.example_id)

    if not train_items:
        raise ValueError("No train split hybrid samples are available for native hybrid training.")

    device = "cpu"
    if runtime_target == "local_gpu" and bool(getattr(torch, "cuda", None)) and bool(torch.cuda.is_available()):
        device = "cuda"

    graph_in_dim = int(train_items[0].x.shape[1])
    attr_dim = int(train_items[0].attr_x.shape[1])
    hidden_dim = int(training_cfg.get("hidden_dim", 64))
    epochs = int(training_cfg.get("epochs", 40))
    learning_rate = float(training_cfg.get("learning_rate", 5e-4))
    out_dim = 2 if task == "classification" else 1
    model = HybridGCNModel(graph_in_dim, attr_dim, hidden_dim, out_dim).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    loss_fn = torch.nn.CrossEntropyLoss() if task == "classification" else torch.nn.MSELoss()
    train_loader = DataLoader(train_items, batch_size=max(1, min(16, len(train_items))), shuffle=True)
    val_loader = DataLoader(val_items, batch_size=max(1, min(16, len(val_items))), shuffle=False) if val_items else None
    history: list[dict[str, float]] = []
    best_score = -1.0 if task == "classification" else float("inf")
    best_state: dict[str, Any] | None = None

    def _evaluate(loader: Any) -> tuple[list[float], list[float]]:
        predictions: list[float] = []
        truths: list[float] = []
        model.eval()
        with torch.no_grad():
            for batch in loader:
                batch = batch.to(device)
                output = model(batch)
                if task == "classification":
                    preds = torch.argmax(output, dim=1).detach().cpu().numpy().tolist()
                    labels = batch.y.detach().cpu().numpy().tolist()
                else:
                    preds = output.reshape(-1).detach().cpu().numpy().tolist()
                    labels = batch.y.reshape(-1).detach().cpu().numpy().tolist()
                predictions.extend(float(value) for value in preds)
                truths.extend(float(value) for value in labels)
        return truths, predictions

    def _classification_f1(y_true: list[float], y_pred: list[float]) -> float:
        truth = [int(value) for value in y_true]
        pred = [int(value) for value in y_pred]
        tp = sum(1 for t, p in zip(truth, pred) if t == 1 and p == 1)
        fp = sum(1 for t, p in zip(truth, pred) if t == 0 and p == 1)
        fn = sum(1 for t, p in zip(truth, pred) if t == 1 and p == 0)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        return (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    def _rmse(y_true: list[float], y_pred: list[float]) -> float:
        if not y_true:
            return 0.0
        total = sum((pred - truth) ** 2 for truth, pred in zip(y_true, y_pred)) / len(y_true)
        return float(total ** 0.5)

    for epoch in range(1, epochs + 1):
        model.train()
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            output = model(batch)
            target = batch.y if task == "classification" else batch.y.reshape(-1, 1)
            loss = loss_fn(output, target)
            loss.backward()
            optimizer.step()

        train_truth, train_pred = _evaluate(train_loader)
        if val_loader is not None:
            val_truth, val_pred = _evaluate(val_loader)
        else:
            val_truth, val_pred = train_truth, train_pred

        if task == "classification":
            train_metric = _classification_f1(train_truth, train_pred)
            val_metric = _classification_f1(val_truth, val_pred)
            history.append({"epoch": float(epoch), "train_metric": float(train_metric), "val_metric": float(val_metric)})
            if val_metric >= best_score:
                best_score = val_metric
                best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
        else:
            train_metric = _rmse(train_truth, train_pred)
            val_metric = _rmse(val_truth, val_pred)
            history.append({"epoch": float(epoch), "train_metric": float(train_metric), "val_metric": float(val_metric)})
            if val_metric <= best_score:
                best_score = val_metric
                best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)
    train_truth, train_pred = _evaluate(train_loader)
    val_truth, val_pred = _evaluate(val_loader) if val_loader is not None else ([], [])
    test_loader = DataLoader(test_items, batch_size=max(1, min(16, len(test_items))), shuffle=False) if test_items else None
    test_truth, test_pred = _evaluate(test_loader) if test_loader is not None else ([], [])
    warnings = ["Native hybrid PyG training path is an MVP baseline and should be compared against unimodal and surrogate baselines."]
    return model, {
        "backend": "pyg_hybrid_fusion",
        "device": device,
        "split_predictions": {
            "train": {"example_ids": train_example_ids, "truth": train_truth, "pred": train_pred},
            "val": {"example_ids": val_example_ids, "truth": val_truth, "pred": val_pred},
            "test": {"example_ids": test_example_ids, "truth": test_truth, "pred": test_pred},
        },
    }, history, warnings
