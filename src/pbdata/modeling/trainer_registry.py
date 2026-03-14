"""Trainer backend selection for Model Studio model families."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrainerBackendPlan:
    requested_family: str
    execution_family: str
    backend_id: str
    implementation: str
    native_graph: bool
    warnings: tuple[str, ...]


def _module_available(name: str) -> bool:
    try:
        __import__(name)
        return True
    except Exception:
        return False


def _resolve_installed_backends(installed_backends: tuple[str, ...] | None) -> tuple[str, ...]:
    if installed_backends is not None:
        return installed_backends
    detected: list[str] = []
    for module_name, backend_name in (
        ("sklearn", "sklearn"),
        ("xgboost", "xgboost"),
        ("torch", "torch"),
        ("torch_geometric", "torch_geometric"),
    ):
        if _module_available(module_name):
            detected.append(backend_name)
    return tuple(detected)


def resolve_trainer_backend(
    family: str,
    *,
    runtime_target: str = "local_cpu",
    installed_backends: tuple[str, ...] | None = None,
    native_graph_contract_available: bool = False,
) -> TrainerBackendPlan:
    available = set(_resolve_installed_backends(installed_backends))
    warnings: list[str] = []

    if family == "random_forest":
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="random_forest",
            backend_id="sklearn_random_forest",
            implementation="native",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "xgboost":
        if "xgboost" in available:
            return TrainerBackendPlan(
                requested_family=family,
                execution_family="xgboost",
                backend_id="xgboost",
                implementation="native",
                native_graph=False,
                warnings=tuple(warnings),
            )
        warnings.append("xgboost is not installed; falling back to sklearn gradient boosting.")
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="xgboost",
            backend_id="sklearn_hist_gradient_boosting",
            implementation="fallback",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "dense_nn":
        if "torch" in available:
            return TrainerBackendPlan(
                requested_family=family,
                execution_family="dense_nn",
                backend_id="torch_tabular_mlp",
                implementation="native",
                native_graph=False,
                warnings=tuple(warnings),
            )
        warnings.append("PyTorch is not installed; using sklearn MLP backend.")
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="dense_nn",
            backend_id="sklearn_mlp",
            implementation="fallback",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "gnn":
        if "torch" in available and "torch_geometric" in available and native_graph_contract_available:
            return TrainerBackendPlan(
                requested_family=family,
                execution_family="gnn",
                backend_id="pyg_gnn",
                implementation="native",
                native_graph=True,
                warnings=tuple(warnings),
            )
        if "torch" in available and "torch_geometric" in available and not native_graph_contract_available:
            warnings.append("PyG is available, but the workspace does not yet expose enough per-example graph slices for native message-passing training.")
        else:
            warnings.append("torch_geometric is not available; using a graph-aware surrogate backend.")
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="dense_nn",
            backend_id="graph_surrogate_dense",
            implementation="surrogate",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "hybrid_fusion":
        if "torch" in available and "torch_geometric" in available and native_graph_contract_available:
            return TrainerBackendPlan(
                requested_family=family,
                execution_family="hybrid_fusion",
                backend_id="pyg_hybrid_fusion",
                implementation="native",
                native_graph=True,
                warnings=tuple(warnings),
            )
        if "torch" in available and "torch_geometric" in available and not native_graph_contract_available:
            warnings.append("PyG is available, but the workspace does not yet expose enough per-example graph slices for native hybrid graph training.")
        else:
            warnings.append("Native multimodal graph libraries are not available; using an attribute+graph surrogate backend.")
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="dense_nn",
            backend_id="hybrid_surrogate_dense",
            implementation="surrogate",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "clustering":
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="clustering",
            backend_id="sklearn_kmeans",
            implementation="native",
            native_graph=False,
            warnings=tuple(warnings),
        )

    if family == "autoencoder":
        return TrainerBackendPlan(
            requested_family=family,
            execution_family="autoencoder",
            backend_id="pca_autoencoder",
            implementation="fallback",
            native_graph=False,
            warnings=tuple(warnings),
        )

    warnings.append("No executable backend is registered for this family yet.")
    return TrainerBackendPlan(
        requested_family=family,
        execution_family=family,
        backend_id="unsupported",
        implementation="unsupported",
        native_graph=False,
        warnings=tuple(warnings),
    )
