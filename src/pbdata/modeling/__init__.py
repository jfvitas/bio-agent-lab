"""Model Studio backend helpers."""

from pbdata.modeling.runtime import (
    RuntimeCapabilities,
    detect_runtime_capabilities,
    export_training_package,
)
from pbdata.modeling.graph_contract import GraphLearningContract, build_graph_learning_contract
from pbdata.modeling.graph_dataset import materialize_graph_dataset_records
from pbdata.modeling.graph_native_backend import (
    NativeGraphSample,
    build_torch_geometric_data,
    load_native_graph_samples,
)
from pbdata.modeling.graph_pyg_adapter import build_pyg_ready_graph_samples
from pbdata.modeling.graph_samples import build_graph_sample_manifest
from pbdata.modeling.graph_training_payload import materialize_graph_training_payload
from pbdata.modeling.hybrid_training_payload import materialize_hybrid_training_payload
from pbdata.modeling.pyg_training import GraphTrainingRecord, load_graph_training_records, train_pyg_gnn
from pbdata.modeling.pyg_training import HybridTrainingRecord, load_hybrid_training_records, train_pyg_hybrid_fusion
from pbdata.modeling.training_runs import (
    RunComparison,
    RunInspection,
    TrainingRunResult,
    build_training_run_report,
    compare_training_runs,
    execute_training_run,
    import_training_run,
    inspect_training_run,
    run_saved_model_batch_inference,
    run_saved_model_inference,
)
from pbdata.modeling.trainer_registry import TrainerBackendPlan, resolve_trainer_backend
from pbdata.modeling.studio import (
    CompatibilityMessage,
    DatasetProfile,
    ModelRecommendation,
    ModelStudioSelection,
    StarterModelConfig,
    build_dataset_profile,
    build_starter_model_config,
    export_starter_model_config,
    recommend_model_architectures,
    validate_model_studio_selection,
)

__all__ = [
    "CompatibilityMessage",
    "DatasetProfile",
    "GraphLearningContract",
    "GraphTrainingRecord",
    "HybridTrainingRecord",
    "NativeGraphSample",
    "ModelRecommendation",
    "ModelStudioSelection",
    "RuntimeCapabilities",
    "RunComparison",
    "RunInspection",
    "StarterModelConfig",
    "TrainerBackendPlan",
    "TrainingRunResult",
    "build_training_run_report",
    "build_dataset_profile",
    "build_graph_learning_contract",
    "materialize_graph_dataset_records",
    "load_native_graph_samples",
    "build_torch_geometric_data",
    "build_pyg_ready_graph_samples",
    "build_graph_sample_manifest",
    "materialize_graph_training_payload",
    "materialize_hybrid_training_payload",
    "load_graph_training_records",
    "load_hybrid_training_records",
    "train_pyg_gnn",
    "train_pyg_hybrid_fusion",
    "build_starter_model_config",
    "compare_training_runs",
    "detect_runtime_capabilities",
    "execute_training_run",
    "export_starter_model_config",
    "export_training_package",
    "import_training_run",
    "inspect_training_run",
    "recommend_model_architectures",
    "resolve_trainer_backend",
    "run_saved_model_batch_inference",
    "run_saved_model_inference",
    "validate_model_studio_selection",
]
