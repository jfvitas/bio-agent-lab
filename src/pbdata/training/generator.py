"""ML training-example generator.

Supports two states:
- If extracted, graph, and feature data are present, assemble training examples
- Otherwise, write an architecture manifest describing the planned subsystem
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def build_training_manifest(output_dir: Path) -> Path:
    """Write a training-example manifest aligned to the full spec schema."""
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "planned",
        "layer": "training_example",
        "required_sections": [
            "structure",
            "protein",
            "ligand",
            "interaction",
            "experiment",
            "graph_features",
        ],
        "notes": (
            "Architecture scaffold only. Example generation, label assignment, "
            "and split-aware export are not implemented yet."
        ),
    }
    out_path = output_dir / "training_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_path


def build_training_examples(
    extracted_dir: Path,
    features_dir: Path,
    graph_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Assemble training examples from all upstream layers.

    Delegates to the assembler module. Returns (examples_path, manifest_path).
    """
    from pbdata.training.assembler import assemble_training_examples

    return assemble_training_examples(
        extracted_dir, features_dir, graph_dir, output_dir,
    )
