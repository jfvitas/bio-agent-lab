from pathlib import Path
import textwrap

ROOT = Path(".").resolve()

FOLDERS = [
    "specs",
    "prompts",
    "prompts/tasks",
    "configs",
    "scripts",
    "src/pbdata",
    "src/pbdata/schemas",
    "src/pbdata/sources",
    "src/pbdata/parsing",
    "src/pbdata/enrichment",
    "src/pbdata/quality",
    "src/pbdata/dataset",
    "src/pbdata/search",
    "src/pbdata/reports",
    "tests",
    "tests/test_sources",
    "tests/test_quality",
    "tests/test_dataset",
    "tests/fixtures",
    "data/raw",
    "data/interim",
    "data/processed",
    "data/external",
    "data/reports",
    "notebooks",
]

for folder in FOLDERS:
    (ROOT / folder).mkdir(parents=True, exist_ok=True)

def write(relpath: str, content: str) -> None:
    path = ROOT / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8")

write(
    "README.md",
    """
    # pbdata

    Spec-driven repository for constructing, auditing, and versioning
    protein binding datasets for machine learning.

    ## Quick start

    1. Create the repo scaffold:
       `python bootstrap_repo.py`
    2. Validate the schema:
       `python scripts/validate_schema.py`
    3. Run tests:
       `pytest -q`
    """,
)

write(
    ".gitignore",
    """
    __pycache__/
    *.pyc
    .pytest_cache/
    .ruff_cache/
    .venv/
    .env
    data/raw/*
    data/interim/*
    data/processed/*
    data/external/*
    data/reports/*
    !data/raw/.gitkeep
    !data/interim/.gitkeep
    !data/processed/.gitkeep
    !data/external/.gitkeep
    !data/reports/.gitkeep
    """,
)

for gitkeep in [
    "data/raw/.gitkeep",
    "data/interim/.gitkeep",
    "data/processed/.gitkeep",
    "data/external/.gitkeep",
    "data/reports/.gitkeep",
]:
    write(gitkeep, "")

write(
    "pyproject.toml",
    """
    [build-system]
    requires = ["setuptools>=68", "wheel"]
    build-backend = "setuptools.build_meta"

    [project]
    name = "pbdata"
    version = "0.1.0"
    description = "Protein binding dataset platform"
    readme = "README.md"
    requires-python = ">=3.11"
    dependencies = [
      "pydantic>=2.7",
      "pyyaml>=6.0.1",
      "typer>=0.12",
      "rich>=13.7",
    ]

    [project.optional-dependencies]
    dev = [
      "pytest>=8.2",
      "ruff>=0.5",
    ]

    [project.scripts]
    pbdata = "pbdata.cli:app"

    [tool.setuptools]
    package-dir = {"" = "src"}

    [tool.setuptools.packages.find]
    where = ["src"]

    [tool.pytest.ini_options]
    testpaths = ["tests"]

    [tool.ruff]
    line-length = 100
    target-version = "py311"
    """,
)

write(
    "Makefile",
    """
    .PHONY: validate test format tree

    validate:
    \tpython scripts/validate_schema.py

    test:
    \tpytest -q

    format:
    \truff check . --fix

    tree:
    \tfind . -maxdepth 3 | sort
    """,
)

write(
    "specs/repo_contract.md",
    """
    # Repo Contract

    ## Purpose
    This repository builds auditable, versioned protein-binding ML datasets from multiple sources.

    ## Non-negotiable requirements
    - All normalized records must conform to `specs/canonical_schema.yaml`
    - Every transformed field must preserve provenance
    - No source adapter may write directly to final datasets without normalization
    - No incompatible assay types may be merged without explicit transform logic
    - All outputs must be reproducible from config files
    - All modules must be typed
    - All major functions must have tests

    ## Phase 1 deliverables
    - package structure
    - canonical schema models
    - CLI skeleton
    - config loader
    - logging
    - tests
    """,
)

write(
    "specs/coding_standards.md",
    """
    # Coding Standards

    - Python 3.11+
    - Use type hints everywhere practical
    - Prefer small, testable functions
    - No hidden global state
    - Preserve provenance explicitly
    - Raise clear errors for schema/normalization failures
    - Add docstrings to public classes and functions
    """,
)

write(
    "specs/source_requirements.md",
    """
    # Source Requirements

    Planned source adapters:
    - RCSB PDB
    - BindingDB
    - PDBbind
    - BioLiP
    - SKEMPI
    - Literature/manual imports

    Each adapter must:
    - ingest raw records
    - preserve original identifiers
    - attach provenance
    - map into canonical schema only through normalization
    """,
)

write(
    "specs/quality_rules.yaml",
    """
    hard_exclusions:
      - missing_structure_file
      - ambiguous_chain_assignment
      - no_interpretable_assay_value
      - impossible_affinity_value
      - unresolved_partner_for_ppi
      - unparseable_mutation_notation

    soft_penalties:
      - low_resolution
      - missing_interface_residues
      - alternate_conformers_at_interface
      - ic50_without_context
      - missing_metal_annotation
      - covalent_noncovalent_task_mismatch
      - heavy_homolog_redundancy
    """,
)

write(
    "specs/split_policy.yaml",
    """
    split_strategies:
      - random
      - sequence_cluster
      - time_split
      - source_split
      - scaffold_split
      - family_split
      - mutation_split
    """,
)

write(
    "specs/canonical_schema.yaml",
    """
    canonical_binding_sample:
      description: Unified record for protein binding ML datasets
      fields:
        sample_id:
          type: str
          required: true
          description: Globally unique internal sample identifier

        task_type:
          type: str
          required: true
          allowed:
            - protein_ligand
            - protein_protein
            - mutation_ddg

        source_database:
          type: str
          required: true

        source_record_id:
          type: str
          required: true

        pdb_id:
          type: str
          required: false

        assembly_id:
          type: str
          required: false

        chain_ids_receptor:
          type: list[str]
          required: false

        chain_ids_partner:
          type: list[str]
          required: false

        uniprot_ids:
          type: list[str]
          required: false

        sequence_receptor:
          type: str
          required: false

        sequence_partner:
          type: str
          required: false

        taxonomy_ids:
          type: list[int]
          required: false

        ligand_id:
          type: str
          required: false

        ligand_smiles:
          type: str
          required: false

        ligand_inchi_key:
          type: str
          required: false

        experimental_method:
          type: str
          required: false

        structure_resolution:
          type: float
          required: false

        assay_type:
          type: str
          required: false

        assay_value:
          type: float
          required: false

        assay_unit:
          type: str
          required: false

        assay_value_standardized:
          type: float
          required: false

        assay_value_log10:
          type: float
          required: false

        temperature_c:
          type: float
          required: false

        ph:
          type: float
          required: false

        buffer:
          type: str
          required: false

        ionic_strength:
          type: float
          required: false

        cofactors:
          type: list[str]
          required: false

        mutation_string:
          type: str
          required: false

        wildtype_or_mutant:
          type: str
          required: false
          allowed:
            - wildtype
            - mutant

        curation_level:
          type: str
          required: false

        provenance:
          type: dict
          required: true

        quality_flags:
          type: list[str]
          required: true

        quality_score:
          type: float
          required: true
    """,
)

write(
    "prompts/claude_master_prompt.md",
    """
    You are implementing this repository according to the specification files.

    Read and obey:
    - specs/repo_contract.md
    - specs/canonical_schema.yaml
    - specs/coding_standards.md
    - specs/source_requirements.md

    Your task:
    Implement phase 1 of the repository:
    - package skeleton
    - pydantic schemas
    - Typer CLI
    - YAML config loader
    - logging setup
    - pytest scaffolding

    Requirements:
    - Do not change the canonical schema without explicit justification
    - Keep code modular
    - Add docstrings
    - Add TODOs where source-specific logic remains
    - Ensure tests pass
    """,
)

write(
    "prompts/codex_review_prompt.md",
    """
    Review the repository against:
    - specs/repo_contract.md
    - specs/canonical_schema.yaml
    - specs/coding_standards.md
    - specs/source_requirements.md

    Focus on:
    - correctness
    - schema fidelity
    - reproducibility
    - failure handling
    - scientific/data integrity
    - missing tests
    - architectural weaknesses

    Return:
    1. critical bugs
    2. schema mismatches
    3. scientific concerns
    4. missing tests
    5. precise patch suggestions
    """,
)

write(
    "prompts/tasks/schema_models.md",
    """
    Implement the schema models in src/pbdata/schemas/.

    Requirements:
    - Match specs/canonical_schema.yaml exactly
    - Use pydantic
    - Add tests for required and optional fields
    - Do not invent fields not present in the canonical schema
    """,
)

write(
    "configs/logging.yaml",
    """
    version: 1
    disable_existing_loggers: false
    formatters:
      standard:
        format: "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers:
      console:
        class: logging.StreamHandler
        level: INFO
        formatter: standard
    root:
      level: INFO
      handlers: [console]
    """,
)

write(
    "configs/sources.yaml",
    """
    sources:
      rcsb:
        enabled: true
      bindingdb:
        enabled: false
      pdbbind:
        enabled: false
      biolip:
        enabled: false
      skempi:
        enabled: false
    """,
)

write(
    "scripts/validate_schema.py",
    """
    from pathlib import Path
    import yaml

    def main() -> None:
        schema_path = Path("specs/canonical_schema.yaml")
        if not schema_path.exists():
            raise FileNotFoundError(f"Missing schema file: {schema_path}")

        data = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
        root = data.get("canonical_binding_sample", {})
        fields = root.get("fields", {})

        required_top = ["sample_id", "task_type", "source_database", "source_record_id", "provenance", "quality_flags", "quality_score"]
        missing = [name for name in required_top if name not in fields]
        if missing:
            raise ValueError(f"Schema missing required fields: {missing}")

        print("Schema validation passed.")
        print(f"Field count: {len(fields)}")
        print("Fields:")
        for key in fields:
            print(f" - {key}")

    if __name__ == "__main__":
        main()
    """,
)

write(
    "src/pbdata/__init__.py",
    """
    __all__ = ["__version__"]
    __version__ = "0.1.0"
    """,
)

write(
    "src/pbdata/cli.py",
    """
    import typer

    app = typer.Typer(help="Protein binding dataset platform CLI.")

    @app.command()
    def ingest() -> None:
        \"""Ingest raw records from configured sources.\"""
        typer.echo("TODO: ingest")

    @app.command("normalize")
    def normalize_cmd() -> None:
        \"""Normalize raw records into canonical schema.\"""
        typer.echo("TODO: normalize")

    @app.command()
    def audit() -> None:
        \"""Run dataset quality and robustness audits.\"""
        typer.echo("TODO: audit")

    @app.command()
    def report() -> None:
        \"""Generate dataset reports.\"""
        typer.echo("TODO: report")

    @app.command("build-splits")
    def build_splits() -> None:
        \"""Build leakage-resistant dataset splits.\"""
        typer.echo("TODO: build-splits")

    if __name__ == "__main__":
        app()
    """,
)

write(
    "src/pbdata/schemas/__init__.py",
    """
    from .canonical_sample import CanonicalBindingSample

    __all__ = ["CanonicalBindingSample"]
    """,
)

write(
    "src/pbdata/schemas/canonical_sample.py",
    """
    from typing import Any, Literal

    from pydantic import BaseModel, Field

    class CanonicalBindingSample(BaseModel):
        \"""Unified normalized record for protein binding ML datasets.\"""

        sample_id: str
        task_type: Literal["protein_ligand", "protein_protein", "mutation_ddg"]
        source_database: str
        source_record_id: str

        pdb_id: str | None = None
        assembly_id: str | None = None

        chain_ids_receptor: list[str] | None = None
        chain_ids_partner: list[str] | None = None
        uniprot_ids: list[str] | None = None

        sequence_receptor: str | None = None
        sequence_partner: str | None = None
        taxonomy_ids: list[int] | None = None

        ligand_id: str | None = None
        ligand_smiles: str | None = None
        ligand_inchi_key: str | None = None

        experimental_method: str | None = None
        structure_resolution: float | None = None

        assay_type: str | None = None
        assay_value: float | None = None
        assay_unit: str | None = None
        assay_value_standardized: float | None = None
        assay_value_log10: float | None = None

        temperature_c: float | None = None
        ph: float | None = None
        buffer: str | None = None
        ionic_strength: float | None = None
        cofactors: list[str] | None = None

        mutation_string: str | None = None
        wildtype_or_mutant: Literal["wildtype", "mutant"] | None = None
        curation_level: str | None = None

        provenance: dict[str, Any] = Field(default_factory=dict)
        quality_flags: list[str] = Field(default_factory=list)
        quality_score: float = 0.0
    """,
)

write(
    "src/pbdata/sources/__init__.py",
    """
    __all__ = []
    """,
)

write(
    "src/pbdata/sources/rcsb.py",
    """
    \"""RCSB source adapter placeholder.

    Responsibilities:
    - query or read RCSB metadata
    - normalize records toward CanonicalBindingSample
    - preserve provenance
    - avoid direct writes to processed datasets
    \"""

    from typing import Any

    class RCSBAdapter:
        \"""Adapter for RCSB metadata ingestion.\"""

        def fetch_metadata(self, pdb_id: str) -> dict[str, Any]:
            \"""Fetch metadata for a PDB ID.\"""
            raise NotImplementedError

        def normalize_record(self, raw: dict[str, Any]) -> dict[str, Any]:
            \"""Map a raw RCSB record to canonical internal fields.\"""
            raise NotImplementedError
    """,
)

for module_path in [
    "src/pbdata/parsing/__init__.py",
    "src/pbdata/enrichment/__init__.py",
    "src/pbdata/quality/__init__.py",
    "src/pbdata/dataset/__init__.py",
    "src/pbdata/search/__init__.py",
    "src/pbdata/reports/__init__.py",
]:
    write(module_path, "")

write(
    "tests/test_schema.py",
    """
    from pbdata.schemas.canonical_sample import CanonicalBindingSample

    def test_canonical_binding_sample_has_required_fields():
        fields = CanonicalBindingSample.model_fields
        required = [
            "sample_id",
            "task_type",
            "source_database",
            "source_record_id",
            "provenance",
            "quality_flags",
            "quality_score",
        ]
        for name in required:
            assert name in fields

    def test_task_type_allowed_values():
        item = CanonicalBindingSample(
            sample_id="x1",
            task_type="protein_ligand",
            source_database="RCSB",
            source_record_id="1abc",
            provenance={},
            quality_flags=[],
            quality_score=0.0,
        )
        assert item.task_type == "protein_ligand"
    """,
)

print("Bootstrap complete.")
print(f"Repository written to: {ROOT}")
