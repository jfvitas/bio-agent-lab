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
