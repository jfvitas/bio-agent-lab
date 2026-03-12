from __future__ import annotations

import argparse

from pbdata.config import AppConfig, load_config
from pbdata.demo import export_demo_snapshot
from pbdata.storage import build_storage_layout


def main() -> None:
    parser = argparse.ArgumentParser(description="Export internal demo readiness artifacts.")
    parser.add_argument("--config", default="configs/sources.yaml", help="Path to sources YAML config.")
    parser.add_argument("--storage-root", default=None, help="Override storage root.")
    args = parser.parse_args()

    cfg = load_config(args.config) if args.config else AppConfig()
    layout = build_storage_layout(args.storage_root or cfg.storage_root)
    json_path, md_path, report = export_demo_snapshot(layout, cfg)
    print(f"Readiness: {report['readiness']}")
    print(f"JSON snapshot: {json_path}")
    print(f"Markdown guide: {md_path}")


if __name__ == "__main__":
    main()
