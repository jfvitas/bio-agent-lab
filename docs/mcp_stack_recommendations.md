## Recommended MCP Stack for pbdata

### Priority 1

- GitHub MCP
  Use for issue tracking, PR review, release notes, and tying data-pipeline work to repo changes.

- SQLite MCP
  Use for local querying of `data/catalog/download_manifest.csv`, normalized records, and audit outputs.
  Upgrade to Postgres MCP if multiple users or services need concurrent access.

- Filesystem / Docs MCP
  Expose `specs/`, `configs/`, `data/catalog/`, and generated reports as structured resources for the agent.

### Priority 2

- Spreadsheet / Tabular MCP
  Useful for inspecting and exporting the manifest and summary tables.

- HTTP / OpenAPI MCP
  Useful once more source databases are integrated and the repo wants typed API operations instead of custom `requests` code.

### Not Worth Adding Yet

- Vector DB MCP
- Browser automation MCP
- Cloud / infra MCPs

### Recommended Stack by Workflow

#### Local research workflow

- GitHub MCP
- SQLite MCP
- Filesystem / Docs MCP
- Spreadsheet MCP

#### Production-like multi-source pipeline

- GitHub MCP
- Postgres MCP
- Filesystem / Docs MCP
- HTTP / OpenAPI MCP
- Spreadsheet MCP

### Setup Notes

- Store manifest and cross-source summary data in a database-backed MCP, not only CSVs.
- Expose the spec files and stress-panel definitions through a docs/filesystem MCP so review tasks can consume them directly.
- Keep external API MCPs source-specific; do not add broad scraping MCPs unless a source requires them.
