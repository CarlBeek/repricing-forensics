# EIP-7904 Analysis Workspace

This repo contains the Python analysis workflow for evaluating the impact of the
`7904-prelim` repricing dataset produced by the local `reth-research` fork.

## Workflow

1. Export the SQLite write-store to partitioned Parquet.
2. Query the Parquet lake from DuckDB.
3. Normalize forensic fields into typed derived tables.
4. Enrich contracts and projects via Sourcify.
5. Produce presentation-quality figures and outreach reports.

## Layout

- `src/eip7904_analysis/`: reusable analysis code
- `scripts/`: CLI entrypoints for export, normalization, enrichment, and reports
- `notebooks/`: exploratory and presentation notebooks
- `artifacts/`: generated charts and presentation outputs
- `cache/`: local API and enrichment caches
- `duckdb/`: local DuckDB databases and derived tables

## Environment

The local environment is managed in `.venv`.

Example activation:

```bash
source .venv/bin/activate
```

## Immediate priorities

- export `divergences.db` to Parquet
- validate 7904 coverage and divergence counts
- normalize call-stack and OOG metadata
- identify top impacted projects and fix owners
