# Gas Repricing Analysis Workspace

This repo contains the Python analysis workflow for evaluating the impact of the
alternate gas schedules produced by the local `reth-research` fork.

The default schedule for this branch is `eip-8037`, including static
cost-per-state-byte state gas, reservoir accounting, original-limit fit metrics,
and refund/state-gas detail columns exported by `reth-research`.

## Workflow

1. Export the SQLite write-store to partitioned Parquet.
2. Query the Parquet lake from DuckDB.
3. Normalize forensic fields into typed derived tables.
4. Derive EIP-8037 state-gas impact tables.
5. Enrich contracts and projects via Sourcify.
6. Produce presentation-quality figures and outreach reports.

## Layout

- `src/repricing_forensics/`: reusable analysis code
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
- validate EIP-8037 coverage and divergence counts
- normalize call-stack and OOG metadata
- quantify original-limit failures, minimum gas-limit multipliers, and reservoir spillover
- identify top impacted projects and fix owners
