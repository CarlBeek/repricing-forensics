# Analysis Plan

## Completed foundation

- local `.venv` created
- Parquet export pipeline wired to `../reth`
- DuckDB database initialized over the exported lake
- typed normalization table built for divergence location, OOG info, and operation counts
- first CSV reports materialized under `artifacts/tables/`
- initial Sourcify enrichment flow built and validated on top contracts

## Next implementation passes

1. Build presentation-grade Plotly figures from the derived DuckDB tables.
2. Add contract clustering so token implementations, routers, wallets, and proxies roll up to projects.
3. Add on-demand call-frame expansion for the status-change subset first, not the entire corpus.
4. Improve fixability classification using Sourcify source patterns and proxy detection heuristics.
5. Produce a ranked outreach report with evidence links for each project cluster.

## Current performance stance

- SQLite is treated as write-only source data.
- All interactive analysis should use `research_lake/` Parquet through DuckDB.
- Heavy forensic expansion should be subset-driven, especially for call-frame explosion.
