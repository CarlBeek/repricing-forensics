# EIP-7904 Impact Analysis — Handoff Guide

This document explains how the EIP-7904 repricing analysis system works end-to-end: the reth fork that collects data, the Python pipeline that processes it, the web server that displays it, and how deploys work.

## Architecture Overview

```
reth fork (Rust)         CI Pipeline (Python)           Web Server (FastAPI)
─────────────────       ─────────────────────          ──────────────────────
Re-executes blocks  →   SQLite → Parquet → DuckDB  →  JSON API + HTML pages
with modified gas        + contract enrichment          served via Cloudflare
schedule. Records        + call graph analysis
divergences to SQLite    + CSV reports
```

## Server

- **Host**: `ubuntu@157.180.2.180` (Hetzner, "gas-repricing")
- **Public URL**: https://repricing-forensics.carlbeek.com
- **Cloudflare Tunnel ID**: `22fff779-6755-4e0f-a5bf-313413d12fb4`
- **Tunnel config**: `~/.cloudflared/config.yml` proxies `localhost:8000`

## The Two Repos

### 1. reth fork (`~/reth/` on server, `../reth/` locally)

A modified reth node that re-executes every transaction under the EIP-7904 gas schedule and records divergences. Key files:

- `crates/research/src/multi_schedule_inspector.rs` — The core inspector that modifies gas during EVM execution. Charges per-opcode deltas via `record_cost()`, tracks per-frame repricing deltas, detects OOG.
- `crates/research/src/divergence.rs` — Data types: `CallFrame`, `OperationCounts`, `OutOfGasInfo`, etc.
- `bin/reth-research/src/main.rs` — The ExEx that coordinates baseline vs schedule execution. Records results to SQLite.
- `bin/reth-research/scripts/run_xeon_perf.sh` — Run script. Auto-sizes thread pools and caches. **Only rebuilds if the binary doesn't exist** (check `if [[ ! -x target/release/reth-research ]]`). Delete the binary to force a rebuild.
- `schedules/7904_prelim.csv` — The gas schedule CSV (on server only). Format: `Opcode,Parameter,Current Gas,New Gas`.

**Running reth**: The server runs reth via a systemd unit or manual invocation of `run_xeon_perf.sh`. It writes to `~/reth/divergences.db` (SQLite) and `~/reth/research_lake/` (Parquet).

**Making reth changes**: Edit in the local `../reth/` repo, push to GitHub. On the server, pull and delete the old binary: `rm ~/reth/target/release/reth-research`. The run script will rebuild on next start.

### 2. Analysis dashboard (this repo)

Python pipeline + FastAPI web server.

**GitHub repo**: `CarlBeek/repricing-forensics`

## Data Flow

```
reth node
  ↓ writes
~/reth/divergences.db (SQLite)
  ↓ export_parquet.py
~/reth/research_lake/ (Parquet, partitioned by schedule_name + block_bucket)
  ↓ build_duckdb.py
~/.repricing-forensics-cache/duckdb/eip7904.duckdb
  ↓ enrich_contracts.py + build_contract_labels.py
cache/contract_labels.csv
  ↓ build_call_graph_analysis.py + materialize_reports.py + etc.
artifacts/tables/*.csv
  ↓ web server reads DuckDB + CSVs
https://repricing-forensics.carlbeek.com
```

## CI Pipeline

**File**: `.github/workflows/analysis.yml`
**Schedule**: Every hour (`0 * * * *`) + manual dispatch
**Runner**: Self-hosted on the server (`[self-hosted, reth-server]`)

Steps in order:

1. **Checkout** — fresh clone
2. **Restore persistent cache** — symlinks `~/.repricing-forensics-cache/` dirs (survives across runs)
3. **Setup venv** — `rm -rf .venv && pip install -e .`
4. **Export Parquet** — `python scripts/export_parquet.py` (SQLite → Parquet)
5. **Build DuckDB** — `python scripts/build_duckdb.py --schedule-name 7904-prelim --include-call-frames` (atomic file replacement)
6. **Enrich contracts** — `python scripts/enrich_contracts.py --address-source union` (Sourcify API, up to 1024 new per run)
7. **Build contract labels** — `python scripts/build_contract_labels.py`
8. **Build status call frames** — `python scripts/build_status_call_frames.py --schedule-name 7904-prelim`
9. **Materialize CSV tables** — `python scripts/materialize_reports.py`
10. **Label failure pairs** — `python scripts/label_status_failure_pairs.py`
11. **Build project reports** — `python scripts/build_project_reports.py`
12. **Build call graph** — `python scripts/build_call_graph_analysis.py`
13. **Restart web server** — `sudo systemctl restart eip7904-web`

**Known issue**: If step 8 (build_status_call_frames) fails due to a DuckDB lock conflict with the running web server, subsequent steps (including the restart) don't run. The fix is to stop the web server before the write steps or use a separate DuckDB path for building.

## Web Server

**Systemd unit**: `eip7904-web`
```
WorkingDirectory=/home/ubuntu/actions-runner/_work/repricing-forensics/repricing-forensics
ExecStart=.venv/bin/python scripts/serve.py
```

**Stack**: FastAPI + Jinja2 templates + Plotly.js charts
**DuckDB**: Read-only connection with inode-based auto-reconnection (when CI atomically replaces the DB file, the web server detects the new inode and reopens)

### Key env vars (set in systemd unit)
- `DUCKDB_PATH` — path to eip7904.duckdb
- `RESEARCH_LAKE_PATH` — path to parquet lake
- `CACHE_DIR` — contract caches
- `ARTIFACTS_DIR` — CSV tables
- `SCHEDULE_NAME` — default "7904-prelim"

### Page structure
- `/` — Landing page (landing.html): hero stats, opcode cards, CTAs
- `/overview` — Charts dashboard (briefing.html): funnel Sankey, CDFs, top contracts
- `/forensics` — Deep analysis (forensics.html): breakage rates, gas deltas, motifs
- `/affected` — Contract search (affected.html): paginated table, clickable rows
- `/affected/{address}` — Contract detail (contract.html): stats + tx list
- `/tx/{hash}` — Transaction detail (tx.html): verdict, gas breakdown, Sankey, call stack
- `/about` — Methodology (about.html): static prose, no JS

### Static assets
CSS and JS are served with `?v={server_start_timestamp}` for automatic Cloudflare cache-busting (see `app.py`). No manual version bumps needed.

## Key Concepts

### Wallet-fixable vs contract-broken
- **Wallet-fixable**: Transaction fails at depth 0-1 with no subcalls. The sender just needs a higher gas limit — wallets auto-fix via `eth_estimateGas`. Filtered from all detailed views.
- **Contract-broken**: Failure at depth 2+ or involving subcalls. Hardcoded gas values or 63/64 rule forwarding means the contract needs updating. This is what the dashboard focuses on.
- **Classification**: `wallet_fixable_ids` table in DuckDB, built by `WALLET_FIXABLE_SQL` in `sql.py`.

### SSTORE refund complication
Baseline `gas_used` is post-refund. But the inspector charges gas pre-refund during execution. A transaction can look like it has 5K headroom (post-refund) but actually only have 100 gas of pre-refund headroom. The +165 repricing cost exceeds this, causing OOG. The tx page flags these with a verdict banner and an explainer.

### Per-frame repricing delta
Each `CallFrame` now carries `repricing_gas_delta` — the gas delta from repriced opcodes charged within that specific frame. This enables attribution: for `EOA → Router → Token`, you can see whether the Router's or the Token's repriced opcodes caused the OOG.

## Making Changes

### Changing the web UI
1. Edit templates in `src/repricing_forensics/web/templates/` or static files in `static/`
2. Commit, push to `master`
3. Trigger CI (manual dispatch or wait for hourly cron)
4. CI checks out new code, restarts web server → changes go live
5. Cache-busting is automatic (timestamp-based `?v=`)

### Changing the data pipeline
1. Edit scripts in `scripts/` or pipeline code in `src/repricing_forensics/`
2. Push to `master`
3. CI runs the updated scripts on next cycle
4. DuckDB is rebuilt atomically — web server auto-reconnects

### Changing the reth inspector
1. Edit in `../reth/crates/research/src/`
2. Push to the reth fork on GitHub
3. SSH to server, delete the binary: `rm ~/reth/target/release/reth-research`
4. Restart reth (or let the run script rebuild on next start)
5. **Important**: If you changed the data schema (new fields on CallFrame, etc.), you may need to wipe the divergence DB and research lake to avoid mixing old/new data: `rm ~/reth/divergences.db && rm -rf ~/reth/research_lake/`

### Changing the gas schedule
1. Edit the CSV on the server: `~/reth/schedules/7904_prelim.csv`
2. Format: `Opcode,Parameter,Current Gas,New Gas`
3. Wipe data and restart reth to re-analyze with the new schedule

## Troubleshooting

### CI fails on "Conflicting lock" (DuckDB)
The web server has the DB open while CI tries to write. Either restart the web server before the build step, or the atomic file replacement in `build_duckdb.py` should handle it (it writes to a temp file then replaces).

### Site shows stale CSS
Cloudflare caching. The `?v=` timestamp busts it on server restart. If still stale, purge Cloudflare cache manually (Dashboard → Caching → Purge Everything) or trigger a web server restart.

### Reth not rebuilding after code changes
The run script only builds if the binary doesn't exist. Delete it: `rm ~/reth/target/release/reth-research`

### Contract labels show raw addresses
The enrichment script runs hourly and processes up to 1024 new contracts per run. New contracts take 1-2 CI cycles to get labeled. Check `cache/contract_labels.csv` for current coverage.

## File Locations Quick Reference

| What | Where (server) |
|------|----------------|
| Reth binary | `~/reth/target/release/reth-research` |
| Gas schedule CSV | `~/reth/schedules/7904_prelim.csv` |
| Divergence SQLite | `~/reth/divergences.db` |
| Parquet lake | `~/reth/research_lake/` |
| DuckDB | `~/.repricing-forensics-cache/duckdb/eip7904.duckdb` |
| Contract caches | `~/.repricing-forensics-cache/cache/` |
| Web server code | `~/actions-runner/_work/repricing-forensics/repricing-forensics/` |
| Systemd unit | `/etc/systemd/system/eip7904-web.service` |
| Cloudflare tunnel | `~/.cloudflared/config.yml` |
