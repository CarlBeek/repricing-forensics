# Gas Repricing Forensics

Consumer-side analysis + web dashboard for the Glamsterdam gas-schedule
proposals (EIP-7904 opcode repricing, EIP-8037 native state gas).

Data comes from a producer SQLite database written by the
[`reth-research`](https://github.com/CarlBeek/reth/tree/main/crates/research)
crate (sibling repo). This repo attaches that file read-only via
DuckDB's `sqlite_scanner` extension and serves a FastAPI dashboard over
schedule-scoped views — no consumer-side materialization.

## Quick start

```bash
# Build a synthetic producer DB so the dashboards have data to render
# (skip once you point PRODUCER_DB_PATH at a real reth-research output)
python scripts/build_synthetic_producer_db.py --out ./synthetic.sqlite

# Serve
python scripts/serve.py
# → http://localhost:8000
```

Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `PRODUCER_DB_PATH` | `./synthetic.sqlite` | Producer SQLite file (the source of truth) |
| `SCHEDULE_NAME`    | `eip-8037`            | Which schedule's rows to surface |
| `CACHE_DIR`        | `./cache`             | Optional `contract_labels.csv` for address labeling |
| `DUCKDB_THREADS`   | `cpu_count`           | DuckDB worker threads |
| `HOST`, `PORT`     | `0.0.0.0`, `8000`     | Uvicorn bind |

## Architecture

```
reth-research          ──writes──▶  divergences.sqlite (SQLite WAL — single source of truth)
                                            │
                                            ▼ ATTACH (TYPE sqlite, READ_ONLY) via duckdb sqlite_scanner
                              repricing_forensics.source_db
                                  ├─ block_coverage          (per-block coverage + per-bucket counts)
                                  ├─ block_summaries         (per-(block, bucket) aggregates; JSON arrays for histograms)
                                  ├─ divergences             (drill-in cohort: event-changed + contract-broken)
                                  ├─ call_frames             (per-frame metadata)
                                  ├─ opcode_counts           (per-frame, per-opcode counts)
                                  ├─ event_logs              (per-tx emitted logs)
                                  ├─ contract_metadata       (solc/EVM target, by codehash)
                                  ├─ eip8037_tx_impact       (view — 8037-derived fields)
                                  └─ eip8037_contract_impact (view — per-recipient roll-up)
                                            │
                                            ▼
                              FastAPI app (/api/*, dashboards)
```

Why this shape: the workload is OLTP-write (reth appends per-block) +
OLAP-read (dashboard runs aggregate queries). SQLite WAL is the right
write engine for the first; DuckDB's vectorized engine is the right
query engine for the second. `sqlite_scanner` bridges them with no
duplication — one file on disk, no lock conflict, live reads.

The bucket assignment (`unchanged` / `trace_only` / `gas_only` /
`event_logs_changed` / `wallet_fixable_shallow` /
`wallet_fixable_deep_chain` / `contract_broken`) is owned by the
producer. Aggregate-only buckets never get per-tx rows in `divergences`;
the consumer reads their headline numbers from `block_coverage` and
their gas-delta distributions from `block_summaries`.

See [`docs/storage-redesign.md`](docs/storage-redesign.md) for the full
design (and the companion `crates/research/docs/storage-redesign.md` in
the reth tree for the producer side).

## Layout

- `src/repricing_forensics/`
  - `producer_schema.py` — SQLite DDL contract between producer and consumer
  - `synthetic.py` — fixture builder for local dev/test
  - `source_db.py` — attaches the producer SQLite read-only + creates views
  - `web/` — FastAPI app + dashboards
  - `config.py`, `labels.py` — paths, address labels
- `scripts/serve.py` — runs the web app
- `scripts/build_synthetic_producer_db.py` — fixture CLI
- `docs/storage-redesign.md` — consumer-side design doc
- `notebooks/` — older exploratory notebooks (pre-redesign; not maintained)
