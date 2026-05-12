# Storage redesign — consumer side

This is the consumer-side companion to `crates/research/docs/storage-redesign.md`
in the reth tree. Read that first for the source-of-truth schema; this doc
covers what changes in `repricing-forensics`.

## Goals

1. **Cut disk usage roughly 2×** by collapsing the three-DB pipeline
   (`divergences.db` → `research_lake/*.parquet` → `repricing.duckdb`)
   into a single consolidated DuckDB file owned by the producer.
2. **Stop storing per-tx data for non-actionable txs.** Wallet-fixable,
   gas-only-change, and trace-divergent-only txs collapse into per-block
   aggregates. Event-log-changed and contract-broken txs stay per-tx.
3. **Capture more detail where it matters.** Per-frame opcode counts for
   the drill-in cohort, plus contract bytecode metadata.

## What dies

Everything in this repo that exists to bridge the producer's SQLite to
the consumer's DuckDB:

- `research_lake/` — parquet export goes away. Snapshots are still
  available on-demand via `COPY TABLE TO 'file.parquet'` from the
  producer DB, but they're not a permanent artifact.
- `repricing.duckdb` — no consumer-side materialized DB. The web app
  attaches the producer's DuckDB read-only.
- `src/repricing_forensics/pipeline.py` — most of it. The producer now
  emits the classification, frame data, and opcode counts directly;
  the consumer's job collapses to "create views, run queries".
- `wallet_fixable_ids` — replaced by `divergences.bucket` from the
  producer. The chain-walk classifier still runs, just on the producer
  side (it already does today; we move the bucket assignment to the
  same place).
- `artifacts_7904` and the JSON-blob hot path in `routes_api.py` —
  `baseline_call_frames` and friends become normalized tables.
- The full `NORMALIZED_FORENSICS_SQL` materialization — becomes a view.

## What this repo keeps

- The FastAPI web app (`src/repricing_forensics/web/`)
- The dashboards (`templates/`, `static/`)
- Address labels, outreach CSV, etherscan enrichment scripts
- The opcode-name lookup, divergence-classifier display logic,
  bottleneck-kind UI

The consumer becomes a presentation layer over a single read-only DB.

## New consumer layer

`src/repricing_forensics/pipeline.py` shrinks to a thin module that:

1. Resolves the path to the producer's `.duckdb` file (env var or
   `Paths`).
2. Opens DuckDB in read-only mode and runs `ATTACH 'producer.duckdb'
   AS source (READ_ONLY)`.
3. Creates a fixed set of views over `source.*` that match the names
   the web app currently uses (`hot_7904`, `normalized_forensics`,
   `eip8037_tx_impact`, `eip8037_contract_impact`,
   `wallet_fixable_ids`, etc.), so the API code keeps working with
   minimal changes during the rewrite.
4. Optionally creates a small writable side-DB for the consumer's own
   cached artifacts (labels CSV → table, opcode-impact precompute) if
   we find that views over a remote read-only DB are too slow.

Web-app endpoints don't change their JSON contracts. Their SQL gets
shorter because the producer has already done the classification.

## Schema the consumer relies on

Authoritative version: see the reth doc. Summary the dashboards need:

- `block_coverage(schedule_name, block_number, tx_count, ...)`
- `block_summaries(schedule_name, block_number, bucket, tx_count,
  gas_delta_sum, gas_delta_log2_hist, opcode_count_totals_7904,
  state_gas_totals_8037, multiplier_log2_hist, ...)`
- `divergences(divergence_id, schedule_name, block_number, tx_index,
  tx_hash, recipient, sender, bucket, gas_delta, status_changed,
  event_logs_changed, oog_chain_proportional, oog_bottleneck_kind,
  oog_bottleneck_depth, tx_gas_limit, baseline_gas_used,
  schedule_gas_used, schedule_state_gas_spent, runtime_state_gas,
  runtime_state_gas_spillover, min_multiplier_to_succeed,
  state_gas_category, ...)` — drill-in cohort only
- `divergence_call_frames(divergence_id, call_index, depth, parent_call_index,
  from_address, to_address, codehash, selector, call_type,
  gas_provided, gas_used, gas_margin, success, parent_gas_at_call,
  gas_requested_on_stack, eip150_cap_binding, state_gas_running)`
- `divergence_opcode_counts(divergence_id, call_index, opcode, count,
  gas_baseline, gas_schedule)` — sparse, zeros omitted
- `divergence_event_logs(divergence_id, trace_kind, log_index,
  address, topic0..topic3, data_bytes, data_hash)`
- `contract_metadata(codehash, contract_address, solc_version,
  evm_target, cbor_present, bytecode_len)`
- `analysis_runs(schema_version, schedule_config_hash, reth_commit,
  run_started_at, run_finished_at)` — manifest

`bucket` is one of:
`'unchanged' | 'trace_only' | 'gas_only' | 'event_logs_changed' |
 'wallet_fixable_shallow' | 'wallet_fixable_deep_chain' |
 'contract_broken'`.

Per the goals: rows in the first four `*_only`/`wallet_fixable*`
buckets are aggregated into `block_summaries` and **do not appear**
in `divergences` / `divergence_call_frames` / `divergence_opcode_counts`.

## Dashboard mapping (what changes in each view)

| Today | After |
|---|---|
| `/api/overview` totals | aggregate over `block_summaries` for counts; `divergences` for contract-broken |
| `/api/funnel` | reads counts from `block_summaries` by bucket |
| `/api/opcode-impact` (divergence opcode) | unchanged shape, sourced from `divergences` |
| `/api/forensics/bottleneck-kinds` | unchanged shape, sourced from `divergences` |
| `/api/gas-overhead` (CDF for non-broken) | reconstructed from `block_summaries.gas_delta_log2_hist` |
| `/api/concentration`, `/api/top-contracts` | unchanged shape, sourced from `divergences` |
| `/api/forensics/call-depth` | sourced from `divergence_call_frames` |
| `/api/forensics/failure-motifs` | rebuilt from `divergence_call_frames` (caller, callee selectors) |
| `/api/eip8037/*` | sourced from `divergences` for the drill-in cohort, `block_summaries` for cohort totals |
| `/api/affected/*` | unchanged shape; queries `divergences` + `eip8037_contract_impact` view |
| `/api/tx/{hash}` | unchanged shape; joins `divergences` + `divergence_call_frames` + `divergence_event_logs` + `divergence_opcode_counts` + `contract_metadata` |

The four endpoints with the biggest payload changes are
`/api/forensics/failure-motifs` (now powered by frame-level data
instead of a precomputed CSV), the histograms (now read pre-binned
counts), and `/api/tx/{hash}` (no JSON parsing needed; we read
normalized rows).

## New dashboard surfaces unlocked by the schema

These aren't required for the rewrite to land, but they're cheap once
the data is normalized:

- **Per-opcode heatmap by frame depth.** "At depth 2, 80% of broken
  txs have ≥ 50 KECCAK256 ops in the bottleneck frame." Reads
  `divergence_opcode_counts` × `divergence_call_frames`.
- **Contract clustering by solc version.** "Solidity 0.4.x contracts
  account for X% of contract-broken txs." Reads `contract_metadata`.
- **Selector-keyed breakage.** "The top 10 broken function signatures
  across the dataset." `divergence_call_frames.selector` JOIN with
  external 4byte database.
- **Reservoir drain trace.** For 8037: render a per-frame line chart
  of `state_gas_running` to see exactly where the reservoir tipped.
  Reads `divergence_call_frames.state_gas_running`.

## Migration order across both repos

1. **reth** — implement the new DuckDB schema in
   `crates/research/src/database.rs` (or a new `database_duckdb.rs`
   alongside the existing SQLite module). Behind a feature flag.
2. **reth** — wire the per-tx classifier (`bucket` assignment) into
   the inspector pipeline. Today the consumer derives this in SQL;
   move that logic to the producer.
3. **reth** — add the per-frame opcode-count capture to the
   inspector. Currently `operation_counts` is a tx-level JSON; we
   need it keyed by `(call_index, opcode)`.
4. **reth** — implement the `contract-metadata-backfill`
   subcommand.
5. **reth** — replay a smoke-test schedule end-to-end into a new
   DuckDB to validate the schema.
6. **repricing-forensics (this repo)** — rewrite `pipeline.py` to
   attach the new DB instead of building the lake.
7. **repricing-forensics** — port API endpoints one at a time,
   running both old and new pipelines side-by-side against the same
   replay to validate parity for each chart before flipping.
8. **repricing-forensics** — drop the old pipeline once every
   endpoint has been ported.
9. **Both repos** — remove the SQLite producer code, remove the
   parquet export step, remove the consumer DuckDB build step.

Step 7 is the longest. Each endpoint can be ported independently;
neither old nor new pipeline blocks the other. We can ship the
producer with the new schema, keep the consumer reading the parquet
lake until every chart is migrated, then flip the consumer over.

## Open issues (to resolve before implementation)

- **DuckDB concurrent access during replay.** Reth replays multiple
  blocks per second; need to confirm the consumer can hold a long-
  lived read-only attach while reth holds the writer. DuckDB MVCC
  supports this in v1.x but library-version mismatch between Rust
  and Python clients breaks it. Pin both to the same DuckDB version
  in CI.
- **Block summary write timing.** Block summaries can't be written
  until every tx in a block is classified. Producer needs to buffer
  the per-tx classifications until the block's `end_block` event,
  then emit a single `block_summaries` row.
- **Histograms vs sketches.** Log2 histograms are easy and additive;
  exact percentile reconstruction needs a sketch (t-digest or KLL).
  For our gas-delta range (mostly < 4M) log2 with 12 bins gives
  sufficient resolution for the CDF charts. Revisit if the
  multiplier-needed chart loses too much fidelity.
- **Backwards compat for old data.** We're doing a full rewrite, so
  the old `divergences.db` files become inaccessible to the new
  consumer. Either re-replay or write a one-shot migration script
  that translates old SQLite → new DuckDB. My recommendation: skip
  the migration script (re-replay is mechanical and we want fresh
  bucket assignments + frame opcodes anyway).
