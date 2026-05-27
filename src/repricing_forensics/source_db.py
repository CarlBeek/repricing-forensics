"""Open a read-only consumer session against the producer's SQLite file.

Consumer entry point for the SQLite-writer + DuckDB-reader architecture
(see `docs/storage-redesign.md`). The producer (reth-research) writes
SQLite in WAL mode; the consumer attaches that file read-only via
DuckDB's `sqlite_scanner` extension and runs analytical queries through
DuckDB's vectorized engine.

The producer can write multiple schedules into the same file at once
(e.g. `7904-prelim` and `eip-8037` running in parallel). Views here
are **not** filtered by schedule — every `/api/*` endpoint takes a
`schedule` query parameter and injects `WHERE schedule_name = '...'`
inline. This keeps the per-schedule routing in the page/endpoint
layer rather than in a single global session.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb


def open_session(producer_db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB, attach the producer SQLite file
    read-only via sqlite_scanner, create unfiltered views, return the
    connection."""
    conn = duckdb.connect(":memory:")
    threads = os.environ.get("DUCKDB_THREADS", str(os.cpu_count() or 4))
    conn.execute(f"PRAGMA threads={threads}")
    # sqlite_scanner is an official DuckDB extension; INSTALL fetches it
    # from the central repo once, LOAD makes it available in this
    # connection.
    conn.execute("INSTALL sqlite")
    conn.execute("LOAD sqlite")
    conn.execute(
        f"ATTACH '{str(producer_db_path)}' AS producer (TYPE sqlite, READ_ONLY)"
    )
    create_views(conn)
    return conn


def create_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create unfiltered views over the producer's tables.

    Every view keeps `schedule_name` as a column; endpoints filter on
    it at query time so each request can pick its own schedule. The
    `eip8037_contract_impact` aggregation groups by
    `(schedule_name, recipient)` for the same reason.
    """
    # Pass-throughs.
    conn.execute("""
        CREATE OR REPLACE VIEW block_coverage AS
        SELECT * FROM producer.block_coverage
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW block_summaries AS
        SELECT * FROM producer.block_summaries
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW divergences AS
        SELECT * FROM producer.divergences
    """)

    # Frame / opcode / log tables aren't scheduled directly; they hang off
    # divergences via divergence_id. We surface `schedule_name` on each
    # row via JOIN so endpoints can filter the same way they filter
    # `divergences`.
    conn.execute("""
        CREATE OR REPLACE VIEW call_frames AS
        SELECT f.*, d.schedule_name
        FROM producer.divergence_call_frames f
        JOIN producer.divergences d USING (divergence_id)
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW opcode_counts AS
        SELECT o.*, d.schedule_name
        FROM producer.divergence_opcode_counts o
        JOIN producer.divergences d USING (divergence_id)
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW event_logs AS
        SELECT l.*, d.schedule_name
        FROM producer.divergence_event_logs l
        JOIN producer.divergences d USING (divergence_id)
    """)

    # Contract metadata is schedule-independent.
    conn.execute("""
        CREATE OR REPLACE VIEW contract_metadata AS
        SELECT * FROM producer.contract_metadata
    """)

    # Analysis runs — used by endpoints to discover which schedules
    # have been recorded.
    conn.execute("""
        CREATE OR REPLACE VIEW analysis_runs AS
        SELECT * FROM producer.analysis_runs
    """)

    # Per-tx EIP-8037 view: every drill-in divergence with the 8037
    # columns pre-derived (original_limit_failure, target_address).
    conn.execute("""
        CREATE OR REPLACE VIEW eip8037_tx_impact AS
        SELECT
            divergence_id, schedule_name, tx_hash, block_number, tx_index,
            lower(recipient) AS target_address,
            sender, tx_gas_limit, is_create,
            baseline_success, schedule_success, status_changed,
            baseline_gas_used, schedule_gas_used, gas_delta,
            schedule_total_gas_spent, schedule_state_gas_spent,
            schedule_state_gas_demanded,
            schedule_initial_state_gas, schedule_initial_reservoir,
            schedule_floor_gas, schedule_gas_refunded,
            baseline_total_gas_spent, baseline_gas_refunded,
            runtime_state_gas, runtime_state_gas_spillover,
            would_fit_in_original_limit,
            (NOT coalesce(would_fit_in_original_limit, TRUE)
              AND coalesce(schedule_state_gas_spent, 0) > 0) AS original_limit_failure,
            min_multiplier_to_succeed,
            CASE
                WHEN schedule_success AND min_multiplier_to_succeed IS NOT NULL
                     AND tx_gas_limit > 0
                    THEN ceil(tx_gas_limit * min_multiplier_to_succeed)
                ELSE NULL
            END AS estimated_min_gas_limit,
            CASE
                WHEN schedule_success
                     AND coalesce(would_fit_in_original_limit, TRUE) = FALSE
                THEN schedule_gas_used - tx_gas_limit
                ELSE NULL
            END AS extra_gas_needed,
            state_gas_category, reservoir_exhausted
        FROM divergences
    """)

    # Per-(schedule, recipient) EIP-8037 impact totals. Endpoints filter
    # by `schedule_name` to pick one schedule's view.
    conn.execute("""
        CREATE OR REPLACE VIEW eip8037_contract_impact AS
        SELECT
            schedule_name,
            lower(recipient) AS target_address,
            count(*)                                          AS divergent_txs,
            sum(CASE WHEN status_changed THEN 1 ELSE 0 END)   AS status_changed_txs,
            sum(CASE WHEN NOT would_fit_in_original_limit
                          AND schedule_state_gas_spent > 0 THEN 1 ELSE 0 END)
                                                              AS original_limit_failures,
            sum(CASE WHEN schedule_success
                          AND NOT would_fit_in_original_limit THEN 1 ELSE 0 END)
                                                              AS fixable_with_more_outer_gas,
            sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END) AS reservoir_exhausted_txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
            avg(gas_delta) AS avg_gas_delta,
            sum(gas_delta) AS total_gas_delta,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p95_min_multiplier_to_succeed,
            max(min_multiplier_to_succeed) AS max_min_multiplier_to_succeed,
            sum(CASE WHEN NOT schedule_success
                      AND min_multiplier_to_succeed IS NULL THEN 1 ELSE 0 END)
                AS unresolved_replay_failures,
            max(CASE
                WHEN schedule_success
                     AND coalesce(would_fit_in_original_limit, TRUE) = FALSE
                THEN schedule_gas_used - tx_gas_limit
                ELSE NULL
            END) AS max_extra_gas_needed,
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM divergences
        WHERE recipient IS NOT NULL
        GROUP BY schedule_name, lower(recipient)
    """)


def resolve_producer_db_path() -> Path:
    """Where the producer SQLite file lives on disk. Honors
    PRODUCER_DB_PATH first, then falls back to a synthetic fixture in
    the repo root."""
    explicit = os.environ.get("PRODUCER_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    # Fallback to the synthetic fixture path so a fresh checkout can be
    # demoed without producer data.
    from .config import default_paths
    return (default_paths().repo_root / "synthetic.sqlite").resolve()
