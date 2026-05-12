"""Open a read-only consumer session against the producer's SQLite file.

Consumer entry point for the SQLite-writer + DuckDB-reader architecture
(see `docs/storage-redesign.md`). The producer (reth-research) writes
SQLite in WAL mode; the consumer attaches that file read-only via
DuckDB's `sqlite_scanner` extension and runs analytical queries through
DuckDB's vectorized engine. SQLite WAL handles writer+readers across
processes natively, so there's no lock conflict.

There's no consumer-side materialization step — the in-memory catalog
this module builds is the entire "consumer DB". Views are recreated
each time `open_session` is called.

Compared to the old `pipeline.py`:
- No parquet lake read-through.
- No `normalized_forensics` materialized table.
- No `wallet_fixable_ids` derivation: the producer already tagged the
  bucket; queries source from the schedule-scoped `divergences` view
  for the drill-in cohort and `block_coverage` / `block_summaries`
  for everything else.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

from .config import default_schedule_name


def open_session(producer_db_path: Path, schedule_name: str | None = None
                 ) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB, attach the producer SQLite file
    read-only via sqlite_scanner, create schedule-scoped views, return
    the connection."""
    schedule_name = schedule_name or default_schedule_name()
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
    create_views(conn, schedule_name)
    return conn


def create_views(conn: duckdb.DuckDBPyConnection, schedule_name: str) -> None:
    """Create the schedule-scoped views the API queries against.

    All views live in the consumer's in-memory catalog (the default `memory`
    database) and filter the producer's tables by `schedule_name`. Reads are
    transparent to DuckDB's optimizer — predicate pushdown into the attached
    file works the same as if these queries hit the producer DB directly.
    """
    escaped = schedule_name.replace("'", "''")

    # Pass-throughs filtered by schedule.
    conn.execute(f"""
        CREATE OR REPLACE VIEW block_coverage AS
        SELECT * FROM producer.block_coverage
        WHERE schedule_name = '{escaped}'
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW block_summaries AS
        SELECT * FROM producer.block_summaries
        WHERE schedule_name = '{escaped}'
    """)
    conn.execute(f"""
        CREATE OR REPLACE VIEW divergences AS
        SELECT * FROM producer.divergences
        WHERE schedule_name = '{escaped}'
    """)

    # Frame / opcode / log tables aren't scheduled directly; they hang off
    # divergences via divergence_id. We materialize the schedule filter
    # through a join in the view so the consumer-side queries don't have
    # to remember to filter.
    conn.execute("""
        CREATE OR REPLACE VIEW call_frames AS
        SELECT f.*
        FROM producer.divergence_call_frames f
        JOIN divergences d USING (divergence_id)
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW opcode_counts AS
        SELECT o.*
        FROM producer.divergence_opcode_counts o
        JOIN divergences d USING (divergence_id)
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW event_logs AS
        SELECT l.*
        FROM producer.divergence_event_logs l
        JOIN divergences d USING (divergence_id)
    """)

    # Contract metadata is schedule-independent.
    conn.execute("""
        CREATE OR REPLACE VIEW contract_metadata AS
        SELECT * FROM producer.contract_metadata
    """)

    # Per-tx EIP-8037 view: every drill-in divergence with the 8037
    # columns pre-derived (original_limit_failure, target_address). The
    # old `eip8037_tx_impact` materialized table is gone — DuckDB
    # recomputes this on each query, which is sub-millisecond.
    conn.execute("""
        CREATE OR REPLACE VIEW eip8037_tx_impact AS
        SELECT
            divergence_id, tx_hash, block_number, tx_index,
            lower(recipient) AS target_address,
            sender, tx_gas_limit, is_create,
            baseline_success, schedule_success, status_changed,
            baseline_gas_used, schedule_gas_used, gas_delta,
            schedule_total_gas_spent, schedule_state_gas_spent,
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

    # Common derived view: contract-level EIP-8037 impact totals. Lives
    # here rather than in a materialized table because DuckDB recomputes
    # this in milliseconds.
    conn.execute("""
        CREATE OR REPLACE VIEW eip8037_contract_impact AS
        SELECT
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
        GROUP BY lower(recipient)
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
