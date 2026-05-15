"""Producer-side SQLite schema for the repricing-forensics pipeline.

The producer (reth-research) writes SQLite; the consumer attaches the
file read-only via DuckDB's `sqlite_scanner` extension and runs
analytical queries through DuckDB's vectorized engine. This sidesteps
DuckDB's single-process writer-lock constraint while keeping our
analytical query path fast (DuckDB engine over SQLite storage).

See `docs/storage-redesign.md` for the rationale.

SQLite has no native array / struct types, so where the original
DuckDB schema used `INTEGER[12]` or `STRUCT(opcode, count)[]` we
serialize JSON into a TEXT column. DuckDB can json_each / unnest these
on the read side. SQLite is dynamically typed; the affinity hints in
the DDL below are documentation as much as enforcement.

WAL mode is the bedrock of the concurrency model: one writer + many
readers without blocking. The producer sets WAL on first open; the
consumer doesn't need to (read-only ATTACH respects the mode).
"""
from __future__ import annotations

from enum import Enum
from typing import Iterable


# Bumped on any schema change. The producer writes this into
# analysis_runs.schema_version; the consumer warns if the latest run was
# written with a different version.
SCHEMA_VERSION = 6  # v6: added inconclusive_needs_higher_sweep bucket


class Bucket(str, Enum):
    """How a replayed tx classified, deciding what storage it gets.

    Aggregate-only buckets are folded into block_summaries; drill-in
    buckets get full per-tx rows in `divergences` and the per-frame
    tables.
    """
    UNCHANGED = "unchanged"
    TRACE_ONLY = "trace_only"
    GAS_ONLY = "gas_only"
    EVENT_LOGS_CHANGED = "event_logs_changed"
    SCHEDULE_RESCUED = "schedule_rescued"  # baseline failed, schedule succeeded
    WALLET_FIXABLE_SHALLOW = "wallet_fixable_shallow"
    WALLET_FIXABLE_DEEP_CHAIN = "wallet_fixable_deep_chain"
    # status flipped to failure, highest tier-sweep multiplier still halted
    # OOG, no fixed/fractional/stipend bottleneck proven — needs rerunning
    # with a higher --research.gas-limit-multipliers ceiling before deciding.
    INCONCLUSIVE_NEEDS_HIGHER_SWEEP = "inconclusive_needs_higher_sweep"
    CONTRACT_BROKEN = "contract_broken"


DRILL_IN_BUCKETS: tuple[str, ...] = (
    Bucket.EVENT_LOGS_CHANGED.value,
    Bucket.INCONCLUSIVE_NEEDS_HIGHER_SWEEP.value,
    Bucket.CONTRACT_BROKEN.value,
)
AGGREGATE_ONLY_BUCKETS: tuple[str, ...] = (
    Bucket.TRACE_ONLY.value,
    Bucket.GAS_ONLY.value,
    Bucket.SCHEDULE_RESCUED.value,
    Bucket.WALLET_FIXABLE_SHALLOW.value,
    Bucket.WALLET_FIXABLE_DEEP_CHAIN.value,
)


# ── DDL ───────────────────────────────────────────────────────────────

# Each entry is (table_name, ddl). Order matters when foreign keys are
# enabled; we don't enforce FKs but keep the parent-first order for
# clarity.
_TABLES: tuple[tuple[str, str], ...] = (
    ("analysis_runs", """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            run_id               INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_version       INTEGER NOT NULL,
            schedule_name        TEXT    NOT NULL,
            schedule_config_hash TEXT    NOT NULL,
            reth_commit          TEXT,
            run_started_at       INTEGER NOT NULL,
            run_finished_at      INTEGER,
            blocks_processed     INTEGER,
            notes                TEXT
        )
    """),

    ("block_coverage", """
        CREATE TABLE IF NOT EXISTS block_coverage (
            schedule_name        TEXT    NOT NULL,
            schedule_config_hash TEXT    NOT NULL,
            block_number         INTEGER NOT NULL,
            block_hash           BLOB    NOT NULL,
            parent_hash          BLOB    NOT NULL,
            timestamp            INTEGER NOT NULL,
            tx_count             INTEGER NOT NULL,
            tx_count_unchanged                 INTEGER NOT NULL,
            tx_count_trace_only                INTEGER NOT NULL,
            tx_count_gas_only                  INTEGER NOT NULL,
            tx_count_event_logs_changed        INTEGER NOT NULL,
            tx_count_schedule_rescued          INTEGER NOT NULL,
            tx_count_wallet_fixable_shallow    INTEGER NOT NULL,
            tx_count_wallet_fixable_deep_chain INTEGER NOT NULL,
            tx_count_inconclusive_needs_higher_sweep INTEGER NOT NULL,
            tx_count_contract_broken           INTEGER NOT NULL,
            PRIMARY KEY (schedule_name, block_number, block_hash)
        )
    """),

    ("block_summaries", """
        CREATE TABLE IF NOT EXISTS block_summaries (
            schedule_name TEXT    NOT NULL,
            block_number  INTEGER NOT NULL,
            bucket        TEXT    NOT NULL,
            tx_count      INTEGER NOT NULL,

            -- gas-delta moments + log2 histogram (12 bins, JSON array)
            gas_delta_sum       INTEGER,
            gas_delta_sum_sq    REAL,     -- REAL (loses precision past 2^53) to avoid HUGEINT
            gas_delta_min       INTEGER,
            gas_delta_max       INTEGER,
            gas_delta_log2_hist TEXT,     -- JSON: array of 12 ints

            -- EIP-7904: per-bucket per-opcode totals (sparse JSON list).
            -- One entry per opcode that executed in this (block, bucket):
            -- {opcode: u8, count: u64, gas_baseline: u64, gas_schedule: u64}.
            -- Summed across every frame of every tx in the bucket.
            opcode_totals_7904 TEXT,

            -- EIP-8037: state-gas totals + multiplier histogram + per-category counts
            state_gas_sum           INTEGER,
            state_gas_spillover_sum INTEGER,
            multiplier_log2_hist    TEXT,     -- JSON: array of 12 ints
            tx_count_creation       INTEGER,
            tx_count_authorization  INTEGER,
            tx_count_runtime_state  INTEGER,
            tx_count_no_state       INTEGER,

            PRIMARY KEY (schedule_name, block_number, bucket)
        )
    """),

    ("divergences", """
        CREATE TABLE IF NOT EXISTS divergences (
            divergence_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_name        TEXT    NOT NULL,
            schedule_config_hash TEXT    NOT NULL,
            block_number         INTEGER NOT NULL,
            tx_index             INTEGER NOT NULL,
            tx_hash              BLOB    NOT NULL,
            timestamp            INTEGER NOT NULL,
            bucket               TEXT    NOT NULL,

            sender       TEXT    NOT NULL,
            recipient    TEXT,
            is_create    INTEGER NOT NULL,  -- 0/1
            tx_gas_limit INTEGER NOT NULL,

            baseline_success     INTEGER NOT NULL,
            schedule_success     INTEGER NOT NULL,
            status_changed       INTEGER NOT NULL,
            event_logs_changed   INTEGER NOT NULL,
            output_changed       INTEGER NOT NULL,
            logs_bloom_changed   INTEGER NOT NULL,

            baseline_gas_used        INTEGER NOT NULL,
            schedule_gas_used        INTEGER NOT NULL,
            gas_delta                INTEGER NOT NULL,
            baseline_total_gas_spent INTEGER,
            baseline_gas_refunded    INTEGER,
            schedule_total_gas_spent INTEGER,
            schedule_gas_refunded    INTEGER,
            schedule_intrinsic_gas   INTEGER,
            schedule_floor_gas       INTEGER,
            would_fit_in_original_limit INTEGER,  -- 0/1/NULL
            min_multiplier_to_succeed   REAL,

            -- 7904 OOG location + chain-walk classification
            divergence_contract    TEXT,
            divergence_pc          INTEGER,
            divergence_call_depth  INTEGER,
            divergence_opcode      INTEGER,
            oog_contract           TEXT,
            oog_pc                 INTEGER,
            oog_call_depth         INTEGER,
            oog_opcode             INTEGER,
            oog_pattern            TEXT,
            oog_gas_remaining      INTEGER,
            oog_chain_proportional INTEGER,  -- 0/1/NULL
            oog_bottleneck_depth   INTEGER,
            oog_bottleneck_kind    TEXT,

            -- 8037 state gas
            schedule_state_gas_spent     INTEGER,
            schedule_initial_state_gas   INTEGER,
            schedule_initial_reservoir   INTEGER,
            runtime_state_gas            INTEGER,
            runtime_state_gas_spillover  INTEGER,
            state_gas_category           TEXT,
            reservoir_exhausted          INTEGER,  -- 0/1/NULL

            UNIQUE (schedule_name, block_number, tx_index, schedule_config_hash)
        )
    """),

    ("divergence_call_frames", """
        CREATE TABLE IF NOT EXISTS divergence_call_frames (
            divergence_id          INTEGER NOT NULL,
            call_index             INTEGER NOT NULL,
            parent_call_index      INTEGER,
            depth                  INTEGER NOT NULL,
            from_address           TEXT    NOT NULL,
            to_address             TEXT    NOT NULL,
            code_address           TEXT,
            codehash               BLOB,
            call_type              TEXT    NOT NULL,
            selector               BLOB,
            value_wei              TEXT,
            gas_provided           INTEGER NOT NULL,
            gas_used               INTEGER NOT NULL,
            gas_margin             INTEGER,
            success                INTEGER NOT NULL,  -- 0/1
            parent_gas_at_call     INTEGER,
            gas_requested_on_stack INTEGER,
            eip150_cap_binding     INTEGER,  -- 0/1/NULL
            state_gas_running      INTEGER,
            PRIMARY KEY (divergence_id, call_index)
        )
    """),

    ("divergence_opcode_counts", """
        CREATE TABLE IF NOT EXISTS divergence_opcode_counts (
            divergence_id INTEGER NOT NULL,
            call_index    INTEGER NOT NULL,
            opcode        INTEGER NOT NULL,  -- 0..255
            count         INTEGER NOT NULL,
            gas_baseline  INTEGER NOT NULL,
            gas_schedule  INTEGER NOT NULL,
            PRIMARY KEY (divergence_id, call_index, opcode)
        )
    """),

    ("divergence_event_logs", """
        CREATE TABLE IF NOT EXISTS divergence_event_logs (
            divergence_id INTEGER NOT NULL,
            trace_kind    TEXT    NOT NULL,  -- 'baseline' | 'schedule'
            log_index     INTEGER NOT NULL,
            address       TEXT    NOT NULL,
            topic0        BLOB,
            topic1        BLOB,
            topic2        BLOB,
            topic3        BLOB,
            data_bytes    BLOB,
            data_hash     BLOB,
            PRIMARY KEY (divergence_id, trace_kind, log_index)
        )
    """),

    ("contract_metadata", """
        CREATE TABLE IF NOT EXISTS contract_metadata (
            codehash               BLOB PRIMARY KEY,
            representative_address TEXT,
            solc_version           TEXT,
            solc_commit            TEXT,
            evm_target             TEXT,
            cbor_present           INTEGER NOT NULL,  -- 0/1
            has_metadata_hash      INTEGER NOT NULL,  -- 0/1
            bytecode_len           INTEGER NOT NULL,
            extracted_at           INTEGER NOT NULL
        )
    """),
)


_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_div_schedule       ON divergences(schedule_name)",
    "CREATE INDEX IF NOT EXISTS idx_div_block          ON divergences(schedule_name, block_number)",
    "CREATE INDEX IF NOT EXISTS idx_div_recipient      ON divergences(recipient)",
    "CREATE INDEX IF NOT EXISTS idx_div_bucket         ON divergences(bucket)",
    "CREATE INDEX IF NOT EXISTS idx_dcf_to_addr        ON divergence_call_frames(to_address)",
    "CREATE INDEX IF NOT EXISTS idx_dcf_codehash       ON divergence_call_frames(codehash)",
    "CREATE INDEX IF NOT EXISTS idx_doc_opcode         ON divergence_opcode_counts(opcode)",
    "CREATE INDEX IF NOT EXISTS idx_bs_schedule_block  ON block_summaries(schedule_name, block_number)",
)


# PRAGMAs the producer should set at open time. WAL mode is the
# linchpin of the writer+readers concurrency model. synchronous=NORMAL
# is the standard trade-off for WAL — durable on commit (no torn
# writes) but doesn't fsync on every page write.
PRODUCER_PRAGMAS: tuple[str, ...] = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA foreign_keys = OFF",
    "PRAGMA temp_store = MEMORY",
)


TABLE_NAMES: tuple[str, ...] = tuple(name for name, _ in _TABLES)


def initialize_schema(conn) -> None:
    """Create every table and index on a sqlite3 connection. Idempotent."""
    for pragma in PRODUCER_PRAGMAS:
        conn.execute(pragma)
    for _, ddl in _TABLES:
        conn.execute(ddl)
    for stmt in _INDEXES:
        conn.execute(stmt)


def insert_analysis_run(
    conn,
    *,
    schedule_name: str,
    schedule_config_hash: str,
    reth_commit: str | None = None,
    run_started_at: int,
    run_finished_at: int | None = None,
    blocks_processed: int | None = None,
    notes: str | None = None,
) -> int:
    """Record an analysis run. Returns the new run_id."""
    cur = conn.execute(
        """
        INSERT INTO analysis_runs (
            schema_version, schedule_name, schedule_config_hash,
            reth_commit, run_started_at, run_finished_at, blocks_processed, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SCHEMA_VERSION, schedule_name, schedule_config_hash,
            reth_commit, run_started_at, run_finished_at, blocks_processed, notes,
        ),
    )
    return int(cur.lastrowid)


def latest_schema_version(conn, schedule_name: str | None = None) -> int | None:
    """Return the schema_version on the most recent analysis_runs row.

    Returns None when the DB has no runs yet. Consumer code can use this
    to decide whether to warn the operator about a mismatch.
    """
    if schedule_name is None:
        row = conn.execute(
            "SELECT schema_version FROM analysis_runs ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT schema_version FROM analysis_runs "
            "WHERE schedule_name = ? ORDER BY run_id DESC LIMIT 1",
            (schedule_name,),
        ).fetchone()
    return int(row[0]) if row else None


def all_table_names() -> Iterable[str]:
    return TABLE_NAMES
