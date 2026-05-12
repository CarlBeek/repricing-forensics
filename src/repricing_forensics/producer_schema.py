"""Producer-side DuckDB schema for the consolidated repricing-forensics lake.

This module is the *contract* between reth (`crates/research/`) and this
consumer. Both repos must agree on these table shapes; the producer writes
them, the consumer reads them.

The schema is also created here so we can drive a synthetic test fixture
(`scripts/build_synthetic_producer_db.py`) without needing reth to land
the new producer first. Once reth ships its DuckDB writer, the source of
truth for DDL effectively moves to the producer crate; this module
becomes a duplicate kept for the synthetic fixture and parity tests.

See `docs/storage-redesign.md` (this repo) and
`crates/research/docs/storage-redesign.md` (reth) for the rationale.
"""
from __future__ import annotations

from enum import Enum
from typing import Iterable


# Bumped on any schema change. The producer writes this into
# analysis_runs.schema_version; the consumer warns if the latest run was
# written with a different version.
SCHEMA_VERSION = 1


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
    WALLET_FIXABLE_SHALLOW = "wallet_fixable_shallow"
    WALLET_FIXABLE_DEEP_CHAIN = "wallet_fixable_deep_chain"
    CONTRACT_BROKEN = "contract_broken"


DRILL_IN_BUCKETS: tuple[str, ...] = (
    Bucket.EVENT_LOGS_CHANGED.value,
    Bucket.CONTRACT_BROKEN.value,
)
AGGREGATE_ONLY_BUCKETS: tuple[str, ...] = (
    Bucket.TRACE_ONLY.value,
    Bucket.GAS_ONLY.value,
    Bucket.WALLET_FIXABLE_SHALLOW.value,
    Bucket.WALLET_FIXABLE_DEEP_CHAIN.value,
)


# ── DDL ───────────────────────────────────────────────────────────────

_SEQUENCES: tuple[str, ...] = (
    "CREATE SEQUENCE IF NOT EXISTS seq_divergence_id START 1",
    "CREATE SEQUENCE IF NOT EXISTS seq_analysis_run_id START 1",
)


# Each entry is (table_name, ddl). Order matters: foreign-key references
# require parents first.
_TABLES: tuple[tuple[str, str], ...] = (
    ("analysis_runs", """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            run_id               UBIGINT  PRIMARY KEY DEFAULT nextval('seq_analysis_run_id'),
            schema_version       INTEGER  NOT NULL,
            schedule_name        VARCHAR  NOT NULL,
            schedule_config_hash VARCHAR  NOT NULL,
            reth_commit          VARCHAR,
            run_started_at       UBIGINT  NOT NULL,
            run_finished_at      UBIGINT,
            blocks_processed     UBIGINT,
            notes                VARCHAR
        )
    """),

    ("block_coverage", """
        CREATE TABLE IF NOT EXISTS block_coverage (
            schedule_name        VARCHAR  NOT NULL,
            schedule_config_hash VARCHAR  NOT NULL,
            block_number         UBIGINT  NOT NULL,
            block_hash           BLOB     NOT NULL,
            parent_hash          BLOB     NOT NULL,
            timestamp            UBIGINT  NOT NULL,
            tx_count             UINTEGER NOT NULL,
            tx_count_unchanged                 UINTEGER NOT NULL,
            tx_count_trace_only                UINTEGER NOT NULL,
            tx_count_gas_only                  UINTEGER NOT NULL,
            tx_count_event_logs_changed        UINTEGER NOT NULL,
            tx_count_wallet_fixable_shallow    UINTEGER NOT NULL,
            tx_count_wallet_fixable_deep_chain UINTEGER NOT NULL,
            tx_count_contract_broken           UINTEGER NOT NULL,
            PRIMARY KEY (schedule_name, block_number, block_hash)
        )
    """),

    ("block_summaries", """
        CREATE TABLE IF NOT EXISTS block_summaries (
            schedule_name VARCHAR  NOT NULL,
            block_number  UBIGINT  NOT NULL,
            bucket        VARCHAR  NOT NULL,
            tx_count      UINTEGER NOT NULL,

            -- gas-delta moments + log2 histogram (bins: <=1, 2..2^11+)
            gas_delta_sum       BIGINT,
            gas_delta_sum_sq    HUGEINT,
            gas_delta_min       BIGINT,
            gas_delta_max       BIGINT,
            gas_delta_log2_hist INTEGER[12],

            -- EIP-7904: per-bucket per-opcode totals (sparse; nonzero only)
            opcode_count_totals_7904     STRUCT(opcode UTINYINT, count UBIGINT)[],
            opcode_gas_delta_totals_7904 STRUCT(opcode UTINYINT, delta BIGINT)[],

            -- EIP-8037: state-gas totals + multiplier histogram + per-category counts
            state_gas_sum           UBIGINT,
            state_gas_spillover_sum UBIGINT,
            multiplier_log2_hist    INTEGER[12],
            tx_count_creation       UINTEGER,
            tx_count_authorization  UINTEGER,
            tx_count_runtime_state  UINTEGER,
            tx_count_no_state       UINTEGER,

            PRIMARY KEY (schedule_name, block_number, bucket)
        )
    """),

    ("divergences", """
        CREATE TABLE IF NOT EXISTS divergences (
            divergence_id        UBIGINT  PRIMARY KEY DEFAULT nextval('seq_divergence_id'),
            schedule_name        VARCHAR  NOT NULL,
            schedule_config_hash VARCHAR  NOT NULL,
            block_number         UBIGINT  NOT NULL,
            tx_index             UINTEGER NOT NULL,
            tx_hash              BLOB     NOT NULL,
            timestamp            UBIGINT  NOT NULL,
            bucket               VARCHAR  NOT NULL,

            sender       VARCHAR  NOT NULL,
            recipient    VARCHAR,
            is_create    BOOLEAN  NOT NULL,
            tx_gas_limit UBIGINT  NOT NULL,

            baseline_success     BOOLEAN  NOT NULL,
            schedule_success     BOOLEAN  NOT NULL,
            status_changed       BOOLEAN  NOT NULL,
            event_logs_changed   BOOLEAN  NOT NULL,
            output_changed       BOOLEAN  NOT NULL,
            logs_bloom_changed   BOOLEAN  NOT NULL,

            baseline_gas_used        UBIGINT NOT NULL,
            schedule_gas_used        UBIGINT NOT NULL,
            gas_delta                BIGINT  NOT NULL,
            baseline_total_gas_spent UBIGINT,
            baseline_gas_refunded    UBIGINT,
            schedule_total_gas_spent UBIGINT,
            schedule_gas_refunded    UBIGINT,
            schedule_intrinsic_gas   UBIGINT,
            schedule_floor_gas       UBIGINT,
            would_fit_in_original_limit BOOLEAN,
            min_multiplier_to_succeed   DOUBLE,

            -- 7904 OOG location + chain-walk classification
            divergence_contract    VARCHAR,
            divergence_pc          UINTEGER,
            divergence_call_depth  INTEGER,
            divergence_opcode      UTINYINT,
            oog_contract           VARCHAR,
            oog_pc                 UINTEGER,
            oog_call_depth         INTEGER,
            oog_opcode             UTINYINT,
            oog_pattern            VARCHAR,
            oog_gas_remaining      UBIGINT,
            oog_chain_proportional BOOLEAN,
            oog_bottleneck_depth   INTEGER,
            oog_bottleneck_kind    VARCHAR,

            -- 8037 state gas
            schedule_state_gas_spent     UBIGINT,
            schedule_initial_state_gas   UBIGINT,
            schedule_initial_reservoir   UBIGINT,
            runtime_state_gas            UBIGINT,
            runtime_state_gas_spillover  UBIGINT,
            state_gas_category           VARCHAR,
            reservoir_exhausted          BOOLEAN,

            UNIQUE (schedule_name, block_number, tx_index, schedule_config_hash)
        )
    """),

    ("divergence_call_frames", """
        CREATE TABLE IF NOT EXISTS divergence_call_frames (
            divergence_id          UBIGINT  NOT NULL,
            call_index             UINTEGER NOT NULL,
            parent_call_index      UINTEGER,
            depth                  UINTEGER NOT NULL,
            from_address           VARCHAR  NOT NULL,
            to_address             VARCHAR  NOT NULL,
            code_address           VARCHAR,
            codehash               BLOB,
            call_type              VARCHAR  NOT NULL,
            selector               BLOB,
            value_wei              VARCHAR,
            gas_provided           UBIGINT  NOT NULL,
            gas_used               UBIGINT  NOT NULL,
            gas_margin             BIGINT,
            success                BOOLEAN  NOT NULL,
            parent_gas_at_call     UBIGINT,
            gas_requested_on_stack UBIGINT,
            eip150_cap_binding     BOOLEAN,
            state_gas_running      UBIGINT,
            PRIMARY KEY (divergence_id, call_index)
        )
    """),

    ("divergence_opcode_counts", """
        CREATE TABLE IF NOT EXISTS divergence_opcode_counts (
            divergence_id UBIGINT  NOT NULL,
            call_index    UINTEGER NOT NULL,
            opcode        UTINYINT NOT NULL,
            count         UBIGINT  NOT NULL,
            gas_baseline  UBIGINT  NOT NULL,
            gas_schedule  UBIGINT  NOT NULL,
            PRIMARY KEY (divergence_id, call_index, opcode)
        )
    """),

    ("divergence_event_logs", """
        CREATE TABLE IF NOT EXISTS divergence_event_logs (
            divergence_id UBIGINT  NOT NULL,
            trace_kind    VARCHAR  NOT NULL,
            log_index     UINTEGER NOT NULL,
            address       VARCHAR  NOT NULL,
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
            representative_address VARCHAR,
            solc_version           VARCHAR,
            solc_commit            VARCHAR,
            evm_target             VARCHAR,
            cbor_present           BOOLEAN NOT NULL,
            has_metadata_hash      BOOLEAN NOT NULL,
            bytecode_len           UINTEGER NOT NULL,
            extracted_at           UBIGINT NOT NULL
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


TABLE_NAMES: tuple[str, ...] = tuple(name for name, _ in _TABLES)


def initialize_schema(conn) -> None:
    """Create every sequence, table, and index. Idempotent."""
    for stmt in _SEQUENCES:
        conn.execute(stmt)
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
    row = conn.execute(
        """
        INSERT INTO analysis_runs (
            schema_version, schedule_name, schedule_config_hash,
            reth_commit, run_started_at, run_finished_at, blocks_processed, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING run_id
        """,
        [
            SCHEMA_VERSION, schedule_name, schedule_config_hash,
            reth_commit, run_started_at, run_finished_at, blocks_processed, notes,
        ],
    ).fetchone()
    return int(row[0])


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
            [schedule_name],
        ).fetchone()
    return int(row[0]) if row else None


def all_table_names() -> Iterable[str]:
    return TABLE_NAMES
