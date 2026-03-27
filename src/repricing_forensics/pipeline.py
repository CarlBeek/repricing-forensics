from __future__ import annotations

from pathlib import Path

from .config import Paths, ensure_workspace_dirs
from .duckdb_utils import connect
from .sql import DERIVED_INCIDENTS_SQL, create_views_sql


def initialize_duckdb(paths: Paths, schedule_name: str, db_path: Path | None = None) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(db_path or paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)
        conn.execute(DERIVED_INCIDENTS_SQL)
    finally:
        conn.close()


# ── Pure-SQL normalized forensics ────────────────────────────────────

NORMALIZED_FORENSICS_SQL = """
CREATE OR REPLACE TABLE normalized_forensics AS
SELECT
    divergence_id,
    block_number,
    tx_index,
    tx_hash,

    -- divergence location fields (regex on Rust debug string)
    lower(regexp_extract(divergence_location, 'contract:\\s*(0x[a-fA-F0-9]+)', 1))
        AS divergence_contract,
    TRY_CAST(regexp_extract(divergence_location, 'pc:\\s*(\\d+)', 1) AS INTEGER)
        AS divergence_pc,
    TRY_CAST(regexp_extract(divergence_location, 'call_depth:\\s*(\\d+)', 1) AS INTEGER)
        AS divergence_call_depth,
    TRY_CAST(regexp_extract(divergence_location, 'opcode:\\s*(\\d+)', 1) AS INTEGER)
        AS divergence_opcode,
    regexp_extract(divergence_location, 'opcode_name:\\s*"([^"]+)"', 1)
        AS divergence_opcode_name,

    -- oog fields
    lower(regexp_extract(oog_info, 'contract:\\s*(0x[a-fA-F0-9]+)', 1))
        AS oog_contract,
    TRY_CAST(regexp_extract(oog_info, 'pc:\\s*(\\d+)', 1) AS INTEGER)
        AS oog_pc,
    TRY_CAST(regexp_extract(oog_info, 'call_depth:\\s*(\\d+)', 1) AS INTEGER)
        AS oog_call_depth,
    regexp_extract(oog_info, 'opcode_name:\\s*"([^"]+)"', 1)
        AS oog_opcode_name,
    lower(regexp_extract(oog_info, 'pattern:\\s*([A-Za-z]+)', 1))
        AS oog_pattern,
    TRY_CAST(regexp_extract(oog_info, 'gas_remaining:\\s*(\\d+)', 1) AS BIGINT)
        AS oog_gas_remaining,

    -- operation counts (JSON fields)
    operation_counts AS operation_counts_json,
    TRY_CAST(operation_counts->>'sload_count' AS INTEGER) AS sload_count,
    TRY_CAST(operation_counts->>'sstore_count' AS INTEGER) AS sstore_count,
    TRY_CAST(operation_counts->>'call_count' AS INTEGER) AS call_count,
    TRY_CAST(operation_counts->>'log_count' AS INTEGER) AS log_count,
    TRY_CAST(operation_counts->>'total_ops' AS INTEGER) AS total_ops,
    TRY_CAST(operation_counts->>'memory_words_allocated' AS INTEGER) AS memory_words_allocated,
    TRY_CAST(operation_counts->>'create_count' AS INTEGER) AS create_count
FROM artifacts_7904
"""

NORMALIZED_CALL_FRAMES_SQL = """
CREATE OR REPLACE TABLE normalized_call_frames AS
WITH frames_union AS (
    SELECT
        divergence_id, block_number, tx_index, tx_hash,
        'baseline' AS trace_kind,
        baseline_call_frames AS frames_json
    FROM artifacts_7904
    WHERE baseline_call_frames IS NOT NULL
    UNION ALL
    SELECT
        divergence_id, block_number, tx_index, tx_hash,
        'schedule' AS trace_kind,
        schedule_call_frames AS frames_json
    FROM artifacts_7904
    WHERE schedule_call_frames IS NOT NULL
),
unnested AS (
    SELECT
        divergence_id, block_number, tx_index, tx_hash, trace_kind,
        unnest(from_json(frames_json, '["json"]')) AS frame
    FROM frames_union
)
SELECT
    divergence_id,
    block_number,
    tx_index,
    tx_hash,
    trace_kind,
    TRY_CAST(frame->>'call_index' AS INTEGER) AS call_index,
    TRY_CAST(frame->>'depth' AS INTEGER) AS depth,
    frame->>'from' AS from_address,
    frame->>'to' AS to_address,
    frame->>'call_type' AS call_type,
    TRY_CAST(frame->>'gas_provided' AS BIGINT) AS gas_provided,
    TRY_CAST(frame->>'gas_used' AS BIGINT) AS gas_used,
    TRY_CAST(frame->>'success' AS BOOLEAN) AS success
FROM unnested
"""


def build_normalized_forensics(
    paths: Paths,
    schedule_name: str,
    include_call_frames: bool = False,
    db_path: Path | None = None,
) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(db_path or paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)

        conn.execute(NORMALIZED_FORENSICS_SQL)

        if include_call_frames:
            conn.execute(NORMALIZED_CALL_FRAMES_SQL)
    finally:
        conn.close()


# ── Status-change call frame analysis ────────────────────────────────

STATUS_CALL_FRAMES_SQL = """
CREATE OR REPLACE TABLE status_change_call_frames AS
WITH raw AS (
    SELECT
        h.divergence_id, h.block_number, h.tx_index, h.tx_hash,
        unnest(from_json(a.schedule_call_frames, '["json"]')) AS frame
    FROM hot_7904 h
    JOIN artifacts_7904 a USING (divergence_id)
    WHERE h.status_changed
      AND a.schedule_call_frames IS NOT NULL
)
SELECT
    divergence_id, block_number, tx_index, tx_hash,
    TRY_CAST(frame->>'call_index' AS INTEGER) AS call_index,
    TRY_CAST(frame->>'depth' AS INTEGER) AS depth,
    frame->>'from' AS from_address,
    frame->>'to' AS to_address,
    frame->>'call_type' AS call_type,
    TRY_CAST(frame->>'gas_provided' AS BIGINT) AS gas_provided,
    TRY_CAST(frame->>'gas_used' AS BIGINT) AS gas_used,
    TRY_CAST(frame->>'success' AS BOOLEAN) AS success
FROM raw
"""

STATUS_FAILURE_SUMMARY_SQL = """
CREATE OR REPLACE TABLE status_change_failure_summary AS
WITH raw AS (
    SELECT
        h.divergence_id, h.block_number, h.tx_index, h.tx_hash,
        unnest(from_json(a.schedule_call_frames, '["json"]')) AS frame
    FROM hot_7904 h
    JOIN artifacts_7904 a USING (divergence_id)
    WHERE h.status_changed
      AND a.schedule_call_frames IS NOT NULL
),
failing AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY divergence_id
            ORDER BY TRY_CAST(frame->>'depth' AS INTEGER) DESC,
                     TRY_CAST(frame->>'call_index' AS INTEGER) DESC
        ) AS rn
    FROM raw
    WHERE TRY_CAST(frame->>'success' AS BOOLEAN) = false
)
SELECT
    divergence_id, block_number, tx_index, tx_hash,
    TRY_CAST(frame->>'depth' AS INTEGER) AS failure_depth,
    frame->>'from' AS caller,
    frame->>'to' AS callee,
    frame->>'call_type' AS call_type,
    TRY_CAST(frame->>'gas_provided' AS BIGINT) AS gas_provided,
    TRY_CAST(frame->>'gas_used' AS BIGINT) AS gas_used
FROM failing
WHERE rn = 1
"""


def build_status_change_call_frame_table(paths: Paths, schedule_name: str) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)
        conn.execute(STATUS_CALL_FRAMES_SQL)
        conn.execute(STATUS_FAILURE_SUMMARY_SQL)
    finally:
        conn.close()


def write_query_output(paths: Paths, sql: str, out_path: Path) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df = conn.execute(sql).df()
        df.to_csv(out_path, index=False)
    finally:
        conn.close()
