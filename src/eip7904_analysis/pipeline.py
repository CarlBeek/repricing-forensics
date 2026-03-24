from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .config import Paths, ensure_workspace_dirs
from .duckdb_utils import connect
from .parsers import (
    parse_call_frames,
    parse_operation_counts,
    parse_rust_debug_divergence_location,
    parse_rust_debug_oog_info,
)
from .sql import DERIVED_INCIDENTS_SQL, create_views_sql


def initialize_duckdb(paths: Paths, schedule_name: str) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)
        conn.execute(DERIVED_INCIDENTS_SQL)
    finally:
        conn.close()


def build_normalized_forensics(
    paths: Paths,
    schedule_name: str,
    include_call_frames: bool = False,
) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)

        df = conn.execute(
            """
            SELECT
                divergence_id,
                block_number,
                tx_index,
                tx_hash,
                operation_counts,
                oog_info,
                divergence_location,
                baseline_call_frames,
                schedule_call_frames
            FROM artifacts_7904
            """
        ).df()

        parsed_rows: list[dict[str, object]] = []
        call_frame_rows: list[dict[str, object]] = []

        for row in df.to_dict(orient="records"):
            location = parse_rust_debug_divergence_location(row["divergence_location"])
            oog = parse_rust_debug_oog_info(row["oog_info"])
            operations = parse_operation_counts(row["operation_counts"])
            parsed_rows.append(
                {
                    "divergence_id": row["divergence_id"],
                    "block_number": row["block_number"],
                    "tx_index": row["tx_index"],
                    "tx_hash": row["tx_hash"],
                    "divergence_contract": location["contract"] if location else None,
                    "divergence_pc": location["pc"] if location else None,
                    "divergence_call_depth": location["call_depth"] if location else None,
                    "divergence_opcode": location["opcode"] if location else None,
                    "divergence_opcode_name": location["opcode_name"] if location else None,
                    "selector_stack": json.dumps(location["function_selectors"]) if location else None,
                    "oog_contract": oog["contract"] if oog else None,
                    "oog_pc": oog["pc"] if oog else None,
                    "oog_call_depth": oog["call_depth"] if oog else None,
                    "oog_opcode_name": oog["opcode_name"] if oog else None,
                    "oog_pattern": oog["pattern"] if oog else None,
                    "oog_gas_remaining": oog["gas_remaining"] if oog else None,
                    "operation_counts_json": json.dumps(operations) if operations else None,
                    "sload_count": (operations or {}).get("sload_count"),
                    "sstore_count": (operations or {}).get("sstore_count"),
                    "call_count": (operations or {}).get("call_count"),
                    "log_count": (operations or {}).get("log_count"),
                    "total_ops": (operations or {}).get("total_ops"),
                    "memory_words_allocated": (operations or {}).get("memory_words_allocated"),
                    "create_count": (operations or {}).get("create_count"),
                }
            )

            if include_call_frames:
                for frame_kind, frames_json in [
                    ("baseline", row["baseline_call_frames"]),
                    ("schedule", row["schedule_call_frames"]),
                ]:
                    frames = parse_call_frames(frames_json)
                    if not frames:
                        continue
                    for frame in frames:
                        call_frame_rows.append(
                            {
                                "divergence_id": row["divergence_id"],
                                "block_number": row["block_number"],
                                "tx_index": row["tx_index"],
                                "tx_hash": row["tx_hash"],
                                "trace_kind": frame_kind,
                                "call_index": frame.get("call_index"),
                                "depth": frame.get("depth"),
                                "from_address": frame.get("from"),
                                "to_address": frame.get("to"),
                                "call_type": frame.get("call_type"),
                                "gas_provided": frame.get("gas_provided"),
                                "gas_used": frame.get("gas_used"),
                                "success": frame.get("success"),
                            }
                        )

        conn.register("parsed_forensics_df", pd.DataFrame(parsed_rows))
        conn.execute("CREATE OR REPLACE TABLE normalized_forensics AS SELECT * FROM parsed_forensics_df")

        if include_call_frames:
            conn.register("parsed_call_frames_df", pd.DataFrame(call_frame_rows))
            conn.execute(
                "CREATE OR REPLACE TABLE normalized_call_frames AS SELECT * FROM parsed_call_frames_df"
            )
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


def build_status_change_call_frame_table(paths: Paths, schedule_name: str) -> None:
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    try:
        for statement in create_views_sql(schedule_name, paths.research_lake):
            conn.execute(statement)

        df = conn.execute(
            """
            SELECT
                h.divergence_id,
                h.block_number,
                h.tx_index,
                h.tx_hash,
                a.schedule_call_frames
            FROM hot_7904 h
            JOIN artifacts_7904 a USING (divergence_id)
            WHERE h.status_changed
              AND a.schedule_call_frames IS NOT NULL
            """
        ).df()

        frame_rows: list[dict[str, object]] = []
        summary_rows: list[dict[str, object]] = []

        for row in df.to_dict(orient="records"):
            frames = parse_call_frames(row["schedule_call_frames"])
            if not frames:
                continue

            for frame in frames:
                frame_rows.append(
                    {
                        "divergence_id": row["divergence_id"],
                        "block_number": row["block_number"],
                        "tx_index": row["tx_index"],
                        "tx_hash": row["tx_hash"],
                        "call_index": frame.get("call_index"),
                        "depth": frame.get("depth"),
                        "from_address": frame.get("from"),
                        "to_address": frame.get("to"),
                        "call_type": frame.get("call_type"),
                        "gas_provided": frame.get("gas_provided"),
                        "gas_used": frame.get("gas_used"),
                        "success": frame.get("success"),
                    }
                )

            failing = [frame for frame in frames if frame.get("success") is False]
            if failing:
                failing.sort(key=lambda frame: (frame.get("depth") or -1, frame.get("call_index") or -1))
                frame = failing[-1]
                summary_rows.append(
                    {
                        "divergence_id": row["divergence_id"],
                        "block_number": row["block_number"],
                        "tx_index": row["tx_index"],
                        "tx_hash": row["tx_hash"],
                        "failure_depth": frame.get("depth"),
                        "caller": frame.get("from"),
                        "callee": frame.get("to"),
                        "call_type": frame.get("call_type"),
                        "gas_provided": frame.get("gas_provided"),
                        "gas_used": frame.get("gas_used"),
                    }
                )

        conn.register("status_call_frames_df", pd.DataFrame(frame_rows))
        conn.execute(
            "CREATE OR REPLACE TABLE status_change_call_frames AS SELECT * FROM status_call_frames_df"
        )
        conn.register("status_failure_summary_df", pd.DataFrame(summary_rows))
        conn.execute(
            "CREATE OR REPLACE TABLE status_change_failure_summary AS SELECT * FROM status_failure_summary_df"
        )
    finally:
        conn.close()
