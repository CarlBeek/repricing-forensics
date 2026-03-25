#!/usr/bin/env python3
"""Build call-graph analysis CSVs from DuckDB tables.

Uses normalized_call_frames (built by build_duckdb.py) and pushes all
heavy operations into DuckDB SQL.  Only final aggregated results are
pulled into Python for CSV output.
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from repricing_forensics.config import default_paths
from repricing_forensics.duckdb_utils import connect
from repricing_forensics.labels import infer_project_label


# ── Helpers ──────────────────────────────────────────────────────────

def load_classification(cache_path: Path) -> dict[str, dict[str, str]]:
    if not cache_path.exists():
        return {}
    with cache_path.open() as handle:
        return {
            row["address"].lower(): row
            for row in csv.DictReader(handle)
            if row.get("address")
        }


def label_address(address: str | None, classification: dict[str, dict[str, str]]) -> str:
    if not address:
        return "unknown"
    row = classification.get(address.lower())
    return infer_project_label(
        address.lower(),
        compiled_name=None if row is None else row.get("name"),
        classification=None if row is None else row.get("classification"),
        source_hint=None if row is None else row.get("source_hint"),
    )


def write_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def query_to_csv(conn, sql: str, path: Path) -> pd.DataFrame:
    df = conn.execute(sql).df()
    write_df(df, path)
    return df


# ── SQL-based edge comparison ────────────────────────────────────────

EDGE_COMPARISON_SQL = """
CREATE OR REPLACE TEMP TABLE edge_comparison AS
WITH baseline AS (
    SELECT
        ncf.divergence_id, ncf.block_number, ncf.tx_index, ncf.tx_hash,
        ncf.depth, ncf.from_address AS caller, ncf.to_address AS callee,
        ncf.call_type, ncf.gas_provided, ncf.gas_used, ncf.success,
        ncf.call_index,
        h.recipient, h.gas_delta,
        -- edge_occurrence: nth time this (div_id, depth, caller, callee, call_type) appears
        ROW_NUMBER() OVER (
            PARTITION BY ncf.divergence_id, ncf.depth, ncf.from_address, ncf.to_address, ncf.call_type
            ORDER BY ncf.call_index
        ) - 1 AS edge_occurrence
    FROM normalized_call_frames ncf
    JOIN hot_7904 h USING (divergence_id)
    WHERE ncf.trace_kind = 'baseline'
      AND h.status_changed
),
schedule AS (
    SELECT
        ncf.divergence_id, ncf.block_number, ncf.tx_index, ncf.tx_hash,
        ncf.depth, ncf.from_address AS caller, ncf.to_address AS callee,
        ncf.call_type, ncf.gas_provided, ncf.gas_used, ncf.success,
        ncf.call_index,
        h.recipient, h.gas_delta,
        ROW_NUMBER() OVER (
            PARTITION BY ncf.divergence_id, ncf.depth, ncf.from_address, ncf.to_address, ncf.call_type
            ORDER BY ncf.call_index
        ) - 1 AS edge_occurrence
    FROM normalized_call_frames ncf
    JOIN hot_7904 h USING (divergence_id)
    WHERE ncf.trace_kind = 'schedule'
      AND h.status_changed
)
SELECT
    COALESCE(b.divergence_id, s.divergence_id) AS divergence_id,
    COALESCE(b.block_number, s.block_number) AS block_number,
    COALESCE(b.tx_index, s.tx_index) AS tx_index,
    COALESCE(b.tx_hash, s.tx_hash) AS tx_hash,
    COALESCE(b.depth, s.depth) AS depth,
    COALESCE(b.caller, s.caller) AS caller,
    COALESCE(b.callee, s.callee) AS callee,
    COALESCE(b.call_type, s.call_type) AS call_type,
    COALESCE(b.edge_occurrence, s.edge_occurrence) AS edge_occurrence,
    COALESCE(b.recipient, s.recipient) AS recipient,
    COALESCE(b.gas_delta, s.gas_delta) AS gas_delta,
    b.call_index AS call_index_baseline,
    s.call_index AS call_index_schedule,
    b.gas_provided AS gas_provided_baseline,
    s.gas_provided AS gas_provided_schedule,
    b.gas_used AS gas_used_baseline,
    s.gas_used AS gas_used_schedule,
    b.success AS success_baseline,
    s.success AS success_schedule,
    (b.divergence_id IS NULL) AS only_in_schedule,
    (s.divergence_id IS NULL) AS only_in_baseline,
    (b.success IS NOT NULL AND s.success IS NOT NULL AND b.success != s.success) AS success_flip,
    COALESCE(s.gas_provided, 0) - COALESCE(b.gas_provided, 0) AS gas_provided_delta,
    COALESCE(s.gas_used, 0) - COALESCE(b.gas_used, 0) AS gas_used_delta,
    (
        (b.divergence_id IS NULL)
        OR (s.divergence_id IS NULL)
        OR (b.success IS NOT NULL AND s.success IS NOT NULL AND b.success != s.success)
        OR (COALESCE(s.gas_provided, 0) != COALESCE(b.gas_provided, 0))
        OR (COALESCE(s.gas_used, 0) != COALESCE(b.gas_used, 0))
    ) AS edge_changed,
    COALESCE(s.call_index, b.call_index) AS effective_call_index
FROM baseline b
FULL OUTER JOIN schedule s
    ON b.divergence_id = s.divergence_id
    AND b.depth = s.depth
    AND b.caller IS NOT DISTINCT FROM s.caller
    AND b.callee IS NOT DISTINCT FROM s.callee
    AND b.call_type IS NOT DISTINCT FROM s.call_type
    AND b.edge_occurrence = s.edge_occurrence
"""

FIRST_CHANGED_NONROOT_SQL = """
CREATE OR REPLACE TEMP TABLE first_changed_nonroot AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY divergence_id
            ORDER BY effective_call_index, depth
        ) AS rn
    FROM edge_comparison
    WHERE edge_changed AND depth > 0
)
SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
"""

DEEPEST_FAILING_PATH_SQL = """
CREATE OR REPLACE TEMP TABLE deepest_failing AS
WITH schedule_frames AS (
    SELECT
        ncf.divergence_id, ncf.block_number, ncf.tx_index, ncf.tx_hash,
        ncf.depth, ncf.from_address, ncf.to_address,
        ncf.call_type, ncf.gas_provided, ncf.gas_used, ncf.success,
        ncf.call_index,
        h.recipient, h.gas_delta
    FROM normalized_call_frames ncf
    JOIN hot_7904 h USING (divergence_id)
    WHERE ncf.trace_kind = 'schedule'
      AND h.status_changed
),
-- Find the deepest failing frame per tx
deepest_fail AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY divergence_id
            ORDER BY depth DESC, call_index DESC
        ) AS rn
    FROM schedule_frames
    WHERE success = false
),
failing AS (
    SELECT * EXCLUDE (rn) FROM deepest_fail WHERE rn = 1
),
-- Find the parent frame (caller of the failing frame)
parent AS (
    SELECT
        f.divergence_id,
        sf.to_address AS parent_to,
        sf.depth AS parent_depth,
        ROW_NUMBER() OVER (
            PARTITION BY f.divergence_id
            ORDER BY sf.call_index DESC
        ) AS rn
    FROM failing f
    JOIN schedule_frames sf
        ON sf.divergence_id = f.divergence_id
        AND sf.depth = f.depth - 1
        AND sf.call_index < f.call_index
),
-- Find the root frame (depth 0)
root AS (
    SELECT
        f.divergence_id,
        sf.to_address AS root_callee,
        ROW_NUMBER() OVER (
            PARTITION BY f.divergence_id
            ORDER BY sf.call_index
        ) AS rn
    FROM failing f
    JOIN schedule_frames sf
        ON sf.divergence_id = f.divergence_id
        AND sf.depth = 0
)
SELECT
    f.divergence_id,
    f.block_number,
    f.tx_index,
    f.tx_hash,
    f.recipient,
    f.gas_delta,
    r.root_callee,
    COALESCE(p.parent_to, f.from_address) AS failing_caller,
    f.to_address AS failing_callee,
    f.depth AS failure_depth,
    f.call_type AS failing_call_type,
    f.gas_provided,
    f.gas_used
FROM failing f
LEFT JOIN parent p ON p.divergence_id = f.divergence_id AND p.rn = 1
LEFT JOIN root r ON r.divergence_id = f.divergence_id AND r.rn = 1
"""


def main() -> None:
    paths = default_paths()
    conn = connect(paths.duckdb_path)
    classification = load_classification(paths.cache_dir / "contract_classification.csv")
    tables_dir = paths.artifacts_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    # Build the heavy comparison tables in DuckDB
    print("Building edge comparison...")
    conn.execute(EDGE_COMPARISON_SQL)

    print("Finding first changed non-root edges...")
    conn.execute(FIRST_CHANGED_NONROOT_SQL)

    print("Finding deepest failing paths...")
    conn.execute(DEEPEST_FAILING_PATH_SQL)

    # ── Pull aggregated results and label in Python ──────────────────

    # 1. Changed edge motifs (small aggregation)
    print("Computing changed edge motifs...")
    changed_edge_motifs = conn.execute("""
        SELECT caller, callee, depth,
            count(*) AS changed_edges,
            count(DISTINCT divergence_id) AS distinct_txs,
            sum(CASE WHEN success_flip THEN 1 ELSE 0 END) AS success_flip_edges
        FROM edge_comparison
        WHERE edge_changed AND depth > 0
        GROUP BY caller, callee, depth
        ORDER BY changed_edges DESC
    """).df()
    changed_edge_motifs["caller_project"] = changed_edge_motifs["caller"].map(
        lambda a: label_address(a, classification)
    )
    changed_edge_motifs["callee_project"] = changed_edge_motifs["callee"].map(
        lambda a: label_address(a, classification)
    )
    write_df(changed_edge_motifs, tables_dir / "changed_edge_motifs.csv")

    # 2. Changed non-root sankey
    print("Computing changed non-root sankey edges...")
    changed_sankey = conn.execute("""
        SELECT recipient, caller, callee,
            count(*) AS changed_edges,
            count(DISTINCT divergence_id) AS distinct_txs
        FROM edge_comparison
        WHERE edge_changed AND depth > 0
        GROUP BY recipient, caller, callee
        ORDER BY changed_edges DESC
    """).df()
    for col in ["recipient", "caller", "callee"]:
        changed_sankey[f"{col}_project"] = changed_sankey[col].map(
            lambda a: label_address(a, classification)
        )
    write_df(changed_sankey, tables_dir / "changed_nonroot_sankey_edges.csv")

    # 3. Non-root intermediaries
    print("Computing non-root intermediaries...")
    nonroot_int = conn.execute("""
        SELECT
            caller AS address,
            count(*) AS changed_edges,
            count(DISTINCT callee) AS distinct_downstream_callees,
            count(DISTINCT divergence_id) AS distinct_txs,
            avg(depth) AS avg_depth,
            sum(CASE WHEN success_flip THEN 1 ELSE 0 END) AS success_flip_edges,
            sum(CASE WHEN only_in_schedule THEN 1 ELSE 0 END) AS schedule_only_edges,
            sum(CASE WHEN only_in_baseline THEN 1 ELSE 0 END) AS baseline_only_edges
        FROM edge_comparison
        WHERE edge_changed AND depth > 0
        GROUP BY caller
        ORDER BY changed_edges DESC
    """).df()
    nonroot_int["project"] = nonroot_int["address"].map(
        lambda a: label_address(a, classification)
    )
    # Count distinct downstream projects
    downstream_proj = conn.execute("""
        SELECT caller AS address, count(DISTINCT callee) AS cnt
        FROM edge_comparison
        WHERE edge_changed AND depth > 0
        GROUP BY caller
    """).df()
    proj_map = dict(zip(downstream_proj["address"], downstream_proj["cnt"]))
    nonroot_int["distinct_downstream_projects"] = nonroot_int["address"].map(
        lambda a: proj_map.get(a, 0)
    )
    nonroot_int["intermediary_score"] = (
        nonroot_int["changed_edges"] * 2
        + nonroot_int["distinct_downstream_projects"] * 25
        + nonroot_int["distinct_txs"] * 3
        + nonroot_int["success_flip_edges"] * 10
    )
    nonroot_int = nonroot_int.sort_values(by="intermediary_score", ascending=False)
    write_df(nonroot_int, tables_dir / "changed_nonroot_intermediaries.csv")

    # 4. First changed non-root edges
    print("Exporting first changed non-root edges...")
    first_changed = conn.execute("SELECT * FROM first_changed_nonroot").df()
    first_changed["caller_project"] = first_changed["caller"].map(
        lambda a: label_address(a, classification)
    )
    first_changed["callee_project"] = first_changed["callee"].map(
        lambda a: label_address(a, classification)
    )
    first_changed["recipient_project"] = first_changed["recipient"].map(
        lambda a: label_address(a, classification)
    )
    first_changed["change_reason"] = first_changed.apply(
        lambda row: ",".join(filter(None, [
            "only_in_baseline" if row.get("only_in_baseline") else "",
            "only_in_schedule" if row.get("only_in_schedule") else "",
            "success_flip" if row.get("success_flip") else "",
            "gas_delta" if row.get("gas_used_delta", 0) != 0 else "",
        ])),
        axis=1,
    )
    write_df(first_changed, tables_dir / "first_changed_nonroot_edges.csv")

    # 5. First changed non-root motifs
    print("Computing first changed non-root motifs...")
    motif_df = first_changed.groupby(
        ["recipient_project", "caller_project", "callee_project", "depth", "change_reason"],
        dropna=False,
    ).agg(
        txs=("divergence_id", "count"),
        success_flip_txs=("success_flip", "sum"),
        avg_gas_used_delta=("gas_used_delta", "mean"),
    ).reset_index().sort_values(by=["txs", "success_flip_txs"], ascending=False)
    write_df(motif_df, tables_dir / "first_changed_nonroot_motifs.csv")

    # 6. First changed non-root sankey
    first_sankey = first_changed.groupby(
        ["recipient_project", "caller_project", "callee_project"],
        dropna=False,
    ).agg(txs=("divergence_id", "count")).reset_index().sort_values(by="txs", ascending=False)
    write_df(first_sankey, tables_dir / "first_changed_nonroot_sankey_edges.csv")

    # 7. Intermediary breakpoints
    print("Computing intermediary breakpoints...")
    bp = first_changed.groupby(["caller", "caller_project"], dropna=False).agg(
        breakpoint_txs=("divergence_id", "count"),
        distinct_root_projects=("recipient_project", "nunique"),
        distinct_downstream_projects=("callee_project", "nunique"),
        success_flip_txs=("success_flip", "sum"),
        avg_depth=("depth", "mean"),
        avg_gas_used_delta=("gas_used_delta", "mean"),
    ).reset_index().rename(columns={"caller": "address", "caller_project": "project"})
    bp["breakpoint_score"] = (
        bp["breakpoint_txs"] * 5
        + bp["distinct_root_projects"] * 20
        + bp["distinct_downstream_projects"] * 20
        + bp["success_flip_txs"] * 10
    )
    bp = bp.sort_values(by="breakpoint_score", ascending=False)
    write_df(bp, tables_dir / "intermediary_breakpoints.csv")

    # 8. Deepest failing paths + aggregations
    print("Exporting failure paths...")
    paths_df = conn.execute("SELECT * FROM deepest_failing").df()
    paths_df["root_project"] = paths_df["root_callee"].map(
        lambda a: label_address(a, classification)
    )
    paths_df["failing_caller_project"] = paths_df["failing_caller"].map(
        lambda a: label_address(a, classification)
    )
    paths_df["failing_callee_project"] = paths_df["failing_callee"].map(
        lambda a: label_address(a, classification)
    )
    paths_df["recipient_project"] = paths_df["recipient"].map(
        lambda a: label_address(a, classification)
    )
    paths_df["pair_motif"] = paths_df["failing_caller_project"] + " -> " + paths_df["failing_callee_project"]
    write_df(paths_df, tables_dir / "tx_failure_paths.csv")

    # Failure motifs
    motifs = (
        paths_df.groupby(["pair_motif"], dropna=False)
        .agg(
            status_failures=("divergence_id", "count"),
            avg_gas_provided=("gas_provided", "mean"),
            avg_gas_used=("gas_used", "mean"),
        )
        .reset_index()
        .sort_values(by="status_failures", ascending=False)
    )
    write_df(motifs, tables_dir / "failure_motifs.csv")

    # Failure path sankey
    sankey = (
        paths_df.groupby(
            ["root_project", "failing_caller_project", "failing_callee_project"], dropna=False
        )
        .agg(status_failures=("divergence_id", "count"))
        .reset_index()
        .sort_values(by="status_failures", ascending=False)
    )
    write_df(sankey, tables_dir / "failure_path_sankey_edges.csv")

    # Intermediary centrality
    intermediaries = (
        paths_df.groupby(["failing_caller", "failing_caller_project"], dropna=False)
        .agg(
            status_changed_txs=("divergence_id", "count"),
            distinct_failing_callees=("failing_callee_project", "nunique"),
            distinct_root_projects=("root_project", "nunique"),
            avg_gas_provided=("gas_provided", "mean"),
            avg_gas_used=("gas_used", "mean"),
        )
        .reset_index()
        .rename(columns={"failing_caller": "address", "failing_caller_project": "project"})
    )
    intermediaries["mediation_score"] = (
        intermediaries["status_changed_txs"] * 10
        + intermediaries["distinct_failing_callees"] * 20
        + intermediaries["distinct_root_projects"] * 5
    )
    intermediaries = intermediaries.sort_values(by="mediation_score", ascending=False)
    write_df(intermediaries, tables_dir / "intermediary_centrality.csv")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
