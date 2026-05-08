"""JSON API endpoints for the gas repricing analysis web server."""
from __future__ import annotations

import math

from fastapi import APIRouter, Query

from .db import (
    SCHEDULE_NAME,
    db_mtime,
    label_address,
    query,
    query_df,
    query_scalar,
    read_csv,
)

router = APIRouter(prefix="/api")

# Filter clause to exclude wallet-fixable breakages (depth ≤ 1, no subcalls).
# These are just tight gas estimates that wallets auto-fix via eth_estimateGas.
NOT_WALLET_FIXABLE = """
    AND divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
"""

FORENSIC_OPCODE_NAMES = {
    "0x04": "DIV",
    "0x05": "SDIV",
    "0x06": "MOD",
    "0x07": "SMOD",
    "0x08": "ADDMOD",
    "0x09": "MULMOD",
    "0x0a": "EXP",
    "0x20": "KECCAK256",
}

# Same mapping keyed by the numeric opcode value, since the pipeline's
# named-string column (`divergence_opcode_name`) ends up empty for every
# row — the Rust Debug output writes `opcode_name: KECCAK256` (no quotes)
# but the regex looks for a quoted string. The numeric `divergence_opcode`
# column is extracted with a `\d+` pattern that does match, so we read
# from there and map int -> name on the Python side.
EIP7904_OPCODE_INT_NAMES = {
    0x04: "DIV",
    0x05: "SDIV",
    0x06: "MOD",
    0x07: "SMOD",
    0x08: "ADDMOD",
    0x09: "MULMOD",
    0x0a: "EXP",
    0x20: "KECCAK256",
}


def _int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, float) and math.isnan(value):
        return 0
    return int(value)


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    value = float(value)
    return None if math.isnan(value) else value


def _float(value) -> float:
    value = _float_or_none(value)
    return value if value is not None else 0.0


# ── Briefing endpoints ────────────────────────────────────────────────


@router.get("/overview")
def overview():
    total_divergent = query_scalar("SELECT count(*) FROM hot_7904")
    broken = query_scalar("SELECT count(*) FROM hot_7904 WHERE status_changed")
    wallet_fixable = query_scalar(
        f"SELECT count(*) FROM hot_7904 WHERE status_changed"
        f" AND divergence_id IN (SELECT divergence_id FROM wallet_fixable_ids)"
    )
    total_analyzed = query_scalar("SELECT sum(tx_count) FROM coverage_7904")
    contract_broken = broken - (wallet_fixable or 0)
    return {
        "total_analyzed": _int(total_analyzed),
        "divergent_txs": int(total_divergent),
        "broken_txs": int(broken),
        "wallet_fixable_txs": int(wallet_fixable or 0),
        "contract_broken_txs": int(contract_broken),
        "breakage_rate": round(broken / total_analyzed * 100, 2) if total_analyzed else 0,
        "contract_breakage_rate": round(contract_broken / total_analyzed * 100, 2) if total_analyzed else 0,
    }


@router.get("/funnel")
def funnel():
    """Bucket every divergent tx by observable impact.

    `trace_divergent_only` is the previously-mislabelled cohort: txs whose
    intermediate EVM trace differs from baseline but whose final outcome
    (gas used, event logs, status) matches — i.e. no observable change.
    """
    row = query("""
        SELECT
            count(*) AS total,
            sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS broken,
            sum(CASE WHEN event_logs_changed AND NOT status_changed THEN 1 ELSE 0 END)
                AS event_log_changed,
            sum(CASE WHEN NOT status_changed AND NOT event_logs_changed
                          AND gas_delta > 0 THEN 1 ELSE 0 END) AS gas_only_change,
            sum(CASE WHEN NOT status_changed AND NOT event_logs_changed
                          AND gas_delta <= 0 THEN 1 ELSE 0 END) AS trace_divergent_only
        FROM hot_7904
    """)[0]
    return {
        "divergent_txs": _int(row["total"]),
        "broken_txs": _int(row["broken"]),
        "event_log_changed": _int(row["event_log_changed"]),
        "gas_only_change": _int(row["gas_only_change"]),
        "trace_divergent_only": _int(row["trace_divergent_only"]),
    }


@router.get("/opcode-impact")
def opcode_impact():
    rows = query(f"""
        SELECT divergence_opcode AS opcode_num, count(*) AS cnt
        FROM normalized_forensics
        WHERE divergence_opcode IS NOT NULL
          AND divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
        GROUP BY 1 ORDER BY cnt DESC
    """)
    total = sum(r["cnt"] for r in rows)
    return [
        {
            "opcode": f"0x{int(r['opcode_num']):02x}",
            "name": EIP7904_OPCODE_INT_NAMES.get(int(r["opcode_num"]), f"0x{int(r['opcode_num']):02x}"),
            "count": int(r["cnt"]),
            "share": round(r["cnt"] / total * 100, 1) if total else 0,
        }
        for r in rows
    ]


@router.get("/gas-overhead")
def gas_overhead():
    stats = query("""
        SELECT
            count(*) as cnt,
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.05) WITHIN GROUP (ORDER BY gas_delta) as p5,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM hot_7904 WHERE NOT status_changed
    """)[0]
    # Log-scale PMF: use power-of-2 buckets for the gas delta
    histogram = query("""
        WITH bucketed AS (
            SELECT
                CASE WHEN gas_delta <= 0 THEN 0
                     ELSE floor(log2(gas_delta))::int
                END AS log_bin,
                count(*) AS cnt
            FROM hot_7904 WHERE NOT status_changed
            GROUP BY 1
        )
        SELECT log_bin, cnt FROM bucketed ORDER BY log_bin
    """)
    total = sum(r["cnt"] for r in histogram) or 1
    return {
        "stats": {k: float(v) if v is not None else 0 for k, v in stats.items()},
        "histogram": [
            {
                "bin_start": 2 ** int(r["log_bin"]) if r["log_bin"] > 0 else 0,
                "label": f'{2**int(r["log_bin"])}–{2**(int(r["log_bin"])+1)}' if r["log_bin"] > 0 else '≤1',
                "count": int(r["cnt"]),
                "density": round(r["cnt"] / total, 6),
            }
            for r in histogram
        ],
    }


@router.get("/concentration")
def concentration():
    df = query_df(f"""
        SELECT recipient, count(*) as broken_txs
        FROM hot_7904 WHERE status_changed {NOT_WALLET_FIXABLE}
        GROUP BY recipient ORDER BY broken_txs DESC
    """)
    df["cumulative"] = df["broken_txs"].cumsum()
    total = df["broken_txs"].sum()
    df["cum_pct"] = df["cumulative"] / total * 100
    return [
        {
            "rank": i + 1,
            "recipient": row["recipient"] if isinstance(row["recipient"], str) else None,
            "name": label_address(row["recipient"]),
            "broken_txs": _int(row["broken_txs"]),
            "cum_pct": round(_float(row["cum_pct"]), 2),
        }
        for i, row in df.head(50).iterrows()
    ]


@router.get("/top-contracts")
def top_contracts(limit: int = Query(default=10, le=500)):
    rows = query(f"""
        SELECT recipient, count(*) as broken_txs,
               avg(gas_delta) as avg_delta, sum(gas_delta) as total_delta
        FROM hot_7904 WHERE status_changed {NOT_WALLET_FIXABLE}
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT {int(limit)}
    """)
    return [
        {
            "recipient": r["recipient"],
            "name": label_address(r["recipient"]),
            "broken_txs": _int(r["broken_txs"]),
            "avg_delta": _float(r["avg_delta"]),
            "total_delta": _float(r["total_delta"]),
        }
        for r in rows
    ]


# ── Forensics endpoints ──────────────────────────────────────────────


@router.get("/forensics/time-series")
def forensics_time_series():
    return query("""
        WITH bounds AS (
            SELECT min(block_number) AS mn, max(block_number) AS mx FROM coverage_7904
        ),
        buckets AS (
            SELECT (mx - mn) / 300 AS bucket_size, mn FROM bounds
        ),
        broken_per_bucket AS (
            SELECT
                b.mn + ((h.block_number - b.mn) // b.bucket_size) * b.bucket_size AS block_group,
                count(*) AS broken
            FROM hot_7904 h, buckets b
            WHERE h.status_changed
              AND h.divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
            GROUP BY block_group
        ),
        total_per_bucket AS (
            SELECT
                b.mn + ((c.block_number - b.mn) // b.bucket_size) * b.bucket_size AS block_group,
                sum(c.tx_count) AS total_txs
            FROM coverage_7904 c, buckets b
            GROUP BY block_group
        )
        SELECT
            t.block_group,
            coalesce(bp.broken, 0) AS broken,
            t.total_txs,
            CASE WHEN t.total_txs > 0
                 THEN round(coalesce(bp.broken, 0) * 100.0 / t.total_txs, 4)
                 ELSE 0
            END AS broken_pct
        FROM total_per_bucket t
        LEFT JOIN broken_per_bucket bp ON t.block_group = bp.block_group
        ORDER BY t.block_group
    """)


@router.get("/forensics/gas-delta")
def forensics_gas_delta():
    stats = query(f"""
        SELECT
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_delta) as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM hot_7904 WHERE status_changed {NOT_WALLET_FIXABLE}
    """)[0]
    histogram = query(f"""
        WITH bucketed AS (
            SELECT
                CASE WHEN gas_delta <= 0 THEN 0
                     ELSE floor(log2(gas_delta))::int
                END AS log_bin,
                count(*) AS cnt
            FROM hot_7904 WHERE status_changed {NOT_WALLET_FIXABLE}
            GROUP BY 1
        )
        SELECT log_bin, cnt FROM bucketed ORDER BY log_bin
    """)
    total = sum(r["cnt"] for r in histogram) or 1
    return {
        "stats": {k: float(v) if v is not None else 0 for k, v in stats.items()},
        "histogram": [
            {
                "bin_start": 2 ** int(r["log_bin"]) if r["log_bin"] > 0 else 0,
                "label": f'{2**int(r["log_bin"])}–{2**(int(r["log_bin"])+1)}' if r["log_bin"] > 0 else '≤1',
                "count": int(r["cnt"]),
                "density": round(r["cnt"] / total, 6),
            }
            for r in histogram
        ],
    }


@router.get("/forensics/call-depth")
def forensics_call_depth():
    return query(f"""
        SELECT
            coalesce(nf.divergence_call_depth, -1) AS divergence_call_depth,
            count(*) AS divergent_txs
        FROM normalized_forensics nf
        JOIN hot_7904 h USING (divergence_id)
        WHERE h.status_changed
          AND h.divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
        GROUP BY 1 ORDER BY 1
    """)


@router.get("/forensics/failure-motifs")
def forensics_failure_motifs():
    df = read_csv("failure_motifs.csv")
    if df.empty:
        return []
    return df.head(15).to_dict(orient="records")


@router.get("/forensics/failure-flow")
def forensics_failure_flow():
    """Return pre-processed Sankey data for the failure flow diagram."""
    import pandas as pd
    df = read_csv("failure_path_sankey_edges.csv")
    if df.empty:
        return {"labels": [], "sources": [], "targets": [], "values": [], "link_colors": []}

    df["status_failures"] = pd.to_numeric(df["status_failures"], errors="coerce").fillna(0).astype(int)

    rc = df.groupby(["root_project", "failing_caller_project"])["status_failures"].sum().reset_index()
    rc = rc.nlargest(15, "status_failures")
    cc = df.groupby(["failing_caller_project", "failing_callee_project"])["status_failures"].sum().reset_index()
    cc = cc.nlargest(15, "status_failures")

    used_labels = set()
    for _, row in rc.iterrows():
        used_labels.add(row["root_project"])
        used_labels.add(row["failing_caller_project"])
    for _, row in cc.iterrows():
        used_labels.add(row["failing_caller_project"])
        used_labels.add(row["failing_callee_project"])
    labels = sorted(used_labels)
    label_idx = {l: i for i, l in enumerate(labels)}

    sources, targets, values, link_colors = [], [], [], []
    for _, row in rc.iterrows():
        sources.append(label_idx[row["root_project"]])
        targets.append(label_idx[row["failing_caller_project"]])
        values.append(int(row["status_failures"]))
        link_colors.append("rgba(52,152,219,0.3)")
    for _, row in cc.iterrows():
        sources.append(label_idx[row["failing_caller_project"]])
        targets.append(label_idx[row["failing_callee_project"]])
        values.append(int(row["status_failures"]))
        link_colors.append("rgba(231,76,60,0.3)")

    display_labels = [label_address(l) if l.startswith("0x") else l for l in labels]
    return {
        "labels": display_labels,
        "sources": sources,
        "targets": targets,
        "values": values,
        "link_colors": link_colors,
    }


@router.get("/forensics/remediation")
def forensics_remediation():
    """Sankey: Top 10 contracts → owner bucket → remediation bucket."""
    import pandas as pd

    df = read_csv("project_owner_summary.csv")
    if df.empty:
        return {"labels": [], "sources": [], "targets": [], "values": [], "link_colors": []}

    known = df[df["owner_bucket"] != "unknown_owner"].copy()
    if known.empty:
        return {"labels": [], "sources": [], "targets": [], "values": [], "link_colors": []}

    # Top 10 projects by status_changed_txs; rest grouped as "Other"
    known["status_changed_txs"] = pd.to_numeric(known["status_changed_txs"], errors="coerce").fillna(0)
    project_totals = known.groupby("divergence_project")["status_changed_txs"].sum().sort_values(ascending=False)
    top_projects = set(project_totals.head(10).index)
    known["project_label"] = known["divergence_project"].apply(lambda p: p if p in top_projects else "Other")

    # Aggregate: project → owner
    po = known.groupby(["project_label", "owner_bucket"])["status_changed_txs"].sum().reset_index()
    po = po[po["status_changed_txs"] > 0]
    # Aggregate: owner → remediation
    or_ = known.groupby(["owner_bucket", "remediation_bucket"])["status_changed_txs"].sum().reset_index()
    or_ = or_[or_["status_changed_txs"] > 0]

    # Build label list: projects first, then owner buckets, then remediation buckets
    project_labels = sorted(po["project_label"].unique(), key=lambda p: (p == "Other", p))
    owner_labels = sorted(po["owner_bucket"].unique())
    remed_labels = sorted(or_["remediation_bucket"].unique())
    all_labels = list(project_labels) + list(owner_labels) + list(remed_labels)
    idx = {l: i for i, l in enumerate(all_labels)}

    sources, targets, values, link_colors = [], [], [], []
    for _, row in po.iterrows():
        sources.append(idx[row["project_label"]])
        targets.append(idx[row["owner_bucket"]])
        values.append(int(row["status_changed_txs"]))
        link_colors.append("rgba(52,152,219,0.3)")
    for _, row in or_.iterrows():
        sources.append(idx[row["owner_bucket"]])
        targets.append(idx[row["remediation_bucket"]])
        values.append(int(row["status_changed_txs"]))
        link_colors.append("rgba(231,76,60,0.3)")

    return {
        "labels": all_labels,
        "sources": sources,
        "targets": targets,
        "values": values,
        "link_colors": link_colors,
    }


# ── EIP-8037 state-gas endpoints ──────────────────────────────────────


@router.get("/eip8037/overview")
def eip8037_overview():
    stats = query("""
        SELECT
            count(*) AS divergent_txs,
            sum(CASE WHEN original_limit_failure THEN 1 ELSE 0 END)
                AS original_limit_failures,
            sum(CASE WHEN schedule_success AND original_limit_failure THEN 1 ELSE 0 END)
                AS fixable_with_more_outer_gas,
            sum(CASE WHEN baseline_success AND NOT schedule_success THEN 1 ELSE 0 END)
                AS baseline_success_schedule_failures,
            sum(CASE WHEN NOT schedule_success AND min_multiplier_to_succeed IS NULL THEN 1 ELSE 0 END)
                AS unresolved_replay_failures,
            sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END)
                AS reservoir_exhausted_txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            sum(runtime_state_gas) AS total_runtime_state_gas,
            sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
            avg(schedule_state_gas_spent) AS avg_state_gas_spent,
            max(schedule_state_gas_spent) AS max_state_gas_spent,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p50_min_multiplier_to_succeed,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p95_min_multiplier_to_succeed,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p99_min_multiplier_to_succeed,
            max(min_multiplier_to_succeed) AS max_min_multiplier_to_succeed,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY extra_gas_needed)
                AS p95_extra_gas_needed,
            max(extra_gas_needed) AS max_extra_gas_needed
        FROM eip8037_tx_impact
    """)[0]
    total_analyzed = query_scalar("SELECT sum(tx_count) FROM coverage_schedule", default=0)
    block_range = query("SELECT min(block_number) AS mn, max(block_number) AS mx FROM coverage_schedule")
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}

    return {
        "schedule_name": SCHEDULE_NAME,
        "total_analyzed": _int(total_analyzed),
        "min_block": _int(br["mn"]),
        "max_block": _int(br["mx"]),
        "divergent_txs": _int(stats["divergent_txs"]),
        "original_limit_failures": _int(stats["original_limit_failures"]),
        "fixable_with_more_outer_gas": _int(stats["fixable_with_more_outer_gas"]),
        "baseline_success_schedule_failures": _int(stats["baseline_success_schedule_failures"]),
        "unresolved_replay_failures": _int(stats["unresolved_replay_failures"]),
        "reservoir_exhausted_txs": _int(stats["reservoir_exhausted_txs"]),
        "total_state_gas_spent": _int(stats["total_state_gas_spent"]),
        "total_runtime_state_gas": _int(stats["total_runtime_state_gas"]),
        "total_runtime_state_gas_spillover": _int(stats["total_runtime_state_gas_spillover"]),
        "avg_state_gas_spent": _float(stats["avg_state_gas_spent"]),
        "max_state_gas_spent": _int(stats["max_state_gas_spent"]),
        "p50_min_multiplier_to_succeed": _float_or_none(stats["p50_min_multiplier_to_succeed"]),
        "p95_min_multiplier_to_succeed": _float_or_none(stats["p95_min_multiplier_to_succeed"]),
        "p99_min_multiplier_to_succeed": _float_or_none(stats["p99_min_multiplier_to_succeed"]),
        "max_min_multiplier_to_succeed": _float_or_none(stats["max_min_multiplier_to_succeed"]),
        "p95_extra_gas_needed": _int(stats["p95_extra_gas_needed"]),
        "max_extra_gas_needed": _int(stats["max_extra_gas_needed"]),
    }


@router.get("/eip8037/multiplier-histogram")
def eip8037_multiplier_histogram():
    rows = query("""
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN would_fit_in_original_limit THEN 0
                    WHEN min_multiplier_to_succeed IS NULL THEN 99
                    WHEN min_multiplier_to_succeed <= 1.25 THEN 1
                    WHEN min_multiplier_to_succeed <= 1.50 THEN 2
                    WHEN min_multiplier_to_succeed <= 2.00 THEN 3
                    WHEN min_multiplier_to_succeed <= 4.00 THEN 4
                    WHEN min_multiplier_to_succeed <= 8.00 THEN 5
                    ELSE 6
                END AS sort_key,
                CASE
                    WHEN would_fit_in_original_limit THEN 'fits original'
                    WHEN min_multiplier_to_succeed IS NULL THEN 'unresolved'
                    WHEN min_multiplier_to_succeed <= 1.25 THEN '1.00-1.25x'
                    WHEN min_multiplier_to_succeed <= 1.50 THEN '1.25-1.50x'
                    WHEN min_multiplier_to_succeed <= 2.00 THEN '1.50-2.00x'
                    WHEN min_multiplier_to_succeed <= 4.00 THEN '2.00-4.00x'
                    WHEN min_multiplier_to_succeed <= 8.00 THEN '4.00-8.00x'
                    ELSE '>8.00x'
                END AS bucket,
                count(*) AS txs,
                sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
                max(min_multiplier_to_succeed) AS max_multiplier
            FROM eip8037_tx_impact
            GROUP BY 1, 2
        )
        SELECT * FROM bucketed ORDER BY sort_key
    """)
    return [
        {
            "bucket": r["bucket"],
            "txs": _int(r["txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
            "max_multiplier": _float_or_none(r["max_multiplier"]),
        }
        for r in rows
    ]


@router.get("/eip8037/reservoir")
def eip8037_reservoir():
    """Reservoir-utilization view: how full does the per-tx state-gas
    reservoir get in practice, and what happens to the overflow tail."""
    headline = query("""
        SELECT
            count(*) AS total_txs,
            sum(CASE WHEN runtime_state_gas > 0 THEN 1 ELSE 0 END) AS state_touching_txs,
            sum(CASE WHEN runtime_state_gas_spillover > 0 THEN 1 ELSE 0 END) AS overflow_txs,
            percentile_cont(0.95) WITHIN GROUP (
                ORDER BY runtime_state_gas::DOUBLE / NULLIF(schedule_initial_reservoir, 0)
            ) FILTER (WHERE schedule_initial_reservoir > 0 AND runtime_state_gas > 0)
                AS p95_utilization_state_touching,
            percentile_cont(0.50) WITHIN GROUP (
                ORDER BY runtime_state_gas::DOUBLE / NULLIF(schedule_initial_reservoir, 0)
            ) FILTER (WHERE schedule_initial_reservoir > 0 AND runtime_state_gas > 0)
                AS p50_utilization_state_touching
        FROM eip8037_tx_impact
    """)[0]

    util_rows = query("""
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN runtime_state_gas <= 0 THEN 0
                    WHEN schedule_initial_reservoir <= 0
                         OR runtime_state_gas > schedule_initial_reservoir THEN 6
                    WHEN runtime_state_gas <= 0.10 * schedule_initial_reservoir THEN 1
                    WHEN runtime_state_gas <= 0.25 * schedule_initial_reservoir THEN 2
                    WHEN runtime_state_gas <= 0.50 * schedule_initial_reservoir THEN 3
                    WHEN runtime_state_gas <= 0.75 * schedule_initial_reservoir THEN 4
                    ELSE 5
                END AS sort_key,
                count(*) AS txs,
                sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs
            FROM eip8037_tx_impact
            GROUP BY 1
        )
        SELECT * FROM bucketed ORDER BY sort_key
    """)
    UTIL_LABELS = {
        0: "no state", 1: "0–10%", 2: "10–25%", 3: "25–50%",
        4: "50–75%", 5: "75–100%", 6: "overflow",
    }
    utilization = [
        {
            "bucket": UTIL_LABELS.get(int(r["sort_key"]), str(r["sort_key"])),
            "sort_key": int(r["sort_key"]),
            "txs": _int(r["txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
        }
        for r in util_rows
    ]

    # Spillover severity (overflow cohort only) — log2-bucketed CDF input.
    spillover_rows = query("""
        WITH bucketed AS (
            SELECT
                floor(log2(runtime_state_gas_spillover))::int AS log_bin,
                count(*) AS cnt
            FROM eip8037_tx_impact
            WHERE runtime_state_gas_spillover > 0
            GROUP BY 1
        )
        SELECT log_bin, cnt FROM bucketed ORDER BY log_bin
    """)
    spillover_total = sum(int(r["cnt"]) for r in spillover_rows) or 1
    spillover_histogram = [
        {
            "bin_start": 2 ** int(r["log_bin"]),
            "label": f'{2**int(r["log_bin"])}–{2**(int(r["log_bin"])+1)}',
            "count": _int(r["cnt"]),
            "density": round(int(r["cnt"]) / spillover_total, 6),
        }
        for r in spillover_rows
    ]

    category_rows = query("""
        SELECT
            coalesce(state_gas_category, 'uncategorized') AS category,
            count(*) AS total_txs,
            sum(CASE WHEN runtime_state_gas <= 0 THEN 1 ELSE 0 END) AS no_state_txs,
            sum(CASE WHEN runtime_state_gas > 0 AND runtime_state_gas_spillover = 0
                          THEN 1 ELSE 0 END) AS fits_txs,
            sum(CASE WHEN runtime_state_gas_spillover > 0 THEN 1 ELSE 0 END) AS overflow_txs
        FROM eip8037_tx_impact
        GROUP BY 1
        ORDER BY total_txs DESC
    """)
    category_split = [
        {
            "category": r["category"],
            "total_txs": _int(r["total_txs"]),
            "no_state_txs": _int(r["no_state_txs"]),
            "fits_txs": _int(r["fits_txs"]),
            "overflow_txs": _int(r["overflow_txs"]),
        }
        for r in category_rows
    ]

    return {
        "headline": {
            "total_txs": _int(headline["total_txs"]),
            "state_touching_txs": _int(headline["state_touching_txs"]),
            "overflow_txs": _int(headline["overflow_txs"]),
            "p50_utilization_state_touching": _float_or_none(headline["p50_utilization_state_touching"]),
            "p95_utilization_state_touching": _float_or_none(headline["p95_utilization_state_touching"]),
        },
        "utilization": utilization,
        "spillover_histogram": spillover_histogram,
        "category_split": category_split,
    }


@router.get("/eip8037/state-gas-by-category")
def eip8037_state_gas_by_category():
    rows = query("""
        SELECT
            state_gas_category,
            count(*) AS txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            avg(schedule_state_gas_spent) AS avg_state_gas_spent,
            sum(runtime_state_gas) AS total_runtime_state_gas,
            sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
            sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END) AS reservoir_exhausted_txs,
            avg(min_multiplier_to_succeed) AS avg_min_multiplier_to_succeed
        FROM eip8037_tx_impact
        GROUP BY 1
        ORDER BY total_state_gas_spent DESC, txs DESC
    """)
    return [
        {
            "category": r["state_gas_category"],
            "txs": _int(r["txs"]),
            "total_state_gas_spent": _int(r["total_state_gas_spent"]),
            "avg_state_gas_spent": _float(r["avg_state_gas_spent"]),
            "total_runtime_state_gas": _int(r["total_runtime_state_gas"]),
            "total_runtime_state_gas_spillover": _int(r["total_runtime_state_gas_spillover"]),
            "reservoir_exhausted_txs": _int(r["reservoir_exhausted_txs"]),
            "avg_min_multiplier_to_succeed": _float_or_none(r["avg_min_multiplier_to_succeed"]),
        }
        for r in rows
    ]


@router.get("/eip8037/top-contracts")
def eip8037_top_contracts(limit: int = Query(default=20, le=500)):
    rows = query(f"""
        SELECT *
        FROM eip8037_contract_impact
        WHERE original_limit_failures > 0
           OR status_changed_txs > 0
           OR total_state_gas_spent > 0
        ORDER BY original_limit_failures DESC,
                 total_state_gas_spent DESC,
                 status_changed_txs DESC,
                 divergent_txs DESC
        LIMIT {int(limit)}
    """)
    return [
        {
            "target_address": r["target_address"],
            "name": label_address(r["target_address"]),
            "divergent_txs": _int(r["divergent_txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
            "original_limit_failures": _int(r["original_limit_failures"]),
            "fixable_with_more_outer_gas": _int(r["fixable_with_more_outer_gas"]),
            "unresolved_replay_failures": _int(r["unresolved_replay_failures"]),
            "total_state_gas_spent": _int(r["total_state_gas_spent"]),
            "total_runtime_state_gas_spillover": _int(r["total_runtime_state_gas_spillover"]),
            "reservoir_exhausted_txs": _int(r["reservoir_exhausted_txs"]),
            "p95_min_multiplier_to_succeed": _float_or_none(r["p95_min_multiplier_to_succeed"]),
            "max_min_multiplier_to_succeed": _float_or_none(r["max_min_multiplier_to_succeed"]),
            "max_extra_gas_needed": _int(r["max_extra_gas_needed"]),
            "min_block": _int(r["min_block"]),
            "max_block": _int(r["max_block"]),
        }
        for r in rows
    ]


@router.get("/eip8037/examples")
def eip8037_examples(limit: int = Query(default=50, le=500)):
    rows = query(f"""
        SELECT
            tx_hash, block_number, tx_index, target_address, tx_gas_limit,
            baseline_success, schedule_success, status_changed,
            baseline_gas_used, schedule_gas_used, gas_delta,
            would_fit_in_original_limit, min_multiplier_to_succeed,
            extra_gas_needed, estimated_min_gas_limit, state_gas_category,
            schedule_state_gas_spent, runtime_state_gas,
            schedule_initial_reservoir, runtime_state_gas_spillover,
            reservoir_exhausted
        FROM eip8037_tx_impact
        WHERE original_limit_failure
           OR status_changed
           OR reservoir_exhausted
        ORDER BY
            CASE
                WHEN baseline_success AND NOT schedule_success THEN 0
                WHEN original_limit_failure THEN 1
                WHEN reservoir_exhausted THEN 2
                ELSE 3
            END,
            coalesce(min_multiplier_to_succeed, 999999) DESC,
            schedule_state_gas_spent DESC
        LIMIT {int(limit)}
    """)
    return [
        {
            "tx_hash": r["tx_hash"],
            "block_number": _int(r["block_number"]),
            "tx_index": _int(r["tx_index"]),
            "target_address": r["target_address"],
            "target_name": label_address(r["target_address"]),
            "tx_gas_limit": _int(r["tx_gas_limit"]),
            "baseline_success": r["baseline_success"],
            "schedule_success": r["schedule_success"],
            "status_changed": r["status_changed"],
            "baseline_gas_used": _int(r["baseline_gas_used"]),
            "schedule_gas_used": _int(r["schedule_gas_used"]),
            "gas_delta": _int(r["gas_delta"]),
            "would_fit_in_original_limit": r["would_fit_in_original_limit"],
            "min_multiplier_to_succeed": _float_or_none(r["min_multiplier_to_succeed"]),
            "extra_gas_needed": _int(r["extra_gas_needed"]),
            "estimated_min_gas_limit": _int(r["estimated_min_gas_limit"]),
            "state_gas_category": r["state_gas_category"],
            "schedule_state_gas_spent": _int(r["schedule_state_gas_spent"]),
            "runtime_state_gas": _int(r["runtime_state_gas"]),
            "schedule_initial_reservoir": _int(r["schedule_initial_reservoir"]),
            "runtime_state_gas_spillover": _int(r["runtime_state_gas_spillover"]),
            "reservoir_exhausted": r["reservoir_exhausted"],
        }
        for r in rows
    ]


# ── Affected parties endpoints ────────────────────────────────────────


_AFFECTED_BASE_CTE = f"""
    WITH e7904 AS (
        SELECT lower(recipient) AS addr,
               count(*) AS broken_txs_7904,
               avg(gas_delta) AS avg_delta_7904,
               sum(gas_delta) AS total_delta_7904,
               min(block_number) AS min_block_7904,
               max(block_number) AS max_block_7904
        FROM hot_7904
        WHERE status_changed {NOT_WALLET_FIXABLE}
        GROUP BY lower(recipient)
    ),
    e8037 AS (
        SELECT lower(target_address) AS addr,
               original_limit_failures AS need_higher_limit_8037,
               reservoir_exhausted_txs AS reservoir_exhausted_8037,
               status_changed_txs AS status_changed_8037,
               p95_min_multiplier_to_succeed AS p95_multiplier_8037,
               min_block AS min_block_8037,
               max_block AS max_block_8037
        FROM eip8037_contract_impact
        WHERE original_limit_failures > 0
           OR status_changed_txs > 0
           OR reservoir_exhausted_txs > 0
    ),
    affected_combined AS (
        SELECT
            coalesce(e7.addr, e8.addr) AS addr,
            coalesce(e7.broken_txs_7904, 0) AS broken_txs_7904,
            coalesce(e7.avg_delta_7904, 0) AS avg_delta_7904,
            coalesce(e7.total_delta_7904, 0) AS total_delta_7904,
            coalesce(e8.need_higher_limit_8037, 0) AS need_higher_limit_8037,
            coalesce(e8.reservoir_exhausted_8037, 0) AS reservoir_exhausted_8037,
            coalesce(e8.status_changed_8037, 0) AS status_changed_8037,
            e8.p95_multiplier_8037,
            least(coalesce(e7.min_block_7904, 99999999999),
                  coalesce(e8.min_block_8037, 99999999999)) AS min_block,
            greatest(coalesce(e7.max_block_7904, 0),
                     coalesce(e8.max_block_8037, 0)) AS max_block
        FROM e7904 e7
        FULL OUTER JOIN e8037 e8 USING (addr)
    )
"""


@router.get("/affected")
def affected(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
):
    """Paginated affected contracts across EIP-7904 and EIP-8037."""
    offset = (page - 1) * per_page
    total_count = query_scalar(
        _AFFECTED_BASE_CTE + " SELECT count(*) FROM affected_combined",
        default=0,
    )
    rows = query(_AFFECTED_BASE_CTE + f"""
        SELECT * FROM affected_combined
        ORDER BY broken_txs_7904 DESC, need_higher_limit_8037 DESC
        LIMIT {int(per_page)} OFFSET {int(offset)}
    """)

    outreach = read_csv("outreach_priority.csv")
    outreach_dict = {}
    if not outreach.empty:
        for _, row in outreach.iterrows():
            outreach_dict[row["project"]] = {
                "owner_buckets": str(row.get("owner_buckets", "")),
                "remediation_buckets": str(row.get("remediation_buckets", "")),
            }

    items = []
    for r in rows:
        addr = r["addr"]
        name = label_address(addr)
        info = outreach_dict.get(name, {})
        items.append({
            "recipient": addr,
            "name": name,
            "broken_txs_7904": _int(r["broken_txs_7904"]),
            "avg_delta_7904": _float(r["avg_delta_7904"]),
            "total_delta_7904": _float(r["total_delta_7904"]),
            "need_higher_limit_8037": _int(r["need_higher_limit_8037"]),
            "reservoir_exhausted_8037": _int(r["reservoir_exhausted_8037"]),
            "status_changed_8037": _int(r["status_changed_8037"]),
            "p95_multiplier_8037": _float_or_none(r["p95_multiplier_8037"]),
            "min_block": _int(r["min_block"]),
            "max_block": _int(r["max_block"]),
            "owner": info.get("owner_buckets", ""),
            "remediation": info.get("remediation_buckets", ""),
        })
    return {
        "items": items,
        "total": int(total_count),
        "page": page,
        "per_page": per_page,
        "total_pages": (int(total_count) + per_page - 1) // per_page,
    }


def _with_shares(rows: list[dict], count_key: str = "count") -> list[dict]:
    total = sum(r[count_key] for r in rows) or 1
    return [{**r, "share": round(r[count_key] / total * 100, 1)} for r in rows]


@router.get("/affected/{address}")
def affected_detail(address: str):
    """Single contract detail with EIP-7904 and EIP-8037 diagnostics."""
    addr = address.lower()

    # ── EIP-7904 stats ──
    eip7904_stats = query(f"""
        SELECT count(*) as broken_txs,
               avg(gas_delta) as avg_delta,
               sum(gas_delta) as total_delta,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95_delta,
               min(block_number) as min_block,
               max(block_number) as max_block
        FROM hot_7904
        WHERE status_changed {NOT_WALLET_FIXABLE} AND lower(recipient) = '{addr}'
    """)[0]
    eip7904_wallet = query_scalar(f"""
        SELECT count(*) FROM hot_7904
        WHERE status_changed
          AND divergence_id IN (SELECT divergence_id FROM wallet_fixable_ids)
          AND lower(recipient) = '{addr}'
    """, default=0)
    opcodes_raw = query(f"""
        SELECT n.divergence_opcode AS op_num, count(*) AS cnt
        FROM normalized_forensics n
        JOIN hot_7904 h USING (divergence_id)
        WHERE h.status_changed
          AND n.divergence_opcode IS NOT NULL
          AND n.divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
          AND lower(h.recipient) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    depths_raw = query(f"""
        SELECT coalesce(n.divergence_call_depth, -1) as depth, count(*) as cnt
        FROM normalized_forensics n
        JOIN hot_7904 h USING (divergence_id)
        WHERE h.status_changed
          AND n.divergence_id NOT IN (SELECT divergence_id FROM wallet_fixable_ids)
          AND lower(h.recipient) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    eip7904_txs = query(f"""
        SELECT tx_hash, block_number, gas_delta
        FROM hot_7904
        WHERE status_changed {NOT_WALLET_FIXABLE} AND lower(recipient) = '{addr}'
        ORDER BY gas_delta DESC LIMIT 20
    """)

    # ── EIP-8037 stats ──
    eip8037_rows = query(f"""
        SELECT * FROM eip8037_contract_impact
        WHERE lower(target_address) = '{addr}'
    """)
    eip8037_row = eip8037_rows[0] if eip8037_rows else None
    categories_raw = query(f"""
        SELECT coalesce(state_gas_category, 'uncategorized') as cat, count(*) as cnt
        FROM eip8037_tx_impact
        WHERE lower(target_address) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    eip8037_txs = query(f"""
        SELECT tx_hash, block_number, tx_gas_limit, min_multiplier_to_succeed,
               reservoir_exhausted, state_gas_category,
               runtime_state_gas_spillover, schedule_state_gas_spent
        FROM eip8037_tx_impact
        WHERE lower(target_address) = '{addr}'
          AND (original_limit_failure OR status_changed OR reservoir_exhausted)
        ORDER BY
            CASE WHEN baseline_success AND NOT schedule_success THEN 0
                 WHEN original_limit_failure THEN 1
                 WHEN reservoir_exhausted THEN 2 ELSE 3 END,
            coalesce(min_multiplier_to_succeed, 999999) DESC
        LIMIT 20
    """)

    eip7904_broken = _int(eip7904_stats["broken_txs"])
    eip7904_wallet_n = _int(eip7904_wallet)
    eip8037_div = _int(eip8037_row["divergent_txs"]) if eip8037_row else 0
    found = eip7904_broken > 0 or eip7904_wallet_n > 0 or eip8037_div > 0

    name = label_address(addr)
    outreach = read_csv("outreach_priority.csv")
    info = {}
    if not outreach.empty:
        match = outreach[outreach["project"] == name]
        if not match.empty:
            row = match.iloc[0]
            info = {
                "owner_buckets": str(row.get("owner_buckets", "")),
                "remediation_buckets": str(row.get("remediation_buckets", "")),
            }

    return {
        "found": found,
        "address": addr,
        "name": name,
        "owner": info.get("owner_buckets", ""),
        "remediation": info.get("remediation_buckets", ""),
        "eip7904": {
            "broken_txs": eip7904_broken,
            "wallet_fixable_txs": eip7904_wallet_n,
            "avg_delta": _float(eip7904_stats["avg_delta"]),
            "total_delta": _float(eip7904_stats["total_delta"]),
            "p95_delta": _float(eip7904_stats["p95_delta"]),
            "min_block": _int(eip7904_stats["min_block"]),
            "max_block": _int(eip7904_stats["max_block"]),
            "opcodes": _with_shares([
                {
                    "name": EIP7904_OPCODE_INT_NAMES.get(int(r["op_num"]), f"0x{int(r['op_num']):02x}"),
                    "count": _int(r["cnt"]),
                }
                for r in opcodes_raw
            ]),
            "depths": _with_shares([
                {"depth": _int(r["depth"]), "count": _int(r["cnt"])}
                for r in depths_raw
            ]),
            "transactions": [
                {
                    "tx_hash": t["tx_hash"],
                    "block_number": _int(t["block_number"]),
                    "gas_delta": _float(t["gas_delta"]),
                }
                for t in eip7904_txs
            ],
        },
        "eip8037": {
            "divergent_txs": _int(eip8037_row["divergent_txs"]) if eip8037_row else 0,
            "status_changed_txs": _int(eip8037_row["status_changed_txs"]) if eip8037_row else 0,
            "original_limit_failures": _int(eip8037_row["original_limit_failures"]) if eip8037_row else 0,
            "fixable_with_more_outer_gas": _int(eip8037_row["fixable_with_more_outer_gas"]) if eip8037_row else 0,
            "reservoir_exhausted_txs": _int(eip8037_row["reservoir_exhausted_txs"]) if eip8037_row else 0,
            "total_state_gas_spent": _int(eip8037_row["total_state_gas_spent"]) if eip8037_row else 0,
            "p95_min_multiplier_to_succeed": _float_or_none(eip8037_row["p95_min_multiplier_to_succeed"]) if eip8037_row else None,
            "max_min_multiplier_to_succeed": _float_or_none(eip8037_row["max_min_multiplier_to_succeed"]) if eip8037_row else None,
            "max_extra_gas_needed": _int(eip8037_row["max_extra_gas_needed"]) if eip8037_row else 0,
            "categories": _with_shares([
                {"category": r["cat"], "count": _int(r["cnt"])}
                for r in categories_raw
            ]),
            "transactions": [
                {
                    "tx_hash": t["tx_hash"],
                    "block_number": _int(t["block_number"]),
                    "tx_gas_limit": _int(t["tx_gas_limit"]),
                    "min_multiplier_to_succeed": _float_or_none(t["min_multiplier_to_succeed"]),
                    "reservoir_exhausted": bool(t["reservoir_exhausted"]),
                    "state_gas_category": t["state_gas_category"],
                    "runtime_state_gas_spillover": _int(t["runtime_state_gas_spillover"]),
                    "schedule_state_gas_spent": _int(t["schedule_state_gas_spent"]),
                }
                for t in eip8037_txs
            ],
        },
    }


@router.get("/tx/{tx_hash}")
def tx_detail(tx_hash: str):
    """Detailed view of a single broken transaction: gas info, divergence location, call stack."""
    tx_hash = tx_hash.lower().strip()

    # Core tx info from hot table
    hot = query(f"""
        SELECT h.divergence_id, h.block_number, h.tx_index, h.tx_hash,
               h.baseline_success, h.schedule_success,
               h.baseline_gas_used, h.schedule_gas_used, h.gas_delta,
               h.tx_gas_limit, h.sender, h.recipient,
               e.would_fit_in_original_limit, e.min_multiplier_to_succeed,
               e.extra_gas_needed, e.estimated_min_gas_limit,
               e.schedule_total_gas_spent, e.schedule_state_gas_spent,
               e.schedule_initial_state_gas, e.runtime_state_gas,
               e.schedule_initial_reservoir, e.runtime_state_gas_spillover,
               e.schedule_floor_gas, e.schedule_gas_refunded,
               e.baseline_total_gas_spent, e.baseline_gas_refunded,
               e.state_gas_category, e.reservoir_exhausted
        FROM hot_7904 h
        LEFT JOIN eip8037_tx_impact e USING (divergence_id)
        WHERE lower(h.tx_hash) = '{tx_hash}'
        LIMIT 1
    """)
    if not hot:
        return {"found": False}
    h = hot[0]
    div_id = h["divergence_id"]

    # Forensic info: divergence location + OOG info
    forensics = query(f"""
        SELECT divergence_contract, divergence_call_depth, divergence_opcode,
               divergence_opcode_name,
               oog_contract, oog_call_depth, oog_opcode_name, oog_pattern, oog_gas_remaining,
               sload_count, sstore_count, call_count, log_count, total_ops
        FROM normalized_forensics
        WHERE divergence_id = {div_id}
        LIMIT 1
    """)

    # Raw artifacts: call frames + operation counts
    artifacts_raw = query(f"""
        SELECT schedule_call_frames, operation_counts
        FROM artifacts_7904
        WHERE divergence_id = {div_id}
        LIMIT 1
    """)
    frames_raw = artifacts_raw

    call_stack = []
    if frames_raw and frames_raw[0].get("schedule_call_frames"):
        import json
        try:
            frames = json.loads(frames_raw[0]["schedule_call_frames"])
            for f in frames:
                to_addr = f.get("to") or ""
                input_hex = f.get("input") or ""
                selector = input_hex[:10] if len(input_hex) >= 10 else ""
                call_stack.append({
                    "depth": f.get("depth", 0),
                    "call_type": f.get("call_type", ""),
                    "from": label_address(f.get("from", "")),
                    "from_address": f.get("from", ""),
                    "to": label_address(to_addr),
                    "to_address": to_addr,
                    "selector": selector,
                    "gas_provided": f.get("gas_provided", 0),
                    "gas_used": f.get("gas_used", 0),
                    "success": f.get("success", False),
                })
        except (json.JSONDecodeError, TypeError):
            pass

    # Per-opcode gas breakdown from operation_counts JSON
    gas_breakdown = []
    if artifacts_raw and artifacts_raw[0].get("operation_counts"):
        import json
        try:
            oc = json.loads(artifacts_raw[0]["operation_counts"]) if isinstance(
                artifacts_raw[0]["operation_counts"], str
            ) else artifacts_raw[0]["operation_counts"]
            REPRICED_OPCODES = [
                ("DIV", "div"), ("SDIV", "sdiv"), ("MOD", "mod"), ("SMOD", "smod"),
                ("ADDMOD", "addmod"), ("MULMOD", "mulmod"), ("EXP", "exp"), ("KECCAK256", "keccak256"),
            ]
            for name, key in REPRICED_OPCODES:
                count = int(oc.get(f"{key}_count", 0))
                delta = int(oc.get(f"{key}_gas_delta", 0))
                if count > 0 or delta != 0:
                    gas_breakdown.append({
                        "opcode": name,
                        "count": count,
                        "gas_delta": delta,
                    })
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    f = forensics[0] if forensics else {}
    return {
        "found": True,
        "tx_hash": h["tx_hash"],
        "block_number": int(h["block_number"]),
        "tx_index": int(h["tx_index"]),
        "sender": h["sender"],
        "recipient": h["recipient"],
        "recipient_name": label_address(h["recipient"]),
        "baseline_success": h["baseline_success"],
        "schedule_success": h["schedule_success"],
        "baseline_gas_used": int(h["baseline_gas_used"]),
        "schedule_gas_used": int(h["schedule_gas_used"]),
        "gas_delta": int(h["gas_delta"]),
        "gas_limit": int(h["tx_gas_limit"]),
        "divergence": {
            "contract": label_address(f.get("divergence_contract") or ""),
            "contract_address": f.get("divergence_contract") or "",
            "call_depth": f.get("divergence_call_depth"),
            "opcode": (
                EIP7904_OPCODE_INT_NAMES.get(int(f["divergence_opcode"]), f"0x{int(f['divergence_opcode']):02x}")
                if f.get("divergence_opcode") is not None
                else (f.get("divergence_opcode_name") or "")
            ),
        } if f else None,
        "oog": {
            "contract": label_address(f.get("oog_contract") or ""),
            "contract_address": f.get("oog_contract") or "",
            "call_depth": f.get("oog_call_depth"),
            "opcode": f.get("oog_opcode_name") or "",
            "pattern": f.get("oog_pattern") or "",
            "gas_remaining": f.get("oog_gas_remaining"),
        } if f and f.get("oog_contract") else None,
        "op_counts": {
            "sload": f.get("sload_count", 0),
            "sstore": f.get("sstore_count", 0),
            "call": f.get("call_count", 0),
            "log": f.get("log_count", 0),
            "total": f.get("total_ops", 0),
        } if f else None,
        "eip8037": {
            "would_fit_in_original_limit": h.get("would_fit_in_original_limit"),
            "min_multiplier_to_succeed": _float_or_none(h.get("min_multiplier_to_succeed")),
            "extra_gas_needed": _int(h.get("extra_gas_needed")),
            "estimated_min_gas_limit": _int(h.get("estimated_min_gas_limit")),
            "schedule_total_gas_spent": _int(h.get("schedule_total_gas_spent")),
            "schedule_state_gas_spent": _int(h.get("schedule_state_gas_spent")),
            "schedule_initial_state_gas": _int(h.get("schedule_initial_state_gas")),
            "runtime_state_gas": _int(h.get("runtime_state_gas")),
            "schedule_initial_reservoir": _int(h.get("schedule_initial_reservoir")),
            "runtime_state_gas_spillover": _int(h.get("runtime_state_gas_spillover")),
            "schedule_floor_gas": _int(h.get("schedule_floor_gas")),
            "schedule_gas_refunded": _int(h.get("schedule_gas_refunded")),
            "baseline_total_gas_spent": _int(h.get("baseline_total_gas_spent")),
            "baseline_gas_refunded": _int(h.get("baseline_gas_refunded")),
            "state_gas_category": h.get("state_gas_category"),
            "reservoir_exhausted": h.get("reservoir_exhausted"),
        },
        "call_stack": call_stack,
        "gas_breakdown": gas_breakdown,
    }


@router.get("/search")
def search(q: str = Query(default="")):
    """Search affected contracts (EIP-7904 ∪ EIP-8037) by address prefix or name."""
    if not q or len(q) < 2:
        return []
    term = q.lower()
    rows = query(_AFFECTED_BASE_CTE + """
        SELECT addr, broken_txs_7904,
               need_higher_limit_8037, reservoir_exhausted_8037, status_changed_8037
        FROM affected_combined
        ORDER BY broken_txs_7904 DESC, need_higher_limit_8037 DESC
    """)
    results = []
    for r in rows:
        addr = (r["addr"] or "").lower()
        name_lc = label_address(addr).lower()
        if term in addr or term in name_lc:
            results.append({
                "recipient": addr,
                "name": label_address(addr),
                "broken_txs_7904": int(r["broken_txs_7904"]),
                "impact_8037": max(
                    int(r["need_higher_limit_8037"]),
                    int(r["reservoir_exhausted_8037"]),
                    int(r["status_changed_8037"]),
                ),
            })
            if len(results) >= 20:
                break
    return results


@router.get("/_debug/data-audit")
def debug_data_audit():
    """One-shot audit of upstream replay-data quality.

    Profiles every numeric / string / boolean column on the derived
    tables that feed the web UI in a single batched SELECT per table,
    and auto-flags fields that look broken (uniformly NULL, all zero,
    all empty, single distinct value, always-true / always-false)."""

    SCOPED_TABLES = [
        "normalized_forensics",
        "eip8037_tx_impact",
        "eip8037_contract_impact",
        "hot_7904",
    ]
    SKIP_TYPES = ("JSON", "STRUCT", "MAP", "LIST", "[]", "UNION")

    def is_numeric(t: str) -> bool:
        u = t.upper()
        return any(k in u for k in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "REAL"))

    def is_string(t: str) -> bool:
        u = t.upper()
        return "VARCHAR" in u or "TEXT" in u or "CHAR" in u

    def is_bool(t: str) -> bool:
        return "BOOL" in t.upper()

    def profile_table(table):
        """One scan, all column profiles."""
        try:
            cols = query(f"DESCRIBE {table}")
        except Exception as e:
            return [{
                "table": table, "field": "<table>", "kind": "table",
                "verdict": f"ERROR: {str(e)[:120]}",
            }]
        col_meta = []  # (name, kind)
        aggs = ["count(*) AS total"]
        for col in cols:
            name = col["column_name"]
            ctype = col["column_type"]
            if any(s in ctype.upper() for s in SKIP_TYPES):
                continue
            qf = f'"{name}"'
            if is_numeric(ctype):
                aggs += [
                    f'count({qf}) AS "{name}__non_null"',
                    f'sum(CASE WHEN {qf} = 0 THEN 1 ELSE 0 END) AS "{name}__zero"',
                    f'sum(CASE WHEN {qf} > 0 THEN 1 ELSE 0 END) AS "{name}__positive"',
                    f'sum(CASE WHEN {qf} < 0 THEN 1 ELSE 0 END) AS "{name}__negative"',
                    f'min({qf}) AS "{name}__min"',
                    f'max({qf}) AS "{name}__max"',
                    f'avg({qf}) AS "{name}__avg"',
                ]
                col_meta.append((name, "numeric", ctype))
            elif is_string(ctype):
                aggs += [
                    f'count({qf}) AS "{name}__non_null"',
                    f"sum(CASE WHEN {qf} = '' THEN 1 ELSE 0 END) AS \"{name}__empty\"",
                    f'min({qf}) AS "{name}__min"',
                    f'max({qf}) AS "{name}__max"',
                ]
                col_meta.append((name, "string", ctype))
            elif is_bool(ctype):
                aggs += [
                    f'sum(CASE WHEN {qf} THEN 1 ELSE 0 END) AS "{name}__true"',
                    f'sum(CASE WHEN NOT {qf} THEN 1 ELSE 0 END) AS "{name}__false"',
                    f'count(*) FILTER (WHERE {qf} IS NULL) AS "{name}__null"',
                ]
                col_meta.append((name, "bool", ctype))
        if not col_meta:
            return []
        try:
            row = query(f"SELECT {', '.join(aggs)} FROM {table}")[0]
        except Exception as e:
            return [{
                "table": table, "field": "<batch>", "kind": "batch",
                "verdict": f"ERROR: {str(e)[:200]}",
            }]
        total = _int(row["total"])
        results = []
        for name, kind, ctype in col_meta:
            if kind == "numeric":
                non_null = _int(row[f"{name}__non_null"])
                zero = _int(row[f"{name}__zero"])
                pos = _int(row[f"{name}__positive"])
                neg = _int(row[f"{name}__negative"])
                min_v = _float_or_none(row[f"{name}__min"])
                max_v = _float_or_none(row[f"{name}__max"])
                avg_v = _float_or_none(row[f"{name}__avg"])
                if non_null == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif min_v == max_v:
                    verdict = f"CONSTANT ({min_v})"
                elif zero == non_null:
                    verdict = "ALL ZERO"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "numeric",
                    "total": total, "non_null": non_null,
                    "zero": zero, "positive": pos, "negative": neg,
                    "min": min_v, "max": max_v, "avg": avg_v,
                    "verdict": verdict,
                })
            elif kind == "string":
                non_null = _int(row[f"{name}__non_null"])
                empty = _int(row[f"{name}__empty"])
                min_v = row[f"{name}__min"]
                max_v = row[f"{name}__max"]
                if non_null == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif empty == non_null:
                    verdict = "ALL EMPTY"
                elif min_v == max_v:
                    verdict = f"CONSTANT ({str(min_v)[:50]})"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "string",
                    "total": total, "non_null": non_null, "empty": empty,
                    "min": str(min_v)[:80] if min_v is not None else None,
                    "max": str(max_v)[:80] if max_v is not None else None,
                    "verdict": verdict,
                })
            elif kind == "bool":
                t_n = _int(row[f"{name}__true"])
                f_n = _int(row[f"{name}__false"])
                null_n = _int(row[f"{name}__null"])
                if t_n == 0 and f_n == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif t_n == 0:
                    verdict = "ALWAYS FALSE"
                elif f_n == 0:
                    verdict = "ALWAYS TRUE"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "bool",
                    "total": total, "true": t_n, "false": f_n, "null": null_n,
                    "verdict": verdict,
                })
        return results

    checks = []
    for table in SCOPED_TABLES:
        checks.extend(profile_table(table))

    # Cross-field invariants
    def safe_count(sql):
        try:
            row = query(sql)[0]
            return _int(row[list(row.keys())[0]])
        except Exception as e:
            return f"ERROR: {str(e)[:100]}"

    invariants = [
        {
            "table": "hot_7904",
            "name": "gas_delta == schedule_gas_used - baseline_gas_used",
            "violations": safe_count("""
                SELECT count(*) FROM hot_7904
                WHERE gas_delta IS NOT NULL
                  AND schedule_gas_used IS NOT NULL
                  AND baseline_gas_used IS NOT NULL
                  AND gas_delta <> schedule_gas_used - baseline_gas_used
            """),
        },
        {
            "table": "hot_7904",
            "name": "status_changed iff baseline_success <> schedule_success",
            "violations": safe_count("""
                SELECT count(*) FROM hot_7904
                WHERE status_changed <> (baseline_success <> schedule_success)
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "runtime_state_gas_spillover <= runtime_state_gas",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE runtime_state_gas_spillover > runtime_state_gas
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "spillover == GREATEST(0, runtime_state_gas - schedule_initial_reservoir)",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE runtime_state_gas_spillover <>
                      GREATEST(0, runtime_state_gas - schedule_initial_reservoir)
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "would_fit_in_original_limit implies schedule_success",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE would_fit_in_original_limit AND NOT schedule_success
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "original_limit_failure implies NOT would_fit_in_original_limit",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE original_limit_failure AND would_fit_in_original_limit
            """),
        },
    ]

    return {"checks": checks, "invariants": invariants}


@router.get("/_debug/divergence-sample")
def debug_divergence_sample():
    """Diagnostic: confirm whether the numeric divergence_opcode column
    is populated and surface a few raw divergence_location strings so we
    can see the Rust serializer format. Remove once verified."""
    counts = query("""
        SELECT
            count(*) AS rows_total,
            count(*) FILTER (WHERE divergence_opcode IS NOT NULL) AS with_opcode_int,
            count(*) FILTER (WHERE divergence_opcode_name <> '' AND divergence_opcode_name IS NOT NULL) AS with_opcode_name,
            count(*) FILTER (WHERE divergence_call_depth IS NOT NULL) AS with_call_depth
        FROM normalized_forensics
    """)[0]
    samples = query("""
        SELECT divergence_location, oog_info
        FROM artifacts_7904
        WHERE divergence_location IS NOT NULL
        LIMIT 3
    """)
    top_opcodes = query("""
        SELECT divergence_opcode AS op_num, count(*) AS cnt
        FROM normalized_forensics
        WHERE divergence_opcode IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)
    return {
        "counts": {k: _int(v) for k, v in counts.items()},
        "samples": [
            {
                "divergence_location": s["divergence_location"],
                "oog_info": s["oog_info"],
            }
            for s in samples
        ],
        "top_opcodes_int": [
            {
                "opcode_int": _int(r["op_num"]),
                "opcode_hex": f"0x{int(r['op_num']):02x}",
                "name": EIP7904_OPCODE_INT_NAMES.get(int(r["op_num"]), f"0x{int(r['op_num']):02x}"),
                "count": _int(r["cnt"]),
            }
            for r in top_opcodes
        ],
    }


@router.get("/metadata")
def metadata():
    block_range = query("SELECT min(block_number) as mn, max(block_number) as mx FROM hot_7904")
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}
    affected_count = query_scalar(
        _AFFECTED_BASE_CTE + " SELECT count(*) FROM affected_combined",
        default=0,
    )
    return {
        "schedule_name": SCHEDULE_NAME,
        "min_block": int(br["mn"]) if br["mn"] else 0,
        "max_block": int(br["mx"]) if br["mx"] else 0,
        "last_updated": db_mtime().isoformat(),
        "total_contracts_affected": _int(affected_count),
    }
