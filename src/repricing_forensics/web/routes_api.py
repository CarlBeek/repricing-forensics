"""JSON API endpoints for the EIP-7904 analysis web server."""
from __future__ import annotations

from fastapi import APIRouter, Query

from .db import (
    db_mtime,
    label_address,
    query,
    query_df,
    query_scalar,
    read_csv,
)

router = APIRouter(prefix="/api")

FORENSIC_OPCODE_NAMES = {
    "0x04": "DIV",
    "0x05": "SDIV",
    "0x06": "MOD",
    "0x07": "SMOD",
    "0x08": "ADDMOD",
    "0x09": "MULMOD",
    "0x20": "KECCAK256",
}


# ── Briefing endpoints ────────────────────────────────────────────────


@router.get("/overview")
def overview():
    total_divergent = query_scalar("SELECT count(*) FROM hot_7904")
    broken = query_scalar("SELECT count(*) FROM hot_7904 WHERE status_changed")
    total_analyzed = query_scalar("SELECT sum(tx_count) FROM coverage_7904")
    return {
        "total_analyzed": int(total_analyzed),
        "divergent_txs": int(total_divergent),
        "broken_txs": int(broken),
        "breakage_rate": round(broken / total_analyzed * 100, 2) if total_analyzed else 0,
    }


@router.get("/funnel")
def funnel():
    total = query_scalar("SELECT count(*) FROM hot_7904")
    broken = query_scalar("SELECT count(*) FROM hot_7904 WHERE status_changed")
    event_log_changed = query_scalar(
        "SELECT count(*) FROM hot_7904 WHERE event_logs_changed AND NOT status_changed"
    )
    cost_only = total - broken - event_log_changed
    return {
        "divergent_txs": int(total),
        "cost_only": int(cost_only),
        "event_log_changed": int(event_log_changed),
        "broken_txs": int(broken),
    }


@router.get("/opcode-impact")
def opcode_impact():
    rows = query("""
        SELECT divergence_opcode_name as opcode, count(*) as cnt
        FROM normalized_forensics
        WHERE divergence_opcode_name IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC
    """)
    total = sum(r["cnt"] for r in rows)
    return [
        {
            "opcode": r["opcode"],
            "name": FORENSIC_OPCODE_NAMES.get(r["opcode"], r["opcode"]),
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
    df = query_df("""
        SELECT recipient, count(*) as broken_txs
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC
    """)
    df["cumulative"] = df["broken_txs"].cumsum()
    total = df["broken_txs"].sum()
    df["cum_pct"] = df["cumulative"] / total * 100
    return [
        {
            "rank": i + 1,
            "recipient": row["recipient"],
            "name": label_address(row["recipient"]),
            "broken_txs": int(row["broken_txs"]),
            "cum_pct": round(float(row["cum_pct"]), 2),
        }
        for i, row in df.head(50).iterrows()
    ]


@router.get("/top-contracts")
def top_contracts(limit: int = Query(default=10, le=500)):
    rows = query(f"""
        SELECT recipient, count(*) as broken_txs,
               avg(gas_delta) as avg_delta, sum(gas_delta) as total_delta
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT {int(limit)}
    """)
    return [
        {
            "recipient": r["recipient"],
            "name": label_address(r["recipient"]),
            "broken_txs": int(r["broken_txs"]),
            "avg_delta": float(r["avg_delta"]),
            "total_delta": float(r["total_delta"]),
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
    stats = query("""
        SELECT
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_delta) as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM hot_7904 WHERE status_changed
    """)[0]
    histogram = query("""
        WITH bucketed AS (
            SELECT
                CASE WHEN gas_delta <= 0 THEN 0
                     ELSE floor(log2(gas_delta))::int
                END AS log_bin,
                count(*) AS cnt
            FROM hot_7904 WHERE status_changed
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
    df = read_csv("call_depth_distribution.csv")
    if df.empty:
        return []
    return df.to_dict(orient="records")


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


# ── Affected parties endpoints ────────────────────────────────────────


@router.get("/affected")
def affected(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
):
    """Paginated affected contracts, ordered by broken_txs desc."""
    offset = (page - 1) * per_page
    total_count = query_scalar(
        "SELECT count(DISTINCT recipient) FROM hot_7904 WHERE status_changed",
        default=0,
    )
    rows = query(f"""
        SELECT recipient,
               count(*) as broken_txs,
               avg(gas_delta) as avg_delta,
               sum(gas_delta) as total_delta,
               min(block_number) as min_block,
               max(block_number) as max_block
        FROM hot_7904
        WHERE status_changed
        GROUP BY recipient
        ORDER BY broken_txs DESC
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
        name = label_address(r["recipient"])
        info = outreach_dict.get(name, {})
        items.append({
            "recipient": r["recipient"],
            "name": name,
            "broken_txs": int(r["broken_txs"]),
            "avg_delta": float(r["avg_delta"]),
            "total_delta": float(r["total_delta"]),
            "min_block": int(r["min_block"]),
            "max_block": int(r["max_block"]),
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


@router.get("/affected/{address}")
def affected_detail(address: str):
    """Single contract detail with sample transactions."""
    addr = address.lower()
    stats = query(f"""
        SELECT count(*) as broken_txs,
               avg(gas_delta) as avg_delta,
               sum(gas_delta) as total_delta,
               min(block_number) as min_block,
               max(block_number) as max_block
        FROM hot_7904
        WHERE status_changed AND lower(recipient) = '{addr}'
    """)
    if not stats or stats[0]["broken_txs"] == 0:
        return {"found": False, "address": address}

    s = stats[0]
    txs = query(f"""
        SELECT tx_hash, block_number, gas_delta
        FROM hot_7904
        WHERE status_changed AND lower(recipient) = '{addr}'
        ORDER BY gas_delta DESC
        LIMIT 50
    """)

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
        "found": True,
        "address": addr,
        "name": name,
        "broken_txs": int(s["broken_txs"]),
        "avg_delta": float(s["avg_delta"]),
        "total_delta": float(s["total_delta"]),
        "min_block": int(s["min_block"]),
        "max_block": int(s["max_block"]),
        "owner": info.get("owner_buckets", ""),
        "remediation": info.get("remediation_buckets", ""),
        "transactions": [
            {
                "tx_hash": t["tx_hash"],
                "block_number": int(t["block_number"]),
                "gas_delta": float(t["gas_delta"]),
            }
            for t in txs
        ],
    }


@router.get("/search")
def search(q: str = Query(default="")):
    """Search contracts by address prefix or project name."""
    if not q or len(q) < 2:
        return []
    term = q.lower()
    rows = query(f"""
        SELECT recipient, count(*) as broken_txs
        FROM hot_7904
        WHERE status_changed
        GROUP BY recipient
        ORDER BY broken_txs DESC
    """)
    results = []
    for r in rows:
        addr = (r["recipient"] or "").lower()
        name = label_address(addr).lower()
        if term in addr or term in name:
            results.append({
                "recipient": r["recipient"],
                "name": label_address(r["recipient"]),
                "broken_txs": int(r["broken_txs"]),
            })
            if len(results) >= 20:
                break
    return results


@router.get("/metadata")
def metadata():
    block_range = query("SELECT min(block_number) as mn, max(block_number) as mx FROM hot_7904")
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}
    affected_count = query_scalar(
        "SELECT count(DISTINCT recipient) FROM hot_7904 WHERE status_changed"
    )
    return {
        "min_block": int(br["mn"]) if br["mn"] else 0,
        "max_block": int(br["mx"]) if br["mx"] else 0,
        "last_updated": db_mtime().isoformat(),
        "total_contracts_affected": int(affected_count),
    }
