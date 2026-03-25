#!/usr/bin/env python3
"""Build 3 self-contained HTML reports for EIP-7904 repricing analysis.

Outputs:
  artifacts/reports/acd_briefing.html        — ACD presentation material
  artifacts/reports/breakage_forensics.html   — Technical breakage deep-dive
  artifacts/reports/affected_parties.html     — Per-contract outreach data

Usage:
  python scripts/build_html_reports.py
"""
from __future__ import annotations

import html
import json
import sys
from pathlib import Path

# Ensure project root is on sys.path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "notebooks"))

import duckdb
import pandas as pd
import plotly.graph_objects as go

from helpers import (
    COLORS,
    DIFFICULTY_COLORS,
    SANKEY_PALETTE,
    build_sankey_data,
    fmt_count,
    fmt_gas,
    fmt_pct,
    plotly_layout,
)
from repricing_forensics.config import default_paths
from repricing_forensics.duckdb_utils import connect
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS, infer_project_label

# ── Paths ─────────────────────────────────────────────────────────────

_paths = default_paths()
TABLES_DIR = _paths.artifacts_dir / "tables"
REPORTS_DIR = _paths.artifacts_dir / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
LABELS_CSV = _paths.cache_dir / "contract_labels.csv"

# ── Load comprehensive labels ─────────────────────────────────────────

CONTRACT_LABELS: dict[str, str] = {}
if LABELS_CSV.exists():
    _labels_df = pd.read_csv(LABELS_CSV)
    for _, row in _labels_df.iterrows():
        CONTRACT_LABELS[str(row["address"]).lower()] = row["name"]

# ── DuckDB connection ─────────────────────────────────────────────────

conn = connect(_paths.duckdb_path)


def query(sql: str) -> pd.DataFrame:
    return conn.execute(sql).df()


def query_scalar(sql: str):
    return conn.execute(sql).fetchone()[0]


def read_table(name: str) -> pd.DataFrame:
    return pd.read_csv(TABLES_DIR / name)


# ── Opcode name map ──────────────────────────────────────────────────

OPCODE_NAMES = {
    0x04: "DIV",
    0x05: "SDIV",
    0x06: "MOD",
    0x07: "SMOD",
    0x08: "ADDMOD",
    0x09: "MULMOD",
    0x20: "KECCAK256",
}

FORENSIC_OPCODE_NAMES = {
    "0x04": "DIV",
    "0x05": "SDIV",
    "0x06": "MOD",
    "0x07": "SMOD",
    "0x08": "ADDMOD",
    "0x09": "MULMOD",
    "0x20": "KECCAK256",
}


# ── Plotly → HTML helper ─────────────────────────────────────────────

def fig_to_html(fig: go.Figure) -> str:
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


def label_address(addr: str) -> str:
    """Return project label for an address, or the address itself."""
    if addr is None:
        return "unknown"
    norm = addr.lower()
    # Check comprehensive labels first, then fall back to hardcoded
    return CONTRACT_LABELS.get(norm, ADDRESS_PROJECT_LABELS.get(norm, norm))


# ── CSS ───────────────────────────────────────────────────────────────

COMMON_CSS = """
<style>
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    max-width: 1200px; margin: 0 auto; padding: 20px 30px;
    color: #2c3e50; background: #fafbfc; line-height: 1.6;
  }
  h1 { color: #e74c3c; border-bottom: 3px solid #e74c3c; padding-bottom: 10px; }
  h2 { color: #34495e; margin-top: 40px; border-bottom: 1px solid #ddd; padding-bottom: 6px; }
  h3 { color: #555; }
  .poster-grid {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin: 20px 0;
  }
  .poster-card {
    background: white; border-radius: 12px; padding: 24px; text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-top: 4px solid #e74c3c;
  }
  .poster-card .number { font-size: 2.4em; font-weight: 700; color: #e74c3c; }
  .poster-card .label { font-size: 0.9em; color: #7f8c8d; margin-top: 4px; }
  table {
    width: 100%; border-collapse: collapse; margin: 16px 0;
    background: white; border-radius: 8px; overflow: hidden;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  th {
    background: #34495e; color: white; padding: 10px 14px;
    text-align: left; font-weight: 600; font-size: 0.9em;
    cursor: pointer; user-select: none; white-space: nowrap;
  }
  th:hover { background: #4a6580; }
  td { padding: 8px 14px; border-bottom: 1px solid #ecf0f1; font-size: 0.9em; }
  tr:hover td { background: #f8f9fa; }
  .tag {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.8em; font-weight: 600;
  }
  .tag-red { background: #fde8e8; color: #e74c3c; }
  .tag-orange { background: #fef3e2; color: #e67e22; }
  .tag-green { background: #e8f8f0; color: #27ae60; }
  .tag-blue { background: #e8f0fe; color: #3498db; }
  .tag-purple { background: #f0e8fe; color: #8e44ad; }
  .funnel-step {
    display: flex; align-items: center; gap: 12px; margin: 8px 0;
  }
  .funnel-bar {
    height: 36px; border-radius: 4px; display: flex; align-items: center;
    padding: 0 12px; color: white; font-weight: 600; font-size: 0.95em;
    min-width: 60px;
  }
  .funnel-label { font-size: 0.9em; color: #555; min-width: 200px; }
  .search-box {
    width: 100%; padding: 10px 14px; border: 2px solid #ddd; border-radius: 8px;
    font-size: 1em; margin-bottom: 16px;
  }
  .search-box:focus { outline: none; border-color: #3498db; }
  .mono { font-family: 'SF Mono', 'Consolas', monospace; font-size: 0.85em; }
  .copy-btn {
    background: none; border: 1px solid #ddd; border-radius: 4px;
    padding: 2px 6px; cursor: pointer; font-size: 0.75em; color: #777;
  }
  .copy-btn:hover { background: #f0f0f0; }
  details { margin: 10px 0; }
  summary {
    cursor: pointer; font-weight: 600; padding: 8px 12px;
    background: #f8f9fa; border-radius: 6px; border: 1px solid #e0e0e0;
  }
  summary:hover { background: #ecf0f1; }
  .detail-content { padding: 12px 16px; }
  a { color: #3498db; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .subtitle { color: #7f8c8d; font-size: 0.95em; margin-top: -10px; }
  .note { color: #95a5a6; font-size: 0.85em; font-style: italic; }
  @media (max-width: 800px) {
    .poster-grid { grid-template-columns: repeat(2, 1fr); }
  }
</style>
"""


def html_page(title: str, subtitle: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
{COMMON_CSS}
</head>
<body>
<h1>{html.escape(title)}</h1>
<p class="subtitle">{html.escape(subtitle)}</p>
{body}
<hr style="margin-top:40px">
<p class="note">Generated by build_html_reports.py &mdash; EIP-7904 Advanced Repricing Analysis</p>
</body>
</html>"""


# ======================================================================
# Report 1: ACD Briefing
# ======================================================================

def build_acd_briefing() -> str:
    print("Building Report 1: ACD Briefing...")
    sections = []

    # ── 1. Poster stats ──
    total_txs = query_scalar("SELECT count(*) FROM hot_7904")
    divergent_txs = total_txs  # all rows in hot_7904 are divergent
    broken_txs = query_scalar("SELECT count(*) FROM hot_7904 WHERE status_changed")
    breakage_rate = broken_txs / divergent_txs * 100

    # Get total txs analyzed (from coverage)
    total_analyzed = query_scalar("SELECT sum(tx_count) FROM coverage_7904")

    sections.append(f"""
    <h2>1. Headline Numbers</h2>
    <div class="poster-grid">
      <div class="poster-card">
        <div class="number">{fmt_count(total_analyzed)}</div>
        <div class="label">Total Txs Analyzed</div>
      </div>
      <div class="poster-card">
        <div class="number">{fmt_count(divergent_txs)}</div>
        <div class="label">Divergent Txs</div>
      </div>
      <div class="poster-card">
        <div class="number">{fmt_count(broken_txs)}</div>
        <div class="label">Broken Txs<br>(success→fail)</div>
      </div>
      <div class="poster-card">
        <div class="number">{fmt_pct(breakage_rate)}</div>
        <div class="label">Breakage Rate<br>(of divergent)</div>
      </div>
    </div>
    """)

    # ── 2. The funnel ──
    cost_only = divergent_txs - broken_txs
    event_log_changed = query_scalar(
        "SELECT count(*) FROM hot_7904 WHERE event_logs_changed AND NOT status_changed"
    )
    call_tree_only = cost_only - event_log_changed
    sections.append(f"""
    <h2>2. The Funnel</h2>
    <p>How 5.6M divergent transactions break down:</p>
    <div style="margin: 20px 0;">
      <div class="funnel-step">
        <div class="funnel-label">All divergent txs</div>
        <div class="funnel-bar" style="width:100%; background:{COLORS['changed']}">{fmt_count(divergent_txs)}</div>
      </div>
      <div class="funnel-step">
        <div class="funnel-label">Cost-only change (gas increased)</div>
        <div class="funnel-bar" style="width:{cost_only/divergent_txs*100:.0f}%; background:{COLORS['saved']}">{fmt_count(cost_only)}</div>
      </div>
      <div class="funnel-step">
        <div class="funnel-label">Event logs also changed</div>
        <div class="funnel-bar" style="width:{event_log_changed/divergent_txs*100:.0f}%; background:#f39c12">{fmt_count(event_log_changed)}</div>
      </div>
      <div class="funnel-step">
        <div class="funnel-label"><strong>Broken (success → fail)</strong></div>
        <div class="funnel-bar" style="width:max(3%,{broken_txs/divergent_txs*100:.1f}%); background:{COLORS['broken']}">{fmt_count(broken_txs)}</div>
      </div>
    </div>
    <p class="note">{fmt_pct(100 - breakage_rate)} of divergent transactions keep the same success/fail status — they just cost slightly more gas.</p>
    """)

    # ── 3. Opcode impact table ──
    forensics_opcode = query("""
        SELECT divergence_opcode_name as opcode, count(*) as cnt
        FROM normalized_forensics
        WHERE divergence_opcode_name IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC
    """)
    total_forensic = forensics_opcode["cnt"].sum()
    rows_html = ""
    for _, row in forensics_opcode.iterrows():
        name = FORENSIC_OPCODE_NAMES.get(row["opcode"], row["opcode"])
        pct = row["cnt"] / total_forensic * 100
        rows_html += f"<tr><td><strong>{name}</strong></td><td>{fmt_count(row['cnt'])}</td><td>{pct:.1f}%</td></tr>\n"

    sections.append(f"""
    <h2>3. Opcode Impact</h2>
    <p>Which repriced opcodes cause divergence at the point of failure (from forensic trace analysis):</p>
    <table>
      <thead><tr><th>Opcode</th><th>Divergent Txs</th><th>Share</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    <p class="note">Based on {fmt_count(total_forensic)} transactions with forensic trace data.</p>
    """)

    # ── 4. Gas overhead for non-broken txs ──
    gas_stats = query("""
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
    """).iloc[0]

    # Gas overhead histogram
    gas_hist = query("""
        SELECT
            CASE
                WHEN gas_delta < 50 THEN '<50'
                WHEN gas_delta < 100 THEN '50-100'
                WHEN gas_delta < 200 THEN '100-200'
                WHEN gas_delta < 500 THEN '200-500'
                WHEN gas_delta < 1000 THEN '500-1K'
                WHEN gas_delta < 5000 THEN '1K-5K'
                WHEN gas_delta < 10000 THEN '5K-10K'
                ELSE '10K+'
            END as bucket,
            count(*) as cnt
        FROM hot_7904
        WHERE NOT status_changed
        GROUP BY 1
    """)
    bucket_order = ["<50", "50-100", "100-200", "200-500", "500-1K", "1K-5K", "5K-10K", "10K+"]
    gas_hist["bucket"] = pd.Categorical(gas_hist["bucket"], categories=bucket_order, ordered=True)
    gas_hist = gas_hist.sort_values("bucket")

    fig_gas = go.Figure(data=[go.Bar(
        x=gas_hist["bucket"], y=gas_hist["cnt"],
        marker_color=COLORS["changed"],
    )])
    fig_gas.update_layout(**plotly_layout(
        title_text="Gas Overhead Distribution (Non-Broken Txs)",
        xaxis_title="Additional Gas (delta)",
        yaxis_title="Transaction Count",
        height=400,
    ))

    sections.append(f"""
    <h2>4. Gas Overhead for Non-Broken Txs</h2>
    <p>For the {fmt_count(int(gas_stats['cnt']))} transactions that don't break:</p>
    <div class="poster-grid" style="grid-template-columns: repeat(4, 1fr);">
      <div class="poster-card" style="border-top-color: {COLORS['changed']}">
        <div class="number" style="color:{COLORS['changed']}">{fmt_gas(gas_stats['median_delta'])}</div>
        <div class="label">Median Extra Gas</div>
      </div>
      <div class="poster-card" style="border-top-color: {COLORS['changed']}">
        <div class="number" style="color:{COLORS['changed']}">{fmt_gas(gas_stats['mean_delta'])}</div>
        <div class="label">Mean Extra Gas</div>
      </div>
      <div class="poster-card" style="border-top-color: {COLORS['changed']}">
        <div class="number" style="color:{COLORS['changed']}">{fmt_gas(gas_stats['p95'])}</div>
        <div class="label">95th Percentile</div>
      </div>
      <div class="poster-card" style="border-top-color: {COLORS['changed']}">
        <div class="number" style="color:{COLORS['changed']}">{fmt_gas(gas_stats['p99'])}</div>
        <div class="label">99th Percentile</div>
      </div>
    </div>
    {fig_to_html(fig_gas)}
    <p class="note">The vast majority of transactions see minimal gas increase. Complexity does not correlate with gas delta.</p>
    """)

    # ── 5. Concentration curve ──
    top_contracts = query("""
        SELECT recipient, count(*) as broken_txs
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC
    """)
    top_contracts["cumulative"] = top_contracts["broken_txs"].cumsum()
    top_contracts["cum_pct"] = top_contracts["cumulative"] / top_contracts["broken_txs"].sum() * 100
    top_contracts["rank"] = range(1, len(top_contracts) + 1)

    # Find where 95% is reached
    idx_95 = (top_contracts["cum_pct"] >= 95).idxmax()
    n_95 = top_contracts.loc[idx_95, "rank"]

    fig_conc = go.Figure()
    fig_conc.add_trace(go.Scatter(
        x=top_contracts["rank"].head(50),
        y=top_contracts["cum_pct"].head(50),
        mode="lines+markers",
        marker=dict(size=5, color=COLORS["broken"]),
        line=dict(color=COLORS["broken"], width=2),
        name="Cumulative %",
    ))
    fig_conc.add_hline(y=95, line_dash="dash", line_color="#95a5a6",
                       annotation_text="95%", annotation_position="top left")
    fig_conc.add_vline(x=n_95, line_dash="dash", line_color="#95a5a6")
    fig_conc.update_layout(**plotly_layout(
        title_text="Breakage Concentration: Top Contracts",
        xaxis_title="Number of Contracts (ranked by breakage)",
        yaxis_title="Cumulative % of Broken Txs",
        height=420,
    ))

    sections.append(f"""
    <h2>5. Breakage Concentration</h2>
    <p>Breakage is highly concentrated: <strong>top {n_95} contracts account for 95%</strong> of all broken transactions.</p>
    {fig_to_html(fig_conc)}
    """)

    # ── 6. Top 10 affected contracts ──
    top10 = query("""
        SELECT recipient, count(*) as broken_txs,
               avg(gas_delta) as avg_delta, sum(gas_delta) as total_delta
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT 10
    """)

    rows_html = ""
    for i, row in top10.iterrows():
        name = label_address(row["recipient"])
        addr_short = row["recipient"][:10] + "..." if row["recipient"] else "?"
        rows_html += f"""<tr>
          <td>{i+1}</td>
          <td><strong>{html.escape(name)}</strong><br><span class="mono" style="color:#999">{addr_short}</span></td>
          <td style="text-align:right">{fmt_count(row['broken_txs'])}</td>
          <td style="text-align:right">{fmt_gas(row['avg_delta'])}</td>
          <td style="text-align:right">{fmt_gas(row['total_delta'])}</td>
        </tr>\n"""

    sections.append(f"""
    <h2>6. Top 10 Affected Contracts</h2>
    <table>
      <thead><tr><th>#</th><th>Contract</th><th>Broken Txs</th><th>Avg Gas Δ</th><th>Total Gas Δ</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    """)

    body = "\n".join(sections)
    block_range = query("SELECT min(block_number), max(block_number) FROM hot_7904").iloc[0]
    subtitle = f"EIP-7904 Repricing Impact — Blocks {block_range.iloc[0]:,}–{block_range.iloc[1]:,}"
    return html_page("EIP-7904 ACD Briefing", subtitle, body)


# ======================================================================
# Report 2: Breakage Forensics
# ======================================================================

def build_breakage_forensics() -> str:
    print("Building Report 2: Breakage Forensics...")
    sections = []

    broken_txs = query_scalar("SELECT count(*) FROM hot_7904 WHERE status_changed")

    # ── 1. Status change overview ──
    # Time series bucketed into ~300 groups for reasonable chart size
    ts = query("""
        WITH bounds AS (
            SELECT min(block_number) as mn, max(block_number) as mx
            FROM hot_7904 WHERE status_changed
        )
        SELECT
            mn + ((block_number - mn) // ((mx - mn) / 300)) * ((mx - mn) / 300) as block_group,
            count(*) as broken
        FROM hot_7904, bounds
        WHERE status_changed
        GROUP BY block_group ORDER BY block_group
    """)

    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(
        x=ts["block_group"], y=ts["broken"],
        mode="lines", fill="tozeroy",
        line=dict(color=COLORS["broken"], width=1),
        fillcolor="rgba(231,76,60,0.2)",
    ))
    fig_ts.update_layout(**plotly_layout(
        title_text="Broken Transactions per Block",
        xaxis_title="Block Number", yaxis_title="Broken Txs",
        height=380,
    ))

    sections.append(f"""
    <h2>1. Status Change Overview</h2>
    <p><strong>{fmt_count(broken_txs)}</strong> transactions change from success to failure under EIP-7904 repricing.</p>
    {fig_to_html(fig_ts)}
    """)

    # ── 2. Gas delta distribution ──
    gas_stats = query("""
        SELECT
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_delta) as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM hot_7904 WHERE status_changed
    """).iloc[0]

    broken_gas = query("""
        SELECT
            CASE
                WHEN gas_delta < 1000 THEN '<1K'
                WHEN gas_delta < 5000 THEN '1K-5K'
                WHEN gas_delta < 10000 THEN '5K-10K'
                WHEN gas_delta < 50000 THEN '10K-50K'
                WHEN gas_delta < 100000 THEN '50K-100K'
                ELSE '100K+'
            END as bucket,
            count(*) as cnt
        FROM hot_7904 WHERE status_changed
        GROUP BY 1
    """)
    bucket_order = ["<1K", "1K-5K", "5K-10K", "10K-50K", "50K-100K", "100K+"]
    broken_gas["bucket"] = pd.Categorical(broken_gas["bucket"], categories=bucket_order, ordered=True)
    broken_gas = broken_gas.sort_values("bucket")

    fig_hist = go.Figure(data=[go.Bar(
        x=broken_gas["bucket"], y=broken_gas["cnt"],
        marker_color=COLORS["broken"],
    )])
    fig_hist.update_layout(**plotly_layout(
        title_text="Gas Delta Distribution (Broken Txs)",
        xaxis_title="Gas Delta", yaxis_title="Count",
        height=380,
    ))

    percentile_rows = ""
    for label, val in [("Median", gas_stats["median_delta"]),
                       ("25th", gas_stats["p25"]), ("75th", gas_stats["p75"]),
                       ("90th", gas_stats["p90"]), ("95th", gas_stats["p95"]),
                       ("99th", gas_stats["p99"])]:
        percentile_rows += f"<tr><td>{label}</td><td style='text-align:right'>{fmt_gas(val)}</td></tr>\n"

    sections.append(f"""
    <h2>2. Gas Delta Distribution</h2>
    <p>How much extra gas would broken transactions have needed to survive:</p>
    {fig_to_html(fig_hist)}
    <table style="max-width:300px">
      <thead><tr><th>Percentile</th><th>Gas Delta</th></tr></thead>
      <tbody>{percentile_rows}</tbody>
    </table>
    <p class="note">Most broken txs need &lt;10K extra gas to survive. Median: {fmt_gas(gas_stats['median_delta'])}.</p>
    """)

    # ── 3. Opcode breakdown ──
    forensics_opcode = query("""
        SELECT divergence_opcode_name as opcode, count(*) as cnt
        FROM normalized_forensics
        WHERE divergence_opcode_name IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC
    """)
    total_forensic = forensics_opcode["cnt"].sum()
    colors = ["#e74c3c", "#3498db", "#27ae60", "#f39c12", "#8e44ad", "#1abc9c"]

    opcode_labels = [FORENSIC_OPCODE_NAMES.get(r["opcode"], r["opcode"]) for _, r in forensics_opcode.iterrows()]
    fig_opcode = go.Figure(data=[go.Pie(
        labels=opcode_labels,
        values=forensics_opcode["cnt"],
        marker_colors=colors[:len(opcode_labels)],
        textinfo="label+percent",
        hole=0.4,
    )])
    fig_opcode.update_layout(**plotly_layout(
        title_text="Divergence Point — Opcode at Failure",
        height=400,
    ))

    sections.append(f"""
    <h2>3. Opcode Breakdown at Divergence Point</h2>
    {fig_to_html(fig_opcode)}
    """)

    # ── 4. Call depth distribution ──
    depth_df = read_table("call_depth_distribution.csv")
    depth_df = depth_df.sort_values("divergence_call_depth")
    depth_df["depth_label"] = depth_df["divergence_call_depth"].apply(
        lambda x: f"Depth {int(x)}" if x >= 0 else "Unknown"
    )

    total_depth = depth_df["divergent_txs"].sum()
    shallow = depth_df[depth_df["divergence_call_depth"].between(0, 1)]["divergent_txs"].sum()
    shallow_pct = shallow / total_depth * 100

    fig_depth = go.Figure(data=[go.Bar(
        x=depth_df["depth_label"],
        y=depth_df["divergent_txs"],
        marker_color=COLORS["call_tree"],
    )])
    fig_depth.update_layout(**plotly_layout(
        title_text="Call Depth at Divergence Point",
        xaxis_title="Call Depth", yaxis_title="Divergent Txs",
        height=380,
    ))

    sections.append(f"""
    <h2>4. Call Depth Distribution</h2>
    <p><strong>{shallow_pct:.0f}%</strong> of divergences occur at shallow call depth (0–1), meaning the repriced opcode
    is typically near the top of the call stack.</p>
    {fig_to_html(fig_depth)}
    """)

    # ── 5. Top 20 contracts by breakage ──
    top20 = query("""
        SELECT recipient, count(*) as broken_txs,
               avg(gas_delta) as avg_delta
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT 20
    """)
    top20["name"] = top20["recipient"].apply(label_address)

    fig_top20 = go.Figure(data=[go.Bar(
        x=top20["broken_txs"],
        y=[f"{r['name'][:25]}" for _, r in top20.iterrows()],
        orientation="h",
        marker_color=COLORS["broken"],
    )])
    fig_top20.update_layout(**plotly_layout(
        title_text="Top 20 Contracts by Broken Tx Count",
        xaxis_title="Broken Txs",
        height=600, margin=dict(l=200, r=20, t=50, b=20),
    ))
    fig_top20.update_yaxes(autorange="reversed")

    sections.append(f"""
    <h2>5. Top 20 Contracts by Breakage</h2>
    <p>Ranked by number of transactions that change from success to failure.</p>
    {fig_to_html(fig_top20)}
    """)

    # ── 6. Top failure motifs ──
    motifs = read_table("failure_motifs.csv").head(15)

    motif_rows = ""
    for _, row in motifs.iterrows():
        motif_rows += f"""<tr>
          <td>{html.escape(str(row.get('pair_motif', '')))}</td>
          <td>{html.escape(str(row.get('triple_motif', '')))}</td>
          <td style="text-align:right">{fmt_count(row['status_failures'])}</td>
          <td style="text-align:right">{fmt_gas(row.get('avg_gas_provided', 0))}</td>
        </tr>\n"""

    sections.append(f"""
    <h2>6. Top Failure Motifs</h2>
    <p>Most common caller→callee patterns in failing call frames:</p>
    <table>
      <thead><tr><th>Pair Motif</th><th>Triple Motif</th><th>Failures</th><th>Avg Gas Provided</th></tr></thead>
      <tbody>{motif_rows}</tbody>
    </table>
    """)

    # ── 7. Failure flow Sankey ──
    sankey_edges = read_table("failure_path_sankey_edges.csv")

    # Aggregate to top flows only, then build label set from just those edges
    sankey_edges["status_failures"] = pd.to_numeric(sankey_edges["status_failures"], errors="coerce").fillna(0).astype(int)
    rc = sankey_edges.groupby(["root_project", "failing_caller_project"])["status_failures"].sum().reset_index()
    rc = rc.nlargest(15, "status_failures")
    cc = sankey_edges.groupby(["failing_caller_project", "failing_callee_project"])["status_failures"].sum().reset_index()
    cc = cc.nlargest(15, "status_failures")

    # Build label set from only the edges we'll actually use
    used_labels = set()
    for _, row in rc.iterrows():
        used_labels.add(row["root_project"])
        used_labels.add(row["failing_caller_project"])
    for _, row in cc.iterrows():
        used_labels.add(row["failing_caller_project"])
        used_labels.add(row["failing_callee_project"])
    labels_list = sorted(used_labels)
    label_idx = {l: i for i, l in enumerate(labels_list)}

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

    # Shorten long hex addresses in labels for display
    display_labels = [label_address(l) if l.startswith("0x") else l for l in labels_list]
    node_colors = [SANKEY_PALETTE[i % len(SANKEY_PALETTE)] for i in range(len(labels_list))]
    fig_sankey = go.Figure(data=[go.Sankey(
        node=dict(label=display_labels, color=node_colors, pad=20, thickness=25),
        link=dict(source=sources, target=targets, value=values, color=link_colors),
    )])
    fig_sankey.update_layout(**plotly_layout(
        title_text="Failure Flow: Root → Caller → Callee",
        height=550, width=1000,
    ))

    sections.append(f"""
    <h2>7. Failure Flow Sankey</h2>
    <p>How failures propagate: root transaction contract → failing caller → failing callee.</p>
    {fig_to_html(fig_sankey)}
    """)

    # ── 8. Remediation outlook ──
    owner_summary = read_table("project_owner_summary.csv")
    # Exclude 'unknown' for cleaner view
    owner_known = owner_summary[owner_summary["owner_bucket"] != "unknown_owner"]

    # Owner bucket donut
    owner_agg = owner_known.groupby("owner_bucket")["status_changed_txs"].sum().sort_values(ascending=False)
    fig_owner = go.Figure(data=[go.Pie(
        labels=owner_agg.index.tolist(),
        values=owner_agg.values.tolist(),
        hole=0.45,
        textinfo="label+percent",
        marker_colors=colors[:len(owner_agg)],
    )])
    fig_owner.update_layout(**plotly_layout(title_text="Owner Bucket (who can fix)", height=400))

    # Remediation bucket donut
    remed_agg = owner_known.groupby("remediation_bucket")["status_changed_txs"].sum().sort_values(ascending=False)
    fig_remed = go.Figure(data=[go.Pie(
        labels=remed_agg.index.tolist(),
        values=remed_agg.values.tolist(),
        hole=0.45,
        textinfo="label+percent",
        marker_colors=colors[:len(remed_agg)],
    )])
    fig_remed.update_layout(**plotly_layout(title_text="Remediation Bucket (how to fix)", height=400))

    sections.append(f"""
    <h2>8. Remediation Outlook</h2>
    <p>For contracts with known ownership (excluding unknown):</p>
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
      <div>{fig_to_html(fig_owner)}</div>
      <div>{fig_to_html(fig_remed)}</div>
    </div>
    """)

    body = "\n".join(sections)
    return html_page(
        "EIP-7904 Breakage Forensics",
        f"{fmt_count(broken_txs)} broken transactions analyzed in depth",
        body,
    )


# ======================================================================
# Report 3: Affected Parties
# ======================================================================

def build_affected_parties() -> str:
    print("Building Report 3: Affected Parties...")
    sections = []

    # Get per-contract data with only the top 20 tx hashes (not all)
    contracts = query("""
        WITH stats AS (
            SELECT recipient,
                   count(*) as broken_txs,
                   avg(gas_delta) as avg_delta,
                   sum(gas_delta) as total_delta,
                   min(block_number) as min_block,
                   max(block_number) as max_block
            FROM hot_7904
            WHERE status_changed
            GROUP BY recipient
        ),
        top_hashes AS (
            SELECT recipient, tx_hash, gas_delta,
                   ROW_NUMBER() OVER (PARTITION BY recipient ORDER BY gas_delta DESC) as rn
            FROM hot_7904
            WHERE status_changed
        ),
        hashes_agg AS (
            SELECT recipient,
                   array_agg(tx_hash ORDER BY gas_delta DESC) FILTER (WHERE rn <= 5) as sample_hashes,
                   array_agg(tx_hash ORDER BY gas_delta DESC) FILTER (WHERE rn <= 20) as detail_hashes
            FROM top_hashes
            WHERE rn <= 20
            GROUP BY recipient
        )
        SELECT s.*, h.sample_hashes, h.detail_hashes
        FROM stats s
        LEFT JOIN hashes_agg h USING (recipient)
        ORDER BY broken_txs DESC
        LIMIT 1000
    """)

    total_broken = contracts["broken_txs"].sum()
    unique_contracts = len(contracts)

    # Load outreach priority for remediation info
    outreach = read_table("outreach_priority.csv")

    # Create address→outreach lookup
    # outreach is by project name, so we need to map recipient→project→outreach
    outreach_dict = {}
    for _, row in outreach.iterrows():
        outreach_dict[row["project"]] = {
            "priority_score": row.get("priority_score", 0),
            "owner_buckets": str(row.get("owner_buckets", "")),
            "remediation_buckets": str(row.get("remediation_buckets", "")),
        }

    # ── 1. Summary header ──
    block_range = query("SELECT min(block_number), max(block_number) FROM hot_7904 WHERE status_changed").iloc[0]

    sections.append(f"""
    <h2>Summary</h2>
    <div class="poster-grid" style="grid-template-columns: repeat(3, 1fr);">
      <div class="poster-card">
        <div class="number">{fmt_count(total_broken)}</div>
        <div class="label">Total Broken Txs</div>
      </div>
      <div class="poster-card">
        <div class="number">{fmt_count(unique_contracts)}</div>
        <div class="label">Unique Contracts Affected</div>
      </div>
      <div class="poster-card">
        <div class="number">{block_range.iloc[0]:,}–{block_range.iloc[1]:,}</div>
        <div class="label">Block Range</div>
      </div>
    </div>
    """)

    # ── 2. Searchable/sortable table ──
    table_rows = []
    for _, row in contracts.iterrows():
        addr = row["recipient"] or ""
        project = label_address(addr)
        outreach_info = outreach_dict.get(project, {})
        owner = outreach_info.get("owner_buckets", "")
        remed = outreach_info.get("remediation_buckets", "")

        # Format sample tx hashes as etherscan links
        sample_hashes = row["sample_hashes"] if row["sample_hashes"] is not None else []
        tx_links = " ".join(
            f'<a href="https://etherscan.io/tx/{h}" target="_blank" class="mono">{h[:10]}…</a>'
            for h in sample_hashes[:5]
        )

        # Owner/remediation tags
        owner_tag = ""
        if owner:
            first_owner = owner.split(";")[0]
            tag_class = "tag-blue" if "proxy" in first_owner else "tag-purple" if "direct" in first_owner else "tag-orange"
            owner_tag = f'<span class="tag {tag_class}">{html.escape(first_owner)}</span>'

        remed_tag = ""
        if remed:
            first_remed = remed.split(";")[0]
            tag_class = "tag-green" if "upgrade" in first_remed else "tag-orange" if "integration" in first_remed else "tag-red"
            remed_tag = f'<span class="tag {tag_class}">{html.escape(first_remed)}</span>'

        table_rows.append(f"""<tr data-search="{html.escape(addr.lower())} {html.escape(project.lower())}">
          <td class="mono" style="font-size:0.8em">
            {html.escape(addr)}
            <button class="copy-btn" onclick="navigator.clipboard.writeText('{html.escape(addr)}')">copy</button>
          </td>
          <td><strong>{html.escape(project)}</strong></td>
          <td style="text-align:right" data-sort="{row['broken_txs']}">{fmt_count(row['broken_txs'])}</td>
          <td style="text-align:right" data-sort="{row['avg_delta']:.0f}">{fmt_gas(row['avg_delta'])}</td>
          <td style="text-align:right" data-sort="{row['total_delta']:.0f}">{fmt_gas(row['total_delta'])}</td>
          <td>{tx_links}</td>
          <td>{remed_tag}</td>
          <td>{owner_tag}</td>
        </tr>\n""")

    table_html = "\n".join(table_rows)

    sections.append(f"""
    <h2>Affected Contracts</h2>
    <input type="text" class="search-box" id="contractSearch"
           placeholder="Search by address or project name..."
           oninput="filterTable()">
    <p class="note">Click column headers to sort. {unique_contracts} contracts shown.</p>
    <div style="overflow-x: auto;">
    <table id="contractTable">
      <thead><tr>
        <th onclick="sortTable(0)">Address</th>
        <th onclick="sortTable(1)">Project</th>
        <th onclick="sortTable(2)">Broken Txs ↕</th>
        <th onclick="sortTable(3)">Avg Gas Δ ↕</th>
        <th onclick="sortTable(4)">Total Gas Δ ↕</th>
        <th>Example Txs</th>
        <th onclick="sortTable(6)">Remediation</th>
        <th onclick="sortTable(7)">Owner</th>
      </tr></thead>
      <tbody id="contractBody">
        {table_html}
      </tbody>
    </table>
    </div>
    """)

    # ── 3. Per-contract detail sections for top 20 ──
    detail_sections = []
    for i, row in contracts.head(20).iterrows():
        addr = row["recipient"] or ""
        project = label_address(addr)
        detail_hashes = row["detail_hashes"] if row["detail_hashes"] is not None else []
        outreach_info = outreach_dict.get(project, {})

        tx_list = "\n".join(
            f'<li><a href="https://etherscan.io/tx/{h}" target="_blank" class="mono">{h}</a></li>'
            for h in detail_hashes[:20]
        )

        owner = outreach_info.get("owner_buckets", "unknown")
        remed = outreach_info.get("remediation_buckets", "unknown")

        detail_sections.append(f"""
        <details>
          <summary>
            #{i+1} — <strong>{html.escape(project)}</strong>
            ({fmt_count(row['broken_txs'])} broken txs, avg Δ {fmt_gas(row['avg_delta'])})
          </summary>
          <div class="detail-content">
            <p><strong>Address:</strong> <span class="mono">{html.escape(addr)}</span>
              <button class="copy-btn" onclick="navigator.clipboard.writeText('{html.escape(addr)}')">copy</button>
            </p>
            <p><strong>Broken Txs:</strong> {fmt_count(row['broken_txs'])} &nbsp;|&nbsp;
               <strong>Avg Gas Delta:</strong> {fmt_gas(row['avg_delta'])} &nbsp;|&nbsp;
               <strong>Total Gas Delta:</strong> {fmt_gas(row['total_delta'])}</p>
            <p><strong>Owner:</strong> {html.escape(owner)} &nbsp;|&nbsp;
               <strong>Remediation:</strong> {html.escape(remed)}</p>
            <p><strong>Example Transactions</strong> (up to 20):</p>
            <ol style="font-size:0.85em">{tx_list}</ol>
          </div>
        </details>
        """)

    sections.append(f"""
    <h2>Top 20 Contract Details</h2>
    <p>Expandable sections with full transaction lists and remediation info.</p>
    {"".join(detail_sections)}
    """)

    # ── JavaScript for sorting/filtering ──
    js = """
<script>
function filterTable() {
  const q = document.getElementById('contractSearch').value.toLowerCase();
  const rows = document.getElementById('contractBody').getElementsByTagName('tr');
  for (let row of rows) {
    const searchData = row.getAttribute('data-search') || '';
    row.style.display = searchData.includes(q) ? '' : 'none';
  }
}

let sortDir = {};
function sortTable(col) {
  const table = document.getElementById('contractTable');
  const tbody = document.getElementById('contractBody');
  const rows = Array.from(tbody.getElementsByTagName('tr'));
  const dir = sortDir[col] = !(sortDir[col] || false);

  rows.sort((a, b) => {
    let aVal = a.cells[col].getAttribute('data-sort') || a.cells[col].textContent.trim();
    let bVal = b.cells[col].getAttribute('data-sort') || b.cells[col].textContent.trim();
    // Try numeric
    const aNum = parseFloat(aVal.replace(/[^0-9.-]/g, ''));
    const bNum = parseFloat(bVal.replace(/[^0-9.-]/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return dir ? bNum - aNum : aNum - bNum;
    }
    return dir ? bVal.localeCompare(aVal) : aVal.localeCompare(bVal);
  });

  rows.forEach(row => tbody.appendChild(row));
}
</script>
"""

    body = "\n".join(sections) + js
    return html_page(
        "EIP-7904 Affected Parties — Outreach Data",
        "Per-contract breakdown for direct outreach to affected parties",
        body,
    )


# ======================================================================
# Main
# ======================================================================

def main():
    print("=" * 60)
    print("Building EIP-7904 HTML Reports")
    print("=" * 60)

    report1 = build_acd_briefing()
    path1 = REPORTS_DIR / "acd_briefing.html"
    path1.write_text(report1)
    print(f"  ✓ {path1} ({len(report1)//1024} KB)")

    report2 = build_breakage_forensics()
    path2 = REPORTS_DIR / "breakage_forensics.html"
    path2.write_text(report2)
    print(f"  ✓ {path2} ({len(report2)//1024} KB)")

    report3 = build_affected_parties()
    path3 = REPORTS_DIR / "affected_parties.html"
    path3.write_text(report3)
    print(f"  ✓ {path3} ({len(report3)//1024} KB)")

    print("\n" + "=" * 60)
    print("Done! Open in browser:")
    print(f"  open {path1}")
    print(f"  open {path2}")
    print(f"  open {path3}")
    print("=" * 60)


if __name__ == "__main__":
    main()
