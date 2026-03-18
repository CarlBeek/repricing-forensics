"""Shared notebook utilities for EIP-7904 advanced repricing analysis.

Mirrors the style of repricing-impact-analysis/helpers.py — semantic colors,
Plotly defaults, Sankey builders, DuckDB query helpers, and formatters.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go

# ── Paths ──────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = PROJECT_ROOT / "duckdb" / "eip7904.duckdb"
TABLES_DIR = PROJECT_ROOT / "artifacts" / "tables"
FIGURES_DIR = PROJECT_ROOT / "artifacts" / "figures"

# ── Colors ─────────────────────────────────────────────────────────────

COLORS = {
    "7904": "#e74c3c",
    "broken": "#e74c3c",
    "changed": "#e67e22",
    "saved": "#27ae60",
    "neutral": "#95a5a6",
    "call_tree": "#3498db",
    "status": "#e74c3c",
    "event_logs": "#f39c12",
    "increased": "#e67e22",
}

DIVERGENCE_TYPE_COLORS = {
    "status": "#e74c3c",
    "call_tree": "#3498db",
    "event_logs": "#f39c12",
}

DIFFICULTY_COLORS = {
    "Easy": "#27ae60",
    "Medium": "#f39c12",
    "Hard": "#e74c3c",
    "Very hard": "#8e44ad",
}

SANKEY_PALETTE = ["#3498db", "#e67e22", "#27ae60", "#8e44ad", "#e74c3c"]

# ── DuckDB ─────────────────────────────────────────────────────────────

_conn: duckdb.DuckDBPyConnection | None = None


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a shared read-only DuckDB connection.

    DuckDB views reference parquet files via relative paths from the project
    root, so we chdir there before opening the connection.
    """
    global _conn
    if _conn is None:
        os.chdir(PROJECT_ROOT)
        _conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        _conn.execute("PRAGMA threads=8")
    return _conn


def query(sql: str) -> pd.DataFrame:
    """Execute *sql* against the DuckDB and return a DataFrame."""
    return get_conn().execute(sql).df()


def query_scalar(sql: str):
    """Execute *sql* and return the single scalar result."""
    return get_conn().execute(sql).fetchone()[0]


def read_table(name: str) -> pd.DataFrame:
    """Read a CSV from artifacts/tables/."""
    return pd.read_csv(TABLES_DIR / name)


# ── Formatting ─────────────────────────────────────────────────────────


def fmt_gas(n: float | int) -> str:
    """Format a gas number for display: 1.2B, 45.3M, 7.8K, etc."""
    n = float(n)
    if abs(n) >= 1e12:
        return f"{n / 1e12:.1f}T"
    if abs(n) >= 1e9:
        return f"{n / 1e9:.1f}B"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{n:.0f}"


def fmt_pct(n: float) -> str:
    """Format a percentage value with appropriate precision."""
    if abs(n) < 0.01:
        return f"{n:.4f}%"
    if abs(n) < 0.1:
        return f"{n:.3f}%"
    if abs(n) < 1:
        return f"{n:.2f}%"
    return f"{n:.1f}%"


def fmt_count(n: int | float) -> str:
    """Format a count for display: 1.23M, 45.3K, 1,234, etc."""
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1e6:.2f}M"
    if n >= 10_000:
        return f"{n / 1e3:.1f}K"
    return f"{n:,}"


# ── Plotly layout ──────────────────────────────────────────────────────


def plotly_layout(**overrides) -> dict:
    """Return default Plotly layout kwargs, merged with *overrides*."""
    defaults = dict(
        template="plotly_white",
        font=dict(size=13),
        margin=dict(l=20, r=20, t=50, b=20),
    )
    defaults.update(overrides)
    return defaults


# ── Sankey helpers ─────────────────────────────────────────────────────


def _hex_to_rgba(hex_color: str, alpha: float = 0.35) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def build_sankey_data(
    df: pd.DataFrame,
    columns: list[str],
    value_col: str | None = None,
    min_flow: int = 2,
    top_n: int | None = None,
) -> dict:
    """Build Sankey node/link data from a DataFrame.

    Parameters
    ----------
    df : DataFrame with categorical columns.
    columns : Ordered list of stage columns (≥ 2).
    value_col : Column to sum as flow weight.  If *None*, row counts are used.
    min_flow : Drop links below this threshold.
    top_n : Bucket rare values outside top *N* as "Others".
    """
    stages: list[tuple[pd.DataFrame, int]] = []
    for i in range(len(columns) - 1):
        src_col, tgt_col = columns[i], columns[i + 1]
        if value_col:
            agg = df.groupby([src_col, tgt_col])[value_col].sum().reset_index()
            agg.columns = ["source", "target", "value"]
        else:
            agg = df.groupby([src_col, tgt_col]).size().reset_index(name="value")

        if top_n:
            top_sources = agg.groupby("source")["value"].sum().nlargest(top_n).index
            top_targets = agg.groupby("target")["value"].sum().nlargest(top_n).index
            agg.loc[~agg["source"].isin(top_sources), "source"] = "Others"
            agg.loc[~agg["target"].isin(top_targets), "target"] = "Others"
            agg = agg.groupby(["source", "target"])["value"].sum().reset_index()

        agg = agg[agg["value"] >= min_flow]
        stages.append((agg, i))

    # Build unique label list preserving order.
    labels: list[str] = []
    label_set: set[str] = set()
    for agg, _ in stages:
        for col in ["source", "target"]:
            for val in agg[col].unique():
                if val not in label_set:
                    labels.append(val)
                    label_set.add(val)

    label_index = {label: idx for idx, label in enumerate(labels)}

    # Assign node colors by earliest stage.
    node_colors = []
    for label in labels:
        stage = 0
        for agg, stage_idx in stages:
            if label in agg["source"].values:
                stage = stage_idx
                break
            if label in agg["target"].values:
                stage = stage_idx + 1
                break
        node_colors.append(SANKEY_PALETTE[stage % len(SANKEY_PALETTE)])

    sources, targets, values, link_colors = [], [], [], []
    for agg, stage_idx in stages:
        for _, row in agg.iterrows():
            sources.append(label_index[row["source"]])
            targets.append(label_index[row["target"]])
            values.append(row["value"])
            link_colors.append(
                _hex_to_rgba(SANKEY_PALETTE[stage_idx % len(SANKEY_PALETTE)])
            )

    return dict(
        node_labels=labels,
        node_colors=node_colors,
        sources=sources,
        targets=targets,
        values=values,
        link_colors=link_colors,
    )


def plot_sankey(
    df: pd.DataFrame,
    columns: list[str],
    title: str,
    value_col: str | None = None,
    min_flow: int = 2,
    top_n: int | None = None,
    width: int = 900,
    height: int = 500,
) -> go.Figure:
    """End-to-end Sankey visualization from a DataFrame."""
    data = build_sankey_data(
        df, columns, value_col=value_col, min_flow=min_flow, top_n=top_n
    )
    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    label=data["node_labels"],
                    color=data["node_colors"],
                    pad=20,
                    thickness=25,
                    line=dict(color="white", width=0.5),
                ),
                link=dict(
                    source=data["sources"],
                    target=data["targets"],
                    value=data["values"],
                    color=data["link_colors"],
                ),
            )
        ]
    )
    fig.update_layout(**plotly_layout(title_text=title, width=width, height=height))
    return fig
