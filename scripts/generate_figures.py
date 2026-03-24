#!/usr/bin/env python3
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from repricing_forensics.config import default_paths, ensure_workspace_dirs
from repricing_forensics.duckdb_utils import connect


def main() -> None:
    paths = default_paths()
    ensure_workspace_dirs(paths)
    figures_dir = paths.artifacts_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(paths.duckdb_path)

    incident_df = conn.execute(
        """
        SELECT divergence_type, divergent_txs
        FROM incident_summary
        ORDER BY divergent_txs DESC
        """
    ).df()
    fig = px.bar(
        incident_df,
        x="divergence_type",
        y="divergent_txs",
        title="7904 Divergence Mix",
        color="divergence_type",
    )
    fig.write_html(figures_dir / "divergence_mix.html")

    contract_df = conn.execute(
        """
        SELECT
            nf.divergence_contract,
            count(*) AS divergent_txs,
            sum(CASE WHEN h.status_changed THEN 1 ELSE 0 END) AS status_changed_txs
        FROM normalized_forensics nf
        JOIN hot_7904 h USING (divergence_id)
        WHERE nf.divergence_contract IS NOT NULL
        GROUP BY 1
        ORDER BY status_changed_txs DESC, divergent_txs DESC
        LIMIT 20
        """
    ).df()
    fig = px.bar(
        contract_df,
        x="divergence_contract",
        y="status_changed_txs",
        hover_data=["divergent_txs"],
        title="Top Contracts By 7904 Status Changes",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "top_status_change_contracts.html")

    depth_df = conn.execute(
        """
        SELECT
            coalesce(divergence_call_depth, -1) AS divergence_call_depth,
            count(*) AS divergent_txs,
            sum(CASE WHEN h.status_changed THEN 1 ELSE 0 END) AS status_changed_txs
        FROM normalized_forensics nf
        JOIN hot_7904 h USING (divergence_id)
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    fig = px.bar(
        depth_df,
        x="divergence_call_depth",
        y="status_changed_txs",
        hover_data=["divergent_txs"],
        title="Status Changes By First-Divergence Call Depth",
    )
    fig.write_html(figures_dir / "status_change_call_depth.html")

    owner_df = pd.read_csv(paths.artifacts_dir / "tables" / "project_owner_summary.csv").head(20)
    fig = px.bar(
        owner_df,
        x="divergence_project",
        y="status_changed_txs",
        color="owner_bucket",
        hover_data=["divergent_txs", "remediation_bucket"],
        title="Top 7904 Project Clusters By Status Change Ownership",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "project_owner_summary.html")

    sankey_df = pd.read_csv(paths.artifacts_dir / "tables" / "project_sankey_edges.csv").head(40)
    labels = sorted(
        set(sankey_df["source_project"].tolist()) | set(sankey_df["target_project"].tolist())
    )
    label_index = {label: idx for idx, label in enumerate(labels)}
    sankey = go.Figure(
        data=[
            go.Sankey(
                node=dict(label=labels, pad=20, thickness=14),
                link=dict(
                    source=[label_index[label] for label in sankey_df["source_project"]],
                    target=[label_index[label] for label in sankey_df["target_project"]],
                    value=sankey_df["divergent_txs"],
                    customdata=sankey_df["owner_bucket"],
                    hovertemplate="%{source.label} -> %{target.label}<br>divergent_txs=%{value}<br>owner=%{customdata}<extra></extra>",
                ),
            )
        ]
    )
    sankey.update_layout(title_text="Cross-Project 7904 Divergence Flow")
    sankey.write_html(figures_dir / "project_sankey.html")

    failure_pairs_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "status_failure_call_pairs_labeled.csv"
    ).head(20)
    failure_pairs_df["edge"] = (
        failure_pairs_df["caller_project"] + " -> " + failure_pairs_df["callee_project"]
    )
    fig = px.bar(
        failure_pairs_df,
        x="edge",
        y="status_failures",
        hover_data=["avg_gas_provided", "avg_gas_used"],
        title="Top Failing Caller -> Callee Pairs In 7904 Status Changes",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "status_failure_call_pairs.html")

    changed_intermediaries_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "changed_nonroot_intermediaries.csv"
    ).head(20)
    fig = px.bar(
        changed_intermediaries_df,
        x="project",
        y="intermediary_score",
        hover_data=["changed_edges", "distinct_downstream_projects", "distinct_txs"],
        title="Top Intermediaries In Changed Non-Root Call Edges",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "changed_nonroot_intermediaries.html")

    changed_motifs_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "changed_edge_motifs.csv"
    ).head(20)
    changed_motifs_df["edge"] = (
        changed_motifs_df["effective_caller_project"]
        + " -> "
        + changed_motifs_df["effective_callee_project"]
    )
    fig = px.bar(
        changed_motifs_df,
        x="edge",
        y="changed_edges",
        color="depth",
        hover_data=["distinct_txs", "success_flip_edges"],
        title="Top Changed Non-Root Caller -> Callee Motifs",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "changed_edge_motifs.html")

    changed_sankey_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "changed_nonroot_sankey_edges.csv"
    ).head(40)
    labels = sorted(
        set(changed_sankey_df["root_project"].fillna("").tolist())
        | set(changed_sankey_df["effective_caller_project"].fillna("").tolist())
        | set(changed_sankey_df["effective_callee_project"].fillna("").tolist())
    )
    labels = [label if label else "unknown" for label in labels]
    label_index = {label: idx for idx, label in enumerate(labels)}
    sankey = go.Figure(
        data=[
            go.Sankey(
                node=dict(label=labels, pad=20, thickness=14),
                link=dict(
                    source=[
                        label_index[(label if label else "unknown")]
                        for label in changed_sankey_df["root_project"].fillna("")
                    ],
                    target=[
                        label_index[(label if label else "unknown")]
                        for label in changed_sankey_df["effective_caller_project"].fillna("")
                    ],
                    value=changed_sankey_df["changed_edges"],
                    customdata=changed_sankey_df["effective_callee_project"],
                    hovertemplate="%{source.label} -> %{target.label}<br>changed_edges=%{value}<br>callee=%{customdata}<extra></extra>",
                ),
            )
        ]
    )
    sankey.update_layout(title_text="Changed Non-Root Call Flow")
    sankey.write_html(figures_dir / "changed_nonroot_sankey.html")

    breakpoint_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "intermediary_breakpoints.csv"
    ).head(20)
    fig = px.bar(
        breakpoint_df,
        x="project",
        y="breakpoint_score",
        hover_data=["breakpoint_txs", "distinct_root_projects", "distinct_downstream_projects"],
        title="Top First-Changed Non-Root Breakpoint Intermediaries",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "intermediary_breakpoints.html")

    breakpoint_motifs_df = pd.read_csv(
        paths.artifacts_dir / "tables" / "first_changed_nonroot_motifs.csv"
    ).head(20)
    breakpoint_motifs_df["edge"] = (
        breakpoint_motifs_df["caller_project"] + " -> " + breakpoint_motifs_df["callee_project"]
    )
    fig = px.bar(
        breakpoint_motifs_df,
        x="edge",
        y="txs",
        color="depth",
        hover_data=["root_project", "change_reason", "success_flip_txs"],
        title="Top First-Changed Non-Root Motifs",
    )
    fig.update_layout(xaxis_tickangle=-45)
    fig.write_html(figures_dir / "first_changed_nonroot_motifs.html")
    conn.close()


if __name__ == "__main__":
    main()
