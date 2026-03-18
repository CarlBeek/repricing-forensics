#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

import pandas as pd

from eip7904_analysis.config import default_paths
from eip7904_analysis.duckdb_utils import connect
from eip7904_analysis.labels import infer_project_label
from eip7904_analysis.parsers import parse_call_frames


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


def normalize_frames(frames_json: str | None) -> list[dict]:
    frames = parse_call_frames(frames_json)
    if not frames:
        return []
    return sorted(frames, key=lambda frame: (frame.get("call_index", 0), frame.get("depth", 0)))


def explode_edges(
    tx_meta: dict[str, object],
    frames: list[dict],
    trace_kind: str,
    classification: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    edges: list[dict[str, object]] = []
    duplicate_counts: dict[tuple[object, ...], int] = defaultdict(int)

    for frame in frames:
        caller = frame.get("from")
        callee = frame.get("to")
        edge_key = (
            tx_meta["divergence_id"],
            frame.get("depth"),
            caller,
            callee,
            frame.get("call_type"),
        )
        occurrence = duplicate_counts[edge_key]
        duplicate_counts[edge_key] += 1
        edges.append(
            {
                **tx_meta,
                "trace_kind": trace_kind,
                "call_index": frame.get("call_index"),
                "depth": frame.get("depth"),
                "caller": caller,
                "callee": callee,
                "caller_project": label_address(caller, classification),
                "callee_project": label_address(callee, classification),
                "call_type": frame.get("call_type"),
                "gas_provided": frame.get("gas_provided"),
                "gas_used": frame.get("gas_used"),
                "success": frame.get("success"),
                "edge_occurrence": occurrence,
            }
        )
    return edges


def deepest_failing_path(
    tx_meta: dict[str, object],
    frames: list[dict],
    classification: dict[str, dict[str, str]],
) -> dict[str, object] | None:
    if not frames:
        return None

    stack: list[dict | None] = []
    candidate: dict[str, object] | None = None

    for frame in frames:
        depth = frame.get("depth") or 0
        if len(stack) > depth:
            stack = stack[:depth]
        while len(stack) < depth:
            stack.append(None)

        current = {
            "frame": frame,
            "path": [entry for entry in stack if entry is not None] + [frame],
        }
        if frame.get("success") is False:
            candidate = current

        stack.append(frame)

    if candidate is None:
        return None

    path_frames = candidate["path"]
    failing = candidate["frame"]
    parent = path_frames[-2] if len(path_frames) > 1 else None
    return {
        **tx_meta,
        "root_callee": path_frames[0].get("to"),
        "root_project": label_address(path_frames[0].get("to"), classification),
        "failing_caller": parent.get("to") if parent else failing.get("from"),
        "failing_caller_project": label_address(
            parent.get("to") if parent else failing.get("from"), classification
        ),
        "failing_callee": failing.get("to"),
        "failing_callee_project": label_address(failing.get("to"), classification),
        "failure_depth": failing.get("depth"),
        "failing_call_type": failing.get("call_type"),
        "gas_provided": failing.get("gas_provided"),
        "gas_used": failing.get("gas_used"),
        "path_addresses": json.dumps([frame.get("to") for frame in path_frames]),
        "path_projects": json.dumps([label_address(frame.get("to"), classification) for frame in path_frames]),
        "path_length": len(path_frames),
        "pair_motif": " -> ".join(
            filter(
                None,
                [
                    label_address(parent.get("to"), classification) if parent else None,
                    label_address(failing.get("to"), classification),
                ],
            )
        ),
        "triple_motif": " -> ".join(
            [
                label_address(frame.get("to"), classification)
                for frame in path_frames[-3:]
                if frame.get("to") is not None
            ]
        ),
    }


def write_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    paths = default_paths()
    conn = connect(paths.duckdb_path)
    classification = load_classification(paths.cache_dir / "contract_classification.csv")

    tx_rows = conn.execute(
        """
        SELECT
            h.divergence_id,
            h.block_number,
            h.tx_index,
            h.tx_hash,
            h.recipient,
            h.status_changed,
            h.gas_delta,
            a.baseline_call_frames,
            a.schedule_call_frames
        FROM hot_7904 h
        JOIN artifacts_7904 a USING (divergence_id)
        WHERE h.status_changed
          AND a.schedule_call_frames IS NOT NULL
          AND a.baseline_call_frames IS NOT NULL
        """
    ).df()
    conn.close()

    edge_rows: list[dict[str, object]] = []
    path_rows: list[dict[str, object]] = []

    for row in tx_rows.to_dict(orient="records"):
        tx_meta = {
            "divergence_id": row["divergence_id"],
            "block_number": row["block_number"],
            "tx_index": row["tx_index"],
            "tx_hash": row["tx_hash"],
            "recipient": row["recipient"],
            "recipient_project": label_address(row["recipient"], classification),
            "gas_delta": row["gas_delta"],
        }
        baseline_frames = normalize_frames(row["baseline_call_frames"])
        schedule_frames = normalize_frames(row["schedule_call_frames"])
        edge_rows.extend(explode_edges(tx_meta, baseline_frames, "baseline", classification))
        edge_rows.extend(explode_edges(tx_meta, schedule_frames, "schedule", classification))
        path = deepest_failing_path(tx_meta, schedule_frames, classification)
        if path is not None:
            path_rows.append(path)

    edges_df = pd.DataFrame(edge_rows)
    baseline_df = edges_df[edges_df["trace_kind"] == "baseline"].copy()
    schedule_df = edges_df[edges_df["trace_kind"] == "schedule"].copy()

    join_cols = [
        "divergence_id",
        "block_number",
        "tx_index",
        "tx_hash",
        "depth",
        "caller",
        "callee",
        "call_type",
        "edge_occurrence",
    ]
    edge_cmp = baseline_df.merge(
        schedule_df,
        on=join_cols,
        how="outer",
        suffixes=("_baseline", "_schedule"),
        indicator=True,
    )
    edge_cmp["only_in_baseline"] = edge_cmp["_merge"] == "left_only"
    edge_cmp["only_in_schedule"] = edge_cmp["_merge"] == "right_only"
    edge_cmp["success_flip"] = (
        edge_cmp["success_baseline"].notna()
        & edge_cmp["success_schedule"].notna()
        & (edge_cmp["success_baseline"] != edge_cmp["success_schedule"])
    )
    edge_cmp["gas_provided_delta"] = (
        edge_cmp["gas_provided_schedule"].fillna(0) - edge_cmp["gas_provided_baseline"].fillna(0)
    )
    edge_cmp["gas_used_delta"] = (
        edge_cmp["gas_used_schedule"].fillna(0) - edge_cmp["gas_used_baseline"].fillna(0)
    )
    edge_cmp["edge_changed"] = (
        edge_cmp["only_in_baseline"]
        | edge_cmp["only_in_schedule"]
        | edge_cmp["success_flip"]
        | (edge_cmp["gas_provided_delta"] != 0)
        | (edge_cmp["gas_used_delta"] != 0)
    )
    edge_cmp["effective_call_index"] = edge_cmp["call_index_schedule"].fillna(
        edge_cmp["call_index_baseline"]
    )

    edge_cmp["effective_caller"] = edge_cmp["caller"].fillna(edge_cmp["caller"])
    edge_cmp["effective_callee"] = edge_cmp["callee"].fillna(edge_cmp["callee"])
    edge_cmp["effective_caller_project"] = edge_cmp["caller_project_schedule"].fillna(
        edge_cmp["caller_project_baseline"]
    )
    edge_cmp["effective_callee_project"] = edge_cmp["callee_project_schedule"].fillna(
        edge_cmp["callee_project_baseline"]
    )

    first_diff_rows: list[dict[str, object]] = []
    for _, group in edge_cmp.groupby("divergence_id", sort=False):
        changed = group[group["edge_changed"]].sort_values(
            by=["depth", "edge_occurrence"], ascending=[True, True]
        )
        if changed.empty:
            continue
        row = changed.iloc[0]
        first_diff_rows.append(
            {
                "divergence_id": row["divergence_id"],
                "first_diff_caller": row["caller"],
                "first_diff_caller_project": row.get("caller_project_schedule")
                or row.get("caller_project_baseline"),
                "first_diff_callee": row["callee"],
                "first_diff_callee_project": row.get("callee_project_schedule")
                or row.get("callee_project_baseline"),
                "first_diff_depth": row["depth"],
                "first_diff_reason": ",".join(
                    filter(
                        None,
                        [
                            "only_in_baseline" if row["only_in_baseline"] else "",
                            "only_in_schedule" if row["only_in_schedule"] else "",
                            "success_flip" if row["success_flip"] else "",
                            "gas_delta" if row["gas_used_delta"] != 0 else "",
                        ],
                    )
                ),
            }
        )

    first_diff_df = pd.DataFrame(first_diff_rows)
    paths_df = pd.DataFrame(path_rows).merge(first_diff_df, on="divergence_id", how="left")

    intermediaries: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {
            "status_changed_txs": 0,
            "distinct_callees": set(),
            "distinct_root_projects": set(),
            "failing_parent_txs": 0,
            "first_diff_txs": 0,
            "distinct_failing_callees": set(),
            "total_gas_provided_to_failing_children": 0,
            "total_gas_used_by_failing_children": 0,
        }
    )

    for row in paths_df.to_dict(orient="records"):
        caller = row["failing_caller"]
        caller_project = row["failing_caller_project"]
        key = (caller or "unknown", caller_project or "unknown")
        bucket = intermediaries[key]
        bucket["status_changed_txs"] += 1
        bucket["distinct_callees"].add(row["failing_callee_project"])
        bucket["distinct_root_projects"].add(row["root_project"])
        bucket["failing_parent_txs"] += 1
        bucket["distinct_failing_callees"].add(row["failing_callee_project"])
        bucket["total_gas_provided_to_failing_children"] += int(row["gas_provided"] or 0)
        bucket["total_gas_used_by_failing_children"] += int(row["gas_used"] or 0)
        if row.get("first_diff_caller") == caller:
            bucket["first_diff_txs"] += 1

    intermediary_rows = []
    for (address, project), stats in intermediaries.items():
        mediated = stats["status_changed_txs"]
        victims = len(stats["distinct_failing_callees"])
        roots = len(stats["distinct_root_projects"])
        score = mediated * 10 + victims * 20 + roots * 5 + stats["first_diff_txs"] * 15
        intermediary_rows.append(
            {
                "address": address,
                "project": project,
                "mediation_score": score,
                "status_changed_txs": mediated,
                "distinct_failing_callees": victims,
                "distinct_root_projects": roots,
                "first_diff_txs": stats["first_diff_txs"],
                "avg_gas_provided_to_failing_children": stats["total_gas_provided_to_failing_children"]
                / max(mediated, 1),
                "avg_gas_used_by_failing_children": stats["total_gas_used_by_failing_children"]
                / max(mediated, 1),
            }
        )

    motifs = (
        paths_df.groupby(["pair_motif", "triple_motif"], dropna=False)
        .agg(
            status_failures=("divergence_id", "count"),
            avg_gas_provided=("gas_provided", "mean"),
            avg_gas_used=("gas_used", "mean"),
        )
        .reset_index()
        .sort_values(by="status_failures", ascending=False)
    )

    sankey = (
        paths_df.groupby(
            ["root_project", "failing_caller_project", "failing_callee_project"], dropna=False
        )
        .agg(status_failures=("divergence_id", "count"))
        .reset_index()
        .sort_values(by="status_failures", ascending=False)
    )

    changed_nonroot = edge_cmp[(edge_cmp["edge_changed"]) & (edge_cmp["depth"] > 0)].copy()
    first_changed_nonroot = (
        changed_nonroot.sort_values(
            by=["divergence_id", "effective_call_index", "depth"],
            ascending=[True, True, True],
        )
        .groupby("divergence_id", as_index=False)
        .first()
    )
    first_changed_nonroot["change_reason"] = first_changed_nonroot.apply(
        lambda row: ",".join(
            filter(
                None,
                [
                    "only_in_baseline" if row["only_in_baseline"] else "",
                    "only_in_schedule" if row["only_in_schedule"] else "",
                    "success_flip" if row["success_flip"] else "",
                    "gas_delta" if row["gas_used_delta"] != 0 else "",
                ],
            )
        ),
        axis=1,
    )
    first_changed_nonroot = first_changed_nonroot.rename(
        columns={
            "effective_caller_project": "caller_project",
            "effective_callee_project": "callee_project",
            "recipient_project_baseline": "root_project",
        }
    )

    first_changed_nonroot_motifs = (
        first_changed_nonroot.groupby(
            ["root_project", "caller_project", "callee_project", "depth", "change_reason"],
            dropna=False,
        )
        .agg(
            txs=("divergence_id", "count"),
            success_flip_txs=("success_flip", "sum"),
            avg_gas_used_delta=("gas_used_delta", "mean"),
        )
        .reset_index()
        .sort_values(by=["txs", "success_flip_txs"], ascending=False)
    )

    intermediary_breakpoints = (
        first_changed_nonroot.groupby(["caller", "caller_project"], dropna=False)
        .agg(
            breakpoint_txs=("divergence_id", "count"),
            distinct_root_projects=("root_project", "nunique"),
            distinct_downstream_projects=("callee_project", "nunique"),
            success_flip_txs=("success_flip", "sum"),
            avg_depth=("depth", "mean"),
            avg_gas_used_delta=("gas_used_delta", "mean"),
        )
        .reset_index()
        .rename(columns={"caller": "address", "caller_project": "project"})
    )
    intermediary_breakpoints["breakpoint_score"] = (
        intermediary_breakpoints["breakpoint_txs"] * 5
        + intermediary_breakpoints["distinct_root_projects"] * 20
        + intermediary_breakpoints["distinct_downstream_projects"] * 20
        + intermediary_breakpoints["success_flip_txs"] * 10
    )
    intermediary_breakpoints = intermediary_breakpoints.sort_values(
        by="breakpoint_score", ascending=False
    )

    first_changed_sankey = (
        first_changed_nonroot.groupby(
            ["root_project", "caller_project", "callee_project"], dropna=False
        )
        .agg(txs=("divergence_id", "count"))
        .reset_index()
        .sort_values(by="txs", ascending=False)
    )

    nonroot_intermediaries = (
        changed_nonroot.groupby(["effective_caller", "effective_caller_project"], dropna=False)
        .agg(
            changed_edges=("divergence_id", "count"),
            distinct_downstream_callees=("effective_callee", "nunique"),
            distinct_downstream_projects=("effective_callee_project", "nunique"),
            distinct_txs=("divergence_id", "nunique"),
            avg_depth=("depth", "mean"),
            success_flip_edges=("success_flip", "sum"),
            schedule_only_edges=("only_in_schedule", "sum"),
            baseline_only_edges=("only_in_baseline", "sum"),
        )
        .reset_index()
        .rename(
            columns={
                "effective_caller": "address",
                "effective_caller_project": "project",
            }
        )
    )
    nonroot_intermediaries["intermediary_score"] = (
        nonroot_intermediaries["changed_edges"] * 2
        + nonroot_intermediaries["distinct_downstream_projects"] * 25
        + nonroot_intermediaries["distinct_txs"] * 3
        + nonroot_intermediaries["success_flip_edges"] * 10
    )
    nonroot_intermediaries = nonroot_intermediaries.sort_values(
        by="intermediary_score", ascending=False
    )

    changed_edge_motifs = (
        changed_nonroot.groupby(
            ["effective_caller_project", "effective_callee_project", "depth"], dropna=False
        )
        .agg(
            changed_edges=("divergence_id", "count"),
            distinct_txs=("divergence_id", "nunique"),
            success_flip_edges=("success_flip", "sum"),
        )
        .reset_index()
        .sort_values(by=["changed_edges", "distinct_txs"], ascending=False)
    )

    changed_sankey = (
        changed_nonroot.groupby(
            ["recipient_project_baseline", "effective_caller_project", "effective_callee_project"],
            dropna=False,
        )
        .agg(changed_edges=("divergence_id", "count"), distinct_txs=("divergence_id", "nunique"))
        .reset_index()
        .rename(columns={"recipient_project_baseline": "root_project"})
        .sort_values(by=["changed_edges", "distinct_txs"], ascending=False)
    )

    write_df(edges_df, paths.artifacts_dir / "tables" / "call_graph_edges.csv")
    write_df(edge_cmp, paths.artifacts_dir / "tables" / "call_graph_edge_comparison.csv")
    write_df(paths_df, paths.artifacts_dir / "tables" / "tx_failure_paths.csv")
    write_df(
        pd.DataFrame(intermediary_rows).sort_values(by="mediation_score", ascending=False),
        paths.artifacts_dir / "tables" / "intermediary_centrality.csv",
    )
    write_df(motifs, paths.artifacts_dir / "tables" / "failure_motifs.csv")
    write_df(sankey, paths.artifacts_dir / "tables" / "failure_path_sankey_edges.csv")
    write_df(
        nonroot_intermediaries,
        paths.artifacts_dir / "tables" / "changed_nonroot_intermediaries.csv",
    )
    write_df(
        changed_edge_motifs,
        paths.artifacts_dir / "tables" / "changed_edge_motifs.csv",
    )
    write_df(
        changed_sankey,
        paths.artifacts_dir / "tables" / "changed_nonroot_sankey_edges.csv",
    )
    write_df(
        first_changed_nonroot,
        paths.artifacts_dir / "tables" / "first_changed_nonroot_edges.csv",
    )
    write_df(
        first_changed_nonroot_motifs,
        paths.artifacts_dir / "tables" / "first_changed_nonroot_motifs.csv",
    )
    write_df(
        intermediary_breakpoints,
        paths.artifacts_dir / "tables" / "intermediary_breakpoints.csv",
    )
    write_df(
        first_changed_sankey,
        paths.artifacts_dir / "tables" / "first_changed_nonroot_sankey_edges.csv",
    )


if __name__ == "__main__":
    main()
