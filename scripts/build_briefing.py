#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path

from repricing_forensics.config import default_paths


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    paths = default_paths()
    outreach = read_csv(paths.artifacts_dir / "tables" / "outreach_priority.csv")[:15]
    pairs = read_csv(paths.artifacts_dir / "tables" / "status_failure_call_pairs_labeled.csv")[:20]
    intermediaries = read_csv(paths.artifacts_dir / "tables" / "changed_nonroot_intermediaries.csv")[:15]
    motifs = read_csv(paths.artifacts_dir / "tables" / "changed_edge_motifs.csv")[:15]
    breakpoints = read_csv(paths.artifacts_dir / "tables" / "intermediary_breakpoints.csv")[:15]
    breakpoint_motifs = read_csv(paths.artifacts_dir / "tables" / "first_changed_nonroot_motifs.csv")[:15]

    lines: list[str] = []
    lines.append("# EIP-7904 Outreach Briefing")
    lines.append("")
    lines.append("## Top Contact Priorities")
    lines.append("")
    for row in outreach[:10]:
        lines.append(
            f"- {row['project']}: status_changed={row['status_changed_txs']}, divergent_txs={row['divergent_txs']}, "
            f"owners={row['owner_buckets']}, remediation={row['remediation_buckets']}"
        )

    lines.append("")
    lines.append("## Strongest Failing Caller -> Callee Evidence")
    lines.append("")
    for row in pairs[:12]:
        caller = row["caller_project"]
        callee = row["callee_project"]
        lines.append(
            f"- {caller} -> {callee}: failures={row['status_failures']}, "
            f"avg_gas_provided={row['avg_gas_provided']}, avg_gas_used={row['avg_gas_used']}"
        )

    lines.append("")
    lines.append("## Top Non-Root Intermediaries In Changed Call Graphs")
    lines.append("")
    for row in intermediaries[:10]:
        lines.append(
            f"- {row['project']}: changed_edges={row['changed_edges']}, distinct_txs={row['distinct_txs']}, "
            f"distinct_downstream_projects={row['distinct_downstream_projects']}, success_flip_edges={row['success_flip_edges']}"
        )

    lines.append("")
    lines.append("## Top Changed Non-Root Motifs")
    lines.append("")
    for row in motifs[:10]:
        lines.append(
            f"- {row['caller_project']} -> {row['callee_project']} @ depth {row['depth']}: "
            f"changed_edges={row['changed_edges']}, distinct_txs={row['distinct_txs']}, success_flip_edges={row['success_flip_edges']}"
        )

    lines.append("")
    lines.append("## Top First-Changed Non-Root Breakpoints")
    lines.append("")
    for row in breakpoints[:10]:
        lines.append(
            f"- {row['project']}: breakpoint_txs={row['breakpoint_txs']}, distinct_root_projects={row['distinct_root_projects']}, "
            f"distinct_downstream_projects={row['distinct_downstream_projects']}, avg_depth={row['avg_depth']}"
        )

    lines.append("")
    lines.append("## Top First-Changed Non-Root Motifs")
    lines.append("")
    for row in breakpoint_motifs[:10]:
        lines.append(
            f"- {row['recipient_project']} -> {row['caller_project']} -> {row['callee_project']} @ depth {row['depth']}: "
            f"txs={row['txs']}, reason={row['change_reason']}, avg_gas_used_delta={row['avg_gas_used_delta']}"
        )

    lines.append("")
    lines.append("## Current Read")
    lines.append("")
    lines.append(
        "- Tether USDT is still the dominant cluster, but there is clear upstream-mediated failure in addition to direct contract-level divergence."
    )
    lines.append(
        "- Circle USDC currently looks more like an admin-upgrade/proxy-governed remediation case than a pure immutable-token case."
    )
    lines.append(
        "- Routers and integrators remain material: Uniswap V2 Router, SushiSwap Router, Permit2, and 1inch-linked paths all show nontrivial status-change exposure."
    )
    lines.append(
        "- The graph view is more informative than the failing-frame view for stack analysis: there are hundreds of thousands of changed non-root edges even though most final failing frames are recorded at depth 0."
    )
    lines.append(
        "- The best stack-level root-cause primitive so far is the first changed non-root edge per tx, not the final failing frame."
    )
    lines.append(
        "- Unknown / unverified clusters still account for meaningful failure volume and need another pass before the outreach list is presentation-final."
    )

    out_path = paths.artifacts_dir / "briefing.md"
    out_path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
