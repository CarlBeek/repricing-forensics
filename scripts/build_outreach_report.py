#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict

from repricing_forensics.config import default_paths


OWNER_WEIGHT = {
    "direct_project_fix": 3,
    "proxy_wallet_or_upgrade_admin": 2,
    "upstream_integrator_gas_budget": 2,
    "front_door_or_router_fix": 2,
    "unknown_owner": 0,
}

REMEDIATION_WEIGHT = {
    "immutable_contract_or_migration": 3,
    "admin_upgrade_possible": 2,
    "integration_update": 2,
    "manual_triage": 1,
    "unknown": 1,
}


def main() -> None:
    paths = default_paths()
    summary_path = paths.artifacts_dir / "tables" / "project_owner_summary.csv"
    out_path = paths.artifacts_dir / "tables" / "outreach_priority.csv"

    grouped: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "divergent_txs": 0,
            "status_changed_txs": 0,
            "total_gas_delta": 0,
            "owner_buckets": set(),
            "remediation_buckets": set(),
        }
    )

    with summary_path.open() as handle:
        for row in csv.DictReader(handle):
            project = row["divergence_project"]
            if project == "unknown":
                continue
            bucket = grouped[project]
            bucket["divergent_txs"] += int(row["divergent_txs"])
            bucket["status_changed_txs"] += int(row["status_changed_txs"])
            bucket["total_gas_delta"] += int(row["total_gas_delta"])
            bucket["owner_buckets"].add(row["owner_bucket"])
            bucket["remediation_buckets"].add(row["remediation_bucket"])

    rows = []
    for project, stats in grouped.items():
        owner_score = max(OWNER_WEIGHT.get(v, 0) for v in stats["owner_buckets"])
        remediation_score = max(
            REMEDIATION_WEIGHT.get(v, 0) for v in stats["remediation_buckets"]
        )
        severity_score = stats["status_changed_txs"] * 10 + stats["divergent_txs"] // 100
        priority_score = severity_score + owner_score * 100 + remediation_score * 50
        rows.append(
            {
                "project": project,
                "priority_score": priority_score,
                "status_changed_txs": stats["status_changed_txs"],
                "divergent_txs": stats["divergent_txs"],
                "total_gas_delta": stats["total_gas_delta"],
                "owner_buckets": ";".join(sorted(stats["owner_buckets"])),
                "remediation_buckets": ";".join(sorted(stats["remediation_buckets"])),
            }
        )

    rows.sort(key=lambda row: (row["priority_score"], row["status_changed_txs"]), reverse=True)

    with out_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "project",
                "priority_score",
                "status_changed_txs",
                "divergent_txs",
                "total_gas_delta",
                "owner_buckets",
                "remediation_buckets",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
