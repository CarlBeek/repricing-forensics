#!/usr/bin/env python3
from __future__ import annotations

import csv

from repricing_forensics.config import default_paths, ensure_workspace_dirs
from repricing_forensics.duckdb_utils import connect
from repricing_forensics.labels import infer_project_label


def owner_bucket(
    recipient_project: str,
    divergence_project: str,
    call_depth: int | None,
    classification: str | None,
) -> str:
    if divergence_project == "unknown":
        return "unknown_owner"
    if classification in {"proxy", "upgradeable", "wallet_or_safe"}:
        return "proxy_wallet_or_upgrade_admin"
    if recipient_project == divergence_project:
        return "direct_project_fix"
    if call_depth is not None and call_depth > 1:
        return "upstream_integrator_gas_budget"
    return "front_door_or_router_fix"


def remediation_bucket(owner: str, classification: str | None) -> str:
    if owner == "proxy_wallet_or_upgrade_admin":
        return "admin_upgrade_possible"
    if owner in {"upstream_integrator_gas_budget", "front_door_or_router_fix"}:
        return "integration_update"
    if classification == "verified_immutable":
        return "immutable_contract_or_migration"
    if classification == "unverified":
        return "manual_triage"
    return "unknown"


def main() -> None:
    paths = default_paths()
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    class_rows = conn.execute(
        """
        SELECT *
        FROM read_csv_auto('cache/contract_classification.csv', header=true)
        """
    ).df()
    classification_by_address = {
        row["address"].lower(): row for row in class_rows.to_dict(orient="records")
    }

    rows = conn.execute(
        """
        SELECT
            h.divergence_id,
            h.block_number,
            h.tx_index,
            h.tx_hash,
            h.sender,
            h.recipient,
            h.status_changed,
            h.gas_delta,
            nf.divergence_contract,
            nf.divergence_call_depth
        FROM hot_7904 h
        LEFT JOIN normalized_forensics nf USING (divergence_id)
        """
    ).df()
    conn.close()

    detailed_path = paths.artifacts_dir / "tables" / "project_attribution.csv"
    summary_path = paths.artifacts_dir / "tables" / "project_owner_summary.csv"
    sankey_path = paths.artifacts_dir / "tables" / "project_sankey_edges.csv"
    detailed_path.parent.mkdir(parents=True, exist_ok=True)

    summary: dict[tuple[str, str, str], dict[str, int]] = {}
    sankey: dict[tuple[str, str, str], int] = {}

    with detailed_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "divergence_id",
                "block_number",
                "tx_index",
                "tx_hash",
                "recipient",
                "recipient_project",
                "divergence_contract",
                "divergence_project",
                "divergence_call_depth",
                "owner_bucket",
                "remediation_bucket",
                "status_changed",
                "gas_delta",
            ],
        )
        writer.writeheader()

        for row in rows.to_dict(orient="records"):
            recipient = (row["recipient"] or "").lower() or None
            divergence_contract = (row["divergence_contract"] or "").lower() or None
            recipient_class = classification_by_address.get(recipient or "")
            divergence_class = classification_by_address.get(divergence_contract or "")
            recipient_project = infer_project_label(
                recipient,
                compiled_name=None if recipient_class is None else recipient_class.get("name"),
                classification=None
                if recipient_class is None
                else recipient_class.get("classification"),
                source_hint=None if recipient_class is None else recipient_class.get("source_hint"),
            )
            divergence_project = infer_project_label(
                divergence_contract,
                compiled_name=None if divergence_class is None else divergence_class.get("name"),
                classification=None
                if divergence_class is None
                else divergence_class.get("classification"),
                source_hint=None if divergence_class is None else divergence_class.get("source_hint"),
            )
            owner = owner_bucket(
                recipient_project,
                divergence_project,
                row["divergence_call_depth"],
                None if divergence_class is None else divergence_class.get("classification"),
            )
            remediation = remediation_bucket(
                owner,
                None if divergence_class is None else divergence_class.get("classification"),
            )

            writer.writerow(
                {
                    "divergence_id": row["divergence_id"],
                    "block_number": row["block_number"],
                    "tx_index": row["tx_index"],
                    "tx_hash": row["tx_hash"],
                    "recipient": recipient,
                    "recipient_project": recipient_project,
                    "divergence_contract": divergence_contract,
                    "divergence_project": divergence_project,
                    "divergence_call_depth": row["divergence_call_depth"],
                    "owner_bucket": owner,
                    "remediation_bucket": remediation,
                    "status_changed": row["status_changed"],
                    "gas_delta": row["gas_delta"],
                }
            )

            key = (divergence_project, owner, remediation)
            bucket = summary.setdefault(
                key,
                {"divergent_txs": 0, "status_changed_txs": 0, "total_gas_delta": 0},
            )
            bucket["divergent_txs"] += 1
            bucket["status_changed_txs"] += int(bool(row["status_changed"]))
            bucket["total_gas_delta"] += int(row["gas_delta"] or 0)

            if recipient_project != divergence_project:
                sankey[(recipient_project, divergence_project, owner)] = (
                    sankey.get((recipient_project, divergence_project, owner), 0) + 1
                )

    with summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "divergence_project",
                "owner_bucket",
                "remediation_bucket",
                "divergent_txs",
                "status_changed_txs",
                "total_gas_delta",
            ],
        )
        writer.writeheader()
        for (project, owner, remediation), stats in sorted(
            summary.items(),
            key=lambda item: (item[1]["status_changed_txs"], item[1]["divergent_txs"]),
            reverse=True,
        ):
            writer.writerow(
                {
                    "divergence_project": project,
                    "owner_bucket": owner,
                    "remediation_bucket": remediation,
                    **stats,
                }
            )

    with sankey_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["source_project", "target_project", "owner_bucket", "divergent_txs"],
        )
        writer.writeheader()
        for (source_project, target_project, owner), count in sorted(
            sankey.items(), key=lambda item: item[1], reverse=True
        ):
            writer.writerow(
                {
                    "source_project": source_project,
                    "target_project": target_project,
                    "owner_bucket": owner,
                    "divergent_txs": count,
                }
            )


if __name__ == "__main__":
    main()
