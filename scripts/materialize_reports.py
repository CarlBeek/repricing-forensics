#!/usr/bin/env python3
from __future__ import annotations

from repricing_forensics.config import default_paths, ensure_workspace_dirs
from repricing_forensics.pipeline import write_query_output


def main() -> None:
    paths = default_paths()
    ensure_workspace_dirs(paths)

    queries = {
        "incident_summary.csv": """
            SELECT *
            FROM incident_summary
            ORDER BY divergent_txs DESC
        """,
        "top_divergence_contracts.csv": """
            SELECT
                nf.divergence_contract,
                count(*) AS divergent_txs,
                sum(CASE WHEN h.status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
                avg(h.gas_delta) AS avg_gas_delta,
                sum(h.gas_delta) AS total_gas_delta
            FROM normalized_forensics nf
            JOIN hot_7904 h USING (divergence_id)
            WHERE nf.divergence_contract IS NOT NULL
            GROUP BY 1
            ORDER BY status_changed_txs DESC, divergent_txs DESC
            LIMIT 500
        """,
        "call_depth_distribution.csv": """
            SELECT
                coalesce(divergence_call_depth, -1) AS divergence_call_depth,
                count(*) AS divergent_txs,
                sum(CASE WHEN h.status_changed THEN 1 ELSE 0 END) AS status_changed_txs
            FROM normalized_forensics nf
            JOIN hot_7904 h USING (divergence_id)
            GROUP BY 1
            ORDER BY 1
        """,
        "top_status_failures.csv": """
            SELECT
                block_number,
                tx_index,
                tx_hash,
                sender,
                recipient,
                gas_delta,
                tx_gas_limit,
                baseline_gas_used,
                schedule_gas_used
            FROM hot_7904
            WHERE status_changed
            ORDER BY abs(gas_delta) DESC
            LIMIT 1000
        """,
    }

    for file_name, sql in queries.items():
        write_query_output(paths, sql, paths.artifacts_dir / "tables" / file_name)


if __name__ == "__main__":
    main()
