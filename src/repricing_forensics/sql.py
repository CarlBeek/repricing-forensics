from __future__ import annotations

from pathlib import Path


def _glob(lake: Path, dataset: str) -> str:
    return str(lake / dataset / "schedule_name=*" / "block_bucket=*" / "*.parquet")


def create_views_sql(schedule_name: str, research_lake: Path) -> list[str]:
    escaped = schedule_name.replace("'", "''")
    hot = _glob(research_lake, "divergences_hot")
    artifact = _glob(research_lake, "divergence_artifacts")
    coverage = _glob(research_lake, "block_coverage")
    return [
        f"""
        CREATE OR REPLACE VIEW hot_7904 AS
        SELECT *
        FROM read_parquet('{hot}')
        WHERE schedule_name = '{escaped}'
        """,
        f"""
        CREATE OR REPLACE VIEW artifacts_7904 AS
        SELECT *
        FROM read_parquet('{artifact}')
        WHERE schedule_name = '{escaped}'
        """,
        f"""
        CREATE OR REPLACE VIEW coverage_7904 AS
        SELECT *
        FROM read_parquet('{coverage}')
        WHERE schedule_name = '{escaped}'
        """,
    ]


DERIVED_INCIDENTS_SQL = """
CREATE OR REPLACE TABLE incident_summary AS
SELECT
    divergence_type,
    count(*) AS divergent_txs,
    sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
    sum(CASE WHEN call_tree_changed THEN 1 ELSE 0 END) AS call_tree_changed_txs,
    sum(CASE WHEN event_logs_changed THEN 1 ELSE 0 END) AS event_logs_changed_txs,
    sum(gas_delta) AS total_gas_delta,
    avg(gas_delta) AS avg_gas_delta
FROM hot_7904
GROUP BY 1
ORDER BY divergent_txs DESC
"""

# Wallet-fixable = divergence at depth ≤ 1 with no internal calls.
# These are just tight gas estimates that wallets will auto-fix via
# eth_estimateGas against the new schedule. We filter them from the
# detailed forensics/affected views to focus on real contract breakage.
WALLET_FIXABLE_SQL = """
CREATE OR REPLACE TABLE wallet_fixable_ids AS
SELECT nf.divergence_id
FROM normalized_forensics nf
WHERE nf.divergence_call_depth IS NOT NULL
  AND nf.divergence_call_depth <= 1
  AND coalesce(nf.call_count, 0) = 0
"""

