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
        CREATE OR REPLACE VIEW hot_schedule AS
        SELECT *
        FROM read_parquet('{hot}', union_by_name = true)
        WHERE schedule_name = '{escaped}'
        """,
        f"""
        CREATE OR REPLACE VIEW artifacts_schedule AS
        SELECT *
        FROM read_parquet('{artifact}', union_by_name = true)
        WHERE schedule_name = '{escaped}'
        """,
        f"""
        CREATE OR REPLACE VIEW coverage_schedule AS
        SELECT *
        FROM read_parquet('{coverage}', union_by_name = true)
        WHERE schedule_name = '{escaped}'
        """,
        """
        CREATE OR REPLACE VIEW hot_7904 AS
        SELECT * FROM hot_schedule
        """,
        """
        CREATE OR REPLACE VIEW artifacts_7904 AS
        SELECT * FROM artifacts_schedule
        """,
        """
        CREATE OR REPLACE VIEW coverage_7904 AS
        SELECT * FROM coverage_schedule
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


EIP8037_TX_IMPACT_SQL = """
CREATE OR REPLACE TABLE eip8037_tx_impact AS
WITH base AS (
    SELECT
        h.*,
        coalesce(TRY_CAST(h.schedule_intrinsic_gas AS UBIGINT), h.baseline_intrinsic_gas)
            AS effective_schedule_intrinsic_gas,
        CASE
            WHEN h.schedule_state_gas_spent >= h.schedule_initial_state_gas
                THEN h.schedule_state_gas_spent - h.schedule_initial_state_gas
            ELSE 0
        END AS runtime_state_gas,
        CASE
            WHEN h.schedule_total_gas_spent >= h.schedule_state_gas_spent
                THEN h.schedule_total_gas_spent - h.schedule_state_gas_spent
            ELSE 0
        END AS schedule_regular_gas_spent,
        CASE
            WHEN h.baseline_total_gas_spent >= h.baseline_gas_refunded
                THEN h.baseline_total_gas_spent - h.baseline_gas_refunded
            ELSE h.baseline_gas_used
        END AS baseline_post_refund_spent,
        CASE
            WHEN h.schedule_total_gas_spent >= h.schedule_gas_refunded
                THEN h.schedule_total_gas_spent - h.schedule_gas_refunded
            ELSE h.schedule_gas_used
        END AS schedule_post_refund_spent
    FROM hot_schedule h
),
classified AS (
    SELECT
        *,
        least(runtime_state_gas, schedule_initial_reservoir)
            AS runtime_state_gas_covered_by_reservoir,
        CASE
            WHEN runtime_state_gas > schedule_initial_reservoir
                THEN runtime_state_gas - schedule_initial_reservoir
            ELSE 0
        END AS runtime_state_gas_spillover,
        CASE
            WHEN schedule_success AND NOT would_fit_in_original_limit
                THEN schedule_gas_used - tx_gas_limit
            ELSE NULL
        END AS extra_gas_needed,
        CASE
            WHEN min_multiplier_to_succeed IS NOT NULL
                THEN ceil(tx_gas_limit * min_multiplier_to_succeed)
            ELSE NULL
        END AS estimated_min_gas_limit,
        CASE
            WHEN tx_category IS NOT NULL THEN tx_category
            WHEN is_create THEN 'contract_creation'
            WHEN authorization_count > 0 THEN 'authorization'
            WHEN schedule_state_gas_spent > 0 THEN 'runtime_state_creation'
            ELSE 'no_state_creation'
        END AS state_gas_category,
        lower(coalesce(
            TRY_CAST(recipient AS VARCHAR),
            TRY_CAST(schedule_created_address AS VARCHAR),
            TRY_CAST(baseline_created_address AS VARCHAR),
            TRY_CAST(sender AS VARCHAR)
        )) AS target_address
    FROM base
)
SELECT
    *,
    runtime_state_gas > schedule_initial_reservoir AS reservoir_exhausted,
    NOT would_fit_in_original_limit AS original_limit_failure,
    CAST(baseline_gas_refunded AS BIGINT) - CAST(schedule_gas_refunded AS BIGINT)
        AS refund_delta,
    CAST(schedule_post_refund_spent AS BIGINT) - CAST(baseline_post_refund_spent AS BIGINT)
        AS post_refund_spent_delta
FROM classified
"""


EIP8037_CONTRACT_IMPACT_SQL = """
CREATE OR REPLACE TABLE eip8037_contract_impact AS
SELECT
    target_address,
    count(*) AS divergent_txs,
    sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
    sum(CASE WHEN original_limit_failure THEN 1 ELSE 0 END) AS original_limit_failures,
    sum(CASE WHEN baseline_success AND NOT schedule_success THEN 1 ELSE 0 END)
        AS baseline_success_schedule_failures,
    sum(CASE WHEN schedule_success AND NOT would_fit_in_original_limit THEN 1 ELSE 0 END)
        AS fixable_with_more_outer_gas,
    sum(CASE WHEN NOT schedule_success AND min_multiplier_to_succeed IS NULL THEN 1 ELSE 0 END)
        AS unresolved_replay_failures,
    avg(gas_delta) AS avg_gas_delta,
    sum(gas_delta) AS total_gas_delta,
    avg(schedule_state_gas_spent) AS avg_state_gas_spent,
    sum(schedule_state_gas_spent) AS total_state_gas_spent,
    sum(runtime_state_gas) AS total_runtime_state_gas,
    sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
    sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END) AS reservoir_exhausted_txs,
    max(min_multiplier_to_succeed) AS max_min_multiplier_to_succeed,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
        AS p95_min_multiplier_to_succeed,
    max(extra_gas_needed) AS max_extra_gas_needed,
    min(block_number) AS min_block,
    max(block_number) AS max_block
FROM eip8037_tx_impact
WHERE target_address IS NOT NULL
GROUP BY target_address
ORDER BY original_limit_failures DESC, status_changed_txs DESC, divergent_txs DESC
"""
