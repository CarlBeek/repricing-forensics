# Wallet-fixable / contract-broken classifier — known bug and fix path

## Summary

The current `wallet_fixable_ids` rule in `src/repricing_forensics/sql.py` is too coarse:

```sql
WHERE nf.divergence_call_depth IS NOT NULL
  AND nf.divergence_call_depth <= 1
  AND coalesce(nf.call_count, 0) = 0
```

It marks a transaction as wallet-fixable **only when there are no internal calls**. Every transaction with even one subcall falls through to "contract-broken", regardless of whether those subcalls forwarded gas via the EIP-150 63/64 rule (which propagates extra wallet gas down the stack) or with a hardcoded amount (which doesn't).

The reth-research tool (this repo's data producer) was extended to capture the per-frame data needed to classify accurately and to emit a derived classification on each divergence row. This doc explains the problem, the new schema, and the SQL drop-in.

## Why the current rule misclassifies

Two real cases that the dashboard currently flags as contract-broken but are wallet-fixable:

1. **Uniswap V2 Router → WETH.withdraw** path that ends in OOG at depth 1. The Router does forward subcalls (so `call_count > 0`), but every hop on the broken path uses proportional 63/64 forwarding. WETH's `.transfer()` 2300 stipend is on a *sibling* path (Router's own `receive()`, just CALLER/EQ/REVERT — not repriced), not on the broken path. Raising the wallet's gas resolves these.
2. **USDC.transfer → DELEGATECALL FiatTokenV2_2.transfer** OOGs. DELEGATECALL never throttles. The transaction has `call_count >= 1`, so the rule classifies it as contract-broken. Raising wallet gas resolves these.

The rule's blind spot: it never looks at *how* gas was passed at each hop.

## What the data producer now captures

Per `CallFrame` (stored as JSON inside the divergence row):

| Field                     | Type           | Meaning                                                                                                              |
| ------------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `gas_requested_on_stack`  | `Option<u64>`  | Raw gas arg the caller pushed at CALL/CALLCODE/DELEGATECALL/STATICCALL. `null` for CREATE/CREATE2 and the root frame. |
| `parent_gas_at_call`      | `Option<u64>`  | Parent's remaining gas the moment the CALL opcode executed (before the EIP-150 cap was applied).                     |

Per divergence row (new columns on `schedule_divergences`, surfaced in the Hot Parquet too):

| Column                     | Type      | Meaning                                                                                                                                                                                                                                            |
| -------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `oog_chain_proportional`   | `BOOLEAN` | `TRUE` when every parent→child hop from tx root to OOG frame got `gas_requested_on_stack >= floor(parent_gas_at_call * 63/64) − 100`. `FALSE` when some hop throttled. `NULL` for non-OOG divergences (gas-pattern, log changes, etc.) and for old rows. |
| `oog_bottleneck_depth`     | `INTEGER` | 0-based depth of the first throttled hop walking root→OOG. `NULL` when proportional or non-OOG.                                                                                                                                                    |
| `oog_bottleneck_kind`      | `TEXT`    | One of `'Stipend2300'`, `'FixedGas'`, `'FractionalGas'`. `NULL` when proportional or non-OOG.                                                                                                                                                      |

Classification rules used by the producer:

- `gas_requested_on_stack == 2300` → `Stipend2300` (Solidity `.transfer()`/`.send()`)
- `gas_requested_on_stack < 100_000` → `FixedGas` (small hardcoded constant)
- otherwise → `FractionalGas` (e.g. `gas() / 2`)
- CREATE/CREATE2 are always treated as proportional (the EVM auto-forwards 63/64).

Tolerance of 100 gas absorbs the EIP-150 floor rounding plus the few-hundred-gas CALL overhead, so a caller passing `gas()` is classified as proportional even when integer rounding makes the cap technically larger.

The full classifier lives in `crates/research/src/oog_chain.rs` in the reth-research repo, with twelve unit tests covering edge cases (root-frame OOG, multi-hop throttles, DELEGATECALL with `gas()`, CREATE, missing data).

## Drop-in SQL fix

Replace `WALLET_FIXABLE_SQL` with a rule that prefers the producer's classification and falls back to the old heuristic when the column is `NULL` (rows analyzed before the new code shipped):

```sql
WALLET_FIXABLE_SQL = """
CREATE OR REPLACE TABLE wallet_fixable_ids AS
SELECT nf.divergence_id
FROM normalized_forensics nf
WHERE
    -- Preferred path: the producer's chain-walk classification.
    -- TRUE  → every hop from root to OOG frame got proportional gas (63/64
    --         cap was binding); raising the wallet gas can clear it.
    -- FALSE → some hop throttled (Stipend2300, FixedGas, FractionalGas);
    --         contract-broken.
    -- NULL  → non-OOG divergence OR a row analyzed before the producer
    --         emitted these columns. Fall back to the old heuristic so
    --         the classification doesn't regress on historical data.
    nf.oog_chain_proportional = TRUE
    OR (
        nf.oog_chain_proportional IS NULL
        AND nf.divergence_call_depth IS NOT NULL
        AND nf.divergence_call_depth <= 1
        AND coalesce(nf.call_count, 0) = 0
    )
"""
```

This also assumes `normalized_forensics` exposes `oog_chain_proportional` (and ideally `oog_bottleneck_depth`, `oog_bottleneck_kind` for downstream views). Add them to the SELECT in whichever step builds `normalized_forensics`.

## Recommended dashboard surface

Once the new columns are wired into views, two small UI improvements come for free:

1. **Bottleneck attribution**: on the contract-broken list, show `oog_bottleneck_kind` next to each row so reviewers can immediately see whether the contract uses the 2300 stipend (definitely needs a code change), a hardcoded constant (probably a code change), or a fractional pattern (might be tunable). Group by `oog_bottleneck_kind` for the "what kinds of breakage" overview.
2. **Wallet-fixable carve-out by depth**: filter `oog_chain_proportional = TRUE` rows out of the contract-broken count entirely, and present them as a separate "wallet-fixable (deep call chain)" bucket. Today these are buried in the contract-broken total, inflating it.

## Migration / re-analysis

- The new columns are added via `ALTER TABLE` migrations on the producer side. Existing `divergences.db` files keep their data and gain the columns as `NULL`. The fallback in the SQL above keeps those rows classified by the old rule.
- To populate the new columns for historical blocks, re-run the producer with the same `--research.db-path`. The producer's `--research.backfill` mode will refill any block whose coverage row is missing for the *current* `schedule_config_hash`, which includes anything that now needs the new columns. (If you only want to refresh classifications without rerunning analysis, consider deleting just the divergence rows for the schedule of interest before backfill — coverage rows still serve as the dedupe key for "we've seen this block".)

## Verification queries

Sanity-check the new fields against the cases that motivated this work:

```sql
-- USDC simple transfer false positives: DELEGATECALL forwards all gas, so
-- these should be wallet-fixable.
SELECT divergence_id, oog_chain_proportional, oog_bottleneck_kind
FROM normalized_forensics
WHERE recipient = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'  -- USDC
  AND status_changed = TRUE
LIMIT 20;
-- Expect: oog_chain_proportional = TRUE, oog_bottleneck_kind = NULL.

-- Genuine .transfer() 2300 stipend cases: any contract that calls
-- payable(addr).transfer(value) and OOGs under the schedule.
SELECT contract, COUNT(*)
FROM normalized_forensics
WHERE oog_bottleneck_kind = 'Stipend2300'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
-- Expect: gas-stipend-using contracts (older proxies, naive payment
-- splitters, some auction contracts).
```

## References

- Producer-side classifier: `crates/research/src/oog_chain.rs` in the reth-research repo (with unit tests)
- Producer-side data capture: `crates/research/src/multi_schedule_inspector.rs`
- Producer-side schema: `crates/research/src/database.rs`
- Hot Parquet schema: `crates/research/src/export.rs` (`hot_schema`, `hot_batch`)
