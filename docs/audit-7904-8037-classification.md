# Audit: contract-broken / wallet-fixable classification for EIP-7904 and EIP-8037

Audit date: 2026-05-14. Live snapshot pulled from `repricing-forensics.carlbeek.com`.

The forensics pipeline classifies every replayed tx into one of seven
buckets. The two we care about here are `contract_broken` (structural
failure, needs a code change) and `wallet_fixable_*` (raising the
wallet's gas limit resolves it). The dashboard's "affected contracts"
list is the contract_broken cohort, grouped by `recipient`.

The current classifier is producing a contract_broken cohort that is
**dominated by false positives** on both schedules. The headline
numbers from `block_coverage`:

| schedule         | total       | `contract_broken` | `wallet_fixable_shallow` | `wallet_fixable_deep_chain` |
| ---------------- | ----------- | ----------------- | ------------------------ | --------------------------- |
| `7904-prelim`    | 5,232,107   | 1,985             | 902                      | 1                           |
| `eip-8037`       | 5,232,344   | 903,460           | 0                        | 5,719                       |

`/api/forensics/break-reason` confirms that almost none of the
"contract_broken" cohort is actually an out-of-gas event:

| schedule         | `contract_broken` total | actually OOG       | non-OOG revert (= classifier blind) |
| ---------------- | ----------------------- | ------------------ | ----------------------------------- |
| `7904-prelim`    | 1,985                   | **0 (0.0%)**       | 1,985 (100.0%)                      |
| `eip-8037`       | 903,460                 | 5,620 (0.6%)       | 897,840 (99.4%)                     |

The producer's `_debug/divergence-sample` confirms the chain-walk
classifier is silent on almost every row it should classify:

| schedule         | divergence rows | rows with `oog_chain_proportional` set | rows with bottleneck kind |
| ---------------- | --------------- | -------------------------------------- | ------------------------- |
| `7904-prelim`    | 56,818          | 149                                    | 116                       |
| `eip-8037`       | 932,428         | 22,134                                 | 19,366                    |

The combination is unambiguous: the contract_broken bucket is being
populated by rows for which the OOG-chain classifier never even ran,
because the underlying signals (oog\_call\_depth, call frames at the
right depth) are missing or unit-mismatched. The classifier itself —
the work in `oog-classifier-fix.md` — is sound, but it's being asked to
make a decision about the wrong rows.

This audit walks the producer + consumer stack and lists the concrete
bugs in priority order. Section §1 is the smoking guns; §2 covers the
chain-walk classifier's blind spots; §3 covers data-pipeline issues
that prevent the classifier from running at all.

## §1. The status-flip is symmetric — beneficial flips are counted as broken

**Severity: critical. Almost certainly accounts for most of the 7904
false-positives and a significant share of 8037's.**

In `crates/research/bin/reth-research/src/main.rs` (~line 1873):

```rust
let status_changed = schedule_success != normal_success;
```

In `crates/research/src/divergence.rs` `classify_bucket`:

```rust
if input.status_changed {
    if is_shallow_oog(...) { Bucket::WalletFixableShallow }
    else if input.oog_chain_proportional == Some(true) { Bucket::WalletFixableDeepChain }
    else { Bucket::ContractBroken }
}
```

The branch fires for any `success` flip — including the **beneficial**
direction (`baseline_success = false, schedule_success = true`). Three
sampled txs from the Uniswap V2 Router contract_broken list, fetched
from `/api/tx/<hash>?schedule=7904-prelim`:

| tx                                               | baseline | schedule | gas_delta | divergence opcode |
| ------------------------------------------------ | -------- | -------- | --------- | ----------------- |
| 0xd7857b95f65a0b2d506a062722267d3b3a32a67e0d4... | **0**    | **1**    | -3,567    | KECCAK256         |
| 0xc6ac8e8d031f50b24f5d157316c15bdcfc71e2a212... | **0**    | **1**    | -7,537    | DIV               |
| 0x79d2eedd27d02ec236a2f1edf43aff77cd23ea6d10... | **0**    | **1**    | -12,357   | DIV               |

These transactions **succeeded under the schedule** while the baseline
replay produced a failure. The dashboard files them under "Uniswap V2
Router needs a code change". That is the opposite of what the data
says.

Top-contracts table reinforces the pattern: most of the contracts at
the top of the 7904 contract_broken list have **negative average
gas_delta** (e.g. Uniswap V2 Router `avg_delta_7904 = −15,932`,
`0x4c82d1fbf… = −28,827`, `0xccc88a9d1b…= −67,711`). A net-cheaper
schedule producing more failures than the baseline does not have a
plausible mechanism — the population is shaped by the opposite case,
schedule-rescued txs being mislabelled as schedule-broken.

### Fix

`status_changed` is the wrong field to gate "contract is broken". The
producer should record direction:

```rust
let baseline_to_schedule_break = normal_success && !schedule_success;
```

`classify_bucket` should only enter the wallet/contract branch on
`baseline_to_schedule_break`. The opposite flip (schedule rescued a
baseline failure) deserves its own bucket — `ScheduleRescued` or
similar — that gets surfaced in the dashboard separately. It's a
genuine *finding* (the new schedule fixes some failures) but it has no
business in the "contracts that need code changes" pile.

A separate `event_logs_changed` / `gas_delta` priority still applies
when the outcome is identical, so the ladder becomes:

1. `baseline_to_schedule_break` → wallet-fixable or contract-broken
2. `baseline_to_schedule_rescue` → `ScheduleRescued`
3. `event_logs_changed` → `EventLogsChanged`
4. `gas_delta != 0` → `GasOnly`
5. trace flags → `TraceOnly`
6. otherwise → `Unchanged`

## §2. The OOG-chain classifier has structural blind spots

The chain-walk does work *when it runs*, but the way it walks the chain
hides two classes of failure.

### §2.1 `classify_chain` skips `chain[0]`, which is often the OOG frame itself

`ancestor_chain` reconstructs the path root → OOG. revm never fires
`call_end` on the root frame, so for a depth-1 OOG the chain has **one
element** — the depth-1 frame. After `chain.reverse()`, `chain[0]` is
that single frame.

`classify_chain` then:

```rust
for frame in chain.iter().skip(1) {  // ← only inspects chain[1..]
    ...
}
OogChainAnalysis::proportional()
```

The "skip(1)" is correct *if* `chain[0]` is the root (a frame with no
parent → child hop to classify). But when the root isn't captured,
`chain[0]` is the OOG frame, and its `gas_requested_on_stack` /
`parent_gas_at_call` (which describe how the root invoked it) are
never inspected. The function returns `proportional()` regardless of
what the root → depth-1 hop looked like.

**Impact:**

- A root contract that calls `target.transfer(1 ether)` (Solidity
  `.transfer()` → CALL with 2300 gas → child OOGs inside the stipend)
  will classify as **proportional** and be moved out of
  contract_broken — exactly the case the bottleneck classifier was
  built to catch.
- A root contract that calls `target.call{gas: 30_000}(...)` and OOGs
  is also marked proportional.
- A root → DELEGATECALL with `gas()` (the USDC.transfer case) is
  *correctly* classified as proportional, but only by accident — the
  classifier never actually looks at the hop.

### Fix

`classify_chain` needs to inspect every hop the chain represents,
including the synthetic-root → `chain[0]` hop when the root frame
isn't captured. The cleanest framing:

```rust
fn classify_chain(chain: &[&CallFrame], root_captured: bool) -> OogChainAnalysis {
    let start = if root_captured { 1 } else { 0 };
    for frame in chain.iter().skip(start) {
        // existing per-hop logic using gas_requested_on_stack / parent_gas_at_call
    }
    OogChainAnalysis::proportional()
}
```

`root_captured` can be derived from whether `chain[0].depth == 0` (the
inspector pushes its first sub-call onto an empty `call_stack`, so
captured frames always have `depth >= 1` unless we explicitly
synthesize a root entry). The `ancestor_chain` walker already reports
the OOG frame's depth, so the producer can decide at the call site.

### §2.2 `record_oog` emits a 0-based depth while the function expects 1-based

`crates/research/src/multi_schedule_inspector.rs` `record_oog`:

```rust
self.oog_info = Some(OutOfGasInfo {
    ...
    call_depth: self.call_stack.len(),  // 0 when OOG is in the root frame
    ...
});
```

`oog_chain.rs::classify_oog_chain` documents the input as **1-based**
(`root frame = 1`) and short-circuits to `proportional()` when
`oog_call_depth = 1`. But the inspector emits **0-based** depth for
the root frame, and:

```rust
let oog_frame_depth = oog_call_depth.checked_sub(1)?;
```

`0.checked_sub(1)` is `None` → the classifier returns `None` →
`oog_chain_proportional = NULL` → bucket falls through to
`ContractBroken` (assuming `call_count > 0`).

This unit-mismatch only affects the `record_oog` path (i.e. 7904's
direct apply\_gas\_delta OOGs at the root frame). The other path
(`record_frame_*` from `call_end`) writes `popped.depth + 1`, which is
1-based. The producer's synthetic root\_halt writes `call_depth: 1`
correctly. So this is a real inconsistency.

The unit-test suite locks in the 1-based contract:

```rust
#[test]
fn oog_in_root_is_proportional() {
    let frames = vec![frame(0, false, CallType::Call, None, None)];
    let res = classify_oog_chain(&frames, 1).unwrap();  // ← 1-based input
    ...
}
```

…but the inspector violates it.

### Fix

Either:

- Change `record_oog` to write `self.call_stack.len() + 1` and update
  any other call site that consumes it to expect 1-based.
- Or change `classify_oog_chain` to accept 0-based input throughout
  and update the test fixtures + producer call sites. (More churn,
  less consistent with the rest of the producer.)

The first option is the minimum-diff fix.

### §2.3 `is_shallow_oog` uses divergence depth, not OOG depth

`classify_bucket` short-circuits to `WalletFixableShallow` only when:

```rust
matches!(divergence_call_depth, Some(d) if d <= 1) && call_count == 0
```

For 7904, `divergence_call_depth` is where the *first* repriced opcode
ran, not where the OOG occurred. For 8037 it's where the first
SSTORE/CALL/CREATE in the schedule ran, which is usually the first
sub-call regardless of where execution eventually failed.

For a tx like:

```
ROOT
├── KECCAK256              ← first 7904 divergence (depth=0)
├── CALL → sub-contract    (sub-contract returns cleanly)
└── KECCAK256 … OOG        ← OOG actually here (depth=0)
```

…the call into the sub-contract makes `call_count >= 1`, so the
shallow rule fails even though the OOG is at root and is wallet-
fixable.

### Fix

Use `oog_call_depth` for the shallow check, not `divergence_call_depth`:

```rust
const fn is_shallow_oog(oog_call_depth: Option<usize>) -> bool {
    matches!(oog_call_depth, Some(d) if d <= 1)
}
```

The `call_count == 0` guard is the wrong shape — what it's trying to
say is "the OOG happened in the root frame", and `oog_call_depth <= 1`
already conveys that. `call_count` should be dropped from the predicate
entirely.

## §3. Whole categories never reach the chain classifier

The chain-walk is downstream of `r.oog_call_depth`. When the producer
doesn't populate that field, the classifier silently returns `None`
and the bucket falls through to `ContractBroken` for every
status-flipped tx.

### §3.1 EVM-rejection at the intrinsic-gas check is lost

When `evm.transact(...)` returns `Err` (the canonical case: schedule
intrinsic gas exceeds `gas_limit`), `bin/reth-research/src/main.rs`
constructs:

```rust
last_attempt = Some(PerScheduleResult {
    success: false,
    ...
    replay_halt_oog: Some(true),  // ← gas-class halt
    oog_call_depth: None,           // ← lost
    ...
});
continue;  // ← skips the synthesis block below
```

The synthesis path that turns `replay_halt_oog = Some(true)` into a
root-halt `OutOfGasInfo { call_depth: 1, .. }` lives below this branch
and is never reached for the Err case.

For EIP-8037 the intrinsic-gas inflation for state-creating txs is
substantial — `CREATE_ACCESS=9000 + 120 × CPSB=183_600` on top of the
base `21_000`. A swath of historical txs sit in a band where they fit
under baseline but not under 8037's intrinsic. Those land here, with
`oog_call_depth = NULL`, and pour into `contract_broken` despite being
the textbook wallet-fixable case: bump the gas limit, they pass.

This is the dominant component of the 8037 99.4% non-OOG share.

### Fix

The `Err` branch needs to emit the synthetic root-halt directly:

```rust
Err(e) => {
    last_attempt = Some(PerScheduleResult {
        success: false,
        ...
        replay_halt_oog: Some(true),
        oog_call_depth: Some(1),                       // ← synthesize
        oog_info_structured: Some(OutOfGasInfo {       // ← synthesize
            opcode: 0,
            opcode_name: "evm_reject_intrinsic".into(),
            pc: 0,
            contract: Address::ZERO,
            call_depth: 1,
            gas_remaining: 0,
            pattern: OogPattern::Unknown,
        }),
        ...
    });
    continue;
}
```

Same shape as the existing `inspector_oog_info.or_else(...)` path, just
lifted into the early-return branch so it isn't bypassed.

With that in place:

- `divergence_call_depth` stays NULL (no per-opcode divergence happened).
- `oog_call_depth = 1` → `classify_oog_chain(frames=[], 1)` returns
  `proportional()`.
- After the §2.3 fix, `is_shallow_oog(Some(1))` is true → bucket is
  `WalletFixableShallow`.

### §3.2 Non-OOG schedule reverts in the "schedule broke it" direction

The "Non-OOG revert" cohort isn't entirely beneficial flips. Some txs
under a more-expensive schedule revert *because of* the schedule —
e.g., 8037 making a sub-call cheaper to retain (via SSTORE refund
accounting) and an explicit `require(gasleft() > X)` failing, or a
schedule-induced state-gas debt that lands the tx below `floor_gas`.

These are genuinely broken — but the chain-walk classifier never sees
them either, because there's no inspector OOG to anchor the walk.

### Fix

Once §1 is in place, the bucket logic should treat
`baseline_to_schedule_break AND oog_call_depth IS NULL` as a separate
bucket: `ContractBrokenNonOog`. That isolates the cohort that needs
deeper inspection (was it a `require(gasleft())`? a state-gas floor
overshoot? a different control-flow path?) from the structural OOG
cohort that the chain-walk already handles.

The dashboard can still aggregate both under "contract-broken" at the
top level, but giving them distinct internal labels makes the
forensics page useful — right now those rows have nothing to show
because divergence/oog fields are NULL, and the user can't tell why.

### §3.3 Per-frame data on the OOG frame itself is never inspected

For depth-1 OOGs where the root isn't captured (the common case), the
chain has length 1 and the per-frame data on `chain[0]` (which
describes the root → depth-1 hop) is dropped. This is the same bug as
§2.1 from a data perspective. The fix is the same.

A useful sanity check after fixing: re-run on the existing 7904 data,
filter on `bucket = 'contract_broken' AND divergence_call_depth <= 2`,
and confirm at least the `.transfer()`-stipend cases now show up under
`oog_bottleneck_kind = Stipend2300` instead of remaining
"Unclassified".

## §4. Cross-cutting: the dashboard fan-out should mirror the producer's bucket

A small but important point: `routes_api.py` filters everything on
`bucket = 'contract_broken'`. With the bugs in §1–§3 the bucket is
load-bearing in the wrong direction — the consumer can't add a "skip
beneficial flips" filter post-hoc because the data needed to
distinguish direction (baseline_success vs schedule_success) isn't
exposed in the aggregated views.

Once §1 is in place, the consumer's `/affected` and `/forensics/*`
endpoints will become *automatically* correct without API changes,
because the producer-side bucket will already exclude rescued txs. The
new `ScheduleRescued` bucket can have its own endpoint (`/api/rescued`)
for the dashboard's "things the EIP fixed" panel.

## §5. Priority and effort

| # | Bug | Severity | Effort | Re-run needed |
| - | --- | -------- | ------ | ------------- |
| §1 | Symmetric `status_changed` | critical | 1h producer + 1h consumer | yes (or one-time backfill) |
| §3.1 | EVM-Err loses oog_call_depth | high (8037) | 30m producer | yes |
| §2.2 | `record_oog` 0-based vs 1-based | high (7904) | 15m producer | yes |
| §2.1 | `classify_chain` ignores chain[0]'s own hop | high | 1h producer + tests | yes |
| §2.3 | `is_shallow_oog` uses wrong depth | medium | 15m producer | yes |
| §3.2 | Surface non-OOG breakage cohort separately | medium | 30m producer | yes |

§1 alone should cut the 7904 contract_broken cohort from ~1,985 down
to something well under 200 — every Uniswap V2 Router row I sampled
flips out. For 8037, §1 + §3.1 together should be the dominant
correction: between them they explain at least ~800K of the 903K
contract_broken population.

The chain-walk classifier itself (`oog_chain.rs`) is fine. Its
correctness is gated on the producer feeding it well-formed input,
which the bugs above prevent.

## §6. Verification queries (post-fix)

```sql
-- §1 sanity: contract_broken should not contain rescues.
SELECT count(*)
FROM divergences
WHERE bucket = 'contract_broken'
  AND baseline_success = 0
  AND schedule_success = 1;
-- Expect: 0 after §1 lands.

-- §3.1 sanity: EVM-Err runs should classify as WalletFixableShallow.
SELECT count(*)
FROM divergences
WHERE bucket = 'wallet_fixable_shallow'
  AND oog_pattern IS NULL
  AND oog_opcode IS NULL;
-- Expect: nonzero and roughly equal to the previous "non-OOG revert"
-- cohort under 8037.

-- §2.1 sanity: depth-1 OOGs with a 2300 stipend at the root → child
-- hop must now have a non-NULL bottleneck.
SELECT count(*) FROM divergences
WHERE oog_call_depth = 2
  AND oog_bottleneck_kind = 'Stipend2300';
-- Expect: > 0, growing with how many `.transfer()`-stipend cases
-- exist in the analyzed range.
```
