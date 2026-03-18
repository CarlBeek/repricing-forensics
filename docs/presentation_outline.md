# EIP-7904 Impact Presentation Outline

## 1. Headline Impact

- How many replayed transactions diverged under `7904-prelim`
- How many changed only in gas vs changed in outcome
- How many blocks saw concentrated divergence clusters

## 2. What Changes

- Divergence mix: gas-only, status changes, call-tree changes, log changes
- Aggregate gas delta and tails of the gas-impact distribution
- Largest positive and negative gas deltas

## 3. Where Breakage Happens

- First-divergence call-depth distribution
- Top divergence opcodes and affected contracts
- Call-stack Sankey: root caller -> intermediate system -> failing callee

## 4. Who Actually Owns The Fix

- Immutable target contracts
- Upgradeable contracts
- Proxy and wallet/safe mediated systems
- Router / aggregator / integrator gas budgeting issues

## 5. Project Outreach Priorities

- Largest status-change clusters by project
- High-incidence but easy-to-fix systems
- Low-incidence but severe failures
- Unknown / unverified contracts that need manual triage

## 6. Decision Support

- What breaks today in historical replay
- Which failures are probably upstream integration issues
- Which failures look expensive to remediate
- Recommended next contact list before deciding whether to ship 7904
