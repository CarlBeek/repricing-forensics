# EIP-7904 Presentation Idea

## Goal

Present EIP-7904 impact as a decision brief for people who already understand the proposal and want to know:

- how much behavior changes in practice
- what actually breaks vs just gets more expensive
- where in call stacks the failures start
- who owns the fix
- whether the remaining risk looks acceptable

The deck should not feel like a notebook walkthrough. It should feel like a compact answer to "if we ship this, what breaks, why, and who needs to act?"

## Best Overall Framing

The best structure is to combine the strongest parts of `../repricing-impact-analysis` with the stronger graph evidence from this repo:

- from `../repricing-impact-analysis`:
  - separate `changed` from `broken`
  - classify fixability and outreach priority
  - make remediation concrete
- from this repo:
  - show that final failing frames are usually not enough
  - use first changed non-root edges and intermediary analysis to explain where things actually go wrong
  - show recurring routers / aggregators / wrappers, not just downstream victims

That produces a clean core message:

> Most impact is increased gas cost, but the most important breakage is often mediated by recurring intermediaries whose historical gas assumptions stop holding under 7904.

## Recommended Deck Structure

Aim for 8 slides plus appendix.

### 1. Executive Summary

One-slide answer to the whole question.

Include:
- total replayed transactions
- total divergent transactions
- total status-changing transactions
- one sentence on the main finding:
  - many user-visible failures are not isolated target-contract failures
  - they are often routed through repeat intermediary patterns

Use:
- a small KPI strip
- 2-3 bullets only

### 2. Changed vs Broken

This is the most important framing slide from the older analysis.

Show:
- transactions that only cost more gas
- transactions that change behavior
- transactions that go success -> fail

Reason:
- it keeps the audience from over-reading the breakage count
- it establishes that the main concern is concentrated breakage, not universal failure

Use:
- divergence mix chart from [`artifacts/figures/divergence_mix.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/divergence_mix.html)
- optionally one supporting stat from `../repricing-impact-analysis/affected_parties_guide.md`

### 3. What Users Actually Feel

Borrow the story shape from `eip_7904_changed.ipynb`: "what changes for users?"

Show:
- typical gas overhead patterns
- characteristic overhead buckets
- maybe one contract/opcode heatmap or delta distribution

This slide answers:
- is this random noise?
- or are there repeatable gas-cost patterns?

Keep it to one chart and one sentence:
- repricing creates recognizable overhead patterns, not diffuse randomness

### 4. Why Call-Graph Analysis Is Necessary

This is where the new analysis becomes essential.

Explain:
- if you look only at final failing frames, many failures appear to terminate at root depth
- that is not enough to explain responsibility
- the better primitive is the first changed non-root edge per tx

This should be a logic slide, not a data dump.

Suggested visual:
- simple diagram: `root call -> intermediary -> target`
- small annotation that "final revert location" and "first structural break" are often different

### 5. Where Breakage Starts

This is the key data slide.

Show the top first-changed non-root breakpoint intermediaries:
- Uniswap V2 Router
- Uniswap
- SushiSwap Router
- Permit2
- 1inch / 0x style aggregation paths
- other top unlabeled clusters if they remain material

Use:
- [`artifacts/figures/intermediary_breakpoints.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/intermediary_breakpoints.html)
- derived simplified table from [`artifacts/tables/intermediary_breakpoints.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/intermediary_breakpoints.csv)

Core takeaway:
- the same few intermediaries show up repeatedly as the first point where historical gas assumptions stop holding

### 6. Repeated Failure Motifs

This is the strongest explanatory slide.

Instead of showing a pile of addresses, show 4-6 labeled motifs:
- router -> token
- permit2 -> aggregator
- wallet/proxy -> target
- settlement layer -> downstream token

Use:
- [`artifacts/figures/first_changed_nonroot_motifs.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/first_changed_nonroot_motifs.html)
- only after relabeling the ugliest raw-address motifs

This slide should answer:
- are these isolated bugs?
- or repeated ecosystem integration patterns?

### 7. Who Owns The Fix

This should borrow directly from the spirit of `affected_parties_guide.md`.

Bucket affected systems into:
- wallet / sender gas estimation issue
- immutable contract or migration issue
- upgradeable admin fix possible
- router / integrator gas forwarding issue
- deep call-chain / shared responsibility issue

The audience should leave this slide knowing:
- which failures are cheap to fix
- which failures require coordination
- which failures are basically user tooling issues

Suggested format:
- 5 buckets
- 1 sentence on remediation for each
- 1-2 named examples in each bucket

### 8. Recommendation / Decision Slide

End with a policy question, not another chart.

Suggested structure:
- `Safe enough if...`
- `Needs more work before shipping if...`

Include:
- what fraction of breakage appears to be upstream integrator behavior
- what fraction seems tied to immutable / hard-to-fix systems
- whether the top ecosystem intermediaries are reachable and upgradeable

This slide should be explicit about uncertainty:
- unknown / unverified clusters still matter
- but the failure surface is now structured enough to prioritize outreach

## Best Supporting Appendix

Put everything detailed in backup slides:

- top affected contracts by divergent txs
- top status-changing contracts
- opcode breakdown
- gas-delta distributions
- raw Sankey charts
- address-level outreach table
- unlabeled top clusters needing manual review

The appendix can be dense. The main deck should not be.

## Figure Selection

Use only a small subset of current figures in the main deck.

Best main-deck candidates:
- [`artifacts/figures/divergence_mix.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/divergence_mix.html)
- [`artifacts/figures/intermediary_breakpoints.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/intermediary_breakpoints.html)
- [`artifacts/figures/first_changed_nonroot_motifs.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/first_changed_nonroot_motifs.html)
- [`artifacts/figures/changed_nonroot_intermediaries.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/changed_nonroot_intermediaries.html)

Best appendix figures:
- [`artifacts/figures/project_sankey.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/project_sankey.html)
- [`artifacts/figures/changed_nonroot_sankey.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/changed_nonroot_sankey.html)
- [`artifacts/figures/status_failure_call_pairs.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/status_failure_call_pairs.html)
- contract-specific or opcode-specific histograms inspired by `../repricing-impact-analysis/eip_7904_changed.ipynb`

## What To Avoid

- Do not lead with USDT or any single token. It is a large cluster, but it is not the right top-level story.
- Do not use raw address tables in the main deck unless the address is a known public system and the label is trustworthy.
- Do not rely on deepest failing frame as the main root-cause explanation.
- Do not mix "more expensive" and "broken" on the same slide without clearly separating them.
- Do not show too many Sankeys. One is enough in the main talk.

## Suggested Speaker Narrative

Use this flow:

1. `Most transactions do not break, but a meaningful set changes and a smaller set breaks.`
2. `The breakage is not random.`
3. `Looking only at the final revert is misleading.`
4. `The useful signal is where the first changed non-root edge appears.`
5. `That points to recurring intermediaries: routers, aggregators, proxies, wallet layers.`
6. `So the decision is not just about affected target contracts. It is about whether the ecosystem can realistically update the intermediary layer in time.`

## Concrete Deck Title Options

- `EIP-7904: What Changes, What Breaks, and Who Needs to Act`
- `EIP-7904 Impact on Mainnet Replay: Breakpoints, Intermediaries, and Fixability`
- `EIP-7904 Historical Replay: User Impact and Ecosystem Remediation`

## Recommended Next Step Before Building Slides

Before turning this into a polished deck, do one more cleanup pass on labels for:

- top breakpoint intermediaries
- top first-changed motifs
- top unknown clusters in outreach priority

That will let the main deck stay focused on named systems and clear ownership instead of raw addresses.
