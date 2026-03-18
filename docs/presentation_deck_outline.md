# EIP-7904 Deck Outline

## Purpose

This is a concrete slide-by-slide outline for presenting the historical replay impact of EIP-7904 to an audience that already understands the proposal and wants to understand practical consequences.

The main story is:

1. most transactions do not break, but many do change
2. the breakage is structured, not random
3. final failing frames are often misleading
4. recurring intermediaries are where many historical gas assumptions stop holding
5. the decision depends on who owns the fix and how reachable they are

## Slide 1: Title

### Title

`EIP-7904: What Changes, What Breaks, and Who Needs to Act`

### Subtitle

`Historical mainnet replay under the proposed gas schedule`

### Speaker Notes

This talk is not about the proposal mechanics. It is about empirical impact. We replayed historical mainnet transactions under the 7904 pricing schedule and looked at both behavior changes and hard failures.

---

## Slide 2: Executive Summary

### Title

`Executive Summary`

### Main Content

Use a KPI strip with:
- replayed transactions
- divergent transactions
- status-changing transactions

Then add 3 bullets:
- most impact is higher gas cost, not outright breakage
- the breakage that matters is concentrated in recurring call-path patterns
- many visible failures are mediated by routers, aggregators, proxies, and wallet layers rather than isolated target contracts

### Figures / Data

Primary source:
- [`artifacts/briefing.md`](/Users/carl/projects/advanced-repricing-analysis/artifacts/briefing.md)

Optional supporting sources:
- [`artifacts/tables/project_owner_summary.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/project_owner_summary.csv)
- [`artifacts/tables/outreach_priority.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/outreach_priority.csv)

### Speaker Notes

The high-level conclusion is that 7904 does create real breakage, but the story is not simply "some contracts got more expensive." The more useful picture is that the same intermediary systems show up repeatedly as breakpoints in historical call paths.

---

## Slide 3: Changed vs Broken

### Title

`Changed vs Broken`

### Main Content

Show the divergence mix clearly separated into:
- gas-only / gas-pattern changes
- call-tree / output / log changes
- status changes

Add one callout:
- status changes are the most serious subset, but they are only part of the total impact

### Figures / Data

Use:
- [`artifacts/figures/divergence_mix.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/divergence_mix.html)

Optional appendix reference:
- `../repricing-impact-analysis/eip_7904_changed.ipynb`
- `../repricing-impact-analysis/eip_7904_broken.ipynb`

### Speaker Notes

The first thing to separate is "transactions that merely become more expensive" from "transactions that change behavior." If those are mixed together, the audience either overstates or understates the practical risk.

---

## Slide 4: What Users Actually Feel

### Title

`What Users Actually Feel`

### Main Content

Show one chart that emphasizes that gas increases are not diffuse noise. They cluster into characteristic patterns.

Possible framing bullets:
- repricing creates repeatable overhead patterns
- different functions absorb repricing differently
- some systems are consistently pushed toward tighter gas margins

### Figures / Data

Preferred:
- one new derived chart from existing gas-delta tables if available

Fallback:
- adapt a chart from `../repricing-impact-analysis/eip_7904_changed.ipynb`

Supporting tables:
- [`artifacts/tables/incident_summary.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/incident_summary.csv)

### Speaker Notes

Before getting to failures, it helps to establish that 7904 introduces structured overhead. That matters because it explains why breakage later shows up in repeat motifs rather than as one-off accidents.

---

## Slide 5: Why Final Reverts Are Not Enough

### Title

`Why Final Reverts Are Not Enough`

### Main Content

This should be a simple explanatory slide, not a dense chart.

Show:
- a toy call path: `user -> router -> wrapper -> token`
- the final revert appears at the top
- the first structural change happens lower in the stack

Main message:
- final failing frame is often not the right responsibility primitive
- first changed non-root edge is a better root-cause primitive

### Figures / Data

No heavy artifact needed.

Optional small supporting stat:
- cite that most final status-failure summaries terminate at depth 0 while changed non-root edges are numerous

Supporting tables:
- [`artifacts/tables/tx_failure_paths.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/tx_failure_paths.csv)
- [`artifacts/tables/call_graph_edge_comparison.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/call_graph_edge_comparison.csv)

### Speaker Notes

If you only ask "where did the transaction finally revert," you often blame the wrong layer. The stronger signal is the first changed non-root edge in the replayed call graph, because that is where historical gas assumptions first stop holding.

---

## Slide 6: Where Breakage Starts

### Title

`Where Breakage Starts`

### Main Content

Show the top first-changed non-root breakpoint intermediaries.

Highlight the named systems:
- Uniswap V2 Router
- Uniswap
- SushiSwap Router
- Uniswap Permit2
- 1inch / 0x-style aggregation paths

Callout:
- these systems recur across many downstream victims

### Figures / Data

Use:
- [`artifacts/figures/intermediary_breakpoints.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/intermediary_breakpoints.html)

Support with:
- [`artifacts/tables/intermediary_breakpoints.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/intermediary_breakpoints.csv)

### Speaker Notes

This is the first truly explanatory result. The same few intermediaries show up again and again as the first point where replayed behavior diverges. That suggests the ecosystem impact is concentrated in shared routing and forwarding assumptions.

---

## Slide 7: Repeated Failure Motifs

### Title

`Repeated Failure Motifs`

### Main Content

Show 4-6 motifs only.

Ideal motif categories:
- router -> token
- permit2 -> aggregator
- wallet/proxy -> target
- settlement / execution layer -> downstream token

Add one explicit sentence:
- these are not isolated contract-specific bugs; they are repeated integration patterns

### Figures / Data

Use:
- [`artifacts/figures/first_changed_nonroot_motifs.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/first_changed_nonroot_motifs.html)

Support with:
- [`artifacts/tables/first_changed_nonroot_motifs.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/first_changed_nonroot_motifs.csv)

### Speaker Notes

This slide turns the graph signal into something intuitive. Instead of naming a long tail of contracts, it shows repeated path shapes. That is what lets us reason about who owns the fix and whether fixes are likely to generalize.

---

## Slide 8: Who Owns The Fix

### Title

`Who Owns The Fix`

### Main Content

Bucket findings into remediation classes:
- wallet / sender gas estimation issue
- router / integrator gas forwarding issue
- proxy or admin-upgrade fix possible
- immutable contract or migration issue
- deep call-chain shared-responsibility issue

For each bucket, include:
- what the problem looks like
- who has to act
- how hard it is to fix

### Figures / Data

Use summary content from:
- [`artifacts/tables/project_owner_summary.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/project_owner_summary.csv)
- [`artifacts/tables/outreach_priority.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/outreach_priority.csv)
- `../repricing-impact-analysis/affected_parties_guide.md`

### Speaker Notes

This is where the older outreach work is especially valuable. Not all breakage is equally important. Some cases are basically gas-estimation updates. Others require contract upgrades, migrations, or multi-party coordination. The policy significance depends on that split.

---

## Slide 9: Priority Systems To Contact

### Title

`Priority Systems To Contact`

### Main Content

Show a short ranked list only.

Suggested columns:
- project / system
- why it matters
- owner type
- likely remediation path

Keep the list to named, defensible entries.

### Figures / Data

Use:
- [`artifacts/tables/outreach_priority.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/outreach_priority.csv)
- [`artifacts/briefing.md`](/Users/carl/projects/advanced-repricing-analysis/artifacts/briefing.md)

Potential entries:
- Tether / USDT
- Circle / USDC
- Uniswap V2 Router
- SushiSwap Router
- Uniswap Permit2
- 1inch Aggregation Router

### Speaker Notes

The purpose of this slide is not to shame projects. It is to show that the risk surface is concrete enough to support targeted outreach before any decision to ship.

---

## Slide 10: Decision Slide

### Title

`Implications For Shipping 7904`

### Main Content

Use a two-column format:

Left:
- reasons the impact looks manageable

Right:
- reasons caution is still warranted

Examples on the left:
- breakage is concentrated rather than universal
- recurring intermediaries give us leverage
- some important systems appear upgradeable or operationally fixable

Examples on the right:
- immutable high-volume systems still exist
- some breakage is mediated through shared infra
- unknown / unlabeled clusters remain

End with one recommendation sentence:
- either "proceed with targeted outreach and further validation"
- or "do not proceed until the top intermediary clusters are better resolved"

### Figures / Data

No large chart required.

Support from:
- [`artifacts/briefing.md`](/Users/carl/projects/advanced-repricing-analysis/artifacts/briefing.md)
- [`docs/presentation_idea.md`](/Users/carl/projects/advanced-repricing-analysis/docs/presentation_idea.md)

### Speaker Notes

This needs to land as a decision-support slide, not another evidence slide. The central question is whether the identifiable, reachable, and fixable part of the failure surface is large enough to make the remaining residual risk acceptable.

---

## Appendix A: Detailed Contract Rankings

Use for Q&A only.

Include:
- top affected contracts by divergent tx count
- top status-changing contracts
- top raw-address clusters still needing labeling

Sources:
- [`artifacts/tables/top_divergence_contracts.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/top_divergence_contracts.csv)
- [`artifacts/tables/top_status_failures.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/top_status_failures.csv)

---

## Appendix B: Sankey / Flow Views

Use one or more of:
- [`artifacts/figures/project_sankey.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/project_sankey.html)
- [`artifacts/figures/changed_nonroot_sankey.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/changed_nonroot_sankey.html)

Only use in backup unless one becomes especially clean after relabeling.

---

## Appendix C: Specific Caller -> Callee Evidence

Use when challenged on whether intermediary responsibility is real.

Source:
- [`artifacts/figures/status_failure_call_pairs.html`](/Users/carl/projects/advanced-repricing-analysis/artifacts/figures/status_failure_call_pairs.html)
- [`artifacts/tables/status_failure_call_pairs_labeled.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/status_failure_call_pairs_labeled.csv)

This is evidence, not the top-level story.

---

## Build Notes

### Main Deck Rules

- keep the main deck to 10 slides or fewer
- use named systems wherever possible
- avoid raw address-heavy visuals in the main flow
- use one Sankey at most in the main deck
- keep "changed" and "broken" separate
- prefer breakpoint intermediaries over deepest failing frames

### Cleanup Still Worth Doing Before Final Slides

- relabel the top unknown entries in [`artifacts/tables/intermediary_breakpoints.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/intermediary_breakpoints.csv)
- relabel the top unknown entries in [`artifacts/tables/first_changed_nonroot_motifs.csv`](/Users/carl/projects/advanced-repricing-analysis/artifacts/tables/first_changed_nonroot_motifs.csv)
- produce one cleaner gas-overhead chart for Slide 4

### Fastest Path To An Actual Deck

1. export static images from the selected Plotly figures
2. relabel the ugliest top motifs and intermediaries
3. build only Slides 2, 6, 7, 8, and 10 first
4. add the rest once the spine of the talk feels right
