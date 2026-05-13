"""JSON API endpoints for the gas repricing analysis web server."""
from __future__ import annotations

import math

from fastapi import APIRouter, Query

from repricing_forensics.labels import infer_project_label

from .db import (
    SCHEDULE_NAME,
    db_mtime,
    label_address,
    query,
    query_df,
    query_scalar,
)

router = APIRouter(prefix="/api")

# In the new schema (docs/storage-redesign.md) the producer pre-classifies
# every tx into a `bucket` and only emits per-tx rows for the drill-in
# cohort (contract_broken + event_logs_changed). Wallet-fixable rows
# never enter `divergences`, so most "contract-broken" queries simply
# filter `WHERE bucket = 'contract_broken'` instead of the old
# NOT_WALLET_FIXABLE filter that lived in the consumer.

FORENSIC_OPCODE_NAMES = {
    "0x04": "DIV",
    "0x05": "SDIV",
    "0x06": "MOD",
    "0x07": "SMOD",
    "0x08": "ADDMOD",
    "0x09": "MULMOD",
    "0x0a": "EXP",
    "0x20": "KECCAK256",
}

# Full EVM opcode → mnemonic lookup. Divergence and OOG points can fall on
# any opcode (CALL, SSTORE, INVALID, etc.), not just the eight repriced
# ones, so the dashboard needs the whole table to avoid surfacing raw
# `0xfe`-style hex. Covers everything live on mainnet through Cancun
# (PUSH0, MCOPY, transient storage, blob ops); EOF opcodes are not in
# this set, and unknown bytes fall through to `0xXX` as before.
EVM_OPCODE_NAMES = {
    0x00: "STOP", 0x01: "ADD", 0x02: "MUL", 0x03: "SUB",
    0x04: "DIV", 0x05: "SDIV", 0x06: "MOD", 0x07: "SMOD",
    0x08: "ADDMOD", 0x09: "MULMOD", 0x0a: "EXP", 0x0b: "SIGNEXTEND",
    0x10: "LT", 0x11: "GT", 0x12: "SLT", 0x13: "SGT", 0x14: "EQ",
    0x15: "ISZERO", 0x16: "AND", 0x17: "OR", 0x18: "XOR", 0x19: "NOT",
    0x1a: "BYTE", 0x1b: "SHL", 0x1c: "SHR", 0x1d: "SAR",
    0x20: "KECCAK256",
    0x30: "ADDRESS", 0x31: "BALANCE", 0x32: "ORIGIN", 0x33: "CALLER",
    0x34: "CALLVALUE", 0x35: "CALLDATALOAD", 0x36: "CALLDATASIZE",
    0x37: "CALLDATACOPY", 0x38: "CODESIZE", 0x39: "CODECOPY",
    0x3a: "GASPRICE", 0x3b: "EXTCODESIZE", 0x3c: "EXTCODECOPY",
    0x3d: "RETURNDATASIZE", 0x3e: "RETURNDATACOPY", 0x3f: "EXTCODEHASH",
    0x40: "BLOCKHASH", 0x41: "COINBASE", 0x42: "TIMESTAMP",
    0x43: "NUMBER", 0x44: "PREVRANDAO", 0x45: "GASLIMIT", 0x46: "CHAINID",
    0x47: "SELFBALANCE", 0x48: "BASEFEE", 0x49: "BLOBHASH", 0x4a: "BLOBBASEFEE",
    0x50: "POP", 0x51: "MLOAD", 0x52: "MSTORE", 0x53: "MSTORE8",
    0x54: "SLOAD", 0x55: "SSTORE", 0x56: "JUMP", 0x57: "JUMPI",
    0x58: "PC", 0x59: "MSIZE", 0x5a: "GAS", 0x5b: "JUMPDEST",
    0x5c: "TLOAD", 0x5d: "TSTORE", 0x5e: "MCOPY", 0x5f: "PUSH0",
    **{0x60 + i: f"PUSH{i + 1}" for i in range(32)},
    **{0x80 + i: f"DUP{i + 1}" for i in range(16)},
    **{0x90 + i: f"SWAP{i + 1}" for i in range(16)},
    **{0xa0 + i: f"LOG{i}" for i in range(5)},
    0xf0: "CREATE", 0xf1: "CALL", 0xf2: "CALLCODE", 0xf3: "RETURN",
    0xf4: "DELEGATECALL", 0xf5: "CREATE2",
    0xfa: "STATICCALL", 0xfd: "REVERT", 0xfe: "INVALID", 0xff: "SELFDESTRUCT",
}


def opcode_label(op: int | None) -> str:
    """Return the canonical mnemonic for an opcode byte, or '0xXX' if unknown."""
    if op is None:
        return ""
    op = int(op)
    return EVM_OPCODE_NAMES.get(op, f"0x{op:02x}")


# Kept for backwards-compatibility with code that explicitly cared about
# the eight repriced opcodes (e.g. the opcode-impact chart palette).
EIP7904_OPCODE_INT_NAMES = {
    0x04: "DIV", 0x05: "SDIV", 0x06: "MOD", 0x07: "SMOD",
    0x08: "ADDMOD", 0x09: "MULMOD", 0x0a: "EXP", 0x20: "KECCAK256",
}


def _int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, float) and math.isnan(value):
        return 0
    return int(value)


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    value = float(value)
    return None if math.isnan(value) else value


def _float(value) -> float:
    value = _float_or_none(value)
    return value if value is not None else 0.0


def _hex(value) -> str | None:
    """tx_hash / block_hash come back from DuckDB as BLOB → bytes. Render
    as `0x…` for JSON output. Pass-through strings unchanged."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return "0x" + value.hex()
    return str(value)


# ── Briefing endpoints ────────────────────────────────────────────────


@router.get("/overview")
def overview():
    # All headline counts come from block_coverage's per-bucket totals.
    # The producer's classifier is the single source of truth for which
    # bucket a tx belongs to; the consumer doesn't second-guess it.
    row = query("""
        SELECT
            sum(tx_count) AS total_analyzed,
            sum(tx_count - tx_count_unchanged) AS divergent_txs,
            sum(tx_count_contract_broken) AS contract_broken,
            sum(tx_count_wallet_fixable_shallow) AS wallet_fixable_shallow,
            sum(tx_count_wallet_fixable_deep_chain) AS wallet_fixable_deep_chain
        FROM block_coverage
    """)[0]
    total_analyzed = _int(row["total_analyzed"])
    contract_broken = _int(row["contract_broken"])
    wallet_fixable_shallow = _int(row["wallet_fixable_shallow"])
    wallet_fixable_deep_chain = _int(row["wallet_fixable_deep_chain"])
    wallet_fixable = wallet_fixable_shallow + wallet_fixable_deep_chain
    broken = contract_broken + wallet_fixable
    return {
        "total_analyzed": total_analyzed,
        "divergent_txs": _int(row["divergent_txs"]),
        "broken_txs": broken,
        "wallet_fixable_txs": wallet_fixable,
        "wallet_fixable_shallow_txs": wallet_fixable_shallow,
        "wallet_fixable_deep_chain_txs": wallet_fixable_deep_chain,
        "contract_broken_txs": contract_broken,
        "breakage_rate": round(broken / total_analyzed * 100, 2) if total_analyzed else 0,
        "contract_breakage_rate": round(contract_broken / total_analyzed * 100, 2) if total_analyzed else 0,
    }


@router.get("/funnel")
def funnel():
    """Bucket every divergent tx by observable impact.

    `trace_divergent_only` is the previously-mislabelled cohort: txs whose
    intermediate EVM trace differs from baseline but whose final outcome
    (gas used, event logs, status) matches — i.e. no observable change.
    """
    row = query("""
        SELECT
            sum(tx_count - tx_count_unchanged) AS total,
            sum(tx_count_contract_broken
                + tx_count_wallet_fixable_shallow
                + tx_count_wallet_fixable_deep_chain) AS broken,
            sum(tx_count_event_logs_changed) AS event_log_changed,
            sum(tx_count_gas_only)           AS gas_only_change,
            sum(tx_count_trace_only)         AS trace_divergent_only
        FROM block_coverage
    """)[0]
    return {
        "divergent_txs": _int(row["total"]),
        "broken_txs": _int(row["broken"]),
        "event_log_changed": _int(row["event_log_changed"]),
        "gas_only_change": _int(row["gas_only_change"]),
        "trace_divergent_only": _int(row["trace_divergent_only"]),
    }


@router.get("/opcode-impact")
def opcode_impact():
    rows = query("""
        SELECT divergence_opcode AS opcode_num, count(*) AS cnt
        FROM divergences
        WHERE divergence_opcode IS NOT NULL
          AND bucket = 'contract_broken'
        GROUP BY 1 ORDER BY cnt DESC
    """)
    total = sum(r["cnt"] for r in rows)
    return [
        {
            "opcode": f"0x{int(r['opcode_num']):02x}",
            "name": opcode_label(r["opcode_num"]),
            "count": int(r["cnt"]),
            "share": round(r["cnt"] / total * 100, 1) if total else 0,
        }
        for r in rows
    ]


@router.get("/gas-overhead")
def gas_overhead():
    """CDF + stats over the non-broken cohort (gas_only, trace_only,
    event_logs_changed) reconstructed from `block_summaries`'s pre-binned
    log2 histograms.

    Percentiles are bin-aligned (powers of two); we can't recover the
    finer-grained percentiles the old query computed because the
    aggregate cohort isn't stored per-tx anymore. CDF fidelity is
    unchanged — it was already plotted from the same log2 bins.
    """
    rows = query("""
        SELECT bucket, tx_count, gas_delta_sum, gas_delta_min, gas_delta_max,
               gas_delta_log2_hist
        FROM block_summaries
        WHERE bucket IN ('gas_only', 'trace_only', 'event_logs_changed')
    """)
    return _gas_delta_aggregate_response(rows)


def _gas_delta_aggregate_response(rows: list[dict]) -> dict:
    import json

    total_count = 0
    total_sum = 0
    combined_hist = [0] * 12
    gmin = None
    gmax = None
    for r in rows:
        total_count += _int(r["tx_count"])
        total_sum += _int(r["gas_delta_sum"])
        hist_raw = r.get("gas_delta_log2_hist")
        # SQLite stores the histogram as a JSON-encoded TEXT column;
        # decode here. A plain list (legacy in-memory tests) passes
        # through unchanged.
        if isinstance(hist_raw, str):
            try:
                hist_raw = json.loads(hist_raw)
            except (json.JSONDecodeError, TypeError):
                hist_raw = None
        if hist_raw is not None:
            for i, c in enumerate(hist_raw):
                if i < 12:
                    combined_hist[i] += _int(c)
        rmin = r.get("gas_delta_min")
        rmax = r.get("gas_delta_max")
        if rmin is not None:
            gmin = rmin if gmin is None else min(gmin, _int(rmin))
        if rmax is not None:
            gmax = rmax if gmax is None else max(gmax, _int(rmax))

    mean = total_sum / total_count if total_count else 0
    stats = {
        "cnt": total_count,
        "mean_delta": mean,
        "median_delta": _percentile_from_log2_hist(combined_hist, 0.5),
        "p5":  _percentile_from_log2_hist(combined_hist, 0.05),
        "p25": _percentile_from_log2_hist(combined_hist, 0.25),
        "p75": _percentile_from_log2_hist(combined_hist, 0.75),
        "p95": _percentile_from_log2_hist(combined_hist, 0.95),
        "p99": _percentile_from_log2_hist(combined_hist, 0.99),
    }
    total_hist = sum(combined_hist) or 1
    return {
        "stats": stats,
        "histogram": [
            {
                "bin_start": 2 ** i if i > 0 else 0,
                "label": (f"{2**i}–{2**(i+1)}" if i > 0 else "≤1"),
                "count": int(combined_hist[i]),
                "density": round(combined_hist[i] / total_hist, 6),
            }
            for i in range(len(combined_hist))
        ],
    }


def _percentile_from_log2_hist(hist: list[int], p: float) -> float:
    """Inverse-CDF over log2 bins, with linear interpolation inside the
    target bin so percentiles aren't collapsed to bin edges.

    Bin i represents gas-delta in [2^i, 2^(i+1)) for i > 0, [0, 1] for
    i = 0. When the target percentile falls inside bin i, we return
    `lo + frac * (hi - lo)` where `frac` is the position within the bin
    based on the cumulative count. This avoids reporting every
    percentile as the same number when a single bin holds most of the
    mass.
    """
    total = sum(hist)
    if total == 0:
        return 0.0
    target = total * p
    cumulative = 0
    for i, c in enumerate(hist):
        new_cum = cumulative + c
        if new_cum >= target:
            if c == 0:
                # Defensive: shouldn't happen since `c >= target - cumulative > 0`
                # unless `target == cumulative` exactly.
                return float(2 ** i if i > 0 else 0)
            frac = (target - cumulative) / c
            lo = 0.0 if i == 0 else float(2 ** i)
            hi = float(2 ** (i + 1))
            return lo + frac * (hi - lo)
        cumulative = new_cum
    return float(2 ** len(hist))


@router.get("/concentration")
def concentration():
    df = query_df("""
        SELECT recipient, count(*) AS broken_txs
        FROM divergences
        WHERE bucket = 'contract_broken'
        GROUP BY recipient ORDER BY broken_txs DESC
    """)
    df["cumulative"] = df["broken_txs"].cumsum()
    total = df["broken_txs"].sum()
    df["cum_pct"] = df["cumulative"] / total * 100 if total else 0
    return [
        {
            "rank": i + 1,
            "recipient": row["recipient"] if isinstance(row["recipient"], str) else None,
            "name": label_address(row["recipient"]),
            "broken_txs": _int(row["broken_txs"]),
            "cum_pct": round(_float(row["cum_pct"]), 2),
        }
        for i, row in df.head(50).iterrows()
    ]


@router.get("/top-contracts")
def top_contracts(limit: int = Query(default=10, le=500)):
    rows = query(f"""
        SELECT recipient, count(*) AS broken_txs,
               avg(gas_delta) AS avg_delta, sum(gas_delta) AS total_delta
        FROM divergences
        WHERE bucket = 'contract_broken'
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT {int(limit)}
    """)
    return [
        {
            "recipient": r["recipient"],
            "name": label_address(r["recipient"]),
            "broken_txs": _int(r["broken_txs"]),
            "avg_delta": _float(r["avg_delta"]),
            "total_delta": _float(r["total_delta"]),
        }
        for r in rows
    ]


# ── Forensics endpoints ──────────────────────────────────────────────


@router.get("/forensics/time-series")
def forensics_time_series():
    return query("""
        WITH bounds AS (
            SELECT min(block_number) AS mn, max(block_number) AS mx FROM block_coverage
        ),
        buckets AS (
            SELECT greatest((mx - mn) / 300, 1) AS bucket_size, mn FROM bounds
        ),
        broken_per_bucket AS (
            SELECT
                b.mn + ((d.block_number - b.mn) // b.bucket_size) * b.bucket_size AS block_group,
                count(*) AS broken
            FROM divergences d, buckets b
            WHERE d.bucket = 'contract_broken'
            GROUP BY block_group
        ),
        total_per_bucket AS (
            SELECT
                b.mn + ((c.block_number - b.mn) // b.bucket_size) * b.bucket_size AS block_group,
                sum(c.tx_count) AS total_txs
            FROM block_coverage c, buckets b
            GROUP BY block_group
        )
        SELECT
            t.block_group,
            coalesce(bp.broken, 0) AS broken,
            t.total_txs,
            CASE WHEN t.total_txs > 0
                 THEN round(coalesce(bp.broken, 0) * 100.0 / t.total_txs, 4)
                 ELSE 0
            END AS broken_pct
        FROM total_per_bucket t
        LEFT JOIN broken_per_bucket bp ON t.block_group = bp.block_group
        ORDER BY t.block_group
    """)


@router.get("/forensics/gas-delta")
def forensics_gas_delta():
    """Gas-delta stats + histogram for the contract-broken cohort.

    Contract-broken rows are per-tx in `divergences`, so percentiles are
    exact (unlike the aggregate-cohort percentiles in /api/gas-overhead,
    which approximate from log2 bins).
    """
    stats = query("""
        SELECT
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_delta) as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM divergences WHERE bucket = 'contract_broken'
    """)[0]
    histogram = query("""
        WITH bucketed AS (
            SELECT
                CASE WHEN gas_delta <= 0 THEN 0
                     ELSE floor(log2(gas_delta))::int
                END AS log_bin,
                count(*) AS cnt
            FROM divergences WHERE bucket = 'contract_broken'
            GROUP BY 1
        )
        SELECT log_bin, cnt FROM bucketed ORDER BY log_bin
    """)
    total = sum(r["cnt"] for r in histogram) or 1
    return {
        "stats": {k: float(v) if v is not None else 0 for k, v in stats.items()},
        "histogram": [
            {
                "bin_start": 2 ** int(r["log_bin"]) if r["log_bin"] > 0 else 0,
                "label": f'{2**int(r["log_bin"])}–{2**(int(r["log_bin"])+1)}' if r["log_bin"] > 0 else '≤1',
                "count": int(r["cnt"]),
                "density": round(r["cnt"] / total, 6),
            }
            for r in histogram
        ],
    }


@router.get("/forensics/call-depth")
def forensics_call_depth():
    return query("""
        SELECT
            coalesce(divergence_call_depth, -1) AS divergence_call_depth,
            count(*) AS divergent_txs
        FROM divergences
        WHERE bucket = 'contract_broken'
        GROUP BY 1 ORDER BY 1
    """)


@router.get("/forensics/bottleneck-kinds")
def forensics_bottleneck_kinds():
    """How many contract-broken txs hit each kind of gas-forwarding bottleneck.

    The producer's chain-walk classifier tags every contract-broken OOG row
    with the kind of throttle that broke the call chain (Stipend2300 /
    FixedGas / FractionalGas). Rows whose chain was fully proportional are
    not contract-broken — the producer bucketed them as wallet_fixable_*
    and they don't appear in `divergences`. Rows where the classifier
    didn't produce a kind (older runs) bucket as 'Unclassified'.
    """
    rows = query("""
        SELECT
            coalesce(oog_bottleneck_kind, 'Unclassified') AS kind,
            count(*) AS cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
        GROUP BY 1
        ORDER BY cnt DESC
    """)
    total = sum(r["cnt"] for r in rows) or 1
    return [
        {
            "kind": r["kind"],
            "count": _int(r["cnt"]),
            "share": round(r["cnt"] / total * 100, 1),
        }
        for r in rows
    ]


# SQL fragment used by both /forensics/failure-motifs and
# /forensics/failure-flow. "Failing leaf" = the deepest CALL frame in a
# contract-broken tx whose success flag is FALSE — that's the frame
# that OOG'd. We pick the deepest such frame per tx via a row-number
# window. For most real txs every ancestor frame also has success=FALSE
# (the OOG bubbles up), but ranking by depth keeps the leaf consistent.
_FAILING_LEAVES_CTE = """
WITH failing_leaves AS (
    SELECT
        cf.divergence_id,
        cf.from_address,
        cf.to_address,
        cf.gas_provided,
        ROW_NUMBER() OVER (
            PARTITION BY cf.divergence_id
            ORDER BY cf.depth DESC, cf.call_index DESC
        ) AS rn
    FROM call_frames cf
    JOIN divergences d USING (divergence_id)
    WHERE d.bucket = 'contract_broken'
      AND cf.success = FALSE
)
"""


@router.get("/forensics/failure-motifs")
def forensics_failure_motifs():
    """Top caller→callee pairs at the failing leaf frame.

    `pair_motif` is (caller_project, callee_project). `triple_motif` adds
    the root-frame project for context — useful when the same library
    fails from different top-level entry points.
    """
    rows = query(_FAILING_LEAVES_CTE + """,
    roots AS (
        SELECT cf.divergence_id, cf.to_address AS root_to
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken' AND cf.depth = 0
    )
    SELECT
        lower(coalesce(fl.from_address, '')) AS caller,
        lower(coalesce(fl.to_address, ''))   AS callee,
        lower(coalesce(r.root_to, ''))       AS root,
        count(*)                             AS status_failures,
        avg(fl.gas_provided)                 AS avg_gas_provided
    FROM failing_leaves fl
    LEFT JOIN roots r USING (divergence_id)
    WHERE fl.rn = 1
    GROUP BY 1, 2, 3
    ORDER BY status_failures DESC
    LIMIT 50
    """)
    out: list[dict] = []
    for r in rows:
        caller = infer_project_label(r["caller"])
        callee = infer_project_label(r["callee"])
        root = infer_project_label(r["root"])
        out.append({
            "pair_motif":   f"{caller} → {callee}",
            "triple_motif": f"{root} → {caller} → {callee}",
            "status_failures": _int(r["status_failures"]),
            "avg_gas_provided": _float(r["avg_gas_provided"]),
        })
    return out[:15]


@router.get("/forensics/failure-flow")
def forensics_failure_flow():
    """Sankey: root project → failing caller project → failing callee project.

    Sourced from `call_frames` for the contract-broken cohort. Addresses
    that don't have a hardcoded label fall back to the address itself —
    once the producer-side `contract_metadata` table lands we can do
    nicer labeling via codehash.
    """
    rows = query(_FAILING_LEAVES_CTE + """,
    roots AS (
        SELECT cf.divergence_id, cf.to_address AS root_to
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken' AND cf.depth = 0
    )
    SELECT
        lower(coalesce(r.root_to, ''))       AS root_addr,
        lower(coalesce(fl.from_address, '')) AS caller_addr,
        lower(coalesce(fl.to_address, ''))   AS callee_addr,
        count(*) AS status_failures
    FROM failing_leaves fl
    LEFT JOIN roots r USING (divergence_id)
    WHERE fl.rn = 1
    GROUP BY 1, 2, 3
    ORDER BY status_failures DESC
    LIMIT 200
    """)
    if not rows:
        return {"labels": [], "sources": [], "targets": [], "values": [], "link_colors": []}

    # Sum to project-level edges, keep top-15 per edge type.
    from collections import defaultdict
    rc_edges: dict[tuple[str, str], int] = defaultdict(int)
    cc_edges: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        root = infer_project_label(r["root_addr"])
        caller = infer_project_label(r["caller_addr"])
        callee = infer_project_label(r["callee_addr"])
        n = _int(r["status_failures"])
        rc_edges[(root, caller)] += n
        cc_edges[(caller, callee)] += n
    top_rc = sorted(rc_edges.items(), key=lambda kv: kv[1], reverse=True)[:15]
    top_cc = sorted(cc_edges.items(), key=lambda kv: kv[1], reverse=True)[:15]

    used = {p for (a, b), _ in top_rc for p in (a, b)}
    used |= {p for (a, b), _ in top_cc for p in (a, b)}
    labels = sorted(used)
    idx = {l: i for i, l in enumerate(labels)}

    sources, targets, values, link_colors = [], [], [], []
    for (a, b), n in top_rc:
        sources.append(idx[a]); targets.append(idx[b])
        values.append(n); link_colors.append("rgba(52,152,219,0.3)")
    for (a, b), n in top_cc:
        sources.append(idx[a]); targets.append(idx[b])
        values.append(n); link_colors.append("rgba(231,76,60,0.3)")

    display_labels = [label_address(l) if l.startswith("0x") else l for l in labels]
    return {
        "labels": display_labels,
        "sources": sources,
        "targets": targets,
        "values": values,
        "link_colors": link_colors,
    }


# /forensics/remediation retired: the project_owner_summary.csv it
# consumed was hand-curated metadata (project owners + remediation
# buckets) that doesn't live in this repo anymore. When the
# producer-side `contract_metadata` table ships with codehash-keyed
# solc/EVM info, we can compute a different remediation surface from
# that (e.g. cluster by solc version); until then there's nothing to
# show, so the endpoint is gone rather than returning empty stubs.


# ── EIP-8037 state-gas endpoints ──────────────────────────────────────


@router.get("/eip8037/overview")
def eip8037_overview():
    stats = query("""
        SELECT
            count(*) AS divergent_txs,
            sum(CASE WHEN original_limit_failure THEN 1 ELSE 0 END)
                AS original_limit_failures,
            sum(CASE WHEN schedule_success AND original_limit_failure THEN 1 ELSE 0 END)
                AS fixable_with_more_outer_gas,
            sum(CASE WHEN baseline_success AND NOT schedule_success THEN 1 ELSE 0 END)
                AS baseline_success_schedule_failures,
            sum(CASE WHEN NOT schedule_success AND min_multiplier_to_succeed IS NULL THEN 1 ELSE 0 END)
                AS unresolved_replay_failures,
            sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END)
                AS reservoir_exhausted_txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            sum(runtime_state_gas) AS total_runtime_state_gas,
            sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
            avg(schedule_state_gas_spent) AS avg_state_gas_spent,
            max(schedule_state_gas_spent) AS max_state_gas_spent,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p50_min_multiplier_to_succeed,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p95_min_multiplier_to_succeed,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY min_multiplier_to_succeed)
                AS p99_min_multiplier_to_succeed,
            max(min_multiplier_to_succeed) AS max_min_multiplier_to_succeed,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY extra_gas_needed)
                AS p95_extra_gas_needed,
            max(extra_gas_needed) AS max_extra_gas_needed
        FROM eip8037_tx_impact
    """)[0]
    total_analyzed = query_scalar("SELECT sum(tx_count) FROM block_coverage", default=0)
    block_range = query("SELECT min(block_number) AS mn, max(block_number) AS mx FROM block_coverage")
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}

    return {
        "schedule_name": SCHEDULE_NAME,
        "total_analyzed": _int(total_analyzed),
        "min_block": _int(br["mn"]),
        "max_block": _int(br["mx"]),
        "divergent_txs": _int(stats["divergent_txs"]),
        "original_limit_failures": _int(stats["original_limit_failures"]),
        "fixable_with_more_outer_gas": _int(stats["fixable_with_more_outer_gas"]),
        "baseline_success_schedule_failures": _int(stats["baseline_success_schedule_failures"]),
        "unresolved_replay_failures": _int(stats["unresolved_replay_failures"]),
        "reservoir_exhausted_txs": _int(stats["reservoir_exhausted_txs"]),
        "total_state_gas_spent": _int(stats["total_state_gas_spent"]),
        "total_runtime_state_gas": _int(stats["total_runtime_state_gas"]),
        "total_runtime_state_gas_spillover": _int(stats["total_runtime_state_gas_spillover"]),
        "avg_state_gas_spent": _float(stats["avg_state_gas_spent"]),
        "max_state_gas_spent": _int(stats["max_state_gas_spent"]),
        "p50_min_multiplier_to_succeed": _float_or_none(stats["p50_min_multiplier_to_succeed"]),
        "p95_min_multiplier_to_succeed": _float_or_none(stats["p95_min_multiplier_to_succeed"]),
        "p99_min_multiplier_to_succeed": _float_or_none(stats["p99_min_multiplier_to_succeed"]),
        "max_min_multiplier_to_succeed": _float_or_none(stats["max_min_multiplier_to_succeed"]),
        "p95_extra_gas_needed": _int(stats["p95_extra_gas_needed"]),
        "max_extra_gas_needed": _int(stats["max_extra_gas_needed"]),
    }


@router.get("/eip8037/multiplier-histogram")
def eip8037_multiplier_histogram():
    rows = query("""
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN would_fit_in_original_limit THEN 0
                    WHEN min_multiplier_to_succeed IS NULL THEN 99
                    WHEN min_multiplier_to_succeed <= 1.25 THEN 1
                    WHEN min_multiplier_to_succeed <= 1.50 THEN 2
                    WHEN min_multiplier_to_succeed <= 2.00 THEN 3
                    WHEN min_multiplier_to_succeed <= 4.00 THEN 4
                    WHEN min_multiplier_to_succeed <= 8.00 THEN 5
                    ELSE 6
                END AS sort_key,
                CASE
                    WHEN would_fit_in_original_limit THEN 'fits original'
                    WHEN min_multiplier_to_succeed IS NULL THEN 'unresolved'
                    WHEN min_multiplier_to_succeed <= 1.25 THEN '1.00-1.25x'
                    WHEN min_multiplier_to_succeed <= 1.50 THEN '1.25-1.50x'
                    WHEN min_multiplier_to_succeed <= 2.00 THEN '1.50-2.00x'
                    WHEN min_multiplier_to_succeed <= 4.00 THEN '2.00-4.00x'
                    WHEN min_multiplier_to_succeed <= 8.00 THEN '4.00-8.00x'
                    ELSE '>8.00x'
                END AS bucket,
                count(*) AS txs,
                sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
                max(min_multiplier_to_succeed) AS max_multiplier
            FROM eip8037_tx_impact
            GROUP BY 1, 2
        )
        SELECT * FROM bucketed ORDER BY sort_key
    """)
    return [
        {
            "bucket": r["bucket"],
            "txs": _int(r["txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
            "max_multiplier": _float_or_none(r["max_multiplier"]),
        }
        for r in rows
    ]


@router.get("/eip8037/reservoir")
def eip8037_reservoir():
    """Reservoir-utilization view: how full does the per-tx state-gas
    reservoir get in practice, and what happens to the overflow tail."""
    headline = query("""
        SELECT
            count(*) AS total_txs,
            sum(CASE WHEN runtime_state_gas > 0 THEN 1 ELSE 0 END) AS state_touching_txs,
            sum(CASE WHEN runtime_state_gas_spillover > 0 THEN 1 ELSE 0 END) AS overflow_txs,
            percentile_cont(0.95) WITHIN GROUP (
                ORDER BY runtime_state_gas::DOUBLE / NULLIF(schedule_initial_reservoir, 0)
            ) FILTER (WHERE schedule_initial_reservoir > 0 AND runtime_state_gas > 0)
                AS p95_utilization_state_touching,
            percentile_cont(0.50) WITHIN GROUP (
                ORDER BY runtime_state_gas::DOUBLE / NULLIF(schedule_initial_reservoir, 0)
            ) FILTER (WHERE schedule_initial_reservoir > 0 AND runtime_state_gas > 0)
                AS p50_utilization_state_touching
        FROM eip8037_tx_impact
    """)[0]

    util_rows = query("""
        WITH bucketed AS (
            SELECT
                CASE
                    WHEN runtime_state_gas <= 0 THEN 0
                    WHEN schedule_initial_reservoir <= 0
                         OR runtime_state_gas > schedule_initial_reservoir THEN 6
                    WHEN runtime_state_gas <= 0.10 * schedule_initial_reservoir THEN 1
                    WHEN runtime_state_gas <= 0.25 * schedule_initial_reservoir THEN 2
                    WHEN runtime_state_gas <= 0.50 * schedule_initial_reservoir THEN 3
                    WHEN runtime_state_gas <= 0.75 * schedule_initial_reservoir THEN 4
                    ELSE 5
                END AS sort_key,
                count(*) AS txs,
                sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs
            FROM eip8037_tx_impact
            GROUP BY 1
        )
        SELECT * FROM bucketed ORDER BY sort_key
    """)
    UTIL_LABELS = {
        0: "no state", 1: "0–10%", 2: "10–25%", 3: "25–50%",
        4: "50–75%", 5: "75–100%", 6: "overflow",
    }
    utilization = [
        {
            "bucket": UTIL_LABELS.get(int(r["sort_key"]), str(r["sort_key"])),
            "sort_key": int(r["sort_key"]),
            "txs": _int(r["txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
        }
        for r in util_rows
    ]

    # Spillover severity (overflow cohort only) — log2-bucketed CDF input.
    spillover_rows = query("""
        WITH bucketed AS (
            SELECT
                floor(log2(runtime_state_gas_spillover))::int AS log_bin,
                count(*) AS cnt
            FROM eip8037_tx_impact
            WHERE runtime_state_gas_spillover > 0
            GROUP BY 1
        )
        SELECT log_bin, cnt FROM bucketed ORDER BY log_bin
    """)
    spillover_total = sum(int(r["cnt"]) for r in spillover_rows) or 1
    spillover_histogram = [
        {
            "bin_start": 2 ** int(r["log_bin"]),
            "label": f'{2**int(r["log_bin"])}–{2**(int(r["log_bin"])+1)}',
            "count": _int(r["cnt"]),
            "density": round(int(r["cnt"]) / spillover_total, 6),
        }
        for r in spillover_rows
    ]

    category_rows = query("""
        SELECT
            coalesce(state_gas_category, 'uncategorized') AS category,
            count(*) AS total_txs,
            sum(CASE WHEN runtime_state_gas <= 0 THEN 1 ELSE 0 END) AS no_state_txs,
            sum(CASE WHEN runtime_state_gas > 0 AND runtime_state_gas_spillover = 0
                          THEN 1 ELSE 0 END) AS fits_txs,
            sum(CASE WHEN runtime_state_gas_spillover > 0 THEN 1 ELSE 0 END) AS overflow_txs
        FROM eip8037_tx_impact
        GROUP BY 1
        ORDER BY total_txs DESC
    """)
    category_split = [
        {
            "category": r["category"],
            "total_txs": _int(r["total_txs"]),
            "no_state_txs": _int(r["no_state_txs"]),
            "fits_txs": _int(r["fits_txs"]),
            "overflow_txs": _int(r["overflow_txs"]),
        }
        for r in category_rows
    ]

    return {
        "headline": {
            "total_txs": _int(headline["total_txs"]),
            "state_touching_txs": _int(headline["state_touching_txs"]),
            "overflow_txs": _int(headline["overflow_txs"]),
            "p50_utilization_state_touching": _float_or_none(headline["p50_utilization_state_touching"]),
            "p95_utilization_state_touching": _float_or_none(headline["p95_utilization_state_touching"]),
        },
        "utilization": utilization,
        "spillover_histogram": spillover_histogram,
        "category_split": category_split,
    }


@router.get("/eip8037/state-gas-by-category")
def eip8037_state_gas_by_category():
    rows = query("""
        SELECT
            state_gas_category,
            count(*) AS txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            avg(schedule_state_gas_spent) AS avg_state_gas_spent,
            sum(runtime_state_gas) AS total_runtime_state_gas,
            sum(runtime_state_gas_spillover) AS total_runtime_state_gas_spillover,
            sum(CASE WHEN reservoir_exhausted THEN 1 ELSE 0 END) AS reservoir_exhausted_txs,
            avg(min_multiplier_to_succeed) AS avg_min_multiplier_to_succeed
        FROM eip8037_tx_impact
        GROUP BY 1
        ORDER BY total_state_gas_spent DESC, txs DESC
    """)
    return [
        {
            "category": r["state_gas_category"],
            "txs": _int(r["txs"]),
            "total_state_gas_spent": _int(r["total_state_gas_spent"]),
            "avg_state_gas_spent": _float(r["avg_state_gas_spent"]),
            "total_runtime_state_gas": _int(r["total_runtime_state_gas"]),
            "total_runtime_state_gas_spillover": _int(r["total_runtime_state_gas_spillover"]),
            "reservoir_exhausted_txs": _int(r["reservoir_exhausted_txs"]),
            "avg_min_multiplier_to_succeed": _float_or_none(r["avg_min_multiplier_to_succeed"]),
        }
        for r in rows
    ]


@router.get("/eip8037/top-contracts")
def eip8037_top_contracts(limit: int = Query(default=20, le=500)):
    rows = query(f"""
        SELECT *
        FROM eip8037_contract_impact
        WHERE original_limit_failures > 0
           OR status_changed_txs > 0
           OR total_state_gas_spent > 0
        ORDER BY original_limit_failures DESC,
                 total_state_gas_spent DESC,
                 status_changed_txs DESC,
                 divergent_txs DESC
        LIMIT {int(limit)}
    """)
    return [
        {
            "target_address": r["target_address"],
            "name": label_address(r["target_address"]),
            "divergent_txs": _int(r["divergent_txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
            "original_limit_failures": _int(r["original_limit_failures"]),
            "fixable_with_more_outer_gas": _int(r["fixable_with_more_outer_gas"]),
            "unresolved_replay_failures": _int(r["unresolved_replay_failures"]),
            "total_state_gas_spent": _int(r["total_state_gas_spent"]),
            "total_runtime_state_gas_spillover": _int(r["total_runtime_state_gas_spillover"]),
            "reservoir_exhausted_txs": _int(r["reservoir_exhausted_txs"]),
            "p95_min_multiplier_to_succeed": _float_or_none(r["p95_min_multiplier_to_succeed"]),
            "max_min_multiplier_to_succeed": _float_or_none(r["max_min_multiplier_to_succeed"]),
            "max_extra_gas_needed": _int(r["max_extra_gas_needed"]),
            "min_block": _int(r["min_block"]),
            "max_block": _int(r["max_block"]),
        }
        for r in rows
    ]


@router.get("/eip8037/examples")
def eip8037_examples(limit: int = Query(default=50, le=500)):
    rows = query(f"""
        SELECT
            tx_hash, block_number, tx_index, target_address, tx_gas_limit,
            baseline_success, schedule_success, status_changed,
            baseline_gas_used, schedule_gas_used, gas_delta,
            would_fit_in_original_limit, min_multiplier_to_succeed,
            extra_gas_needed, estimated_min_gas_limit, state_gas_category,
            schedule_state_gas_spent, runtime_state_gas,
            schedule_initial_reservoir, runtime_state_gas_spillover,
            reservoir_exhausted
        FROM eip8037_tx_impact
        WHERE original_limit_failure
           OR status_changed
           OR reservoir_exhausted
        ORDER BY
            CASE
                WHEN baseline_success AND NOT schedule_success THEN 0
                WHEN original_limit_failure THEN 1
                WHEN reservoir_exhausted THEN 2
                ELSE 3
            END,
            coalesce(min_multiplier_to_succeed, 999999) DESC,
            schedule_state_gas_spent DESC
        LIMIT {int(limit)}
    """)
    return [
        {
            "tx_hash": _hex(r["tx_hash"]),
            "block_number": _int(r["block_number"]),
            "tx_index": _int(r["tx_index"]),
            "target_address": r["target_address"],
            "target_name": label_address(r["target_address"]),
            "tx_gas_limit": _int(r["tx_gas_limit"]),
            "baseline_success": r["baseline_success"],
            "schedule_success": r["schedule_success"],
            "status_changed": r["status_changed"],
            "baseline_gas_used": _int(r["baseline_gas_used"]),
            "schedule_gas_used": _int(r["schedule_gas_used"]),
            "gas_delta": _int(r["gas_delta"]),
            "would_fit_in_original_limit": r["would_fit_in_original_limit"],
            "min_multiplier_to_succeed": _float_or_none(r["min_multiplier_to_succeed"]),
            "extra_gas_needed": _int(r["extra_gas_needed"]),
            "estimated_min_gas_limit": _int(r["estimated_min_gas_limit"]),
            "state_gas_category": r["state_gas_category"],
            "schedule_state_gas_spent": _int(r["schedule_state_gas_spent"]),
            "runtime_state_gas": _int(r["runtime_state_gas"]),
            "schedule_initial_reservoir": _int(r["schedule_initial_reservoir"]),
            "runtime_state_gas_spillover": _int(r["runtime_state_gas_spillover"]),
            "reservoir_exhausted": r["reservoir_exhausted"],
        }
        for r in rows
    ]


# ── Affected parties endpoints ────────────────────────────────────────


_AFFECTED_BASE_CTE = f"""
    WITH e7904 AS (
        SELECT lower(recipient) AS addr,
               count(*) AS broken_txs_7904,
               avg(gas_delta) AS avg_delta_7904,
               sum(gas_delta) AS total_delta_7904,
               min(block_number) AS min_block_7904,
               max(block_number) AS max_block_7904
        FROM divergences
        WHERE bucket = 'contract_broken'
        GROUP BY lower(recipient)
    ),
    e8037 AS (
        SELECT lower(target_address) AS addr,
               original_limit_failures AS need_higher_limit_8037,
               reservoir_exhausted_txs AS reservoir_exhausted_8037,
               status_changed_txs AS status_changed_8037,
               p95_min_multiplier_to_succeed AS p95_multiplier_8037,
               min_block AS min_block_8037,
               max_block AS max_block_8037
        FROM eip8037_contract_impact
        WHERE original_limit_failures > 0
           OR status_changed_txs > 0
           OR reservoir_exhausted_txs > 0
    ),
    affected_combined AS (
        SELECT
            coalesce(e7.addr, e8.addr) AS addr,
            coalesce(e7.broken_txs_7904, 0) AS broken_txs_7904,
            coalesce(e7.avg_delta_7904, 0) AS avg_delta_7904,
            coalesce(e7.total_delta_7904, 0) AS total_delta_7904,
            coalesce(e8.need_higher_limit_8037, 0) AS need_higher_limit_8037,
            coalesce(e8.reservoir_exhausted_8037, 0) AS reservoir_exhausted_8037,
            coalesce(e8.status_changed_8037, 0) AS status_changed_8037,
            e8.p95_multiplier_8037,
            least(coalesce(e7.min_block_7904, 99999999999),
                  coalesce(e8.min_block_8037, 99999999999)) AS min_block,
            greatest(coalesce(e7.max_block_7904, 0),
                     coalesce(e8.max_block_8037, 0)) AS max_block
        FROM e7904 e7
        FULL OUTER JOIN e8037 e8 USING (addr)
    )
"""


@router.get("/affected")
def affected(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
):
    """Paginated affected contracts across EIP-7904 and EIP-8037."""
    offset = (page - 1) * per_page
    total_count = query_scalar(
        _AFFECTED_BASE_CTE + " SELECT count(*) FROM affected_combined",
        default=0,
    )
    rows = query(_AFFECTED_BASE_CTE + f"""
        SELECT * FROM affected_combined
        ORDER BY broken_txs_7904 DESC, need_higher_limit_8037 DESC
        LIMIT {int(per_page)} OFFSET {int(offset)}
    """)

    # `owner` / `remediation` chips used to come from an outreach CSV
    # of manual classifications. The CSV-generation script is gone; the
    # producer-side contract_metadata table will provide a coarser
    # equivalent (solc/EVM target) when it lands.
    items = []
    for r in rows:
        addr = r["addr"]
        items.append({
            "recipient": addr,
            "name": label_address(addr),
            "broken_txs_7904": _int(r["broken_txs_7904"]),
            "avg_delta_7904": _float(r["avg_delta_7904"]),
            "total_delta_7904": _float(r["total_delta_7904"]),
            "need_higher_limit_8037": _int(r["need_higher_limit_8037"]),
            "reservoir_exhausted_8037": _int(r["reservoir_exhausted_8037"]),
            "status_changed_8037": _int(r["status_changed_8037"]),
            "p95_multiplier_8037": _float_or_none(r["p95_multiplier_8037"]),
            "min_block": _int(r["min_block"]),
            "max_block": _int(r["max_block"]),
            "owner": "",
            "remediation": "",
        })
    return {
        "items": items,
        "total": int(total_count),
        "page": page,
        "per_page": per_page,
        "total_pages": (int(total_count) + per_page - 1) // per_page,
    }


def _with_shares(rows: list[dict], count_key: str = "count") -> list[dict]:
    total = sum(r[count_key] for r in rows) or 1
    return [{**r, "share": round(r[count_key] / total * 100, 1)} for r in rows]


@router.get("/affected/{address}")
def affected_detail(address: str):
    """Single contract detail with EIP-7904 and EIP-8037 diagnostics."""
    addr = address.lower()

    # ── EIP-7904 stats ──
    # Wallet-fixable txs aren't stored per-recipient anymore (the
    # producer aggregates them into block_summaries). The contract
    # detail page only needs contract-broken data.
    eip7904_stats = query(f"""
        SELECT count(*) as broken_txs,
               avg(gas_delta) as avg_delta,
               sum(gas_delta) as total_delta,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95_delta,
               min(block_number) as min_block,
               max(block_number) as max_block
        FROM divergences
        WHERE bucket = 'contract_broken' AND lower(recipient) = '{addr}'
    """)[0]
    eip7904_wallet_n = 0  # No longer tracked per-recipient.
    opcodes_raw = query(f"""
        SELECT divergence_opcode AS op_num, count(*) AS cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND divergence_opcode IS NOT NULL
          AND lower(recipient) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    depths_raw = query(f"""
        SELECT coalesce(divergence_call_depth, -1) as depth, count(*) as cnt
        FROM divergences
        WHERE bucket = 'contract_broken' AND lower(recipient) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    eip7904_txs = query(f"""
        SELECT tx_hash, block_number, gas_delta
        FROM divergences
        WHERE bucket = 'contract_broken' AND lower(recipient) = '{addr}'
        ORDER BY gas_delta DESC LIMIT 20
    """)
    bottleneck_kinds_raw = query(f"""
        SELECT coalesce(oog_bottleneck_kind, 'Unclassified') as kind, count(*) as cnt
        FROM divergences
        WHERE bucket = 'contract_broken' AND lower(recipient) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC
    """)

    # ── EIP-8037 stats ──
    eip8037_rows = query(f"""
        SELECT * FROM eip8037_contract_impact
        WHERE lower(target_address) = '{addr}'
    """)
    eip8037_row = eip8037_rows[0] if eip8037_rows else None
    categories_raw = query(f"""
        SELECT coalesce(state_gas_category, 'uncategorized') as cat, count(*) as cnt
        FROM eip8037_tx_impact
        WHERE lower(target_address) = '{addr}'
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """)
    eip8037_txs = query(f"""
        SELECT tx_hash, block_number, tx_gas_limit, min_multiplier_to_succeed,
               reservoir_exhausted, state_gas_category,
               runtime_state_gas_spillover, schedule_state_gas_spent
        FROM eip8037_tx_impact
        WHERE lower(target_address) = '{addr}'
          AND (original_limit_failure OR status_changed OR reservoir_exhausted)
        ORDER BY
            CASE WHEN baseline_success AND NOT schedule_success THEN 0
                 WHEN original_limit_failure THEN 1
                 WHEN reservoir_exhausted THEN 2 ELSE 3 END,
            coalesce(min_multiplier_to_succeed, 999999) DESC
        LIMIT 20
    """)

    eip7904_broken = _int(eip7904_stats["broken_txs"])
    eip8037_div = _int(eip8037_row["divergent_txs"]) if eip8037_row else 0
    # Wallet-fixable-only contracts aren't 7904-affected in any actionable
    # sense: the wallet auto-resolves it via eth_estimateGas, no code change
    # needed. Don't show them as affected.
    found = eip7904_broken > 0 or eip8037_div > 0

    name = label_address(addr)
    # owner / remediation classification used to come from a hand-curated
    # CSV; without it we return empty strings. See the comment in
    # /api/affected for context.
    return {
        "found": found,
        "address": addr,
        "name": name,
        "owner": "",
        "remediation": "",
        "eip7904": {
            "broken_txs": eip7904_broken,
            "wallet_fixable_txs": eip7904_wallet_n,
            "avg_delta": _float(eip7904_stats["avg_delta"]),
            "total_delta": _float(eip7904_stats["total_delta"]),
            "p95_delta": _float(eip7904_stats["p95_delta"]),
            "min_block": _int(eip7904_stats["min_block"]),
            "max_block": _int(eip7904_stats["max_block"]),
            "opcodes": _with_shares([
                {
                    "name": opcode_label(r["op_num"]),
                    "count": _int(r["cnt"]),
                }
                for r in opcodes_raw
            ]),
            "depths": _with_shares([
                {"depth": _int(r["depth"]), "count": _int(r["cnt"])}
                for r in depths_raw
            ]),
            "bottleneck_kinds": _with_shares([
                {"kind": r["kind"], "count": _int(r["cnt"])}
                for r in bottleneck_kinds_raw
            ]),
            "transactions": [
                {
                    "tx_hash": _hex(t["tx_hash"]),
                    "block_number": _int(t["block_number"]),
                    "gas_delta": _float(t["gas_delta"]),
                }
                for t in eip7904_txs
            ],
        },
        "eip8037": {
            "divergent_txs": _int(eip8037_row["divergent_txs"]) if eip8037_row else 0,
            "status_changed_txs": _int(eip8037_row["status_changed_txs"]) if eip8037_row else 0,
            "original_limit_failures": _int(eip8037_row["original_limit_failures"]) if eip8037_row else 0,
            "fixable_with_more_outer_gas": _int(eip8037_row["fixable_with_more_outer_gas"]) if eip8037_row else 0,
            "reservoir_exhausted_txs": _int(eip8037_row["reservoir_exhausted_txs"]) if eip8037_row else 0,
            "total_state_gas_spent": _int(eip8037_row["total_state_gas_spent"]) if eip8037_row else 0,
            "p95_min_multiplier_to_succeed": _float_or_none(eip8037_row["p95_min_multiplier_to_succeed"]) if eip8037_row else None,
            "max_min_multiplier_to_succeed": _float_or_none(eip8037_row["max_min_multiplier_to_succeed"]) if eip8037_row else None,
            "max_extra_gas_needed": _int(eip8037_row["max_extra_gas_needed"]) if eip8037_row else 0,
            "categories": _with_shares([
                {"category": r["cat"], "count": _int(r["cnt"])}
                for r in categories_raw
            ]),
            "transactions": [
                {
                    "tx_hash": _hex(t["tx_hash"]),
                    "block_number": _int(t["block_number"]),
                    "tx_gas_limit": _int(t["tx_gas_limit"]),
                    "min_multiplier_to_succeed": _float_or_none(t["min_multiplier_to_succeed"]),
                    "reservoir_exhausted": bool(t["reservoir_exhausted"]),
                    "state_gas_category": t["state_gas_category"],
                    "runtime_state_gas_spillover": _int(t["runtime_state_gas_spillover"]),
                    "schedule_state_gas_spent": _int(t["schedule_state_gas_spent"]),
                }
                for t in eip8037_txs
            ],
        },
    }


@router.get("/tx/{tx_hash}")
def tx_detail(tx_hash: str):
    """Detailed view of a single broken transaction: gas info, divergence location, call stack."""
    tx_hash = tx_hash.lower().strip()

    # Core tx info + 8037 derived fields, all from `divergences` now.
    hot = query(f"""
        SELECT
            d.divergence_id, d.block_number, d.tx_index, d.tx_hash, d.bucket,
            d.baseline_success, d.schedule_success, d.status_changed,
            d.event_logs_changed, d.baseline_gas_used, d.schedule_gas_used,
            d.gas_delta, d.tx_gas_limit, d.sender, d.recipient,
            d.divergence_contract, d.divergence_call_depth, d.divergence_opcode,
            d.oog_contract, d.oog_call_depth, d.oog_opcode, d.oog_pattern,
            d.oog_gas_remaining, d.oog_chain_proportional,
            d.oog_bottleneck_depth, d.oog_bottleneck_kind,
            e.would_fit_in_original_limit, e.min_multiplier_to_succeed,
            e.extra_gas_needed, e.estimated_min_gas_limit,
            d.schedule_total_gas_spent, d.schedule_state_gas_spent,
            d.schedule_initial_state_gas, d.runtime_state_gas,
            d.schedule_initial_reservoir, d.runtime_state_gas_spillover,
            d.schedule_floor_gas, d.schedule_gas_refunded,
            d.baseline_total_gas_spent, d.baseline_gas_refunded,
            d.state_gas_category, d.reservoir_exhausted
        FROM divergences d
        LEFT JOIN eip8037_tx_impact e USING (divergence_id)
        WHERE lower(hex(d.tx_hash)) = '{tx_hash.removeprefix("0x")}'
        LIMIT 1
    """)
    if not hot:
        return {"found": False}
    h = hot[0]
    div_id = h["divergence_id"]

    # Per-frame call stack from the normalized frames table (replaces the
    # JSON blob in the old artifacts_7904).
    frames = query(f"""
        SELECT call_index, depth, from_address, to_address, call_type,
               selector, gas_provided, gas_used, success
        FROM call_frames
        WHERE divergence_id = {div_id}
        ORDER BY call_index
    """)
    call_stack = []
    for fr in frames:
        sel_bytes = fr.get("selector") or b""
        selector = "0x" + sel_bytes.hex() if sel_bytes else ""
        to_addr = fr.get("to_address") or ""
        call_stack.append({
            "depth": _int(fr.get("depth")),
            "call_type": fr.get("call_type", ""),
            "from": label_address(fr.get("from_address", "")),
            "from_address": fr.get("from_address", ""),
            "to": label_address(to_addr),
            "to_address": to_addr,
            "selector": selector,
            "gas_provided": _int(fr.get("gas_provided")),
            "gas_used": _int(fr.get("gas_used")),
            "success": bool(fr.get("success")),
        })

    # Per-opcode gas breakdown for the 8 repriced opcodes, summed across
    # all frames in this tx. The new opcode_counts table gives us exact
    # per-frame data — the dashboard table aggregates to tx-level.
    REPRICED_OP_BYTES = {
        0x04: "DIV", 0x05: "SDIV", 0x06: "MOD", 0x07: "SMOD",
        0x08: "ADDMOD", 0x09: "MULMOD", 0x0a: "EXP", 0x20: "KECCAK256",
    }
    placeholders = ", ".join(str(op) for op in REPRICED_OP_BYTES)
    op_rows = query(f"""
        SELECT opcode, sum(count) AS count,
               sum(gas_schedule) - sum(gas_baseline) AS gas_delta
        FROM opcode_counts
        WHERE divergence_id = {div_id} AND opcode IN ({placeholders})
        GROUP BY opcode
    """)
    gas_breakdown = []
    for r in op_rows:
        op = int(r["opcode"])
        cnt = _int(r["count"])
        delta = _int(r["gas_delta"])
        if cnt > 0 or delta != 0:
            gas_breakdown.append({
                "opcode": REPRICED_OP_BYTES[op],
                "count": cnt,
                "gas_delta": delta,
            })

    # Cross-frame opcode totals for SLOAD/SSTORE/CALL/LOG/total — used by
    # the op_counts panel on the tx page.
    counts_by_op = {r["opcode"]: _int(r["count"]) for r in query(f"""
        SELECT opcode, sum(count) AS count
        FROM opcode_counts WHERE divergence_id = {div_id}
        GROUP BY opcode
    """)}
    total_ops = sum(counts_by_op.values())
    log_count = sum(counts_by_op.get(op, 0) for op in (0xa0, 0xa1, 0xa2, 0xa3, 0xa4))
    call_count = sum(counts_by_op.get(op, 0)
                     for op in (0xf1, 0xf2, 0xf4, 0xfa, 0xf0, 0xf5))
    op_counts = {
        "sload": counts_by_op.get(0x54, 0),
        "sstore": counts_by_op.get(0x55, 0),
        "call": call_count,
        "log": log_count,
        "total": total_ops,
    }

    tx_hash_hex = (
        "0x" + h["tx_hash"].hex()
        if isinstance(h["tx_hash"], (bytes, bytearray))
        else h["tx_hash"]
    )
    return {
        "found": True,
        "tx_hash": tx_hash_hex,
        "block_number": int(h["block_number"]),
        "tx_index": int(h["tx_index"]),
        "sender": h["sender"],
        "recipient": h["recipient"],
        "recipient_name": label_address(h["recipient"]),
        "baseline_success": h["baseline_success"],
        "schedule_success": h["schedule_success"],
        "baseline_gas_used": int(h["baseline_gas_used"]),
        "schedule_gas_used": int(h["schedule_gas_used"]),
        "gas_delta": int(h["gas_delta"]),
        "gas_limit": int(h["tx_gas_limit"]),
        "divergence": {
            "contract": label_address(h.get("divergence_contract") or ""),
            "contract_address": h.get("divergence_contract") or "",
            "call_depth": h.get("divergence_call_depth"),
            "opcode": (opcode_label(h["divergence_opcode"])
                       if h.get("divergence_opcode") is not None else ""),
        } if h.get("divergence_contract") else None,
        "oog": {
            "contract": label_address(h.get("oog_contract") or ""),
            "contract_address": h.get("oog_contract") or "",
            "call_depth": h.get("oog_call_depth"),
            "opcode": (opcode_label(h["oog_opcode"])
                       if h.get("oog_opcode") is not None else ""),
            "pattern": h.get("oog_pattern") or "",
            "gas_remaining": h.get("oog_gas_remaining"),
            "chain_proportional": h.get("oog_chain_proportional"),
            "bottleneck_depth": h.get("oog_bottleneck_depth"),
            "bottleneck_kind": h.get("oog_bottleneck_kind"),
        } if h.get("oog_contract") else None,
        "op_counts": op_counts,
        "eip8037": {
            "would_fit_in_original_limit": h.get("would_fit_in_original_limit"),
            "min_multiplier_to_succeed": _float_or_none(h.get("min_multiplier_to_succeed")),
            "extra_gas_needed": _int(h.get("extra_gas_needed")),
            "estimated_min_gas_limit": _int(h.get("estimated_min_gas_limit")),
            "schedule_total_gas_spent": _int(h.get("schedule_total_gas_spent")),
            "schedule_state_gas_spent": _int(h.get("schedule_state_gas_spent")),
            "schedule_initial_state_gas": _int(h.get("schedule_initial_state_gas")),
            "runtime_state_gas": _int(h.get("runtime_state_gas")),
            "schedule_initial_reservoir": _int(h.get("schedule_initial_reservoir")),
            "runtime_state_gas_spillover": _int(h.get("runtime_state_gas_spillover")),
            "schedule_floor_gas": _int(h.get("schedule_floor_gas")),
            "schedule_gas_refunded": _int(h.get("schedule_gas_refunded")),
            "baseline_total_gas_spent": _int(h.get("baseline_total_gas_spent")),
            "baseline_gas_refunded": _int(h.get("baseline_gas_refunded")),
            "state_gas_category": h.get("state_gas_category"),
            "reservoir_exhausted": h.get("reservoir_exhausted"),
        },
        "call_stack": call_stack,
        "gas_breakdown": gas_breakdown,
    }


@router.get("/search")
def search(q: str = Query(default="")):
    """Search affected contracts (EIP-7904 ∪ EIP-8037) by address prefix or name."""
    if not q or len(q) < 2:
        return []
    term = q.lower()
    rows = query(_AFFECTED_BASE_CTE + """
        SELECT addr, broken_txs_7904,
               need_higher_limit_8037, reservoir_exhausted_8037, status_changed_8037
        FROM affected_combined
        ORDER BY broken_txs_7904 DESC, need_higher_limit_8037 DESC
    """)
    results = []
    for r in rows:
        addr = (r["addr"] or "").lower()
        name_lc = label_address(addr).lower()
        if term in addr or term in name_lc:
            results.append({
                "recipient": addr,
                "name": label_address(addr),
                "broken_txs_7904": int(r["broken_txs_7904"]),
                "impact_8037": max(
                    int(r["need_higher_limit_8037"]),
                    int(r["reservoir_exhausted_8037"]),
                    int(r["status_changed_8037"]),
                ),
            })
            if len(results) >= 20:
                break
    return results


@router.get("/_debug/data-audit")
def debug_data_audit():
    """One-shot audit of upstream replay-data quality.

    Profiles every numeric / string / boolean column on the derived
    tables that feed the web UI in a single batched SELECT per table,
    and auto-flags fields that look broken (uniformly NULL, all zero,
    all empty, single distinct value, always-true / always-false)."""

    SCOPED_TABLES = [
        "divergences",
        "call_frames",
        "opcode_counts",
        "eip8037_tx_impact",
        "eip8037_contract_impact",
        "block_coverage",
        "block_summaries",
    ]
    SKIP_TYPES = ("JSON", "STRUCT", "MAP", "LIST", "[]", "UNION")

    def is_numeric(t: str) -> bool:
        u = t.upper()
        return any(k in u for k in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "REAL"))

    def is_string(t: str) -> bool:
        u = t.upper()
        return "VARCHAR" in u or "TEXT" in u or "CHAR" in u

    def is_bool(t: str) -> bool:
        return "BOOL" in t.upper()

    def profile_table(table):
        """One scan, all column profiles."""
        try:
            cols = query(f"DESCRIBE {table}")
        except Exception as e:
            return [{
                "table": table, "field": "<table>", "kind": "table",
                "verdict": f"ERROR: {str(e)[:120]}",
            }]
        col_meta = []  # (name, kind)
        aggs = ["count(*) AS total"]
        for col in cols:
            name = col["column_name"]
            ctype = col["column_type"]
            if any(s in ctype.upper() for s in SKIP_TYPES):
                continue
            qf = f'"{name}"'
            if is_numeric(ctype):
                aggs += [
                    f'count({qf}) AS "{name}__non_null"',
                    f'sum(CASE WHEN {qf} = 0 THEN 1 ELSE 0 END) AS "{name}__zero"',
                    f'sum(CASE WHEN {qf} > 0 THEN 1 ELSE 0 END) AS "{name}__positive"',
                    f'sum(CASE WHEN {qf} < 0 THEN 1 ELSE 0 END) AS "{name}__negative"',
                    f'min({qf}) AS "{name}__min"',
                    f'max({qf}) AS "{name}__max"',
                    f'avg({qf}) AS "{name}__avg"',
                ]
                col_meta.append((name, "numeric", ctype))
            elif is_string(ctype):
                aggs += [
                    f'count({qf}) AS "{name}__non_null"',
                    f"sum(CASE WHEN {qf} = '' THEN 1 ELSE 0 END) AS \"{name}__empty\"",
                    f'min({qf}) AS "{name}__min"',
                    f'max({qf}) AS "{name}__max"',
                ]
                col_meta.append((name, "string", ctype))
            elif is_bool(ctype):
                aggs += [
                    f'sum(CASE WHEN {qf} THEN 1 ELSE 0 END) AS "{name}__true"',
                    f'sum(CASE WHEN NOT {qf} THEN 1 ELSE 0 END) AS "{name}__false"',
                    f'count(*) FILTER (WHERE {qf} IS NULL) AS "{name}__null"',
                ]
                col_meta.append((name, "bool", ctype))
        if not col_meta:
            return []
        try:
            row = query(f"SELECT {', '.join(aggs)} FROM {table}")[0]
        except Exception as e:
            return [{
                "table": table, "field": "<batch>", "kind": "batch",
                "verdict": f"ERROR: {str(e)[:200]}",
            }]
        total = _int(row["total"])
        results = []
        for name, kind, ctype in col_meta:
            if kind == "numeric":
                non_null = _int(row[f"{name}__non_null"])
                zero = _int(row[f"{name}__zero"])
                pos = _int(row[f"{name}__positive"])
                neg = _int(row[f"{name}__negative"])
                min_v = _float_or_none(row[f"{name}__min"])
                max_v = _float_or_none(row[f"{name}__max"])
                avg_v = _float_or_none(row[f"{name}__avg"])
                if non_null == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif min_v == max_v:
                    verdict = f"CONSTANT ({min_v})"
                elif zero == non_null:
                    verdict = "ALL ZERO"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "numeric",
                    "total": total, "non_null": non_null,
                    "zero": zero, "positive": pos, "negative": neg,
                    "min": min_v, "max": max_v, "avg": avg_v,
                    "verdict": verdict,
                })
            elif kind == "string":
                non_null = _int(row[f"{name}__non_null"])
                empty = _int(row[f"{name}__empty"])
                min_v = row[f"{name}__min"]
                max_v = row[f"{name}__max"]
                if non_null == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif empty == non_null:
                    verdict = "ALL EMPTY"
                elif min_v == max_v:
                    verdict = f"CONSTANT ({str(min_v)[:50]})"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "string",
                    "total": total, "non_null": non_null, "empty": empty,
                    "min": str(min_v)[:80] if min_v is not None else None,
                    "max": str(max_v)[:80] if max_v is not None else None,
                    "verdict": verdict,
                })
            elif kind == "bool":
                t_n = _int(row[f"{name}__true"])
                f_n = _int(row[f"{name}__false"])
                null_n = _int(row[f"{name}__null"])
                if t_n == 0 and f_n == 0:
                    verdict = "UNPOPULATED (all NULL)"
                elif t_n == 0:
                    verdict = "ALWAYS FALSE"
                elif f_n == 0:
                    verdict = "ALWAYS TRUE"
                else:
                    verdict = "OK"
                results.append({
                    "table": table, "field": name, "type": ctype, "kind": "bool",
                    "total": total, "true": t_n, "false": f_n, "null": null_n,
                    "verdict": verdict,
                })
        return results

    checks = []
    for table in SCOPED_TABLES:
        checks.extend(profile_table(table))

    # Cross-field invariants
    def safe_count(sql):
        try:
            row = query(sql)[0]
            return _int(row[list(row.keys())[0]])
        except Exception as e:
            return f"ERROR: {str(e)[:100]}"

    invariants = [
        {
            "table": "divergences",
            "name": "gas_delta == schedule_gas_used - baseline_gas_used",
            "violations": safe_count("""
                SELECT count(*) FROM divergences
                WHERE gas_delta IS NOT NULL
                  AND schedule_gas_used IS NOT NULL
                  AND baseline_gas_used IS NOT NULL
                  AND gas_delta <> schedule_gas_used - baseline_gas_used
            """),
        },
        {
            "table": "divergences",
            "name": "status_changed iff baseline_success <> schedule_success",
            "violations": safe_count("""
                SELECT count(*) FROM divergences
                WHERE status_changed <> (baseline_success <> schedule_success)
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "runtime_state_gas_spillover <= runtime_state_gas",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE runtime_state_gas_spillover > runtime_state_gas
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "spillover == GREATEST(0, runtime_state_gas - schedule_initial_reservoir)",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE runtime_state_gas_spillover <>
                      GREATEST(0, runtime_state_gas - schedule_initial_reservoir)
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "would_fit_in_original_limit implies schedule_success",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE would_fit_in_original_limit AND NOT schedule_success
            """),
        },
        {
            "table": "eip8037_tx_impact",
            "name": "original_limit_failure implies NOT would_fit_in_original_limit",
            "violations": safe_count("""
                SELECT count(*) FROM eip8037_tx_impact
                WHERE original_limit_failure AND would_fit_in_original_limit
            """),
        },
    ]

    return {"checks": checks, "invariants": invariants}


@router.get("/_debug/divergence-sample")
def debug_divergence_sample():
    """Diagnostic: confirm the producer is emitting divergence/OOG opcode
    integers and bottleneck classifications on the drill-in cohort."""
    counts = query("""
        SELECT
            count(*) AS rows_total,
            count(*) FILTER (WHERE divergence_opcode IS NOT NULL) AS with_opcode_int,
            count(*) FILTER (WHERE oog_bottleneck_kind IS NOT NULL) AS with_bottleneck_kind,
            count(*) FILTER (WHERE divergence_call_depth IS NOT NULL) AS with_call_depth,
            count(*) FILTER (WHERE oog_chain_proportional IS NOT NULL) AS with_chain_classified
        FROM divergences
    """)[0]
    top_opcodes = query("""
        SELECT divergence_opcode AS op_num, count(*) AS cnt
        FROM divergences
        WHERE divergence_opcode IS NOT NULL
        GROUP BY 1 ORDER BY cnt DESC LIMIT 10
    """)
    return {
        "counts": {k: _int(v) for k, v in counts.items()},
        "top_opcodes_int": [
            {
                "opcode_int": _int(r["op_num"]),
                "opcode_hex": f"0x{int(r['op_num']):02x}",
                "name": opcode_label(r["op_num"]),
                "count": _int(r["cnt"]),
            }
            for r in top_opcodes
        ],
    }


@router.get("/metadata")
def metadata():
    block_range = query("SELECT min(block_number) as mn, max(block_number) as mx FROM block_coverage")
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}
    affected_count = query_scalar(
        _AFFECTED_BASE_CTE + " SELECT count(*) FROM affected_combined",
        default=0,
    )
    return {
        "schedule_name": SCHEDULE_NAME,
        "min_block": int(br["mn"]) if br["mn"] else 0,
        "max_block": int(br["mx"]) if br["mx"] else 0,
        "last_updated": db_mtime().isoformat(),
        "total_contracts_affected": _int(affected_count),
    }
