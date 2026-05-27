"""JSON API endpoints for the gas repricing analysis web server."""
from __future__ import annotations

import math

from fastapi import APIRouter, Query

from repricing_forensics.labels import infer_project_label

from .db import (
    cache_endpoint,
    db_mtime,
    label_address,
    list_schedules,
    query,
    query_df,
    query_scalar,
    query_sqlite,
    query_sqlite_scalar,
    resolve_schedule,
)

# Cache TTL for /api/* endpoints whose SQL aggregates over the full
# producer DB. Under heavy reth replay write load, these queries can
# take 30-60s — cache them so the dashboard renders fast and refreshes
# on a cadence. 30s is short enough that the data feels live; long
# enough that subsequent page loads / multi-endpoint dashboards reuse
# results within a single render.
_AGGREGATE_TTL = 30.0

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


def _percentile(values: list, p: float) -> float | None:
    """Linear-interpolated percentile, matching DuckDB's `percentile_cont`.

    Used where we've moved a point-lookup endpoint off DuckDB (which has
    `percentile_cont`) onto raw SQLite (which doesn't). The input row
    count is bounded — these run on a single contract's cohort — so
    sorting in Python is cheap.
    """
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return float(vals[0])
    k = (len(vals) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return float(vals[int(k)])
    return float(vals[lo]) * (hi - k) + float(vals[hi]) * (k - lo)


# ── Schedules ────────────────────────────────────────────────────────


@router.get("/schedules")
@cache_endpoint(_AGGREGATE_TTL)
def schedules():
    """List every schedule the producer has ever recorded, most recent first.

    Pages use this to discover which producer-side schedule names are
    live (useful for ad-hoc curl and the schedule selector); routine
    page traffic passes the schedule name as a query param.
    """
    return {"schedules": list_schedules()}


# ── Briefing endpoints ────────────────────────────────────────────────


@router.get("/overview")
@cache_endpoint(_AGGREGATE_TTL)
def overview(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    # All headline counts come from block_coverage's per-bucket totals.
    # The producer's classifier is the single source of truth for which
    # bucket a tx belongs to; the consumer doesn't second-guess it.
    #
    # `schedule_rescued` counts the *beneficial* outcome flip (baseline
    # failed, schedule succeeded). It's deliberately excluded from the
    # `broken_txs` and `breakage_rate` totals — the schedule made these
    # txs work, they're not a failure mode of the EIP.
    #
    # `inconclusive_needs_higher_sweep` is failures where the producer's
    # tier-sweep hit its multiplier ceiling and we couldn't prove the
    # bottleneck either way. Surfaced separately so reviewers can plan a
    # re-run with a higher `--research.gas-limit-multipliers` ceiling
    # before treating them as contract-broken.
    row = query_sqlite("""
        SELECT
            sum(tx_count) AS total_analyzed,
            sum(tx_count - tx_count_unchanged) AS divergent_txs,
            sum(tx_count_contract_broken) AS contract_broken,
            sum(tx_count_inconclusive_needs_higher_sweep) AS inconclusive,
            sum(tx_count_schedule_rescued) AS schedule_rescued,
            sum(tx_count_wallet_fixable_shallow) AS wallet_fixable_shallow,
            sum(tx_count_wallet_fixable_deep_chain) AS wallet_fixable_deep_chain
        FROM block_coverage
        WHERE schedule_name = ?
    """, (s,))[0]
    total_analyzed = _int(row["total_analyzed"])
    contract_broken = _int(row["contract_broken"])
    inconclusive = _int(row["inconclusive"])
    schedule_rescued = _int(row["schedule_rescued"])
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
        "inconclusive_needs_higher_sweep_txs": inconclusive,
        "schedule_rescued_txs": schedule_rescued,
        "breakage_rate": round(broken / total_analyzed * 100, 2) if total_analyzed else 0,
        "contract_breakage_rate": round(contract_broken / total_analyzed * 100, 2) if total_analyzed else 0,
        "inconclusive_rate": round(inconclusive / total_analyzed * 100, 2) if total_analyzed else 0,
        "rescue_rate": round(schedule_rescued / total_analyzed * 100, 2) if total_analyzed else 0,
    }


@router.get("/funnel")
@cache_endpoint(_AGGREGATE_TTL)
def funnel(schedule: str = Query(default=None)):
    """Bucket every divergent tx by observable impact.

    `trace_divergent_only` is the previously-mislabelled cohort: txs whose
    intermediate EVM trace differs from baseline but whose final outcome
    (gas used, event logs, status) matches — i.e. no observable change.
    """
    s = resolve_schedule(schedule)
    row = query_sqlite("""
        SELECT
            sum(tx_count - tx_count_unchanged) AS total,
            sum(tx_count_contract_broken
                + tx_count_wallet_fixable_shallow
                + tx_count_wallet_fixable_deep_chain) AS broken,
            sum(tx_count_inconclusive_needs_higher_sweep) AS inconclusive,
            sum(tx_count_schedule_rescued)   AS schedule_rescued,
            sum(tx_count_event_logs_changed) AS event_log_changed,
            sum(tx_count_gas_only)           AS gas_only_change,
            sum(tx_count_trace_only)         AS trace_divergent_only
        FROM block_coverage
        WHERE schedule_name = ?
    """, (s,))[0]
    return {
        "divergent_txs": _int(row["total"]),
        "broken_txs": _int(row["broken"]),
        "inconclusive_needs_higher_sweep": _int(row["inconclusive"]),
        "schedule_rescued": _int(row["schedule_rescued"]),
        "event_log_changed": _int(row["event_log_changed"]),
        "gas_only_change": _int(row["gas_only_change"]),
        "trace_divergent_only": _int(row["trace_divergent_only"]),
    }


@router.get("/opcode-impact")
@cache_endpoint(_AGGREGATE_TTL)
def opcode_impact(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    rows = query(f"""
        SELECT divergence_opcode AS opcode_num, count(*) AS cnt
        FROM divergences
        WHERE divergence_opcode IS NOT NULL
          AND bucket = 'contract_broken'
          AND schedule_name = '{s}'
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


@router.get("/opcode-gas-share")
@cache_endpoint(_AGGREGATE_TTL)
def opcode_gas_share(schedule: str = Query(default=None)):
    """Fraction of total gas each *repriced* opcode burned.

    Aggregates `block_summaries.opcode_totals_7904` over the whole
    schedule, then filters to opcodes where the schedule's per-op
    cost differs from baseline (`gas_schedule != gas_baseline`). The
    `share` denominator is total gas_schedule across **all** opcodes
    — so the returned shares sum to the slice of EVM gas the EIP's
    repricing actually touches (rather than renormalising to 100%).

    Each entry carries:
      - count        — total executions
      - gas_baseline — what these executions would have cost under the
                       baseline schedule
      - gas_schedule — what they actually cost under the replay schedule
      - gas_delta    — schedule - baseline (the added cost the EIP introduces)
      - share        — gas_schedule / total_gas_schedule_all_opcodes (percent)
    """
    s = resolve_schedule(schedule)
    rows = query_sqlite("""
        SELECT
            CAST(json_extract(j.value, '$.opcode') AS INTEGER) AS opcode,
            sum(CAST(json_extract(j.value, '$.count') AS INTEGER)) AS count,
            sum(CAST(json_extract(j.value, '$.gas_baseline') AS INTEGER)) AS gas_baseline,
            sum(CAST(json_extract(j.value, '$.gas_schedule') AS INTEGER)) AS gas_schedule
        FROM block_summaries AS bs, json_each(bs.opcode_totals_7904) AS j
        WHERE bs.opcode_totals_7904 IS NOT NULL
          AND bs.opcode_totals_7904 <> '[]'
          AND bs.schedule_name = ?
        GROUP BY opcode
        ORDER BY gas_schedule DESC
    """, (s,))
    # `total_schedule` covers every opcode (repriced or not) so the
    # returned shares stay comparable to the whole EVM cost surface.
    total_schedule = sum(_int(r["gas_schedule"]) for r in rows) or 1

    out = []
    for r in rows:
        gs = _int(r["gas_schedule"])
        gb = _int(r["gas_baseline"])
        if gs == gb:
            continue
        op = int(r["opcode"])
        out.append({
            "opcode": f"0x{op:02x}",
            "name": opcode_label(op),
            "count": _int(r["count"]),
            "gas_baseline": gb,
            "gas_schedule": gs,
            "gas_delta": gs - gb,
            "share": round(gs / total_schedule * 100, 2),
        })
    return out


@router.get("/gas-overhead")
@cache_endpoint(_AGGREGATE_TTL)
def gas_overhead(schedule: str = Query(default=None)):
    """CDF + stats over the non-broken cohort (gas_only, trace_only,
    event_logs_changed) reconstructed from `block_summaries`'s pre-binned
    log2 histograms.

    Percentiles are bin-aligned (powers of two); we can't recover the
    finer-grained percentiles the old query computed because the
    aggregate cohort isn't stored per-tx anymore. CDF fidelity is
    unchanged — it was already plotted from the same log2 bins.
    """
    s = resolve_schedule(schedule)
    hist_cols = ",\n            ".join(
        "sum(COALESCE(CAST(json_extract(gas_delta_log2_hist, '$[%d]') AS INTEGER), 0)) AS h%d"
        % (i, i)
        for i in range(12)
    )
    rows = query_sqlite(f"""
        SELECT
            sum(tx_count) AS tx_count,
            sum(gas_delta_sum) AS gas_delta_sum,
            min(gas_delta_min) AS gas_delta_min,
            max(gas_delta_max) AS gas_delta_max,
            {hist_cols}
        FROM block_summaries
        WHERE bucket IN ('gas_only', 'trace_only', 'event_logs_changed')
          AND schedule_name = ?
    """, (s,))
    row = rows[0] if rows else {}
    return _gas_delta_aggregate_response([{
        "tx_count": row.get("tx_count"),
        "gas_delta_sum": row.get("gas_delta_sum"),
        "gas_delta_min": row.get("gas_delta_min"),
        "gas_delta_max": row.get("gas_delta_max"),
        "gas_delta_log2_hist": [_int(row.get(f"h{i}")) for i in range(12)],
    }])


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
@cache_endpoint(_AGGREGATE_TTL)
def concentration(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    rows = query_sqlite("""
        SELECT recipient, count(*) AS broken_txs
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
        GROUP BY recipient ORDER BY broken_txs DESC
    """, (s,))
    total = sum(_int(r["broken_txs"]) for r in rows)
    cumulative = 0
    out = []
    for i, row in enumerate(rows[:50]):
        cumulative += _int(row["broken_txs"])
        cum_pct = cumulative / total * 100 if total else 0
        out.append({
            "rank": i + 1,
            "recipient": row["recipient"] if isinstance(row["recipient"], str) else None,
            "name": label_address(row["recipient"]),
            "broken_txs": _int(row["broken_txs"]),
            "cum_pct": round(cum_pct, 2),
        })
    return out


@router.get("/top-contracts")
@cache_endpoint(_AGGREGATE_TTL)
def top_contracts(
    limit: int = Query(default=10, le=500),
    schedule: str = Query(default=None),
):
    s = resolve_schedule(schedule)
    rows = query_sqlite(f"""
        SELECT recipient, count(*) AS broken_txs,
               avg(gas_delta) AS avg_delta, sum(gas_delta) AS total_delta
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
        GROUP BY recipient ORDER BY broken_txs DESC LIMIT {int(limit)}
    """, (s,))
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
@cache_endpoint(_AGGREGATE_TTL)
def forensics_time_series(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    return query(f"""
        WITH bounds AS (
            SELECT min(block_number) AS mn, max(block_number) AS mx
            FROM block_coverage
            WHERE schedule_name = '{s}'
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
              AND d.schedule_name = '{s}'
            GROUP BY block_group
        ),
        total_per_bucket AS (
            SELECT
                b.mn + ((c.block_number - b.mn) // b.bucket_size) * b.bucket_size AS block_group,
                sum(c.tx_count) AS total_txs
            FROM block_coverage c, buckets b
            WHERE c.schedule_name = '{s}'
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
@cache_endpoint(_AGGREGATE_TTL)
def forensics_gas_delta(schedule: str = Query(default=None)):
    """Gas-delta stats + histogram for the contract-broken cohort.

    Contract-broken rows are per-tx in `divergences`, so percentiles are
    exact (unlike the aggregate-cohort percentiles in /api/gas-overhead,
    which approximate from log2 bins).
    """
    s = resolve_schedule(schedule)
    stats = query(f"""
        SELECT
            median(gas_delta) as median_delta,
            avg(gas_delta) as mean_delta,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY gas_delta) as p25,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY gas_delta) as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_delta) as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY gas_delta) as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_delta) as p99
        FROM divergences
        WHERE bucket = 'contract_broken' AND schedule_name = '{s}'
    """)[0]
    histogram = query(f"""
        WITH bucketed AS (
            SELECT
                CASE WHEN gas_delta <= 0 THEN 0
                     ELSE floor(log2(gas_delta))::int
                END AS log_bin,
                count(*) AS cnt
            FROM divergences
            WHERE bucket = 'contract_broken' AND schedule_name = '{s}'
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
@cache_endpoint(_AGGREGATE_TTL)
def forensics_call_depth(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    return query(f"""
        SELECT
            coalesce(divergence_call_depth, -1) AS divergence_call_depth,
            count(*) AS divergent_txs
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = '{s}'
        GROUP BY 1 ORDER BY 1
    """)


@router.get("/forensics/bottleneck-kinds")
@cache_endpoint(_AGGREGATE_TTL)
def forensics_bottleneck_kinds(schedule: str = Query(default=None)):
    """How many contract-broken *OOG* txs hit each kind of gas-forwarding
    bottleneck.

    Only OOG-class breakages (rows with non-NULL `oog_call_depth`) feed
    this chart — the producer's chain-walk classifier only runs on
    OOGs, and a "bottleneck" doesn't apply to a tx that reverted for
    other reasons. The complementary cohort (status-flip without OOG)
    is reported by `/api/forensics/break-reason` and shown alongside
    on the dashboard.

    A row with `oog_call_depth` set but `oog_bottleneck_kind` still
    NULL means the classifier ran but couldn't identify the throttle —
    rare in current runs; surfaced as 'Unclassified' for visibility.
    """
    s = resolve_schedule(schedule)
    rows = query_sqlite("""
        SELECT
            coalesce(oog_bottleneck_kind, 'Unclassified') AS kind,
            count(*) AS cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND oog_call_depth IS NOT NULL
          AND schedule_name = ?
        GROUP BY 1
        ORDER BY cnt DESC
    """, (s,))
    total = sum(r["cnt"] for r in rows) or 1
    return [
        {
            "kind": r["kind"],
            "count": _int(r["cnt"]),
            "share": round(r["cnt"] / total * 100, 1),
        }
        for r in rows
    ]


@router.get("/forensics/break-reason")
@cache_endpoint(_AGGREGATE_TTL)
def forensics_break_reason(schedule: str = Query(default=None)):
    """Split contract-broken txs into OOG vs non-OOG-revert.

    Status-flip is a broader category than "the tx ran out of gas": a
    schedule change can also trigger different revert paths via gas-
    refund accounting, intrinsic gas thresholds, or floor-gas mechanics
    (EIP-7623). This endpoint counts how many contract-broken txs were
    OOGs vs reverts-from-other-causes, so the dashboard doesn't lump
    them together under one misleading bucket.
    """
    s = resolve_schedule(schedule)
    row = query_sqlite("""
        SELECT
            sum(CASE WHEN oog_call_depth IS NOT NULL THEN 1 ELSE 0 END) AS oog,
            sum(CASE WHEN oog_call_depth IS NULL THEN 1 ELSE 0 END)     AS non_oog_revert,
            count(*)                                                    AS total
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
    """, (s,))[0]
    total = _int(row["total"]) or 1
    return [
        {"reason": "OOG",
         "count": _int(row["oog"]),
         "share": round(_int(row["oog"]) / total * 100, 1)},
        {"reason": "Non-OOG revert",
         "count": _int(row["non_oog_revert"]),
         "share": round(_int(row["non_oog_revert"]) / total * 100, 1)},
    ]


# SQL fragment used by both /forensics/failure-motifs and
# /forensics/failure-flow. "Failing leaf" = the deepest CALL frame in a
# contract-broken tx whose success flag is FALSE — that's the frame
# that OOG'd. We pick the deepest such frame per tx via a row-number
# window. For most real txs every ancestor frame also has success=FALSE
# (the OOG bubbles up), but ranking by depth keeps the leaf consistent.
def _failing_leaves_cte(s: str) -> str:
    return f"""
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
      AND d.schedule_name = '{s}'
      AND cf.success = FALSE
)
"""


@router.get("/forensics/failure-motifs")
@cache_endpoint(_AGGREGATE_TTL)
def forensics_failure_motifs(schedule: str = Query(default=None)):
    """Top caller→callee pairs at the failing leaf frame.

    `pair_motif` is (caller_project, callee_project). `triple_motif` adds
    the root-frame project for context — useful when the same library
    fails from different top-level entry points.
    """
    s = resolve_schedule(schedule)
    rows = query(_failing_leaves_cte(s) + f""",
    roots AS (
        SELECT cf.divergence_id, cf.to_address AS root_to
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken'
          AND d.schedule_name = '{s}'
          AND cf.depth = 0
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
@cache_endpoint(_AGGREGATE_TTL)
def forensics_failure_flow(schedule: str = Query(default=None)):
    """Sankey: root project → failing caller project → failing callee project.

    Sourced from `call_frames` for the contract-broken cohort. Addresses
    that don't have a hardcoded label fall back to the address itself —
    once the producer-side `contract_metadata` table lands we can do
    nicer labeling via codehash.
    """
    s = resolve_schedule(schedule)
    rows = query(_failing_leaves_cte(s) + f""",
    roots AS (
        SELECT cf.divergence_id, cf.to_address AS root_to
        FROM call_frames cf
        JOIN divergences d USING (divergence_id)
        WHERE d.bucket = 'contract_broken'
          AND d.schedule_name = '{s}'
          AND cf.depth = 0
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


@router.get("/eip8037/impact-breakdown")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_impact_breakdown(schedule: str = Query(default=None)):
    """Headline anchor: every historical tx in the analyzed window,
    bucketed by the user-facing impact of EIP-8037.

    Sourced from `block_coverage`'s per-bucket counts (the producer's
    classifier owns the bucketing). We collapse the producer's nine
    buckets into five user-visible categories, in order from "no
    user-visible impact" → "needs code change":

      unaffected               — tx didn't diverge under 8037
      paid_more_no_change      — gas_only + trace_only (more expensive,
                                 same outcome and event logs)
      observable_change        — event_logs_changed (different events
                                 emitted; observable to integrators)
      wallet_fixable           — bumping the wallet's gas-limit estimate
                                 resolves it
      contract_broken          — needs a code change (incl. the
                                 inconclusive_needs_higher_sweep cohort,
                                 which is a more-investigation flag, not
                                 a fixability finding)

    `schedule_rescued` is reported separately as a positive sidebar (8037
    fixed these baseline failures) rather than mixed into the failure
    surface.
    """
    s = resolve_schedule(schedule)
    row = query_sqlite("""
        SELECT
            sum(tx_count) AS total,
            sum(tx_count_unchanged) AS unaffected,
            sum(tx_count_gas_only + tx_count_trace_only) AS paid_more_no_change,
            sum(tx_count_event_logs_changed) AS observable_change,
            sum(tx_count_wallet_fixable_shallow + tx_count_wallet_fixable_deep_chain)
                AS wallet_fixable,
            sum(tx_count_contract_broken + tx_count_inconclusive_needs_higher_sweep)
                AS contract_broken,
            sum(tx_count_schedule_rescued) AS schedule_rescued
        FROM block_coverage
        WHERE schedule_name = ?
    """, (s,))[0]

    total = _int(row["total"]) or 1
    segments = [
        ("unaffected",          "Unaffected",                   _int(row["unaffected"])),
        ("paid_more_no_change", "Pays more, same outcome",      _int(row["paid_more_no_change"])),
        ("observable_change",   "Event-log difference",         _int(row["observable_change"])),
        ("wallet_fixable",      "Wallet must raise gas-limit",  _int(row["wallet_fixable"])),
        ("contract_broken",     "Needs contract change",        _int(row["contract_broken"])),
    ]
    return {
        "total": _int(row["total"]),
        "segments": [
            {
                "key": k,
                "label": label,
                "count": n,
                "share": round(n / total * 100, 3),
            }
            for k, label, n in segments
        ],
        "schedule_rescued": _int(row["schedule_rescued"]),
    }


@router.get("/eip8037/state-growth")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_state_growth(schedule: str = Query(default=None)):
    """Daily state-bytes added in the analyzed window, derived from the
    producer's per-(block, bucket) `state_gas_sum`. Dividing by CPSB
    converts the 8037 state-gas charge back to bytes — the EIP's primary
    accounting unit. The resulting series is "new state bytes per day
    under the 8037 schedule's definition of state creation".

    Reported alongside the EIP's target growth rate (120 GiB/year ≈ 329
    MiB/day) so the chart caption can compare actual vs target. The
    window can be quite short for a fresh run; the implied "annualized"
    rate at the bottom of the response simply extrapolates the observed
    daily mean.
    """
    s = resolve_schedule(schedule)
    CPSB = 1530
    rows = query_sqlite("""
        SELECT
            bc.timestamp,
            bc.block_number,
            coalesce(sum(bs.state_gas_sum), 0) AS state_gas_sum
        FROM block_coverage bc
        LEFT JOIN block_summaries bs
            ON bs.schedule_name = bc.schedule_name
            AND bs.block_number = bc.block_number
        WHERE bc.schedule_name = ?
        GROUP BY bc.timestamp, bc.block_number
        ORDER BY bc.timestamp, bc.block_number
    """, (s,))
    if not rows:
        return {"daily": [], "summary": None}

    # Bucket per UTC day. Producer timestamps are unix seconds.
    from collections import defaultdict
    daily_bytes: dict[int, int] = defaultdict(int)
    daily_blocks: dict[int, int] = defaultdict(int)
    for r in rows:
        ts = _int(r["timestamp"])
        day = ts - (ts % 86_400)
        daily_bytes[day] += _int(r["state_gas_sum"]) // CPSB
        daily_blocks[day] += 1

    daily = [
        {
            "day_ts": d,
            "state_bytes_added": daily_bytes[d],
            "blocks": daily_blocks[d],
            "mib": round(daily_bytes[d] / (1024 * 1024), 3),
        }
        for d in sorted(daily_bytes)
    ]
    total_bytes = sum(daily_bytes.values())
    total_days = len(daily) or 1
    mean_per_day = total_bytes / total_days
    return {
        "daily": daily,
        "summary": {
            "blocks": sum(daily_blocks.values()),
            "days_observed": total_days,
            "total_state_bytes": total_bytes,
            "mean_bytes_per_day": round(mean_per_day, 1),
            "implied_annual_gib": round(mean_per_day * 365 / (1024 ** 3), 2),
            "target_bytes_per_day": round(120 * (1024 ** 3) / 365, 1),
            "target_annual_gib": 120,
            "cpsb": CPSB,
        },
    }


@router.get("/eip8037/deployment-ceiling")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_deployment_ceiling(schedule: str = Query(default=None)):
    """Scatter every successful CREATE / CREATE2 frame in the cohort
    against the EIP-7825 per-tx cap, with deployed-bytecode size on the
    x-axis and 8037 gas used on the y-axis.

    Data source: `call_frames.deployed_bytecode_len` (added in
    producer schema v7) records the actual runtime-code length emitted
    by each successful CREATE/CREATE2 frame. Top-level CREATE txs and
    sub-call CREATEs are both included — both shapes are deployments
    in the EIP-8037 sense.

    `schedule_gas` is the frame's own `gas_used` (revm's frame-level
    accounting). For top-level CREATE this is essentially the tx's gas
    used minus tx-intrinsic; for sub-call CREATEs it's the deployment's
    marginal cost to its parent. Either way it's the right "what does
    deploying a contract of size N actually cost under 8037" number.

    A row with `deployed_bytecode_len IS NULL` came from a producer
    run on schema < v7 — surfaced as the legacy approximation
    (bytecode size derived from baseline gas-used) so dashboards don't
    go blank during schema rollover.
    """
    s = resolve_schedule(schedule)
    TX_MAX_GAS_LIMIT = 16_777_216  # 2^24, EIP-7825 cap
    CPSB = 1530                    # EIP-8037 state-gas cost per byte
    # Raw SQLite: drive from divergences (idx_div_schedule) and join
    # call_frames by the divergence_id PK, rather than the DuckDB
    # call_frames view (which scans the whole frames table through
    # sqlite_scanner — ~8.6s cold on the production file).
    rows = query_sqlite("""
        SELECT
            cf.deployed_bytecode_len,
            cf.gas_used        AS frame_gas_used,
            cf.gas_provided    AS frame_gas_provided,
            cf.call_type,
            d.schedule_gas_used,
            d.baseline_gas_used,
            d.schedule_state_gas_spent,
            d.tx_gas_limit,
            d.schedule_success,
            (cf.depth = 0)     AS is_root_create
        FROM divergences d
        JOIN divergence_call_frames cf ON cf.divergence_id = d.divergence_id
        WHERE d.schedule_name = ?
          AND cf.call_type IN ('CREATE', 'CREATE2')
          AND cf.success = 1
        ORDER BY cf.gas_used DESC
        LIMIT 5000
    """, (s,))

    samples = []
    over_cap = 0
    have_real_size = 0
    for r in rows:
        if r["deployed_bytecode_len"] is not None:
            bytes_ = _int(r["deployed_bytecode_len"])
            size_source = "exact"
            have_real_size += 1
        else:
            # Legacy fallback for pre-v7 DBs: derive from baseline gas.
            # 200 gas/byte CODEDEPOSIT + ~53k overhead (intrinsic 32k + init+payload).
            baseline = _int(r["baseline_gas_used"])
            bytes_ = max(0, (baseline - 53_000) // 200)
            size_source = "approx"
        sched = _int(r["frame_gas_used"])
        if sched > TX_MAX_GAS_LIMIT:
            over_cap += 1
        samples.append({
            "bytes":          bytes_,
            "size_source":    size_source,
            "schedule_gas":   sched,
            "call_type":      r["call_type"],
            "is_root_create": bool(r["is_root_create"]),
            "tx_gas_used":    _int(r["schedule_gas_used"]),
            "tx_state_gas":   _int(r["schedule_state_gas_spent"]),
            "tx_gas_limit":   _int(r["tx_gas_limit"]),
            "tx_success":     bool(r["schedule_success"]),
        })
    total = len(samples)
    return {
        "tx_max_gas_limit": TX_MAX_GAS_LIMIT,
        "cpsb": CPSB,
        "state_gas_cap_bytes": TX_MAX_GAS_LIMIT // CPSB,  # size where state-gas alone hits the cap
        "samples": samples,
        "summary": {
            "total_deployments_in_cohort": total,
            "with_exact_bytecode_size":    have_real_size,
            "over_eip7825_cap":            over_cap,
            "share_over_cap":              round(over_cap / total * 100, 2) if total else 0.0,
        },
    }


@router.get("/eip8037/cost-comparison")
def eip8037_cost_comparison(schedule: str = Query(default=None)):
    """Static cost comparison for canonical state-touching operations.

    Numbers come from the EIP-8037 constants currently shipped in the
    reth-research schedule (`crates/research/src/schedule/eip8037.rs`):
    CPSB=1530, 64 state bytes per storage slot, 120 per new account,
    23 per 7702 auth base. Baseline figures are the pre-8037 gas costs
    for the operation as defined in the EIPs they replace (SSTORE_SET=20k,
    NEW_ACCOUNT=25k, PER_AUTH_BASE_COST=12.5k, CODEDEPOSIT=200/byte).

    No DB lookup — this is the "what is the new cost per op" anchor.
    Surfaced via an endpoint so the constants stay one source-of-truth
    (the Rust schedule struct) and the template doesn't hand-encode
    numbers that drift.
    """
    CPSB = 1530
    STATE_BYTES_STORAGE_SET = 64
    STATE_BYTES_NEW_ACCOUNT = 120
    STATE_BYTES_AUTH_BASE = 23

    storage_state_gas = STATE_BYTES_STORAGE_SET * CPSB           # 97,920
    new_account_state_gas = STATE_BYTES_NEW_ACCOUNT * CPSB       # 183,600
    auth_existing_state_gas = STATE_BYTES_AUTH_BASE * CPSB       # 35,190
    auth_new_acc_state_gas = (
        STATE_BYTES_NEW_ACCOUNT + STATE_BYTES_AUTH_BASE) * CPSB  # 218,790
    deploy_bytes = 24_576  # contract size at the runtime cap

    ops = [
        {
            "key": "sstore",
            "label": "Cold SSTORE (0 → non-zero)",
            "detail": "First write to a storage slot",
            "baseline_regular": 20_000,
            "baseline_state":   0,
            "schedule_regular": 20_000,
            "schedule_state":   storage_state_gas,
        },
        {
            "key": "new_account",
            "label": "New account (CALL with value)",
            "detail": "ETH transfer creating a new EOA",
            "baseline_regular": 25_000,
            "baseline_state":   0,
            "schedule_regular": 25_000,
            "schedule_state":   new_account_state_gas,
        },
        {
            "key": "auth_existing",
            "label": "EIP-7702 SetCode (existing target)",
            "detail": "Per authorization, delegate already exists",
            "baseline_regular": 12_500,
            "baseline_state":   0,
            "schedule_regular": 12_500,
            "schedule_state":   auth_existing_state_gas,
        },
        {
            "key": "auth_new",
            "label": "EIP-7702 SetCode (new target)",
            "detail": "Per authorization, target account is new",
            "baseline_regular": 37_500,  # 12.5k auth + 25k new account
            "baseline_state":   0,
            "schedule_regular": 37_500,
            "schedule_state":   auth_new_acc_state_gas,
        },
        {
            "key": "deploy_24k",
            "label": "Contract deployment (24 kB)",
            "detail": "Maximum-size contract at the EIP-170 cap",
            "baseline_regular": 32_000 + deploy_bytes * 200,
            "baseline_state":   0,
            "schedule_regular": 32_000 + deploy_bytes * 200,
            "schedule_state":   new_account_state_gas + deploy_bytes * CPSB,
        },
    ]
    for op in ops:
        baseline_total = op["baseline_regular"] + op["baseline_state"]
        schedule_total = op["schedule_regular"] + op["schedule_state"]
        op["baseline_total"] = baseline_total
        op["schedule_total"] = schedule_total
        op["multiplier"] = (schedule_total / baseline_total) if baseline_total else None

    return {
        "constants": {
            "cpsb": CPSB,
            "state_bytes_per_storage_set": STATE_BYTES_STORAGE_SET,
            "state_bytes_per_new_account": STATE_BYTES_NEW_ACCOUNT,
            "state_bytes_per_auth_base":   STATE_BYTES_AUTH_BASE,
        },
        "operations": ops,
    }


@router.get("/eip8037/overview")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_overview(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    stats = query(f"""
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
        WHERE schedule_name = '{s}'
    """)[0]
    total_analyzed = query_scalar(
        f"SELECT sum(tx_count) FROM block_coverage WHERE schedule_name = '{s}'",
        default=0,
    )
    block_range = query(
        f"SELECT min(block_number) AS mn, max(block_number) AS mx "
        f"FROM block_coverage WHERE schedule_name = '{s}'"
    )
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}

    return {
        "schedule_name": s,
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
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_multiplier_histogram(schedule: str = Query(default=None)):
    # Runs through raw SQLite (idx_div_schedule) rather than the
    # eip8037_tx_impact DuckDB view. The columns it reads
    # (would_fit_in_original_limit, min_multiplier_to_succeed,
    # status_changed) are raw on `divergences`, so the view adds nothing
    # but a full sqlite_scanner pass that fights the writer lock —
    # ~25s cold on the production file vs sub-second through SQLite.
    s = resolve_schedule(schedule)
    rows = query_sqlite("""
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
            FROM divergences
            WHERE schedule_name = ?
            GROUP BY 1, 2
        )
        SELECT * FROM bucketed ORDER BY sort_key
    """, (s,))
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
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_reservoir(schedule: str = Query(default=None)):
    """Reservoir-utilization view: how full does the per-tx state-gas
    reservoir get in practice, and what happens to the overflow tail."""
    s = resolve_schedule(schedule)
    headline = query(f"""
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
        WHERE schedule_name = '{s}'
    """)[0]

    util_rows = query(f"""
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
            WHERE schedule_name = '{s}'
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
    spillover_rows = query(f"""
        WITH bucketed AS (
            SELECT
                floor(log2(runtime_state_gas_spillover))::int AS log_bin,
                count(*) AS cnt
            FROM eip8037_tx_impact
            WHERE runtime_state_gas_spillover > 0
              AND schedule_name = '{s}'
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

    category_rows = query(f"""
        SELECT
            coalesce(state_gas_category, 'uncategorized') AS category,
            count(*) AS total_txs,
            sum(CASE WHEN runtime_state_gas <= 0 THEN 1 ELSE 0 END) AS no_state_txs,
            sum(CASE WHEN runtime_state_gas > 0 AND runtime_state_gas_spillover = 0
                          THEN 1 ELSE 0 END) AS fits_txs,
            sum(CASE WHEN runtime_state_gas_spillover > 0 THEN 1 ELSE 0 END) AS overflow_txs
        FROM eip8037_tx_impact
        WHERE schedule_name = '{s}'
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


@router.get("/eip8037/divergence-reasons")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_divergence_reasons(schedule: str = Query(default=None)):
    """Categorize each divergent tx by **why** it diverged under EIP-8037.

    EIP-7825 currently caps `tx.gas` at 16.7M, which per spec forces
    `state_gas_reservoir == 0` for every historical tx — so the
    reservoir mechanism is dormant on current mainnet, and most
    divergent txs aren't actually exercising 8037's headline feature.
    This breakdown surfaces what they ARE diverging on, in priority
    order (mutually exclusive):

    1. `pre_execution_rejection` — EVM rejected the tx before
       running (e.g. adjusted gas_limit < baseline intrinsic). Producer
       records these with `schedule_total_gas_spent = 0`.
    2. `runtime_state_gas` — EVM charged state-gas during execution
       (SSTORE 0→non-zero, account creation via CALL). 8037's canonical
       mechanism actually firing.
    3. `intrinsic_state_gas_only` — intrinsic state-gas charged at tx
       start (CREATE / 7702 auth) but no runtime state-gas. The
       schedule's intrinsic surcharge drove the divergence.
    4. `event_log_only` — no state-gas involvement; differs in event
       log output via different OOG paths or refund timing.
    5. `other_gas_accounting` — gas_delta != 0 from another path
       (floor gas, refund interactions) without state-gas or event-log
       differences.

    Per category we report total txs, status flips, and newly-broken
    (baseline_success AND NOT schedule_success).
    """
    s = resolve_schedule(schedule)
    rows = query(f"""
        WITH categorized AS (
            SELECT
                CASE
                    WHEN coalesce(schedule_total_gas_spent, 0) = 0
                        THEN 'pre_execution_rejection'
                    WHEN coalesce(schedule_state_gas_spent, 0)
                       - coalesce(schedule_initial_state_gas, 0) > 0
                        THEN 'runtime_state_gas'
                    WHEN coalesce(schedule_initial_state_gas, 0) > 0
                        THEN 'intrinsic_state_gas_only'
                    WHEN event_logs_changed
                        THEN 'event_log_only'
                    ELSE 'other_gas_accounting'
                END AS category,
                status_changed, baseline_success, schedule_success,
                coalesce(schedule_state_gas_spent, 0) AS sg_spent
            FROM divergences
            WHERE schedule_name = '{s}'
        )
        SELECT
            category,
            count(*) AS txs,
            sum(CASE WHEN status_changed THEN 1 ELSE 0 END) AS status_changed_txs,
            sum(CASE WHEN baseline_success AND NOT schedule_success
                          THEN 1 ELSE 0 END)              AS newly_broken_txs,
            sum(sg_spent)                                  AS total_state_gas_spent
        FROM categorized
        GROUP BY category
        ORDER BY txs DESC
    """)
    return [
        {
            "category": r["category"],
            "txs": _int(r["txs"]),
            "status_changed_txs": _int(r["status_changed_txs"]),
            "newly_broken_txs": _int(r["newly_broken_txs"]),
            "total_state_gas_spent": _int(r["total_state_gas_spent"]),
        }
        for r in rows
    ]


@router.get("/eip8037/unresolved-breakdown")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_unresolved_breakdown(schedule: str = Query(default=None)):
    """Decompose the `unresolved_replay_failures` cohort (failed under
    8037 AND `min_multiplier_to_succeed IS NULL`) across the same five
    priority categories as `/eip8037/divergence-reasons`, additionally
    split by whether baseline succeeded.

    Background — the producer's `min_multiplier_to_succeed` is **not**
    a search. It's just `schedule_gas_used / original_gas_limit` IFF
    the single-shot replay (run at `original × gas_limit_multiplier`,
    default `multiplier=1`) succeeded. NULL means the replay failed
    for any reason at the configured budget. So "unresolved" today
    means "failed under 8037 at gas_limit × multiplier, and the
    producer didn't try a higher budget."

    This breakdown answers: of the unresolved 500k+ rows, how many
    are pre-execution rejections (unfixable by more gas), how many
    are baseline-already-failing (not a regression), and how many
    sit in the state-gas cohort (where reservoir lifting / multiplier
    sweep would actually help).
    """
    s = resolve_schedule(schedule)
    rows = query(f"""
        WITH categorized AS (
            SELECT
                CASE
                    WHEN coalesce(schedule_total_gas_spent, 0) = 0
                        THEN 'pre_execution_rejection'
                    WHEN coalesce(schedule_state_gas_spent, 0)
                       - coalesce(schedule_initial_state_gas, 0) > 0
                        THEN 'runtime_state_gas'
                    WHEN coalesce(schedule_initial_state_gas, 0) > 0
                        THEN 'intrinsic_state_gas_only'
                    WHEN event_logs_changed
                        THEN 'event_log_only'
                    ELSE 'other_gas_accounting'
                END AS category,
                baseline_success
            FROM divergences
            WHERE schedule_name = '{s}'
              AND NOT schedule_success
              AND min_multiplier_to_succeed IS NULL
        )
        SELECT
            category,
            count(*)                                                          AS txs,
            sum(CASE WHEN baseline_success THEN 1 ELSE 0 END)                 AS newly_broken_txs,
            sum(CASE WHEN NOT baseline_success THEN 1 ELSE 0 END)             AS already_failing_txs
        FROM categorized
        GROUP BY category
        ORDER BY txs DESC
    """)
    total = sum(_int(r["txs"]) for r in rows) or 1
    return {
        "total_unresolved": total,
        "by_category": [
            {
                "category": r["category"],
                "txs": _int(r["txs"]),
                "newly_broken_txs": _int(r["newly_broken_txs"]),
                "already_failing_txs": _int(r["already_failing_txs"]),
                "share": round(_int(r["txs"]) / total * 100, 1),
            }
            for r in rows
        ],
    }


@router.get("/eip8037/state-gas-by-category")
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_state_gas_by_category(schedule: str = Query(default=None)):
    s = resolve_schedule(schedule)
    rows = query(f"""
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
        WHERE schedule_name = '{s}'
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
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_top_contracts(
    limit: int = Query(default=20, le=500),
    schedule: str = Query(default=None),
):
    s = resolve_schedule(schedule)
    rows = query(f"""
        SELECT *
        FROM eip8037_contract_impact
        WHERE schedule_name = '{s}'
          AND (original_limit_failures > 0
               OR status_changed_txs > 0
               OR total_state_gas_spent > 0)
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
@cache_endpoint(_AGGREGATE_TTL)
def eip8037_examples(
    limit: int = Query(default=50, le=500),
    schedule: str = Query(default=None),
):
    s = resolve_schedule(schedule)
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
        WHERE schedule_name = '{s}'
          AND (original_limit_failure
               OR status_changed
               OR reservoir_exhausted)
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


def _affected_base_cte(s_7904: str, s_8037: str) -> str:
    """Combined 7904 + 8037 affected-contracts CTE.

    Both EIP sides filter to `bucket = 'contract_broken'` — the producer
    classifier puts a tx in that bucket iff its failure is structural
    (no amount of outer-gas estimation rescues it). Wallet-fixable
    cohorts (shallow / deep_chain) are deliberately excluded here: the
    /affected page is for contracts that need code changes, not for
    txs that the wallet's gas-estimate retry already resolves. Per-EIP
    pages (/eip7904, /eip8037) still expose the wallet-fixable cohort
    separately for completeness.

    Pages that aggregate across the two EIPs (landing, /affected) pass
    the producer's 7904 and 8037 schedule names separately; the CTE
    filters each side with its own name. Single-EIP queries can pass
    the same name twice or use the dedicated endpoints.
    """
    return f"""
    WITH e7904 AS (
        SELECT lower(recipient) AS addr,
               count(*) AS broken_txs_7904,
               avg(gas_delta) AS avg_delta_7904,
               sum(gas_delta) AS total_delta_7904,
               min(block_number) AS min_block_7904,
               max(block_number) AS max_block_7904
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = '{s_7904}'
        GROUP BY lower(recipient)
    ),
    e8037 AS (
        SELECT lower(recipient) AS addr,
               count(*)                                              AS broken_txs_8037,
               sum(CASE WHEN status_changed THEN 1 ELSE 0 END)       AS status_changed_8037,
               avg(gas_delta)                                        AS avg_delta_8037,
               min(block_number) AS min_block_8037,
               max(block_number) AS max_block_8037
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = '{s_8037}'
        GROUP BY lower(recipient)
    ),
    affected_combined AS (
        SELECT
            coalesce(e7.addr, e8.addr) AS addr,
            coalesce(e7.broken_txs_7904, 0)    AS broken_txs_7904,
            coalesce(e7.avg_delta_7904, 0)     AS avg_delta_7904,
            coalesce(e7.total_delta_7904, 0)   AS total_delta_7904,
            coalesce(e8.broken_txs_8037, 0)    AS broken_txs_8037,
            coalesce(e8.status_changed_8037, 0) AS status_changed_8037,
            coalesce(e8.avg_delta_8037, 0)     AS avg_delta_8037,
            least(coalesce(e7.min_block_7904, 99999999999),
                  coalesce(e8.min_block_8037, 99999999999)) AS min_block,
            greatest(coalesce(e7.max_block_7904, 0),
                     coalesce(e8.max_block_8037, 0)) AS max_block
        FROM e7904 e7
        FULL OUTER JOIN e8037 e8 USING (addr)
    )
"""


@router.get("/affected")
@cache_endpoint(_AGGREGATE_TTL)
def affected(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=500),
    schedule_7904: str = Query(default=None),
    schedule_8037: str = Query(default=None),
):
    """Paginated affected contracts across EIP-7904 and EIP-8037.

    Takes the two schedule names as separate query params because the
    page joins divergences (7904) with eip8037_contract_impact (8037) —
    those tables can be written under different schedule names by the
    producer (e.g. `7904-prelim` and `eip-8037`).
    """
    s_7904 = resolve_schedule(schedule_7904)
    s_8037 = resolve_schedule(schedule_8037)
    offset = (page - 1) * per_page
    cte = _affected_base_cte(s_7904, s_8037)
    total_count = query_scalar(
        cte + " SELECT count(*) FROM affected_combined",
        default=0,
    )
    rows = query(cte + f"""
        SELECT * FROM affected_combined
        ORDER BY broken_txs_7904 DESC, broken_txs_8037 DESC
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
            "broken_txs_8037": _int(r["broken_txs_8037"]),
            "status_changed_8037": _int(r["status_changed_8037"]),
            "avg_delta_8037": _float(r["avg_delta_8037"]),
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


def _contract_flow_sankey(root_label: str, triples: list[tuple[str, str, int]]) -> dict | None:
    """Build a 3-column Sankey (root → level-1 → level-2) from
    `(l1, l2, count)` rows. Level-2 nodes are shared across level-1 (one
    node per distinct l2 label) so converging reasons read as one band.
    Returns None when there's nothing to show.

    Shape matches the `renderSankey` helper: labels + parallel
    sources/targets/values index arrays.
    """
    triples = [(l1, l2, c) for (l1, l2, c) in triples if c > 0]
    if not triples:
        return None

    labels: list[str] = [root_label]
    node_index: dict[tuple, int] = {}

    def node(key: tuple, label: str) -> int:
        if key not in node_index:
            node_index[key] = len(labels)
            labels.append(label)
        return node_index[key]

    from collections import defaultdict
    sources: list[int] = []
    targets: list[int] = []
    values: list[int] = []

    # root → level-1 (sum the level-2 counts per level-1 bucket)
    l1_totals: dict[str, int] = defaultdict(int)
    for l1, _l2, c in triples:
        l1_totals[l1] += c
    for l1, tot in sorted(l1_totals.items(), key=lambda kv: -kv[1]):
        sources.append(0)
        targets.append(node(("l1", l1), l1))
        values.append(tot)

    # level-1 → level-2
    for l1, l2, c in triples:
        sources.append(node_index[("l1", l1)])
        targets.append(node(("l2", l2), l2))
        values.append(c)

    return {"labels": labels, "sources": sources, "targets": targets, "values": values}


@router.get("/affected/{address}")
@cache_endpoint(_AGGREGATE_TTL)
def affected_detail(
    address: str,
    schedule_7904: str = Query(default=None),
    schedule_8037: str = Query(default=None),
):
    """Single contract detail with EIP-7904 and EIP-8037 diagnostics.

    Runs as indexed point lookups through raw SQLite (`recipient = ?`,
    served by `idx_div_recipient`) rather than DuckDB. The previous
    DuckDB path went through the `eip8037_contract_impact` view, which
    GROUP BYs the entire `divergences` table before filtering to one
    address — fine for a small DB, tens-of-seconds on the production
    file while reth is mid-replay. Percentiles that the view computed
    with `percentile_cont` are recomputed in Python (`_percentile`)
    since SQLite lacks the function; the per-contract cohort is small
    enough to sort in-process.
    """
    addr = address.lower()
    s_7904 = resolve_schedule(schedule_7904)
    s_8037 = resolve_schedule(schedule_8037)

    # ── EIP-7904 stats ──
    # Wallet-fixable txs aren't stored per-recipient anymore (the
    # producer aggregates them into block_summaries). The contract
    # detail page only needs contract-broken data. `recipient` is stored
    # lowercase (`{:#x}`), so an exact match uses idx_div_recipient.
    eip7904_stats = query_sqlite("""
        SELECT count(*) as broken_txs,
               avg(gas_delta) as avg_delta,
               sum(gas_delta) as total_delta,
               min(block_number) as min_block,
               max(block_number) as max_block
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
          AND recipient = ?
    """, (s_7904, addr))[0]
    # p95 gas-delta: SQLite has no percentile_cont, so pull the deltas
    # (bounded per-contract cohort) and interpolate in Python.
    eip7904_deltas = [
        r["gas_delta"] for r in query_sqlite("""
            SELECT gas_delta FROM divergences
            WHERE bucket = 'contract_broken'
              AND schedule_name = ? AND recipient = ?
        """, (s_7904, addr))
    ]
    eip7904_p95_delta = _percentile(eip7904_deltas, 0.95)
    eip7904_wallet_n = 0  # No longer tracked per-recipient.
    opcodes_raw = query_sqlite("""
        SELECT divergence_opcode AS op_num, count(*) AS cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND divergence_opcode IS NOT NULL
          AND schedule_name = ?
          AND recipient = ?
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """, (s_7904, addr))
    depths_raw = query_sqlite("""
        SELECT coalesce(divergence_call_depth, -1) as depth, count(*) as cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
          AND recipient = ?
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """, (s_7904, addr))
    eip7904_txs = query_sqlite("""
        SELECT tx_hash, block_number, gas_delta
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
          AND recipient = ?
        ORDER BY gas_delta DESC LIMIT 20
    """, (s_7904, addr))
    bottleneck_kinds_raw = query_sqlite("""
        SELECT coalesce(oog_bottleneck_kind, 'Unclassified') as kind, count(*) as cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
          AND recipient = ?
        GROUP BY 1 ORDER BY cnt DESC
    """, (s_7904, addr))
    # Joint (opcode × why) for the contract's "why it broke" Sankey:
    # divergence opcode → gas-forwarding reason. The reason is the
    # chain-walk bottleneck kind, falling back to "proportional
    # (wallet-fixable)" when every hop forwarded gas via the 63/64 rule,
    # else "unclassified".
    flow_7904_raw = query_sqlite("""
        SELECT divergence_opcode AS op,
               oog_bottleneck_kind AS kind,
               oog_chain_proportional AS prop,
               count(*) AS cnt
        FROM divergences
        WHERE bucket = 'contract_broken'
          AND schedule_name = ?
          AND recipient = ?
        GROUP BY 1, 2, 3
    """, (s_7904, addr))
    flow_7904_triples = []
    for r in flow_7904_raw:
        op_label = opcode_label(r["op"]) if r["op"] is not None else "unknown op"
        if r["kind"]:
            reason = r["kind"]
        elif r["prop"] == 1:
            reason = "proportional (wallet-fixable)"
        else:
            reason = "unclassified"
        flow_7904_triples.append((op_label, reason, _int(r["cnt"])))
    eip7904_sankey = _contract_flow_sankey(f"{label_address(addr)} — 7904", flow_7904_triples)

    # ── EIP-8037 stats ──
    # Aggregate directly over the recipient-filtered divergences instead
    # of the eip8037_contract_impact view (which aggregates the whole
    # table). The derived `original_limit_failure` predicate is inlined
    # to match the view: NOT would_fit (NULL→treated as fits) AND state
    # gas was actually charged.
    eip8037_agg = query_sqlite("""
        SELECT
            count(*) AS divergent_txs,
            sum(CASE WHEN status_changed = 1 THEN 1 ELSE 0 END) AS status_changed_txs,
            sum(CASE WHEN coalesce(would_fit_in_original_limit, 1) = 0
                      AND coalesce(schedule_state_gas_spent, 0) > 0
                     THEN 1 ELSE 0 END) AS original_limit_failures,
            sum(CASE WHEN schedule_success = 1
                      AND would_fit_in_original_limit = 0
                     THEN 1 ELSE 0 END) AS fixable_with_more_outer_gas,
            sum(CASE WHEN reservoir_exhausted = 1 THEN 1 ELSE 0 END) AS reservoir_exhausted_txs,
            sum(schedule_state_gas_spent) AS total_state_gas_spent,
            max(min_multiplier_to_succeed) AS max_min_multiplier_to_succeed,
            max(CASE WHEN schedule_success = 1
                      AND coalesce(would_fit_in_original_limit, 1) = 0
                     THEN schedule_gas_used - tx_gas_limit ELSE NULL END)
                AS max_extra_gas_needed
        FROM divergences
        WHERE schedule_name = ?
          AND recipient = ?
    """, (s_8037, addr))[0]
    eip8037_div = _int(eip8037_agg["divergent_txs"])
    eip8037_row = eip8037_agg if eip8037_div > 0 else None
    # p95 multiplier in Python (bounded per-contract cohort).
    eip8037_multipliers = [
        r["min_multiplier_to_succeed"] for r in query_sqlite("""
            SELECT min_multiplier_to_succeed FROM divergences
            WHERE schedule_name = ? AND recipient = ?
              AND min_multiplier_to_succeed IS NOT NULL
        """, (s_8037, addr))
    ]
    eip8037_p95_multiplier = _percentile(eip8037_multipliers, 0.95)
    categories_raw = query_sqlite("""
        SELECT coalesce(state_gas_category, 'uncategorized') as cat, count(*) as cnt
        FROM divergences
        WHERE schedule_name = ?
          AND recipient = ?
        GROUP BY 1 ORDER BY cnt DESC LIMIT 6
    """, (s_8037, addr))
    # Joint (state-gas category × outcome) for the contract's 8037 "why"
    # Sankey: what kind of state the txs touch → what happened to them
    # (mutually-exclusive, prioritized so each tx lands once).
    flow_8037_raw = query_sqlite("""
        SELECT coalesce(state_gas_category, 'uncategorized') AS cat,
               CASE
                   WHEN baseline_success = 1 AND schedule_success = 0
                       THEN 'breaks (status flip)'
                   WHEN coalesce(would_fit_in_original_limit, 1) = 0
                        AND coalesce(schedule_state_gas_spent, 0) > 0
                       THEN 'needs higher gas limit'
                   WHEN reservoir_exhausted = 1
                       THEN 'reservoir exhausted'
                   ELSE 'pays more, still fits'
               END AS outcome,
               count(*) AS cnt
        FROM divergences
        WHERE schedule_name = ?
          AND recipient = ?
        GROUP BY 1, 2
    """, (s_8037, addr))
    eip8037_sankey = _contract_flow_sankey(
        f"{label_address(addr)} — 8037",
        [(r["cat"], r["outcome"], _int(r["cnt"])) for r in flow_8037_raw],
    )
    eip8037_txs = query_sqlite("""
        SELECT tx_hash, block_number, tx_gas_limit, min_multiplier_to_succeed,
               reservoir_exhausted, state_gas_category,
               runtime_state_gas_spillover, schedule_state_gas_spent
        FROM divergences
        WHERE schedule_name = ?
          AND recipient = ?
          AND ((coalesce(would_fit_in_original_limit, 1) = 0
                  AND coalesce(schedule_state_gas_spent, 0) > 0)
               OR status_changed = 1
               OR reservoir_exhausted = 1)
        ORDER BY
            CASE WHEN baseline_success = 1 AND schedule_success = 0 THEN 0
                 WHEN coalesce(would_fit_in_original_limit, 1) = 0
                      AND coalesce(schedule_state_gas_spent, 0) > 0 THEN 1
                 WHEN reservoir_exhausted = 1 THEN 2 ELSE 3 END,
            coalesce(min_multiplier_to_succeed, 999999) DESC
        LIMIT 20
    """, (s_8037, addr))

    eip7904_broken = _int(eip7904_stats["broken_txs"])
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
            "p95_delta": _float(eip7904_p95_delta),
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
            "sankey": eip7904_sankey,
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
            "p95_min_multiplier_to_succeed": eip8037_p95_multiplier if eip8037_row else None,
            "max_min_multiplier_to_succeed": _float_or_none(eip8037_row["max_min_multiplier_to_succeed"]) if eip8037_row else None,
            "max_extra_gas_needed": _int(eip8037_row["max_extra_gas_needed"]) if eip8037_row else 0,
            "categories": _with_shares([
                {"category": r["cat"], "count": _int(r["cnt"])}
                for r in categories_raw
            ]),
            "sankey": eip8037_sankey,
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
@cache_endpoint(_AGGREGATE_TTL)
def tx_detail(tx_hash: str, schedule: str = Query(default=None)):
    """Detailed view of a single broken transaction: gas info, divergence location, call stack.

    When `?schedule=` is given we filter to that schedule's row;
    otherwise we accept any schedule's row (tx_hash is globally unique,
    but the producer can write a divergence row per schedule, so the
    explicit param is the safe way to disambiguate when both schedules
    flagged the same tx).

    Runs through raw SQLite as an indexed point lookup (`tx_hash = ?`,
    served by `idx_div_tx_hash`). The previous DuckDB query matched on
    `lower(hex(tx_hash)) = ...`, a computed expression no index can
    serve — so every tx page scanned the whole `divergences` table
    through the sqlite_scanner (tens of seconds on the production file).
    The 8037 fields that the `eip8037_tx_impact` view derived
    (`extra_gas_needed`, `estimated_min_gas_limit`) are recomputed in
    Python from the raw `divergences` columns, so we read one base table
    with no JOIN.
    """
    tx_hash = tx_hash.lower().strip()
    try:
        tx_bytes = bytes.fromhex(tx_hash.removeprefix("0x"))
    except ValueError:
        return {"found": False}

    where = "tx_hash = ?"
    params: list = [tx_bytes]
    if schedule:
        s = resolve_schedule(schedule)
        where += " AND schedule_name = ?"
        params.append(s)

    # Core tx info, all from `divergences` (no view JOIN needed).
    hot_rows = query_sqlite(f"""
        SELECT
            divergence_id, block_number, tx_index, tx_hash, bucket,
            schedule_name,
            baseline_success, schedule_success, status_changed,
            event_logs_changed, baseline_gas_used, schedule_gas_used,
            gas_delta, tx_gas_limit, sender, recipient,
            divergence_contract, divergence_call_depth, divergence_opcode,
            oog_contract, oog_call_depth, oog_opcode, oog_pattern,
            oog_gas_remaining, oog_chain_proportional,
            oog_bottleneck_depth, oog_bottleneck_kind,
            would_fit_in_original_limit, min_multiplier_to_succeed,
            schedule_total_gas_spent, schedule_state_gas_spent,
            schedule_state_gas_demanded,
            schedule_initial_state_gas, runtime_state_gas,
            schedule_initial_reservoir, runtime_state_gas_spillover,
            schedule_floor_gas, schedule_gas_refunded,
            baseline_total_gas_spent, baseline_gas_refunded,
            state_gas_category, reservoir_exhausted
        FROM divergences
        WHERE {where}
        LIMIT 1
    """, tuple(params))
    if not hot_rows:
        return {"found": False}
    h = hot_rows[0]
    div_id = h["divergence_id"]

    # Derive the two 8037 fields the eip8037_tx_impact view computed, so
    # the JSON shape is unchanged now that we read divergences directly.
    _would_fit = h.get("would_fit_in_original_limit")
    _min_mult = h.get("min_multiplier_to_succeed")
    _sched_ok = bool(h.get("schedule_success"))
    _fits = 1 if _would_fit is None else _would_fit  # view coalesces NULL→TRUE
    extra_gas_needed = (
        _int(h.get("schedule_gas_used")) - _int(h.get("tx_gas_limit"))
        if _sched_ok and _fits == 0 else None
    )
    estimated_min_gas_limit = (
        math.ceil(_int(h.get("tx_gas_limit")) * _min_mult)
        if _sched_ok and _min_mult is not None and _int(h.get("tx_gas_limit")) > 0
        else None
    )

    # Per-frame call stack from the normalized frames table, keyed by the
    # divergence_id PK (fast indexed lookup).
    frames = query_sqlite("""
        SELECT call_index, depth, from_address, to_address, call_type,
               selector, gas_provided, gas_used, success
        FROM divergence_call_frames
        WHERE divergence_id = ?
        ORDER BY call_index
    """, (div_id,))
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
    op_rows = query_sqlite(f"""
        SELECT opcode, sum(count) AS count,
               sum(gas_schedule) - sum(gas_baseline) AS gas_delta
        FROM divergence_opcode_counts
        WHERE divergence_id = ? AND opcode IN ({placeholders})
        GROUP BY opcode
    """, (div_id,))
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

    # Full per-opcode breakdown: every opcode that executed in the tx,
    # summed across all frames, with the gas it cost under baseline vs the
    # schedule. This is the per-schedule answer to "how many times was
    # each opcode hit and what did the repricing do to it". `repriced` is
    # a convenience flag so the UI can highlight the schedule-affected ops.
    opcode_breakdown = []
    for r in query_sqlite("""
        SELECT opcode,
               sum(count)        AS count,
               sum(gas_baseline) AS gas_baseline,
               sum(gas_schedule) AS gas_schedule
        FROM divergence_opcode_counts
        WHERE divergence_id = ?
        GROUP BY opcode
    """, (div_id,)):
        op = int(r["opcode"])
        gb = _int(r["gas_baseline"])
        gs = _int(r["gas_schedule"])
        opcode_breakdown.append({
            "opcode":       f"0x{op:02x}",
            "name":         opcode_label(op),
            "count":        _int(r["count"]),
            "gas_baseline": gb,
            "gas_schedule": gs,
            "gas_delta":    gs - gb,
            "repriced":     gs != gb,
        })
    opcode_breakdown.sort(key=lambda d: (-d["gas_delta"], -d["count"]))

    # Cross-frame opcode totals for SLOAD/SSTORE/CALL/LOG/total — used by
    # the op_counts panel on the tx page.
    counts_by_op = {r["opcode"]: _int(r["count"]) for r in query_sqlite("""
        SELECT opcode, sum(count) AS count
        FROM divergence_opcode_counts WHERE divergence_id = ?
        GROUP BY opcode
    """, (div_id,))}
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
        "schedule_name": h.get("schedule_name"),
        "block_number": int(h["block_number"]),
        "tx_index": int(h["tx_index"]),
        "sender": h["sender"],
        "recipient": h["recipient"],
        "recipient_name": label_address(h["recipient"]),
        "baseline_success": bool(h["baseline_success"]),
        "schedule_success": bool(h["schedule_success"]),
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
            "chain_proportional": (None if h.get("oog_chain_proportional") is None
                                   else bool(h.get("oog_chain_proportional"))),
            "bottleneck_depth": h.get("oog_bottleneck_depth"),
            "bottleneck_kind": h.get("oog_bottleneck_kind"),
        } if h.get("oog_contract") else None,
        "op_counts": op_counts,
        "eip8037": {
            "would_fit_in_original_limit": (None if _would_fit is None else bool(_would_fit)),
            "min_multiplier_to_succeed": _float_or_none(h.get("min_multiplier_to_succeed")),
            "extra_gas_needed": _int(extra_gas_needed),
            "estimated_min_gas_limit": _int(estimated_min_gas_limit),
            "schedule_total_gas_spent": _int(h.get("schedule_total_gas_spent")),
            "schedule_state_gas_spent": _int(h.get("schedule_state_gas_spent")),
            "schedule_state_gas_demanded": _int(h.get("schedule_state_gas_demanded")),
            "schedule_initial_state_gas": _int(h.get("schedule_initial_state_gas")),
            "runtime_state_gas": _int(h.get("runtime_state_gas")),
            "schedule_initial_reservoir": _int(h.get("schedule_initial_reservoir")),
            "runtime_state_gas_spillover": _int(h.get("runtime_state_gas_spillover")),
            "schedule_floor_gas": _int(h.get("schedule_floor_gas")),
            "schedule_gas_refunded": _int(h.get("schedule_gas_refunded")),
            "baseline_total_gas_spent": _int(h.get("baseline_total_gas_spent")),
            "baseline_gas_refunded": _int(h.get("baseline_gas_refunded")),
            "state_gas_category": h.get("state_gas_category"),
            "reservoir_exhausted": (None if h.get("reservoir_exhausted") is None
                                    else bool(h.get("reservoir_exhausted"))),
        },
        "call_stack": call_stack,
        "gas_breakdown": gas_breakdown,
        "opcode_breakdown": opcode_breakdown,
    }


@router.get("/search")
def search(
    q: str = Query(default=""),
    schedule_7904: str = Query(default=None),
    schedule_8037: str = Query(default=None),
):
    """Search affected contracts (EIP-7904 ∪ EIP-8037) by address prefix or name."""
    if not q or len(q) < 2:
        return []
    term = q.lower()
    s_7904 = resolve_schedule(schedule_7904)
    s_8037 = resolve_schedule(schedule_8037)
    rows = query(_affected_base_cte(s_7904, s_8037) + """
        SELECT addr, broken_txs_7904, broken_txs_8037
        FROM affected_combined
        ORDER BY broken_txs_7904 DESC, broken_txs_8037 DESC
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
                "broken_txs_8037": int(r["broken_txs_8037"]),
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
def debug_divergence_sample(schedule: str = Query(default=None)):
    """Diagnostic: confirm the producer is emitting divergence/OOG opcode
    integers and bottleneck classifications on the drill-in cohort.

    Scopes to one schedule (defaults to most-recent) so multi-schedule
    runs report sensible counts; pass `?schedule=` to inspect a specific
    one.
    """
    s = resolve_schedule(schedule)
    counts = query(f"""
        SELECT
            count(*) AS rows_total,
            count(*) FILTER (WHERE divergence_opcode IS NOT NULL) AS with_opcode_int,
            count(*) FILTER (WHERE oog_bottleneck_kind IS NOT NULL) AS with_bottleneck_kind,
            count(*) FILTER (WHERE divergence_call_depth IS NOT NULL) AS with_call_depth,
            count(*) FILTER (WHERE oog_chain_proportional IS NOT NULL) AS with_chain_classified
        FROM divergences
        WHERE schedule_name = '{s}'
    """)[0]
    top_opcodes = query(f"""
        SELECT divergence_opcode AS op_num, count(*) AS cnt
        FROM divergences
        WHERE divergence_opcode IS NOT NULL
          AND schedule_name = '{s}'
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
@cache_endpoint(_AGGREGATE_TTL)
def metadata(
    schedule: str = Query(default=None),
    schedule_7904: str = Query(default=None),
    schedule_8037: str = Query(default=None),
):
    """Metadata for the active page.

    `schedule` scopes the block-range numbers to one schedule's coverage
    (single-EIP pages pass this). For pages that aggregate across both
    EIPs (landing, /affected), pass `schedule_7904` and `schedule_8037`
    so `total_contracts_affected` counts the union of the two cohorts;
    when only `schedule` is given we skip the cross-EIP count rather
    than fail (single-EIP pages don't surface it).
    """
    s = resolve_schedule(schedule)
    block_range = query_sqlite(
        "SELECT min(block_number) as mn, max(block_number) as mx "
        "FROM block_coverage WHERE schedule_name = ?",
        (s,),
    )
    br = block_range[0] if block_range else {"mn": 0, "mx": 0}
    affected_count: int = 0
    if schedule_7904 and schedule_8037:
        s_7904 = resolve_schedule(schedule_7904)
        s_8037 = resolve_schedule(schedule_8037)
        affected_count = _int(query_sqlite_scalar("""
            SELECT count(DISTINCT lower(recipient))
            FROM divergences
            WHERE bucket = 'contract_broken'
              AND schedule_name IN (?, ?)
              AND recipient IS NOT NULL
        """, (s_7904, s_8037), default=0))
    return {
        "schedule_name": s,
        "min_block": int(br["mn"]) if br["mn"] else 0,
        "max_block": int(br["mx"]) if br["mx"] else 0,
        "last_updated": db_mtime().isoformat(),
        "total_contracts_affected": affected_count,
    }
