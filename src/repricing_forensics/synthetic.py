"""Synthetic producer-DB generator for consumer-side development.

While reth lands the new producer (see `crates/research/docs/storage-redesign.md`),
the consumer rewrite in this repo needs *something* to attach to.
`build_synthetic_db(path)` emits a small SQLite file with rows covering
every bucket and every dashboard surface, so the consumer code can be
developed and parity-tested without blocking on reth.

This module is deliberately deterministic (fixed scenarios) rather than
randomly generated — the dashboards exercise specific code paths (e.g.
Stipend2300 vs FractionalGas chips, reservoir-exhausted rows) that we
want to hit reliably.

Throwaway when the real producer ships.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .producer_schema import (
    Bucket,
    SCHEMA_VERSION,
    initialize_schema,
    insert_analysis_run,
)


SCHEDULE_NAME = "eip-8037"
SCHEDULE_CONFIG_HASH = "synthetic-v1"


# A few well-known mainnet contracts so the dashboard renders meaningful
# labels via the existing address-labels CSV. If the CSV doesn't have
# them, the address still renders fine.
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
UNI_V2_ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
FIAT_TOKEN_IMPL = "0x43506849d7c04f9138d1a2050bbf3a0c054402dd"
FRESH_CONTRACT = "0x1111111111111111111111111111111111111111"
EOA_SENDER = "0x2222222222222222222222222222222222222222"

ALL_CONTRACTS: tuple[tuple[str, str], ...] = (
    (USDC, "0xUSDC bytecode"),
    (WETH, "0xWETH bytecode"),
    (UNI_V2_ROUTER, "0xUniRouter bytecode"),
    (FIAT_TOKEN_IMPL, "0xFiatTokenImpl bytecode"),
    (FRESH_CONTRACT, "0xFresh bytecode"),
)


# Opcode bytes we care about for synthetic content.
_OP_STOP = 0x00
_OP_KECCAK256 = 0x20
_OP_SLOAD = 0x54
_OP_SSTORE = 0x55
_OP_CALL = 0xf1
_OP_DELEGATECALL = 0xf4
_OP_STATICCALL = 0xfa
_OP_INVALID = 0xfe


@dataclass
class _Tx:
    bucket: str
    recipient: str
    gas_delta: int = 0
    baseline_success: bool = True
    schedule_success: bool = True
    status_changed: bool = False
    event_logs_changed: bool = False
    baseline_gas_used: int = 50_000
    schedule_gas_used: int = 50_000
    tx_gas_limit: int = 100_000
    is_create: bool = False

    # OOG
    divergence_contract: str | None = None
    divergence_call_depth: int | None = None
    divergence_opcode: int | None = None
    oog_contract: str | None = None
    oog_call_depth: int | None = None
    oog_opcode: int | None = None
    oog_pattern: str | None = None
    oog_gas_remaining: int | None = None
    oog_chain_proportional: bool | None = None
    oog_bottleneck_depth: int | None = None
    oog_bottleneck_kind: str | None = None

    # 8037
    schedule_state_gas_spent: int = 0
    runtime_state_gas: int = 0
    runtime_state_gas_spillover: int = 0
    schedule_initial_reservoir: int = 0
    state_gas_category: str | None = None
    reservoir_exhausted: bool = False
    min_multiplier_to_succeed: float | None = None
    would_fit_in_original_limit: bool = True

    frames: list[dict] = field(default_factory=list)
    opcode_counts: list[dict] = field(default_factory=list)
    event_logs: list[dict] = field(default_factory=list)


# ── Scenario factories ──────────────────────────────────────────────────

def _scenario_unchanged() -> _Tx:
    return _Tx(bucket=Bucket.UNCHANGED.value, recipient=USDC)


def _scenario_trace_only() -> _Tx:
    return _Tx(
        bucket=Bucket.TRACE_ONLY.value, recipient=USDC,
        gas_delta=0, baseline_gas_used=51_000, schedule_gas_used=51_000,
    )


def _scenario_gas_only(extra: int) -> _Tx:
    return _Tx(
        bucket=Bucket.GAS_ONLY.value, recipient=USDC,
        gas_delta=extra,
        baseline_gas_used=51_000, schedule_gas_used=51_000 + extra,
    )


def _scenario_event_logs_changed() -> _Tx:
    """Status-stable but emits different event data — e.g. a settlement
    contract where the gas accounting flipped a fee branch."""
    tx = _Tx(
        bucket=Bucket.EVENT_LOGS_CHANGED.value, recipient=USDC,
        gas_delta=2_500,
        baseline_gas_used=80_000, schedule_gas_used=82_500,
        event_logs_changed=True,
    )
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, USDC, "CALL", "0xa9059cbb",
               gas_provided=80_000, gas_used=80_000, success=True),
    ]
    tx.opcode_counts = [
        _op_row(0, _OP_SLOAD, 4, gas_baseline=4 * 800, gas_schedule=4 * 800),
        _op_row(0, _OP_SSTORE, 2, gas_baseline=2 * 5_000, gas_schedule=2 * 5_000),
        _op_row(0, _OP_KECCAK256, 6, gas_baseline=6 * 30, gas_schedule=6 * 45),
    ]
    tx.event_logs = [
        _log("baseline", 0, USDC, topic0=_b("transfer-evt")),
        _log("schedule", 0, USDC, topic0=_b("transfer-evt"), data_hash=_b("alt")),
    ]
    return tx


def _scenario_wallet_fixable_shallow() -> _Tx:
    return _Tx(
        bucket=Bucket.WALLET_FIXABLE_SHALLOW.value, recipient=USDC,
        gas_delta=12_000,
        baseline_gas_used=88_000, schedule_gas_used=100_000,
        tx_gas_limit=95_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        divergence_call_depth=0, divergence_opcode=_OP_KECCAK256,
        oog_contract=USDC, oog_call_depth=0,
        oog_pattern="oog", oog_gas_remaining=0,
    )


def _scenario_wallet_fixable_deep_chain() -> _Tx:
    return _Tx(
        bucket=Bucket.WALLET_FIXABLE_DEEP_CHAIN.value, recipient=UNI_V2_ROUTER,
        gas_delta=15_000,
        baseline_gas_used=185_000, schedule_gas_used=200_000,
        tx_gas_limit=195_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        divergence_call_depth=2, divergence_opcode=_OP_KECCAK256,
        oog_contract=WETH, oog_call_depth=2,
        oog_pattern="oog", oog_gas_remaining=0,
        oog_chain_proportional=True,
    )


def _scenario_contract_broken_stipend2300() -> _Tx:
    tx = _Tx(
        bucket=Bucket.CONTRACT_BROKEN.value, recipient=FRESH_CONTRACT,
        gas_delta=18_000,
        baseline_gas_used=72_000, schedule_gas_used=90_000,
        tx_gas_limit=85_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        divergence_call_depth=1, divergence_opcode=_OP_SSTORE,
        oog_contract=WETH, oog_call_depth=1, oog_opcode=_OP_SSTORE,
        oog_pattern="stipend", oog_gas_remaining=0,
        oog_chain_proportional=False, oog_bottleneck_depth=1,
        oog_bottleneck_kind="Stipend2300",
    )
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, FRESH_CONTRACT, "CALL", "0x12345678",
               gas_provided=72_000, gas_used=72_000, success=False),
        _frame(1, 0, 1, FRESH_CONTRACT, WETH, "CALL", "0xa9059cbb",
               gas_provided=2_300, gas_used=2_300, success=False,
               gas_requested_on_stack=2_300, parent_gas_at_call=50_000,
               eip150_cap_binding=False),
    ]
    tx.opcode_counts = [
        _op_row(0, _OP_KECCAK256, 12, gas_baseline=12 * 30, gas_schedule=12 * 45),
        _op_row(0, _OP_SLOAD, 3, gas_baseline=2_400, gas_schedule=2_400),
        _op_row(0, _OP_CALL, 1, gas_baseline=700, gas_schedule=700),
        _op_row(1, _OP_SSTORE, 1, gas_baseline=5_000, gas_schedule=7_500),
        _op_row(1, _OP_INVALID, 1, gas_baseline=0, gas_schedule=0),
    ]
    return tx


def _scenario_contract_broken_fixed_gas() -> _Tx:
    tx = _Tx(
        bucket=Bucket.CONTRACT_BROKEN.value, recipient=FRESH_CONTRACT,
        gas_delta=22_000,
        baseline_gas_used=140_000, schedule_gas_used=162_000,
        tx_gas_limit=155_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        divergence_call_depth=1, divergence_opcode=_OP_KECCAK256,
        oog_contract=USDC, oog_call_depth=1, oog_opcode=_OP_KECCAK256,
        oog_pattern="oog", oog_gas_remaining=0,
        oog_chain_proportional=False, oog_bottleneck_depth=1,
        oog_bottleneck_kind="FixedGas",
    )
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, FRESH_CONTRACT, "CALL", "0xdeadbeef",
               gas_provided=140_000, gas_used=140_000, success=False),
        _frame(1, 0, 1, FRESH_CONTRACT, USDC, "CALL", "0x23b872dd",
               gas_provided=50_000, gas_used=50_000, success=False,
               gas_requested_on_stack=50_000, parent_gas_at_call=120_000,
               eip150_cap_binding=False),
    ]
    tx.opcode_counts = [
        _op_row(0, _OP_KECCAK256, 8, gas_baseline=240, gas_schedule=360),
        _op_row(0, _OP_SLOAD, 5, gas_baseline=4_000, gas_schedule=4_000),
        _op_row(1, _OP_KECCAK256, 45, gas_baseline=1_350, gas_schedule=2_025),
        _op_row(1, _OP_SLOAD, 6, gas_baseline=4_800, gas_schedule=4_800),
    ]
    return tx


def _scenario_contract_broken_fractional() -> _Tx:
    tx = _Tx(
        bucket=Bucket.CONTRACT_BROKEN.value, recipient=UNI_V2_ROUTER,
        gas_delta=24_000,
        baseline_gas_used=176_000, schedule_gas_used=200_000,
        tx_gas_limit=195_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        divergence_call_depth=2, divergence_opcode=_OP_KECCAK256,
        oog_contract=WETH, oog_call_depth=2, oog_opcode=_OP_KECCAK256,
        oog_pattern="oog", oog_gas_remaining=0,
        oog_chain_proportional=False, oog_bottleneck_depth=1,
        oog_bottleneck_kind="FractionalGas",
    )
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, UNI_V2_ROUTER, "CALL", "0x18cbafe5",
               gas_provided=176_000, gas_used=176_000, success=False),
        _frame(1, 0, 1, UNI_V2_ROUTER, WETH, "CALL", "0x2e1a7d4d",
               gas_provided=80_000, gas_used=80_000, success=False,
               gas_requested_on_stack=80_000, parent_gas_at_call=160_000,
               eip150_cap_binding=False),
        _frame(2, 1, 2, WETH, FRESH_CONTRACT, "CALL", "",
               gas_provided=70_000, gas_used=70_000, success=False,
               gas_requested_on_stack=70_000, parent_gas_at_call=75_000,
               eip150_cap_binding=True),
    ]
    tx.opcode_counts = [
        _op_row(0, _OP_KECCAK256, 20, gas_baseline=600, gas_schedule=900),
        _op_row(1, _OP_SLOAD, 4, gas_baseline=3_200, gas_schedule=3_200),
        _op_row(2, _OP_KECCAK256, 30, gas_baseline=900, gas_schedule=1_350),
        _op_row(2, _OP_SSTORE, 1, gas_baseline=5_000, gas_schedule=7_500),
    ]
    return tx


def _scenario_8037_reservoir_exhausted() -> _Tx:
    tx = _Tx(
        bucket=Bucket.CONTRACT_BROKEN.value, recipient=FRESH_CONTRACT,
        gas_delta=8_500,
        baseline_gas_used=210_000, schedule_gas_used=218_500,
        tx_gas_limit=300_000, is_create=True,
        schedule_state_gas_spent=140_000, runtime_state_gas=140_000,
        runtime_state_gas_spillover=40_000,
        schedule_initial_reservoir=100_000,
        state_gas_category="contract_creation",
        reservoir_exhausted=True,
        min_multiplier_to_succeed=1.05,
    )
    # Top-level CREATE frame at depth 0 — exercises the A4 deployment
    # ceiling chart: a 4_000-byte deployed contract that succeeded.
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, FRESH_CONTRACT, "CREATE", "",
               gas_provided=218_500, gas_used=218_500, success=True,
               deployed_bytecode_len=4_000),
    ]
    return tx


def _scenario_8037_needs_higher_multiplier() -> _Tx:
    tx = _Tx(
        bucket=Bucket.CONTRACT_BROKEN.value, recipient=USDC,
        gas_delta=120_000,
        baseline_gas_used=85_000, schedule_gas_used=205_000,
        tx_gas_limit=100_000,
        baseline_success=True, schedule_success=False, status_changed=True,
        schedule_state_gas_spent=120_000, runtime_state_gas=120_000,
        runtime_state_gas_spillover=20_000,
        schedule_initial_reservoir=100_000,
        state_gas_category="runtime_state_creation",
        reservoir_exhausted=True,
        min_multiplier_to_succeed=2.05,
        would_fit_in_original_limit=False,
        divergence_call_depth=0, divergence_opcode=_OP_SSTORE,
        oog_contract=USDC, oog_call_depth=0, oog_opcode=_OP_SSTORE,
        oog_pattern="oog", oog_gas_remaining=0,
        oog_chain_proportional=False, oog_bottleneck_depth=0,
        oog_bottleneck_kind="FractionalGas",
    )
    tx.frames = [
        _frame(0, None, 0, EOA_SENDER, USDC, "CALL", "0xa9059cbb",
               gas_provided=85_000, gas_used=85_000, success=False,
               state_gas_running=120_000),
    ]
    tx.opcode_counts = [
        _op_row(0, _OP_KECCAK256, 18, gas_baseline=540, gas_schedule=810),
        _op_row(0, _OP_SSTORE, 8, gas_baseline=40_000, gas_schedule=60_000),
        _op_row(0, _OP_SLOAD, 12, gas_baseline=9_600, gas_schedule=9_600),
    ]
    return tx


_SCENARIO_FACTORIES = [
    _scenario_unchanged,
    _scenario_unchanged,
    _scenario_unchanged,
    _scenario_trace_only,
    lambda: _scenario_gas_only(1_200),
    lambda: _scenario_gas_only(3_800),
    _scenario_event_logs_changed,
    _scenario_wallet_fixable_shallow,
    _scenario_wallet_fixable_deep_chain,
    _scenario_contract_broken_stipend2300,
    _scenario_contract_broken_fixed_gas,
    _scenario_contract_broken_fractional,
    _scenario_8037_reservoir_exhausted,
    _scenario_8037_needs_higher_multiplier,
]


# ── Helpers ─────────────────────────────────────────────────────────────

def _b(s: str) -> bytes:
    return s.encode("utf-8")


def _frame(
    call_index: int, parent: int | None, depth: int,
    from_addr: str, to_addr: str, call_type: str, selector_hex: str,
    *, gas_provided: int, gas_used: int, success: bool,
    parent_gas_at_call: int | None = None,
    gas_requested_on_stack: int | None = None,
    eip150_cap_binding: bool | None = None,
    state_gas_running: int | None = None,
    deployed_bytecode_len: int | None = None,
) -> dict:
    sel = bytes.fromhex(selector_hex.removeprefix("0x")) if selector_hex else None
    return dict(
        call_index=call_index, parent_call_index=parent, depth=depth,
        from_address=from_addr, to_address=to_addr,
        code_address=to_addr,
        codehash=_b(f"code-{to_addr}"),
        call_type=call_type, selector=sel,
        value_wei="0",
        gas_provided=gas_provided, gas_used=gas_used,
        gas_margin=gas_provided - gas_used,
        success=success,
        parent_gas_at_call=parent_gas_at_call,
        gas_requested_on_stack=gas_requested_on_stack,
        eip150_cap_binding=eip150_cap_binding,
        state_gas_running=state_gas_running,
        deployed_bytecode_len=deployed_bytecode_len,
    )


def _op_row(call_index: int, opcode: int, count: int,
            *, gas_baseline: int, gas_schedule: int) -> dict:
    return dict(call_index=call_index, opcode=opcode, count=count,
                gas_baseline=gas_baseline, gas_schedule=gas_schedule)


def _log(trace_kind, log_index, address, *, topic0=None, data_hash=None):
    return dict(trace_kind=trace_kind, log_index=log_index, address=address,
                topic0=topic0, topic1=None, topic2=None, topic3=None,
                data_bytes=None, data_hash=data_hash)


# ── Aggregation for block_summaries ─────────────────────────────────────

def _log2_bin(v: int) -> int:
    if v <= 1:
        return 0
    bin_ = 1
    while v > 1 and bin_ < 11:
        v //= 2
        bin_ += 1
    return min(bin_, 11)


def _multiplier_bin(m: float | None) -> int:
    if m is None or m <= 1.0:
        return 0
    cutoffs = [1.25, 1.50, 2.00, 4.00, 8.00]
    for i, c in enumerate(cutoffs, start=1):
        if m <= c:
            return i
    return len(cutoffs) + 1


@dataclass
class _BucketAcc:
    bucket: str
    tx_count: int = 0
    gas_delta_sum: int = 0
    gas_delta_sum_sq: int = 0
    gas_delta_min: int | None = None
    gas_delta_max: int | None = None
    gas_delta_hist: list[int] = field(default_factory=lambda: [0] * 12)
    # Per-opcode aggregates that get serialized into the
    # block_summaries.opcode_totals_7904 JSON column. Key is opcode byte,
    # value is (count, gas_baseline, gas_schedule).
    opcode_totals: dict[int, tuple[int, int, int]] = field(default_factory=dict)
    state_gas_sum: int = 0
    state_gas_spillover_sum: int = 0
    multiplier_hist: list[int] = field(default_factory=lambda: [0] * 12)
    tx_count_creation: int = 0
    tx_count_authorization: int = 0
    tx_count_runtime_state: int = 0
    tx_count_no_state: int = 0


# ── Build ───────────────────────────────────────────────────────────────

def build_synthetic_db(out_path: Path, blocks: int = 5) -> None:
    """Create a SQLite file with `blocks` blocks of synthetic data."""
    out_path = Path(out_path)
    if out_path.exists():
        out_path.unlink()
    # SQLite's WAL also creates -wal and -shm sidecar files; remove
    # those from any prior run too.
    for suffix in ("-wal", "-shm"):
        sidecar = out_path.with_name(out_path.name + suffix)
        if sidecar.exists():
            sidecar.unlink()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(out_path))
    initialize_schema(conn)

    now = 1_700_000_000
    base_block = 22_000_000
    insert_analysis_run(
        conn,
        schedule_name=SCHEDULE_NAME,
        schedule_config_hash=SCHEDULE_CONFIG_HASH,
        reth_commit="synthetic",
        run_started_at=now,
        run_finished_at=now,
        blocks_processed=blocks,
        notes=f"synthetic fixture, schema v{SCHEMA_VERSION}",
    )

    for addr, bytecode_name in ALL_CONTRACTS:
        codehash = _b(f"code-{addr}")
        conn.execute(
            "INSERT INTO contract_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (codehash, addr, "0.8.21", "synthetic", "shanghai",
             1, 1, len(bytecode_name), now),
        )

    for b_idx in range(blocks):
        block_number = base_block + b_idx
        block_hash = _b(f"block-{block_number}")
        parent_hash = _b(f"block-{block_number - 1}")
        timestamp = now + b_idx * 12

        bucket_counts = {b.value: 0 for b in Bucket}
        accs: dict[str, _BucketAcc] = {}

        for tx_idx, factory in enumerate(_SCENARIO_FACTORIES):
            tx = factory()
            bucket_counts[tx.bucket] += 1

            acc = accs.setdefault(tx.bucket, _BucketAcc(tx.bucket))
            acc.tx_count += 1
            if tx.bucket != Bucket.UNCHANGED.value:
                acc.gas_delta_sum += tx.gas_delta
                acc.gas_delta_sum_sq += tx.gas_delta * tx.gas_delta
                acc.gas_delta_min = (tx.gas_delta if acc.gas_delta_min is None
                                     else min(acc.gas_delta_min, tx.gas_delta))
                acc.gas_delta_max = (tx.gas_delta if acc.gas_delta_max is None
                                     else max(acc.gas_delta_max, tx.gas_delta))
                acc.gas_delta_hist[_log2_bin(max(tx.gas_delta, 0))] += 1
                for op_row in tx.opcode_counts:
                    op = op_row["opcode"]
                    prev_count, prev_base, prev_sched = acc.opcode_totals.get(
                        op, (0, 0, 0),
                    )
                    acc.opcode_totals[op] = (
                        prev_count + op_row["count"],
                        prev_base + op_row["gas_baseline"],
                        prev_sched + op_row["gas_schedule"],
                    )
                acc.state_gas_sum += tx.schedule_state_gas_spent
                acc.state_gas_spillover_sum += tx.runtime_state_gas_spillover
                acc.multiplier_hist[_multiplier_bin(tx.min_multiplier_to_succeed)] += 1
                if tx.is_create:
                    acc.tx_count_creation += 1
                elif tx.schedule_state_gas_spent > 0:
                    acc.tx_count_runtime_state += 1
                else:
                    acc.tx_count_no_state += 1

            if tx.bucket in (Bucket.CONTRACT_BROKEN.value,
                             Bucket.EVENT_LOGS_CHANGED.value):
                _insert_drill_in(
                    conn, tx,
                    block_number=block_number, tx_index=tx_idx,
                    timestamp=timestamp,
                )

        conn.execute(
            "INSERT INTO block_coverage VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                SCHEDULE_NAME, SCHEDULE_CONFIG_HASH,
                block_number, block_hash, parent_hash, timestamp,
                len(_SCENARIO_FACTORIES),
                bucket_counts[Bucket.UNCHANGED.value],
                bucket_counts[Bucket.TRACE_ONLY.value],
                bucket_counts[Bucket.GAS_ONLY.value],
                bucket_counts[Bucket.EVENT_LOGS_CHANGED.value],
                bucket_counts[Bucket.SCHEDULE_RESCUED.value],
                bucket_counts[Bucket.WALLET_FIXABLE_SHALLOW.value],
                bucket_counts[Bucket.WALLET_FIXABLE_DEEP_CHAIN.value],
                bucket_counts[Bucket.INCONCLUSIVE_NEEDS_HIGHER_SWEEP.value],
                bucket_counts[Bucket.CONTRACT_BROKEN.value],
            ),
        )

        for bucket, acc in accs.items():
            if bucket == Bucket.UNCHANGED.value:
                continue
            # SQLite has no native array/struct; serialize as JSON. The
            # consumer reads via DuckDB's json_each() to unnest.
            opcode_totals_list = [
                {"opcode": op, "count": cnt,
                 "gas_baseline": base, "gas_schedule": sched}
                for op, (cnt, base, sched) in sorted(acc.opcode_totals.items())
            ]
            conn.execute(
                "INSERT INTO block_summaries VALUES ("
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    SCHEDULE_NAME, block_number, bucket, acc.tx_count,
                    acc.gas_delta_sum, float(acc.gas_delta_sum_sq),
                    acc.gas_delta_min, acc.gas_delta_max,
                    json.dumps(acc.gas_delta_hist),
                    json.dumps(opcode_totals_list),
                    acc.state_gas_sum, acc.state_gas_spillover_sum,
                    json.dumps(acc.multiplier_hist),
                    acc.tx_count_creation, acc.tx_count_authorization,
                    acc.tx_count_runtime_state, acc.tx_count_no_state,
                ),
            )

    conn.commit()
    conn.close()


def _insert_drill_in(conn, tx: _Tx, *, block_number: int, tx_index: int,
                     timestamp: int) -> None:
    """Insert a divergences row + per-frame + opcode-counts + event-logs."""
    cur = conn.execute(
        """
        INSERT INTO divergences (
            schedule_name, schedule_config_hash, block_number, tx_index,
            tx_hash, timestamp, bucket,
            sender, recipient, is_create, tx_gas_limit,
            baseline_success, schedule_success,
            status_changed, event_logs_changed, output_changed, logs_bloom_changed,
            baseline_gas_used, schedule_gas_used, gas_delta,
            would_fit_in_original_limit, min_multiplier_to_succeed,
            divergence_contract, divergence_call_depth, divergence_opcode,
            oog_contract, oog_call_depth, oog_opcode, oog_pattern, oog_gas_remaining,
            oog_chain_proportional, oog_bottleneck_depth, oog_bottleneck_kind,
            schedule_state_gas_spent, schedule_initial_reservoir,
            runtime_state_gas, runtime_state_gas_spillover,
            state_gas_category, reservoir_exhausted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            SCHEDULE_NAME, SCHEDULE_CONFIG_HASH, block_number, tx_index,
            _b(f"tx-{block_number}-{tx_index}"), timestamp, tx.bucket,
            EOA_SENDER, tx.recipient, int(tx.is_create), tx.tx_gas_limit,
            int(tx.baseline_success), int(tx.schedule_success),
            int(tx.status_changed), int(tx.event_logs_changed), 0, 0,
            tx.baseline_gas_used, tx.schedule_gas_used, tx.gas_delta,
            int(tx.would_fit_in_original_limit), tx.min_multiplier_to_succeed,
            tx.divergence_contract or tx.recipient,
            tx.divergence_call_depth, tx.divergence_opcode,
            tx.oog_contract, tx.oog_call_depth, tx.oog_opcode,
            tx.oog_pattern, tx.oog_gas_remaining,
            None if tx.oog_chain_proportional is None else int(tx.oog_chain_proportional),
            tx.oog_bottleneck_depth, tx.oog_bottleneck_kind,
            tx.schedule_state_gas_spent, tx.schedule_initial_reservoir,
            tx.runtime_state_gas, tx.runtime_state_gas_spillover,
            tx.state_gas_category, int(tx.reservoir_exhausted),
        ),
    )
    div_id = int(cur.lastrowid)

    for f in tx.frames:
        conn.execute(
            "INSERT INTO divergence_call_frames VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                div_id, f["call_index"], f["parent_call_index"], f["depth"],
                f["from_address"], f["to_address"], f["code_address"],
                f["codehash"], f["call_type"], f["selector"], f["value_wei"],
                f["gas_provided"], f["gas_used"], f["gas_margin"],
                int(f["success"]),
                f["parent_gas_at_call"], f["gas_requested_on_stack"],
                None if f["eip150_cap_binding"] is None else int(f["eip150_cap_binding"]),
                f["state_gas_running"],
                f.get("deployed_bytecode_len"),
            ),
        )

    for op in tx.opcode_counts:
        conn.execute(
            "INSERT INTO divergence_opcode_counts VALUES (?, ?, ?, ?, ?, ?)",
            (div_id, op["call_index"], op["opcode"], op["count"],
             op["gas_baseline"], op["gas_schedule"]),
        )

    for lg in tx.event_logs:
        conn.execute(
            "INSERT INTO divergence_event_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (div_id, lg["trace_kind"], lg["log_index"], lg["address"],
             lg["topic0"], lg["topic1"], lg["topic2"], lg["topic3"],
             lg["data_bytes"], lg["data_hash"]),
        )
