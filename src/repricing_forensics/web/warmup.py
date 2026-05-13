"""Background cache warmer for /api/* endpoints.

Each cached endpoint has a 30s TTL — but the *first* hit after the TTL
expires pays the full DuckDB-over-80GB-SQLite latency. With ~10
fetchJSON calls per page, a cold cache means tens of seconds of stalls
on the first viewer. This module keeps the cache warm by re-invoking
the endpoint functions on a fixed cadence, sequenced through a small
thread pool so we don't pummel the producer with concurrent scans.

Each warm call routes through the same `@cache_endpoint` wrapper as a
real request, so the resulting value lands in the same cache the API
reads from. Failures are swallowed and logged — a transient producer
outage leaves the previous (stale) value in place rather than evicting
it.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from . import routes_api
from .routes_pages import SCHEDULE_7904 as S7
from .routes_pages import SCHEDULE_8037 as S8

_log = logging.getLogger(__name__)

# Refresh cadence. Must be < `_AGGREGATE_TTL` (30s) so cached values
# don't expire between warm passes. 20s leaves headroom for a slow
# pass without letting the cache go cold.
_WARM_INTERVAL_SECONDS = 20.0

# Parallel warm workers for steady-state passes. Each worker uses its
# own DuckDB cursor so they run concurrently against the producer
# SQLite. Higher counts hit diminishing returns as sqlite_scanner
# bottlenecks on the underlying SQLite reader, and pile load onto a
# producer that may be mid-replay.
_WARM_CONCURRENCY = 4

# Delay before the warmer's first pass. Right after systemd restart,
# the OS page cache for the 80 GB producer file is cold and lifespan
# is paying the full ATTACH cost (observed: ~5 min before uvicorn
# binds). Firing 4 concurrent queries on top of that just deepens the
# hole. 60s lets the foreground request path establish a baseline
# before we start the background load.
_WARM_FIRST_PASS_DELAY = 60.0

# Concurrency for the first pass only. Single-threaded so we don't
# multiply the cold-cache cost; the SQL is still uncached at this
# point, and the producer may still be servicing the lifespan ATTACH.
_WARM_FIRST_PASS_CONCURRENCY = 1


# (callable, kwargs) — calling each tuple refreshes one cached
# endpoint's value. Endpoints whose cache key depends on parameters
# (e.g. `top_contracts(limit=...)`, the cross-EIP variant of
# `metadata`) appear once per parameterization the live pages use, so
# the same cache slot the page reads from is the one we warm.
_WARM_PLAN: list[tuple[Callable[..., Any], dict[str, Any]]] = [
    # Shared across landing / affected / both EIP pages.
    # NB: when FastAPI handles a request without `schedule_7904` /
    # `schedule_8037` query params, it passes them as None. The
    # `@cache_endpoint` key derives from kwargs, so we must pass them
    # explicitly as None to hit the same cache slot live requests use.
    (routes_api.overview,                    {"schedule": S7}),
    (routes_api.overview,                    {"schedule": S8}),
    (routes_api.metadata,                    {"schedule": S7,
                                              "schedule_7904": None,
                                              "schedule_8037": None}),
    (routes_api.metadata,                    {"schedule": S7,
                                              "schedule_7904": S7,
                                              "schedule_8037": S8}),

    # /eip7904
    (routes_api.funnel,                      {"schedule": S7}),
    (routes_api.opcode_gas_share,            {"schedule": S7}),
    (routes_api.gas_overhead,                {"schedule": S7}),
    (routes_api.concentration,               {"schedule": S7}),
    (routes_api.top_contracts,               {"limit": 10, "schedule": S7}),
    (routes_api.forensics_break_reason,      {"schedule": S7}),
    (routes_api.forensics_bottleneck_kinds,  {"schedule": S7}),

    # /eip7904/forensics
    (routes_api.opcode_impact,               {"schedule": S7}),
    (routes_api.forensics_call_depth,        {"schedule": S7}),
    (routes_api.top_contracts,               {"limit": 20, "schedule": S7}),
    (routes_api.forensics_failure_motifs,    {"schedule": S7}),

    # /eip8037
    (routes_api.eip8037_overview,            {"schedule": S8}),
    (routes_api.eip8037_multiplier_histogram, {"schedule": S8}),
    (routes_api.eip8037_state_gas_by_category, {"schedule": S8}),
    (routes_api.eip8037_top_contracts,       {"limit": 20, "schedule": S8}),
    (routes_api.eip8037_examples,            {"limit": 50, "schedule": S8}),
    (routes_api.eip8037_reservoir,           {"schedule": S8}),
    (routes_api.eip8037_divergence_reasons,  {"schedule": S8}),
    (routes_api.eip8037_unresolved_breakdown, {"schedule": S8}),

    # /affected — only the first page is shown by default.
    (routes_api.affected,                    {"page": 1, "per_page": 100,
                                              "schedule_7904": S7,
                                              "schedule_8037": S8}),
]


def _run_one(label: str, fn: Callable[..., Any], kwargs: dict[str, Any]) -> float:
    """Refresh one cached endpoint. Returns elapsed seconds; logs and
    swallows errors so one slow/broken endpoint doesn't break the rest
    of the warm pass."""
    t0 = time.monotonic()
    try:
        fn(**kwargs)
    except Exception as exc:
        _log.warning("warmup %s failed: %s", label, exc)
    return time.monotonic() - t0


def warm_cache_once(concurrency: int = _WARM_CONCURRENCY,
                    stop: threading.Event | None = None) -> dict[str, float]:
    """Run one warm pass with the given concurrency. Returns
    per-endpoint elapsed seconds (for logging / diagnostics).

    `stop` (optional) is checked between submissions so a shutdown
    signal during a long pass can short-circuit the rest of the plan
    — keeps shutdown latency bounded even when individual queries are
    slow against the cold producer."""
    timings: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=max(1, concurrency),
                            thread_name_prefix="warm") as pool:
        futures = {}
        for fn, kwargs in _WARM_PLAN:
            if stop is not None and stop.is_set():
                break
            label = f"{fn.__name__}({','.join(f'{k}={v}' for k, v in kwargs.items())})"
            futures[pool.submit(_run_one, label, fn, kwargs)] = label
        for fut, label in futures.items():
            timings[label] = fut.result()
    return timings


# Run the warmer in a dedicated thread (not via asyncio.to_thread)
# because shutdown needs to guarantee no in-flight cursors when
# `close_conn()` runs — `asyncio.to_thread` only cancels the awaiting
# coroutine, leaving the worker thread alive and racing with conn
# close. A thread + Event lets us join() cleanly before close.
_stop_event = threading.Event()
_warmer_thread: threading.Thread | None = None


def _warm_loop_target() -> None:
    # Hold off on the first pass — see _WARM_FIRST_PASS_DELAY. Use the
    # stop event so a fast shutdown doesn't have to wait through the
    # full delay.
    if _stop_event.wait(_WARM_FIRST_PASS_DELAY):
        _log.info("cache warmer stopped before first pass")
        return

    pass_count = 0
    while not _stop_event.is_set():
        # First pass runs sequentially to avoid stacking concurrent
        # SQL on top of an already-cold producer file. Subsequent
        # passes ramp up to the steady-state concurrency.
        concurrency = (_WARM_FIRST_PASS_CONCURRENCY if pass_count == 0
                       else _WARM_CONCURRENCY)
        t0 = time.monotonic()
        try:
            timings = warm_cache_once(concurrency=concurrency, stop=_stop_event)
            total = time.monotonic() - t0
            slowest = sorted(timings.items(), key=lambda kv: kv[1], reverse=True)[:3]
            _log.info(
                "cache warmer pass %d done in %.2fs (concurrency=%d); slowest: %s",
                pass_count, total, concurrency,
                ", ".join(f"{n}={t:.2f}s" for n, t in slowest),
            )
        except Exception as exc:
            _log.warning("cache warmer pass failed: %s", exc)
        pass_count += 1
        # `Event.wait` is interruptible — stop_warmer() can signal mid-sleep.
        _stop_event.wait(_WARM_INTERVAL_SECONDS)
    _log.info("cache warmer stopped")


def start_warmer() -> None:
    """Spawn the background warmer thread. Idempotent — calling twice
    leaves a single running thread."""
    global _warmer_thread
    if _warmer_thread is not None and _warmer_thread.is_alive():
        return
    _stop_event.clear()
    _log.info("cache warmer starting (interval=%.0fs, plan=%d endpoints)",
              _WARM_INTERVAL_SECONDS, len(_WARM_PLAN))
    _warmer_thread = threading.Thread(
        target=_warm_loop_target, name="cache-warmer", daemon=True,
    )
    _warmer_thread.start()


def stop_warmer(timeout: float = 30.0) -> None:
    """Signal the warmer to exit and wait for the current pass to
    drain. The wait is bounded so a stuck query can't deadlock
    shutdown — if the timeout fires, the thread is daemonized so the
    process can still exit, at the cost of the same shutdown race
    `close_conn()` is trying to avoid."""
    global _warmer_thread
    _stop_event.set()
    if _warmer_thread is not None:
        _warmer_thread.join(timeout=timeout)
        if _warmer_thread.is_alive():
            _log.warning("cache warmer did not stop within %.0fs", timeout)
        _warmer_thread = None
