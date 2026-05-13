#!/usr/bin/env python3
"""Build the dashboard's `cache/contract_labels.csv` from the producer DB
plus Blockscout / Etherscan lookups.

The dashboard's `label_address()` reads two sources:

  1. The hardcoded `ADDRESS_PROJECT_LABELS` in `labels.py` (~30 well-known
     contracts).
  2. `cache/contract_labels.csv` if it exists.

Without (2), the /affected page falls back to raw addresses for every
contract outside the hardcoded set — which is what's currently happening.
This script regenerates that CSV against the new SQLite producer schema.

Replaces the deleted `build_contract_labels.py` that queried the old
parquet-lake `hot_7904` table. New version:

- Reads the producer SQLite directly via `sqlite3` (one open + close,
  no DuckDB hop required).
- Pulls candidates from `divergences WHERE bucket = 'contract_broken'`.
- Same Blockscout + Etherscan free-API enrichment, with persistent
  JSON caches under `cache/`. Re-runs only fetch addresses that don't
  yet have a cached answer.
- Sourcify-cache scanning is dropped (the `enrich_contracts.py` that
  populated it was deleted in the same cleanup).

Run with:

    PRODUCER_DB_PATH=/path/to/divergences.db \\
    ETHERSCAN_API_KEY=... \\
    python scripts/build_contract_labels.py

`ETHERSCAN_API_KEY` is optional; without it the script just uses the
Blockscout free tier. `PRODUCER_DB_PATH` follows the same env var the
web app reads.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    sys.stderr.write(
        "requests is required. Install with: pip install requests\n"
    )
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(_p):
        return None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from repricing_forensics.config import default_paths  # noqa: E402
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS  # noqa: E402

_paths = default_paths()
CACHE_DIR = _paths.cache_dir
LABELS_PATH = CACHE_DIR / "contract_labels.csv"
BLOCKSCOUT_CACHE_PATH = CACHE_DIR / "blockscout_names.json"
ETHERSCAN_CACHE_PATH = CACHE_DIR / "etherscan_names.json"

# Manual labels that aren't in the dashboard's hardcoded map but ought
# to be. Stays here rather than in labels.py so the dashboard's import
# surface stays narrow.
MANUAL_LABELS_EXTRA: dict[str, str] = {
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "Uniswap UNI",
    "0x0000000071727de22e5e9d8baf0edac6f37da032": "ERC-4337 EntryPoint v0.7",
    "0x4337084d9e255ff0702461cf8895ce9e3b5ff108": "ERC-4337 EntryPoint v0.6",
    "0x000000000022d473030f116ddee9f6b43ac78ba3": "Uniswap Permit2",
    "0x43506849d7c04f9138d1a2050bbf3a0c054402dd": "Circle USDC (impl)",
    "0x11b815efb8f581194ae79006d24e0d814b7697f6": "Uniswap V3 Pool",
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3 Pool",
}

# Names that don't help the user — the upstream API returned something
# like "TransparentUpgradeableProxy" which is a deploy template, not a
# project. We discard these so they don't shadow a useful name from a
# later lookup.
GENERIC_NAMES: frozenset[str] = frozenset(
    {
        "TransparentUpgradeableProxy",
        "AdminUpgradeabilityProxy",
        "ERC1967Proxy",
        "Proxy",
        "Implementation",
        "MyContract",
        "Token",
        "Contract",
        "SafeProxy",
        "SafeProxyFactory",
        "GnosisSafeProxy",
        "GnosisSafe",
    }
)


def resolve_producer_db_path() -> Path:
    """Mirror the same env-var resolution the web app uses."""
    explicit = os.environ.get("PRODUCER_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    # Fallback to the synthetic fixture used in `web/db.py`.
    return (PROJECT_ROOT / "synthetic.sqlite").resolve()


def load_candidate_contracts(
    db_path: Path, limit: int
) -> list[tuple[str, int]]:
    """Top `limit` contracts ranked by contract-broken tx count.

    Skips contracts already covered by the manual maps so we don't burn
    Blockscout / Etherscan quota relabeling things we already know.
    """
    known = set(
        addr.lower()
        for addr in (*ADDRESS_PROJECT_LABELS.keys(), *MANUAL_LABELS_EXTRA.keys())
    )
    if not db_path.exists():
        sys.stderr.write(f"Producer DB not found: {db_path}\n")
        sys.exit(2)

    placeholders = ",".join("?" for _ in known) if known else "''"
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            f"""
            SELECT lower(recipient) AS addr, count(*) AS broken_txs
            FROM divergences
            WHERE bucket = 'contract_broken'
              AND recipient IS NOT NULL
              AND lower(recipient) NOT IN ({placeholders})
            GROUP BY lower(recipient)
            ORDER BY broken_txs DESC
            LIMIT ?
            """,
            (*known, limit),
        ).fetchall()
    finally:
        conn.close()
    return [(addr, int(count)) for addr, count in rows]


def load_json_cache(path: Path) -> dict[str, str | None]:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            sys.stderr.write(f"warning: {path} unreadable; ignoring cache\n")
    return {}


def save_json_cache(path: Path, data: dict[str, str | None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def fetch_blockscout_name(address: str) -> str | None:
    """Free Blockscout API — works without a key but is rate-limited
    (~5 rps in practice). Caller sleeps between requests."""
    try:
        resp = requests.get(
            f"https://eth.blockscout.com/api/v2/addresses/{address}",
            timeout=10,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    name = data.get("name")
    # If it's a generic proxy name, fall through to the implementation.
    if not name or name in GENERIC_NAMES:
        for impl in data.get("implementations") or []:
            impl_name = impl.get("name")
            if impl_name and impl_name not in GENERIC_NAMES:
                return impl_name
    if name and name not in GENERIC_NAMES:
        return name
    return None


def fetch_etherscan_name(address: str, api_key: str) -> str | None:
    """Etherscan V2 multichain API. Same `getsourcecode` action the old
    script used; the rate limit on the free tier is 5 rps."""
    try:
        resp = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": "1",
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": api_key,
            },
            timeout=10,
        )
    except requests.RequestException:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    if data.get("status") != "1" or not isinstance(data.get("result"), list):
        return None
    if not data["result"]:
        return None
    name = data["result"][0].get("ContractName") or ""
    if name and name not in GENERIC_NAMES:
        return name
    return None


def enrich(
    candidates: list[tuple[str, int]],
    fetch_fn,
    cache_path: Path,
    sleep_seconds: float,
    label_source: str,
    labels: dict[str, dict[str, str]],
) -> int:
    """Run one enrichment phase against `candidates`, writing into
    `labels` and persisting the cache after every 50 lookups.

    `fetch_fn(address) -> Optional[name]` performs the network call.
    Cache hits (including cached-None) skip the network entirely.
    """
    cache = load_json_cache(cache_path)
    found = 0
    for i, (addr, _broken_txs) in enumerate(candidates, start=1):
        if addr in labels:
            continue
        if addr in cache:
            name = cache[addr]
        else:
            name = fetch_fn(addr)
            cache[addr] = name  # cache None too so we don't re-fetch
            time.sleep(sleep_seconds)
        if name:
            labels[addr] = {"name": name, "source": label_source}
            found += 1
        if i % 50 == 0:
            save_json_cache(cache_path, cache)
            print(f"    ...{i}/{len(candidates)} via {label_source} ({found} found)")
    save_json_cache(cache_path, cache)
    return found


def main() -> None:
    candidate_limit = int(os.environ.get("LABEL_CANDIDATE_LIMIT", "600"))
    db_path = resolve_producer_db_path()
    print(f"Building contract labels from {db_path}")

    candidates = load_candidate_contracts(db_path, candidate_limit)
    print(f"  {len(candidates)} top contract-broken recipients to label")

    labels: dict[str, dict[str, str]] = {}

    # Phase 1: manual labels (shadow everything else).
    for addr, name in ADDRESS_PROJECT_LABELS.items():
        labels[addr.lower()] = {"name": name, "source": "manual_hardcoded"}
    for addr, name in MANUAL_LABELS_EXTRA.items():
        labels[addr.lower()] = {"name": name, "source": "manual_extra"}
    print(f"  Phase 1: {len(labels)} manual labels")

    # Phase 2: Blockscout free API.
    blockscout_top = candidates[:300]
    if blockscout_top:
        print(f"  Phase 2: Blockscout for top {len(blockscout_top)} unlabeled")
        n = enrich(
            blockscout_top,
            fetch_blockscout_name,
            BLOCKSCOUT_CACHE_PATH,
            sleep_seconds=0.3,
            label_source="blockscout",
            labels=labels,
        )
        print(f"  Phase 2: +{n} labels")

    # Phase 3: Etherscan (optional, only if an API key is configured).
    etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if etherscan_key and etherscan_key != "your-key-here":
        # Re-rank: take the top unlabeled candidates AFTER Blockscout, so
        # we don't blow the Etherscan quota on contracts Blockscout
        # already named.
        remaining = [c for c in candidates if c[0] not in labels][:300]
        if remaining:
            print(f"  Phase 3: Etherscan for top {len(remaining)} remaining")
            n = enrich(
                remaining,
                lambda addr: fetch_etherscan_name(addr, etherscan_key),
                ETHERSCAN_CACHE_PATH,
                sleep_seconds=0.25,
                label_source="etherscan",
                labels=labels,
            )
            print(f"  Phase 3: +{n} labels")
    else:
        print("  Phase 3: skipped (no ETHERSCAN_API_KEY)")

    # Write the CSV. The dashboard reads only the `address` and `name`
    # columns; the `source` column is kept for forensics.
    LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LABELS_PATH.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["address", "name", "source"])
        writer.writeheader()
        for addr in sorted(labels):
            writer.writerow(
                {
                    "address": addr,
                    "name": labels[addr]["name"],
                    "source": labels[addr]["source"],
                }
            )

    # Coverage report. Broken-tx coverage tells us how much of the
    # /affected page will pick up named entries vs raw hex.
    total_broken = sum(count for _addr, count in candidates) + sum(
        # rows whose addresses landed in `labels` via the manual phases
        # are also "labeled" but excluded from `candidates` by the
        # load_candidate_contracts filter — fine for a rough number.
        0
        for _ in ()
    )
    labeled_count = sum(1 for addr, _ in candidates if addr in labels)
    print()
    print("=" * 60)
    print(f"Total labels written: {len(labels)}")
    print(
        f"Coverage of candidate set: {labeled_count}/{len(candidates)} "
        f"({labeled_count / max(1, len(candidates)) * 100:.1f}%)"
    )
    print(f"Output: {LABELS_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
