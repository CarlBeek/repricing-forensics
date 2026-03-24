#!/usr/bin/env python3
"""Build a comprehensive contract label mapping from Sourcify metadata + Etherscan.

Reads Sourcify cache files, extracts compilationTarget names, and produces
a CSV mapping address → project_name that can be used across all reports.

Also fetches names from Etherscan's public API for unverified contracts.

Output: cache/contract_labels.csv
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import duckdb
import requests

from repricing_forensics.config import default_paths
from repricing_forensics.labels import ADDRESS_PROJECT_LABELS

_paths = default_paths()
CACHE_DIR = _paths.cache_dir
SOURCIFY_DIR = CACHE_DIR / "sourcify"
LABELS_PATH = CACHE_DIR / "contract_labels.csv"

# Known manual overrides and corrections
MANUAL_LABELS = {
    **{addr: name for addr, name in ADDRESS_PROJECT_LABELS.items()},
    # Common contracts that may not have good Sourcify names
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "Uniswap UNI",
    "0x0000000071727de22e5e9d8baf0edac6f37da032": "ERC-4337 EntryPoint v0.7",
    "0x4337084d9e255ff0702461cf8895ce9e3b5ff108": "ERC-4337 EntryPoint v0.6",
    "0x000000000022d473030f116ddee9f6b43ac78ba3": "Uniswap Permit2",
    "0x43506849d7c04f9138d1a2050bbf3a0c054402dd": "Circle USDC (impl)",
    "0x11b815efb8f581194ae79006d24e0d814b7697f6": "Uniswap V3 Pool",
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3 Pool",
}

# Patterns to clean up generic Sourcify names into more useful ones
GENERIC_NAMES = {
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

# Token-like contract names that should be kept as-is
TOKEN_PATTERN = re.compile(
    r"^(ERC20|ERC721|ERC1155|BEP20|WETH|WBTC|USDT|USDC|DAI|UNI|AAVE|COMP|LINK|MKR|SNX|CRV|BAL|SUSHI|YFI)",
    re.IGNORECASE,
)


def extract_name_from_sourcify(data: dict) -> str | None:
    """Extract a meaningful contract/project name from Sourcify metadata."""
    # Best source: compilationTarget in metadata
    meta = data.get("metadata", {})
    if isinstance(meta, dict):
        comp_target = meta.get("settings", {}).get("compilationTarget", {})
        if comp_target:
            for path, name in comp_target.items():
                if name and name not in GENERIC_NAMES:
                    return name
                # If name is generic, try to extract from path
                if name in GENERIC_NAMES:
                    # Check if path has a useful project prefix
                    parts = path.split("/")
                    for part in parts:
                        if part.startswith("@"):
                            continue
                        if part in ("contracts", "src", "lib", "node_modules"):
                            continue
                        if part.endswith(".sol"):
                            candidate = part.replace(".sol", "")
                            if candidate not in GENERIC_NAMES:
                                return candidate

    # Fallback: look at source file names for project indicators
    sources = list(data.get("sources", {}).keys())
    for s in sources:
        base = os.path.basename(s).replace(".sol", "")
        # Skip very generic/interface names
        if base.startswith("I") and len(base) > 1 and base[1].isupper():
            continue
        if base.lower() in (
            "ownable", "reentrancyguard", "context", "address", "safemath",
            "safeerc20", "math", "strings", "erc20", "erc721", "ierc20",
        ):
            continue
        if "@openzeppelin" in s or "lib/forge-std" in s or "lib/solmate" in s:
            continue
        if base not in GENERIC_NAMES:
            return base

    return None


def extract_project_from_sources(data: dict) -> str | None:
    """Try to extract project name from source paths (e.g., 'contracts/aave/...')."""
    sources = list(data.get("sources", {}).keys())
    for s in sources:
        # Look for common project path patterns
        lower = s.lower()
        # Skip standard library paths
        if any(skip in lower for skip in ["@openzeppelin", "forge-std", "solmate", "node_modules"]):
            continue
        parts = s.split("/")
        for part in parts:
            lower_part = part.lower()
            if lower_part in ("contracts", "src", "lib", "test", "interfaces", "libraries", "utils"):
                continue
            if part.endswith(".sol"):
                continue
            if len(part) > 2 and not part.startswith("."):
                return part

    return None


def fetch_blockscout_name(address: str) -> str | None:
    """Fetch contract name from Blockscout's free API (no key needed).

    For proxies, also checks the implementation contract name.
    """
    try:
        resp = requests.get(
            f"https://eth.blockscout.com/api/v2/addresses/{address}",
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        name = data.get("name")

        # If it's a generic proxy name, try to get the implementation name
        if name in GENERIC_NAMES or not name:
            impls = data.get("implementations") or []
            for impl in impls:
                impl_name = impl.get("name")
                if impl_name and impl_name not in GENERIC_NAMES:
                    return impl_name

        if name and name not in ("", "Contract", "Implementation"):
            return name
    except Exception:
        pass
    return None


def fetch_etherscan_name(address: str, api_key: str) -> str | None:
    """Fetch contract name from Etherscan V2 API."""
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
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            name = data["result"][0].get("ContractName", "")
            if name and name not in ("", "Contract"):
                return name
    except Exception:
        pass
    return None


def main():
    print("Building comprehensive contract labels...")

    # Load DuckDB to get all contracts we care about
    conn = duckdb.connect(str(_paths.duckdb_path), read_only=True)

    # Get all recipient contracts with breakage counts
    contracts = conn.execute("""
        SELECT recipient as address, count(*) as broken_txs,
               avg(gas_delta) as avg_delta
        FROM hot_7904 WHERE status_changed
        GROUP BY recipient ORDER BY broken_txs DESC
    """).df()
    conn.close()

    labels = {}  # address -> {name, source, classification}

    # Phase 1: Manual labels (highest priority)
    for addr, name in MANUAL_LABELS.items():
        labels[addr.lower()] = {"name": name, "source": "manual"}
    print(f"  Phase 1: {len(labels)} manual labels")

    # Phase 2: Sourcify metadata extraction
    sourcify_count = 0
    if SOURCIFY_DIR.exists():
        for fname in os.listdir(SOURCIFY_DIR):
            if not fname.endswith(".json"):
                continue
            addr = fname.replace(".json", "").lower()
            if addr in labels:
                continue  # Manual label takes priority

            try:
                data = json.loads((SOURCIFY_DIR / fname).read_text())
            except Exception:
                continue

            name = extract_name_from_sourcify(data)
            if name:
                labels[addr] = {"name": name, "source": "sourcify"}
                sourcify_count += 1

    print(f"  Phase 2: {sourcify_count} labels from Sourcify cache")

    # Phase 3: Blockscout API for top unlabeled contracts
    unlabeled = contracts[~contracts["address"].str.lower().isin(labels)]
    unlabeled_top = unlabeled.head(300)  # Top 300 unlabeled by breakage
    blockscout_count = 0

    # Load existing Blockscout cache to avoid re-fetching
    blockscout_cache_path = CACHE_DIR / "blockscout_names.json"
    blockscout_cache = {}
    if blockscout_cache_path.exists():
        blockscout_cache = json.loads(blockscout_cache_path.read_text())

    if len(unlabeled_top) > 0:
        print(f"  Phase 3: Fetching names from Blockscout for top {len(unlabeled_top)} unlabeled contracts...")
        for i, (_, row) in enumerate(unlabeled_top.iterrows()):
            addr = row["address"]
            if addr is None:
                continue
            addr_lower = addr.lower()

            # Check cache first
            if addr_lower in blockscout_cache:
                name = blockscout_cache[addr_lower]
            else:
                name = fetch_blockscout_name(addr)
                blockscout_cache[addr_lower] = name  # Cache even None results
                # Rate limit: be polite to Blockscout
                time.sleep(0.3)

            if name:
                labels[addr_lower] = {"name": name, "source": "blockscout"}
                blockscout_count += 1

            if (i + 1) % 50 == 0:
                print(f"    ... {i+1}/{len(unlabeled_top)} ({blockscout_count} found)")
                # Save cache periodically
                blockscout_cache_path.write_text(json.dumps(blockscout_cache, indent=2))

        # Final cache save
        blockscout_cache_path.write_text(json.dumps(blockscout_cache, indent=2))

    print(f"  Phase 3: {blockscout_count} labels from Blockscout")

    # Phase 3b: Etherscan API for remaining unlabeled (requires API key)
    etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")
    etherscan_count = 0
    if etherscan_key and etherscan_key != "your-key-here":
        unlabeled = contracts[~contracts["address"].str.lower().isin(labels)]
        unlabeled_top = unlabeled.head(300)

        etherscan_cache_path = CACHE_DIR / "etherscan_names.json"
        etherscan_cache = {}
        if etherscan_cache_path.exists():
            etherscan_cache = json.loads(etherscan_cache_path.read_text())

        if len(unlabeled_top) > 0:
            print(f"  Phase 3b: Fetching names from Etherscan for top {len(unlabeled_top)} remaining unlabeled...")
            for i, (_, row) in enumerate(unlabeled_top.iterrows()):
                addr = row["address"]
                if addr is None:
                    continue
                addr_lower = addr.lower()

                if addr_lower in etherscan_cache:
                    name = etherscan_cache[addr_lower]
                else:
                    name = fetch_etherscan_name(addr, etherscan_key)
                    etherscan_cache[addr_lower] = name
                    # Etherscan free tier: 5 req/sec
                    time.sleep(0.25)

                if name:
                    labels[addr_lower] = {"name": name, "source": "etherscan"}
                    etherscan_count += 1

                if (i + 1) % 50 == 0:
                    print(f"    ... {i+1}/{len(unlabeled_top)} ({etherscan_count} found)")
                    etherscan_cache_path.write_text(json.dumps(etherscan_cache, indent=2))

            etherscan_cache_path.write_text(json.dumps(etherscan_cache, indent=2))

        print(f"  Phase 3b: {etherscan_count} labels from Etherscan")
    else:
        print("  Phase 3b: Skipped (no ETHERSCAN_API_KEY in .env)")

    # Phase 4: Classification-based fallback labels
    classification_csv = CACHE_DIR / "contract_classification.csv"
    if classification_csv.exists():
        import pandas as pd
        cc = pd.read_csv(classification_csv)
        fallback_count = 0
        for _, row in cc.iterrows():
            addr = str(row["address"]).lower()
            if addr in labels:
                continue
            classification = row.get("classification", "")
            hint = row.get("source_hint", "")
            if isinstance(hint, str) and hint:
                labels[addr] = {"name": hint, "source": "sourcify_hint"}
                fallback_count += 1
            elif classification == "wallet_or_safe":
                labels[addr] = {"name": "Safe / Wallet", "source": "classification"}
                fallback_count += 1
            elif classification == "proxy":
                labels[addr] = {"name": "Proxy Contract", "source": "classification"}
                fallback_count += 1
        print(f"  Phase 4: {fallback_count} classification-based fallback labels")

    # Write output
    LABELS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LABELS_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["address", "name", "source"])
        writer.writeheader()
        for addr in sorted(labels.keys()):
            writer.writerow({
                "address": addr,
                "name": labels[addr]["name"],
                "source": labels[addr]["source"],
            })

    # Coverage stats
    total_broken = contracts["broken_txs"].sum()
    labeled_addrs = set(labels.keys())
    labeled_contracts = contracts[contracts["address"].str.lower().isin(labeled_addrs)]
    labeled_broken = labeled_contracts["broken_txs"].sum()

    print(f"\n{'='*60}")
    print(f"Total labels: {len(labels)}")
    print(f"Coverage: {len(labeled_contracts)}/{len(contracts)} contracts "
          f"= {labeled_broken}/{total_broken} broken txs "
          f"({labeled_broken/total_broken*100:.1f}%)")
    print(f"Output: {LABELS_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
