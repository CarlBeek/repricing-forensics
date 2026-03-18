from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests


SOURCIFY_BASE = "https://sourcify.dev/server/v2/contract/1"


def contract_cache_path(cache_dir: Path, address: str) -> Path:
    return cache_dir / "sourcify" / f"{address.lower()}.json"


def fetch_contract(address: str, cache_dir: Path, force: bool = False) -> dict[str, Any] | None:
    cache_path = contract_cache_path(cache_dir, address)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists() and not force:
        return json.loads(cache_path.read_text())

    response = requests.get(f"{SOURCIFY_BASE}/{address}", params={"fields": "all"}, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()

    payload = response.json()
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def classify_contract(payload: dict[str, Any] | None) -> str:
    if not payload:
        return "unverified"

    names = []
    for key, source in payload.get("sources", {}).items():
        names.append((key or "").lower())
        names.append((source.get("name") or "").lower())
        names.append((source.get("path") or "").lower())

    haystack = " ".join(names)
    if "entrypoint" in haystack or "permit2" in haystack or "aggregationrouter" in haystack:
        return "verified_immutable"
    if "uniswap" in haystack or "sushi" in haystack:
        return "verified_immutable"
    if "proxy" in haystack:
        return "proxy"
    if "upgrade" in haystack or "transparent" in haystack or "uups" in haystack:
        return "upgradeable"
    if "safe" in haystack or "gnosis" in haystack or "wallet" in haystack:
        return "wallet_or_safe"
    return "verified_immutable"


def source_hint(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None

    names = []
    for key, source in payload.get("sources", {}).items():
        names.append((key or "").lower())
        names.append((source.get("name") or "").lower())
        names.append((source.get("path") or "").lower())
    haystack = " ".join(names)

    hints = [
        ("entrypoint", "EntryPoint"),
        ("permit2", "Permit2"),
        ("universalrouter", "UniversalRouter"),
        ("aggregationrouter", "AggregationRouter"),
        ("uniswap", "Uniswap"),
        ("sushi", "SushiSwap"),
        ("safe", "Safe"),
        ("proxy", "Proxy"),
        ("erc1967", "ERC1967"),
    ]
    for needle, label in hints:
        if needle in haystack:
            return label
    return None
