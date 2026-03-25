#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests import RequestException

from repricing_forensics.config import default_paths, ensure_workspace_dirs
from repricing_forensics.duckdb_utils import connect
from repricing_forensics.sourcify import (
    classify_contract,
    contract_cache_path,
    fetch_contract,
    source_hint,
)


def _fetch_one(address: str, cache_dir):
    try:
        return address, fetch_contract(address, cache_dir)
    except RequestException:
        return address, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Sourcify metadata for impacted contracts")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--address-source",
        choices=["divergence", "recipient", "union"],
        default="divergence",
    )
    args = parser.parse_args()

    paths = default_paths()
    ensure_workspace_dirs(paths)
    conn = connect(paths.duckdb_path)
    if args.address_source == "divergence":
        query = """
            SELECT divergence_contract AS address, count(*) AS divergent_txs
            FROM normalized_forensics
            WHERE divergence_contract IS NOT NULL
            GROUP BY 1
            ORDER BY divergent_txs DESC
            LIMIT ?
        """
    elif args.address_source == "recipient":
        query = """
            SELECT recipient AS address, count(*) AS divergent_txs
            FROM hot_7904
            WHERE recipient IS NOT NULL
            GROUP BY 1
            ORDER BY divergent_txs DESC
            LIMIT ?
        """
    else:
        query = """
            WITH all_addresses AS (
                SELECT recipient AS address FROM hot_7904 WHERE recipient IS NOT NULL
                UNION ALL
                SELECT divergence_contract AS address
                FROM normalized_forensics
                WHERE divergence_contract IS NOT NULL
            )
            SELECT address, count(*) AS divergent_txs
            FROM all_addresses
            GROUP BY 1
            ORDER BY divergent_txs DESC
            LIMIT ?
        """
    rows = conn.execute(query, [args.limit]).fetchall()
    conn.close()

    out_path = paths.cache_dir / "contract_classification.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, dict[str, str]] = {}
    if out_path.exists():
        with out_path.open() as handle:
            for row in csv.DictReader(handle):
                existing[row["address"].lower()] = row

    # Split into cached (instant) vs uncached (need HTTP fetch)
    tx_counts = {addr.lower(): txs for addr, txs in rows}
    uncached = [
        addr for addr, _ in rows
        if not contract_cache_path(paths.cache_dir, addr).exists()
    ]
    cached = [addr for addr, _ in rows if addr not in uncached]

    print(f"{len(cached)} cached, {len(uncached)} to fetch ({args.workers} workers)")

    # Load cached results instantly
    merged = dict(existing)
    for address in cached:
        payload = fetch_contract(address, paths.cache_dir)  # reads from disk
        merged[address.lower()] = _classify(address, tx_counts[address.lower()], payload)

    # Fetch uncached in parallel
    if uncached:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(_fetch_one, addr, paths.cache_dir): addr
                for addr in uncached
            }
            done = 0
            for future in as_completed(futures):
                address, payload = future.result()
                merged[address.lower()] = _classify(address, tx_counts[address.lower()], payload)
                done += 1
                if done % 20 == 0:
                    print(f"  fetched {done}/{len(uncached)}")

    with out_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "address",
                "divergent_txs",
                "classification",
                "match_status",
                "name",
                "source_hint",
            ],
        )
        writer.writeheader()
        for row in sorted(
            merged.values(),
            key=lambda item: int(item["divergent_txs"]),
            reverse=True,
        ):
            writer.writerow(row)


def _classify(address, divergent_txs, payload):
    return {
        "address": address,
        "divergent_txs": str(divergent_txs),
        "classification": classify_contract(payload),
        "match_status": None if payload is None else payload.get("match"),
        "name": None if payload is None else payload.get("compiledContract", {}).get("name"),
        "source_hint": source_hint(payload),
    }


if __name__ == "__main__":
    main()
