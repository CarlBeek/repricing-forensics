#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import time
from pathlib import Path

from repricing_forensics.config import default_paths, ensure_workspace_dirs


def _lake_is_fresh(research_lake: Path, max_age_seconds: int) -> bool:
    """Check if any parquet file in the research lake is newer than max_age_seconds."""
    parquets = list(research_lake.rglob("*.parquet"))
    if not parquets:
        return False
    newest = max(p.stat().st_mtime for p in parquets)
    age = time.time() - newest
    return age < max_age_seconds


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the SQLite divergence DB to Parquet")
    parser.add_argument("--reth-dir", type=Path, default=None)
    parser.add_argument("--block-bucket-size", type=int, default=100_000)
    parser.add_argument("--row-group-size", type=int, default=50_000)
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument(
        "--max-age",
        type=int,
        default=0,
        help="Skip export if parquet files are younger than this many seconds (0 = always export)",
    )
    args = parser.parse_args()

    paths = default_paths()
    ensure_workspace_dirs(paths)

    if args.max_age > 0 and not args.full_refresh:
        if _lake_is_fresh(paths.research_lake, args.max_age):
            age_min = args.max_age // 60
            print(f"Parquet files are less than {age_min} minutes old — skipping export.")
            return

    reth_dir = (args.reth_dir or paths.reth_dir).resolve()

    # Use a pre-built binary if available, otherwise cargo run
    prebuilt = reth_dir / "target" / "release" / "reth-research-export-parquet"
    if prebuilt.exists() or shutil.which("reth-research-export-parquet"):
        binary = str(prebuilt) if prebuilt.exists() else "reth-research-export-parquet"
        cmd = [binary]
    else:
        cmd = [
            "cargo", "run", "--release",
            "-p", "reth-research-bin",
            "--bin", "reth-research-export-parquet",
            "--",
        ]

    cmd += [
        "--db-path", str(paths.sqlite_db),
        "--out-dir", str(paths.research_lake),
        "--row-group-size", str(args.row_group_size),
        "--block-bucket-size", str(args.block_bucket_size),
    ]
    if args.full_refresh:
        cmd.append("--full-refresh")

    subprocess.run(cmd, cwd=reth_dir, check=True)


if __name__ == "__main__":
    main()
