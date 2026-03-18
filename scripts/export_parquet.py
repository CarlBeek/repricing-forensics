#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from eip7904_analysis.config import default_paths, ensure_workspace_dirs


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the SQLite divergence DB to Parquet")
    parser.add_argument("--reth-dir", type=Path, default=Path("../reth"))
    parser.add_argument("--block-bucket-size", type=int, default=100_000)
    parser.add_argument("--row-group-size", type=int, default=50_000)
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    paths = default_paths()
    ensure_workspace_dirs(paths)

    cmd = [
        "cargo",
        "run",
        "--release",
        "-p",
        "reth-research-bin",
        "--bin",
        "reth-research-export-parquet",
        "--",
        "--db-path",
        str(paths.sqlite_db),
        "--out-dir",
        str(paths.research_lake),
        "--row-group-size",
        str(args.row_group_size),
        "--block-bucket-size",
        str(args.block_bucket_size),
    ]
    if args.full_refresh:
        cmd.append("--full-refresh")

    subprocess.run(cmd, cwd=args.reth_dir, check=True)


if __name__ == "__main__":
    main()
