#!/usr/bin/env python3
from __future__ import annotations

import argparse

from repricing_forensics.config import default_paths
from repricing_forensics.pipeline import build_normalized_forensics, initialize_duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize DuckDB views and derived tables")
    parser.add_argument("--schedule-name", default="7904-prelim")
    parser.add_argument("--include-call-frames", action="store_true")
    args = parser.parse_args()

    paths = default_paths()
    initialize_duckdb(paths, args.schedule_name)
    build_normalized_forensics(paths, args.schedule_name, include_call_frames=args.include_call_frames)


if __name__ == "__main__":
    main()
