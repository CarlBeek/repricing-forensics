#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

from repricing_forensics.config import default_paths
from repricing_forensics.pipeline import build_normalized_forensics, initialize_duckdb


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize DuckDB views and derived tables")
    parser.add_argument("--schedule-name", default="7904-prelim")
    parser.add_argument("--include-call-frames", action="store_true")
    args = parser.parse_args()

    paths = default_paths()

    # Build into a temporary file so that a concurrent read-only connection
    # (e.g. the web server) is never blocked by the write lock.
    tmp_fd, tmp_path_str = tempfile.mkstemp(
        suffix=".duckdb", dir=paths.duckdb_path.parent
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_path_str)
    try:
        initialize_duckdb(paths, args.schedule_name, db_path=tmp_path)
        build_normalized_forensics(
            paths, args.schedule_name,
            include_call_frames=args.include_call_frames,
            db_path=tmp_path,
        )
        # Atomic replace — existing read-only connections keep their fd to the
        # old (now unlinked) file and pick up the new one on reconnect.
        tmp_path.replace(paths.duckdb_path)
        # Clean up DuckDB sidecar files left under the temp name
        for suffix in (".wal", ".tmp"):
            sidecar = Path(tmp_path_str + suffix)
            sidecar.unlink(missing_ok=True)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
