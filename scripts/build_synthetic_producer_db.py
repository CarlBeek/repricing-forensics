#!/usr/bin/env python3
"""CLI wrapper around `repricing_forensics.synthetic.build_synthetic_db`.

Used while reth lands the new producer schema (see
`docs/storage-redesign.md`). Throwaway when the real producer ships.

    python scripts/build_synthetic_producer_db.py --out /tmp/syn.duckdb --blocks 5
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from repricing_forensics.synthetic import build_synthetic_db


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, default=PROJECT_ROOT / "synthetic.duckdb")
    p.add_argument("--blocks", type=int, default=5)
    args = p.parse_args()

    build_synthetic_db(args.out, blocks=args.blocks)
    print(f"wrote {args.out} ({args.blocks} blocks)")


if __name__ == "__main__":
    main()
