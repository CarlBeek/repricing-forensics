#!/usr/bin/env python3
from __future__ import annotations

import argparse

from eip7904_analysis.config import default_paths
from eip7904_analysis.pipeline import build_status_change_call_frame_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Explode schedule call frames for status-changed txs")
    parser.add_argument("--schedule-name", default="7904-prelim")
    args = parser.parse_args()

    build_status_change_call_frame_table(default_paths(), args.schedule_name)


if __name__ == "__main__":
    main()
