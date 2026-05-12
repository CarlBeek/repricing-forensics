"""Repo paths and schedule name.

Most of the consumer's state lives in the producer DB now (see
`source_db.resolve_producer_db_path`). The only paths the consumer
itself still owns are `cache_dir` (contract_labels.csv) and
`artifacts_dir` (the optional CSVs the failure-motif / remediation
endpoints read).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_SCHEDULE_NAME = "eip-8037"


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    cache_dir: Path
    artifacts_dir: Path


def _env_path(var: str, default: Path) -> Path:
    val = os.environ.get(var)
    return Path(val).expanduser().resolve() if val else default


def default_schedule_name() -> str:
    return os.environ.get("SCHEDULE_NAME", DEFAULT_SCHEDULE_NAME)


def default_paths(repo_root: Path | None = None) -> Paths:
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    return Paths(
        repo_root=root,
        cache_dir=_env_path("CACHE_DIR", root / "cache"),
        artifacts_dir=_env_path("ARTIFACTS_DIR", root / "artifacts"),
    )
