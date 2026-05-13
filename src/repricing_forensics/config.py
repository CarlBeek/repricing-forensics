"""Repo paths.

The producer DB is the source of truth (see `source_db.resolve_producer_db_path`
for how its path is resolved). Schedule routing happens per-endpoint via a
`?schedule=NAME` query parameter — there's no global `SCHEDULE_NAME` env var
anymore, since the producer can write multiple schedules into one file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    cache_dir: Path


def _env_path(var: str, default: Path) -> Path:
    val = os.environ.get(var)
    return Path(val).expanduser().resolve() if val else default


def default_paths(repo_root: Path | None = None) -> Paths:
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    return Paths(
        repo_root=root,
        cache_dir=_env_path("CACHE_DIR", root / "cache"),
    )
