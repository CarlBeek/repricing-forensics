from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    sqlite_db: Path
    research_lake: Path
    duckdb_dir: Path
    duckdb_path: Path
    cache_dir: Path
    artifacts_dir: Path
    notebooks_dir: Path
    reth_dir: Path


def _env_path(var: str, default: Path) -> Path:
    val = os.environ.get(var)
    return Path(val).expanduser().resolve() if val else default


def default_paths(repo_root: Path | None = None) -> Paths:
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    research_lake = _env_path("RESEARCH_LAKE_PATH", root / "research_lake")
    duckdb_dir = _env_path("DUCKDB_DIR", root / "duckdb")
    return Paths(
        repo_root=root,
        sqlite_db=_env_path("DIVERGENCE_DB_PATH", root / "divergences.db"),
        research_lake=research_lake,
        duckdb_dir=duckdb_dir,
        duckdb_path=_env_path("DUCKDB_PATH", duckdb_dir / "eip7904.duckdb"),
        cache_dir=_env_path("CACHE_DIR", root / "cache"),
        artifacts_dir=_env_path("ARTIFACTS_DIR", root / "artifacts"),
        notebooks_dir=root / "notebooks",
        reth_dir=_env_path("RETH_DIR", root.parent / "reth"),
    )


def ensure_workspace_dirs(paths: Paths) -> None:
    for directory in [
        paths.research_lake,
        paths.duckdb_dir,
        paths.cache_dir,
        paths.artifacts_dir,
        paths.notebooks_dir,
    ]:
        directory.mkdir(parents=True, exist_ok=True)
