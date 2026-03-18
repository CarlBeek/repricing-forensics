from __future__ import annotations

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


def default_paths(repo_root: Path | None = None) -> Paths:
    root = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    return Paths(
        repo_root=root,
        sqlite_db=root / "divergences.db",
        research_lake=root / "research_lake",
        duckdb_dir=root / "duckdb",
        duckdb_path=root / "duckdb" / "eip7904.duckdb",
        cache_dir=root / "cache",
        artifacts_dir=root / "artifacts",
        notebooks_dir=root / "notebooks",
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
