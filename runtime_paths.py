from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parent
IS_FROZEN = bool(getattr(sys, "frozen", False))
EXECUTABLE_PATH = Path(sys.executable).resolve()
BUNDLE_ROOT = Path(getattr(sys, "_MEIPASS", SOURCE_ROOT)).resolve()
APP_SUPPORT_DIR = Path.home() / "Library" / "Application Support" / "CBA Search"
APP_SUPPORT_ENV_PATH = APP_SUPPORT_DIR / ".env"


def _resolve_data_root() -> tuple[Path, str]:
    """
    Pick one stable writable root for runtime data.

    Order:
    1. macOS Application Support for frozen PyInstaller builds
    2. the source tree root during normal Python execution
    """
    if IS_FROZEN:
        return APP_SUPPORT_DIR.resolve(), "app_support"

    return SOURCE_ROOT, "source_root"


DATA_ROOT, DATA_ROOT_REASON = _resolve_data_root()
DATA_ROOT.mkdir(parents=True, exist_ok=True)
PROJECT_ROOT = DATA_ROOT
CACHE_ROOT = DATA_ROOT / "_rag_cache"
OUTPUTS_DIR = DATA_ROOT / "outputs"
CHROMA_DB_PATH = CACHE_ROOT / "chroma_db"
INGEST_RUNS_DIR = CACHE_ROOT / "ingest_runs"

if (BUNDLE_ROOT / "frontend" / "dist").exists():
    FRONTEND_DIST = BUNDLE_ROOT / "frontend" / "dist"
else:
    FRONTEND_DIST = SOURCE_ROOT / "frontend" / "dist"


def dotenv_candidates() -> list[Path]:
    """Return candidate `.env` locations in precedence order."""
    candidates = [APP_SUPPORT_ENV_PATH, DATA_ROOT / ".env", SOURCE_ROOT / ".env"]
    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_candidates.append(resolved)
    return unique_candidates


def load_local_dotenv(load_dotenv_func) -> list[str]:
    """Load `.env` files from the runtime data root and source root when present."""
    loaded_paths: list[str] = []
    if load_dotenv_func is None:
        return loaded_paths

    for candidate in dotenv_candidates():
        if candidate.exists():
            load_dotenv_func(candidate, override=False)
            loaded_paths.append(str(candidate))
    return loaded_paths


def runtime_diagnostics() -> dict:
    """Return the current runtime path configuration for debugging."""
    return {
        "frozen": IS_FROZEN,
        "cwd": str(Path.cwd()),
        "source_root": str(SOURCE_ROOT),
        "bundle_root": str(BUNDLE_ROOT),
        "data_root": str(DATA_ROOT),
        "data_root_reason": DATA_ROOT_REASON,
        "cache_root": str(CACHE_ROOT),
        "outputs_dir": str(OUTPUTS_DIR),
        "chroma_db_path": str(CHROMA_DB_PATH),
        "app_support_dir": str(APP_SUPPORT_DIR),
        "frontend_dist": str(FRONTEND_DIST),
        "sys_executable": str(EXECUTABLE_PATH),
        "sys_meipass": str(getattr(sys, "_MEIPASS", "")),
        "dotenv_candidates": [str(path) for path in dotenv_candidates()],
    }


def print_runtime_diagnostics(context: str) -> None:
    """Print a compact runtime path block for normal and frozen app comparisons."""
    diagnostics = runtime_diagnostics()
    print(f"[runtime:{context}]")
    for key, value in diagnostics.items():
        print(f"  {key}: {value}")
