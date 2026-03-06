from __future__ import annotations
import sys
from pathlib import Path
from typing import Final, Optional

def _get_app_root() -> Path:
    """exe 化時は exe の場所を基準にする（配布時は dist/TimeManagement/）。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]  # backend/

APP_ROOT: Final[Path] = _get_app_root()

# exe 化時: exe 横の hidden フォルダ _app / _work を参照
if getattr(sys, "frozen", False):
    _APP_DIR: Final[Path] = APP_ROOT / "_app"
    COMPANIES_DIR: Final[Path] = _APP_DIR / "companies"
    WORK_DIR: Final[Path] = APP_ROOT / "_work"
    JOBS_DIR: Final[Path] = WORK_DIR / "jobs"
    EXCEL_HEADERS_JSON_PATH: Final[Optional[Path]] = _APP_DIR / "engine" / "excel_headers.json"
else:
    COMPANIES_DIR: Final[Path] = APP_ROOT / "companies"
    WORK_DIR: Final[Path] = APP_ROOT / "work"
    JOBS_DIR: Final[Path] = WORK_DIR / "jobs"
    EXCEL_HEADERS_JSON_PATH: Final[Optional[Path]] = None

def ensure_dirs() -> None:
    COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)

def job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id

def job_input_dir(job_id: str) -> Path:
    return job_dir(job_id) / "input"

def job_output_dir(job_id: str) -> Path:
    return job_dir(job_id) / "output"

def job_state_path(job_id: str) -> Path:
    return job_dir(job_id) / "state.json"

def safe_name(name: str) -> str:
    return "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_", " ", "・")).strip()
