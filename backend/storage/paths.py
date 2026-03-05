from __future__ import annotations
from pathlib import Path
from typing import Final

APP_ROOT: Final[Path] = Path(__file__).resolve().parents[1]  # backend/
COMPANIES_DIR: Final[Path] = APP_ROOT / "companies"
WORK_DIR: Final[Path] = APP_ROOT / "work"
JOBS_DIR: Final[Path] = WORK_DIR / "jobs"

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
