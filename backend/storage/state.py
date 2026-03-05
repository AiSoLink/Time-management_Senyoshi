from __future__ import annotations
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Artifacts:
    excel: bool = False
    log: bool = False
    skipped: bool = False

@dataclass
class JobState:
    jobId: str
    company: str
    device: str
    status: str
    totalPdfs: int = 0
    processedPdfs: int = 0
    errorCount: int = 0
    warnCount: int = 0
    startedAt: Optional[str] = None
    finishedAt: Optional[str] = None
    artifacts: Artifacts = field(default_factory=Artifacts)
    pendingRows: Optional[List[Dict[str, Any]]] = None  # 出庫・帰庫が未取得の行（手入力用）

def load_state(path: Path) -> JobState:
    data = json.loads(path.read_text(encoding="utf-8"))
    artifacts = Artifacts(**data.get("artifacts", {}))
    # pendingRows は旧 state に無い場合がある
    kwargs = {k: v for k, v in data.items() if k != "artifacts"}
    kwargs["artifacts"] = artifacts
    if "pendingRows" not in kwargs:
        kwargs["pendingRows"] = None
    return JobState(**kwargs)

def save_state(path: Path, state: JobState) -> None:
    payload: Dict[str, Any] = asdict(state)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
