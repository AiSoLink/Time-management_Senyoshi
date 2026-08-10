from __future__ import annotations
import json
import os
import time
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
    progressPercent: float = 0.0  # 処理の進捗（0〜100）
    progressLabel: str = ""       # いま何をしているかの表示用ラベル

def load_state(path: Path) -> JobState:
    # 書き込み直後の読み取りで空/途中のファイルを掴む競合があり得るため、
    # 解析失敗時は少し待ってリトライする（特に共有ドライブ上で発生しやすい）
    last_err: Optional[Exception] = None
    data: Optional[Dict[str, Any]] = None
    for _ in range(5):
        try:
            text = path.read_text(encoding="utf-8")
            if not text.strip():
                raise json.JSONDecodeError("empty file", "", 0)
            data = json.loads(text)
            break
        except (json.JSONDecodeError, OSError) as e:
            last_err = e
            time.sleep(0.2)
    if data is None:
        raise last_err if last_err else RuntimeError("state.json を読み取れませんでした")
    artifacts = Artifacts(**data.get("artifacts", {}))
    # pendingRows は旧 state に無い場合がある
    kwargs = {k: v for k, v in data.items() if k != "artifacts"}
    kwargs["artifacts"] = artifacts
    if "pendingRows" not in kwargs:
        kwargs["pendingRows"] = None
    return JobState(**kwargs)

def save_state(path: Path, state: JobState) -> None:
    # 一時ファイルへ書いてから置き換えるアトミック書き込み。
    # 読み取り側が空/途中のファイルを見ないようにする
    payload: Dict[str, Any] = asdict(state)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
