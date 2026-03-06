"""
exe 用エントリポイント。ターミナルを表示せずにサーバーを起動し、ブラウザを開く。
"""
from __future__ import annotations

import sys
import threading
import time
import traceback
import webbrowser

# コンソールなし exe では sys.stdout/stderr が None のため、uvicorn の isatty() で落ちるのを防ぐ
if getattr(sys, "frozen", False):
    class _DummyStream:
        def write(self, x: str) -> None: pass
        def flush(self) -> None: pass
        def isatty(self) -> bool: return False
    if sys.stdout is None:
        sys.stdout = _DummyStream()
    if sys.stderr is None:
        sys.stderr = _DummyStream()

import uvicorn

OPEN_BROWSER_DELAY = 4.0
HOST = "127.0.0.1"
PORT = 8000


def _log_error(msg: str) -> None:
    """exe 横にエラーログを書き、起動失敗の原因を残す。"""
    try:
        if getattr(sys, "frozen", False):
            log_path = __import__("pathlib").Path(sys.executable).parent / "TimeManagement_error.log"
        else:
            log_path = __import__("pathlib").Path(__file__).resolve().parent / "TimeManagement_error.log"
        log_path.write_text(msg, encoding="utf-8")
    except Exception:
        pass


def _run_server() -> None:
    try:
        from main import app
        uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
    except Exception:
        _log_error(traceback.format_exc())
        raise


def main() -> None:
    server = threading.Thread(target=_run_server, daemon=False)
    server.start()
    time.sleep(OPEN_BROWSER_DELAY)
    webbrowser.open(f"http://{HOST}:{PORT}")
    server.join()


if __name__ == "__main__":
    main()
