from __future__ import annotations

import threading
import time
import webbrowser

import uvicorn
from main import app


def _open_browser() -> None:
    time.sleep(1.2)
    try:
        webbrowser.open('http://127.0.0.1:8000')
    except Exception:
        pass


if __name__ == '__main__':
    threading.Thread(target=_open_browser, daemon=True).start()
    uvicorn.run(app, host='127.0.0.1', port=8000, reload=False)
