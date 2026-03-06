# -*- mode: python ; coding: utf-8 -*-
# backend フォルダで実行すること。companies/web/engine はビルド後に _app へコピー。

from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

SPEC_DIR = Path(SPECPATH).resolve()
# 仮想環境の site-packages を pathex に含め、uvicorn を確実に検出
_venv_site = SPEC_DIR / '.venv' / 'Lib' / 'site-packages'
_pathex = [str(SPEC_DIR)]
if _venv_site.exists():
    _pathex.append(str(_venv_site))
# uvicorn を丸ごと同梱
try:
    from PyInstaller.utils.hooks import collect_all
    uvicorn_datas, uvicorn_binaries, uvicorn_hidden = collect_all('uvicorn')
except Exception:
    uvicorn_datas, uvicorn_binaries, uvicorn_hidden = [], [], collect_submodules('uvicorn')

a = Analysis(
    [str(SPEC_DIR / 'run_app.py')],
    pathex=_pathex,
    binaries=uvicorn_binaries,
    datas=uvicorn_datas,
    hiddenimports=['uvicorn'] + list(uvicorn_hidden),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='TimeManagement',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='TimeManagement',
)
