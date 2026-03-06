# 指示書に基づく exe 化（onedir）。backend フォルダで実行すること。
# 出力: dist/TimeManagement/ 一式（TimeManagement.exe + _app + _work + 起動方法.txt）

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$BackendDir = $PSScriptRoot
$VenvScript = Join-Path $BackendDir ".venv" "Scripts" "Activate.ps1"
if (Test-Path $VenvScript) {
    Write-Host "仮想環境を有効化しています..."
    & $VenvScript
}
$ProjectRoot = (Get-Item $BackendDir).Parent.FullName
$WebDir = Join-Path $ProjectRoot "web"
$CompaniesDir = Join-Path $BackendDir "companies"
$EngineHeaders = Join-Path $BackendDir "engine" "excel_headers.json"
$DistDir = Join-Path $BackendDir "dist" "TimeManagement"

# PyInstaller が無ければインストール
if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    Write-Host "PyInstaller をインストールしています..."
    pip install pyinstaller
}

# onedir ビルド（spec で uvicorn 等を同梱。venv 有効時は uvicorn を検出して同梱される）
Write-Host "PyInstaller でビルド中..."
pyinstaller --noconfirm TimeManagement.spec

if (-not (Test-Path $DistDir)) {
    Write-Error "ビルド出力が見つかりません: $DistDir"
    exit 1
}

# _app を作成し companies / engine / web をコピー
$AppDir = Join-Path $DistDir "_app"
$AppCompanies = Join-Path $AppDir "companies"
$AppEngine = Join-Path $AppDir "engine"
$AppWeb = Join-Path $AppDir "web"

New-Item -ItemType Directory -Force -Path $AppCompanies | Out-Null
New-Item -ItemType Directory -Force -Path $AppEngine | Out-Null
New-Item -ItemType Directory -Force -Path $AppWeb | Out-Null

Write-Host "_app にリソースをコピー中..."
Copy-Item -Path (Join-Path $CompaniesDir "*") -Destination $AppCompanies -Recurse -Force
Copy-Item -Path $EngineHeaders -Destination $AppEngine -Force
Copy-Item -Path (Join-Path $WebDir "*") -Destination $AppWeb -Recurse -Force

# _work を空で作成
$WorkDir = Join-Path $DistDir "_work"
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

# _app と _work に hidden 属性を付与（Windows）
$attr = [System.IO.FileAttributes]::Hidden
(Get-Item $AppDir).Attributes  = (Get-Item $AppDir).Attributes  -bor $attr
(Get-Item $WorkDir).Attributes = (Get-Item $WorkDir).Attributes -bor $attr

# 起動方法.txt
$ReadmePath = Join-Path $DistDir "起動方法.txt"
@"
時間管理システム（デジタコ・アルコール紐づけ）

【起動方法】
  TimeManagement.exe をダブルクリックしてください。
  しばらくするとブラウザが開き、画面が表示されます。

【注意】
  - 起動中はこのウィンドウを閉じないでください。
  - ブラウザで http://127.0.0.1:8000 が開きます。
  - 会社・機種を選び、PDF 等をアップロードしてご利用ください。

【接続できない場合】
  - 同じフォルダに TimeManagement_error.log ができていないか確認してください。
  - ログにエラー内容が書かれています。
"@ | Set-Content -Path $ReadmePath -Encoding UTF8

Write-Host "完了。配布フォルダ: $DistDir"
Write-Host "  - TimeManagement.exe"
Write-Host "  - 起動方法.txt"
Write-Host "  - _app (hidden), _work (hidden)"
