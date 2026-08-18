Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Set-Location $PSScriptRoot

if (!(Test-Path '.venv')) {
    python -m venv .venv
}

. .\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r .\requirements.txt
pip install "pyinstaller>=6.19,<7"

if (Test-Path .\build) { Remove-Item .\build -Recurse -Force }
if (Test-Path .\dist)  { Remove-Item .\dist  -Recurse -Force }

pyinstaller --clean --noconfirm .\TimeManagementBackend.spec

Write-Host ''
Write-Host '==== build completed ===='
Write-Host 'EXE folder:' (Resolve-Path .\dist\TimeManagementBackend)
Write-Host 'Run this file:' (Resolve-Path .\dist\TimeManagementBackend\TimeManagementBackend.exe)
