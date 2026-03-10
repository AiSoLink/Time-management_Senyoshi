# 再ビルド前に dist を削除する。exe が起動中だと削除できないため、先にプロセスを終了する。
# backend フォルダで実行すること。

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$proc = Get-Process -Name "TimeManagement" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "TimeManagement を終了しています..."
    $proc | Stop-Process -Force
    Start-Sleep -Seconds 1
}

Remove-Item -Recurse -Force .\dist, .\build -ErrorAction SilentlyContinue
Write-Host "dist と build を削除しました。続けて .\build_exe.ps1 で再ビルドできます。"
