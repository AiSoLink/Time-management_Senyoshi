# pdf2excel-localweb-full

## どこにコードを置く？
- **このZIPを解凍したフォルダをそのまま** Cursorで開けばOK（配置換え不要）
- PDF抽出の本体は **backend/engine/pipeline.py** に置く（ここを書き換えていく）
- 会社×機種プリセットJSONは **backend/companies/<会社名>/** に置く

## 起動（PowerShell）
Backend:
```powershell
cd backend
.\run_backend.ps1
```

Frontend（別ターミナル）:
```powershell
cd frontend
.\run_frontend.ps1
```
