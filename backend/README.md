# Backend (FastAPI)

## 起動
```powershell
cd backend
.\run_backend.ps1
```

## どこに置くか（結論）
- 抽出ロジック：`backend/engine/pipeline.py`
- プリセットJSON：`backend/companies/<会社名>/mimamori.json` と `telecom.json`
- ジョブ入出力：`backend/work/jobs/<jobId>/`
