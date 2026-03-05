# 外部仕様書（UI/入出力/API）
版：2.0  作成日：2026-02-16 09:37:28

---

## 1. UI仕様（Next.js）

### 1.1 画面一覧
- `/`：会社選択
- `/device`：機種選択
- `/run`：PDFアップロード＆実行
- `/jobs/[jobId]`：進捗・結果ダウンロード

### 1.2 画面：会社選択
- companies配下の会社名をカード/ボタンで表示
- 「＋会社追加」：会社名入力→作成→一覧に即反映
- 会社選択で次画面へ

### 1.3 画面：機種選択
- `mimamori` / `telecom`
- プリセットJSONが存在しない場合はエラー（作成/配置誘導）

### 1.4 画面：実行
- PDFドラッグ&ドロップ（複数）
- 「実行」押下 → jobIdを受け取り `/jobs/[jobId]` に遷移

### 1.5 画面：ジョブ詳細
- ステータス：queued/running/succeeded/failed
- 進捗：処理済PDF数 / 総PDF数、ERROR/WARN件数
- ダウンロード：
  - Excel（.xlsx）
  - ログ（.csv）
  - スキップ一覧（json/csv）

---

## 2. 出力仕様
- Excel：A〜AM固定ヘッダー、1運行=1行
- ログCSV：抽出失敗/矛盾/警告を記録
- スキップ一覧：ファイル名と理由

---

## 3. API仕様（FastAPI）

### 3.1 エンドポイント
- `GET /api/companies`
- `POST /api/companies`
- `GET /api/companies/{company}/devices`
- `POST /api/jobs`（multipart：company/device + pdfs[]）
- `GET /api/jobs/{jobId}`
- `GET /api/jobs/{jobId}/download/excel`
- `GET /api/jobs/{jobId}/download/log`
- `GET /api/jobs/{jobId}/download/skipped`

### 3.2 ジョブ状態レスポンス例
```json
{
  "jobId":"20260216_123045_abcd",
  "company":"A社",
  "device":"mimamori",
  "status":"running",
  "totalPdfs":12,
  "processedPdfs":5,
  "errorCount":1,
  "warnCount":2,
  "startedAt":"2026-02-16T12:30:45+09:00",
  "finishedAt":null,
  "artifacts":{"excel":false,"log":false,"skipped":true}
}
```
