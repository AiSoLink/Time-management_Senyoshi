# 時間管理システム（デジタコ・アルコール紐づけ）

デジタルタコグラフ（デジタコ）のPDF・対面データ・アルコール検知データをアップロードし、運行ごとの集計Excelを出力するWebアプリです。  
**フロントエンドは静的HTML/CSS/JavaScript（`web/`）で、バックエンドはPython（FastAPI）で動作します。**

---

## 機能概要

1. **会社・機種の選択**  
   会社を選択し、みまもり／テレコムなど機種（デバイス）を選びます。

2. **ファイルアップロード（3) 実行）**  
   - **デジタコ**: PDF（複数可）  
   - **対面**: 対面データ（CSV等、複数可）  
   - **アルキラーNEX**: アルコール検知データ（複数可）  

3. **処理フロー（4) 結果）**  
   - **3時間未満の紐づけ**: 同一乗務員で「帰庫→出庫」が3時間未満の運行を「まとめる／まとめない」を選択  
   - **3時間以上の紐づけ**: 3時間以上空いている運行を「1本にまとめる」グループを指定（検索・複数選択対応）  
   - **同乗者紐づけ**: デジタコがなくアルコールのみの乗務員について「誰と添乗したか」を運行で選択（検索対応）  
   - **アルコールとデジタコの紐づけ**: 出庫・帰庫が取れなかった行の日時を手入力  
   - **完了**: Excelダウンロード（手入力画面の「ダウンロード」で取得）

---

## 技術構成

| 役割 | 技術 | 場所 |
|------|------|------|
| フロントエンド | 静的HTML / CSS / JavaScript | `web/` |
| バックエンド | Python 3, FastAPI, Uvicorn | `backend/` |
| PDF解析・Excel出力 | pdfplumber, openpyxl, pipeline | `backend/engine/` |
| 会社×機種プリセット | JSON | `backend/companies/<会社名>/` |

- バックエンドが **ルート（`/`）で `web/` を静的配信**し、**`/api/*` でAPI**を提供します。  
- フロントは Node.js（Next.js）から **静的HTMLフロントに変更**しています（`frontend/` は旧フロントの残りです）。

---

## 必要な環境

- **Python 3.10 以上**（推奨: 3.11+）
- **pip** でパッケージインストール

---

## セットアップと起動

### 1. リポジトリのクローン

```bash
git clone <リポジトリURL>
cd <リポジトリ名>
```

### 2. バックエンドの起動（これだけでフロントも利用可能）

**PowerShell（推奨）:**

```powershell
cd backend
.\run_backend.ps1
```

- 初回は仮想環境（`.venv`）作成と `pip install -r requirements.txt` が実行されます。  
- 起動後は **http://127.0.0.1:8000** でアクセスできます。  
- ルート（`/`）で `web/index.html` が表示され、会社選択 → 機種選択 → 実行（アップロード）→ 結果 の流れで利用できます。

**手動で起動する場合:**

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m uvicorn main:app --reload --port 8000
```

### 3. （任意）旧Next.jsフロントを使う場合

別ターミナルで:

```powershell
cd frontend
.\run_frontend.ps1
```

- http://localhost:3000 でNext.jsが起動します。  
- **通常利用はバックエンドのみ（静的 `web/`）で問題ありません。**

---

## ディレクトリ構成

```
<リポジトリ>
├── README.md
├── backend/
│   ├── main.py              # FastAPIアプリ・API・静的配信
│   ├── run_app.py            # exe 用エントリ（ターミナル非表示・ブラウザ起動）
│   ├── build_exe.ps1         # exe ビルドスクリプト（onedir）
│   ├── TimeManagement.spec   # PyInstaller 用 spec
│   ├── requirements.txt
│   ├── run_backend.ps1
│   ├── job_runner.py         # ジョブ実行
│   ├── storage/              # パス・状態管理
│   ├── engine/               # PDF解析・Excel・アルコール突合
│   │   ├── pipeline.py
│   │   ├── alcohol_integration.py
│   │   └── excel_headers.json
│   └── companies/            # 会社別プリセット
│       └── <会社名>/
│           ├── mimamori.json
│           └── telecom.json
├── web/                      # 静的フロント（メイン）
│   ├── index.html            # 1) 会社
│   ├── device.html           # 2) 機種
│   ├── run.html              # 3) 実行（アップロード）
│   ├── job.html              # 4) 結果（マージ・紐づけ・同乗者・手入力・完了）
│   ├── style.css
│   └── app.js
└── frontend/                 # 旧Next.jsフロント（任意）
```

---

## exe 化（顧客配布用）

PyInstaller の **onedir** で exe を組み、ターミナルを表示せずブラウザだけ開く配布用フォルダを作成できます。

### 配布フォルダの形

ビルド後の `backend\dist\TimeManagement\` をそのまま顧客に渡します。

```
dist/TimeManagement/
├── TimeManagement.exe    # これをダブルクリックで起動
├── 起動方法.txt
├── _app                  # hidden（companies / engine / web）
│   ├── companies/
│   ├── engine/
│   └── web/
├── _work                 # hidden（ジョブ入出力）
└── （PyInstaller のランタイムファイル）
```

### 初回ビルド手順

1. 仮想環境を用意し、依存を入れる。

   ```powershell
   cd backend
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   pip install pyinstaller
   ```

2. ビルドスクリプトを実行する（スクリプト内で仮想環境を有効化してから PyInstaller を実行します）。

   ```powershell
   .\build_exe.ps1
   ```

3. 出力は **`backend\dist\TimeManagement\`** です。このフォルダ一式を配布します。

### コードを変えたあとに exe をやり直すとき

中身のコード（`main.py`・`web/`・`engine/` など）を変更したら、exe を**作り直す**必要があります。

1. **古いビルド結果を消す**（任意だが推奨）

   ```powershell
   cd backend
   Remove-Item -Recurse -Force .\dist, .\build -ErrorAction SilentlyContinue
   ```

2. **再ビルド**

   ```powershell
   .\build_exe.ps1
   ```

3. 新しい **`dist\TimeManagement\`** を配布用として使います。

※ `build_exe.ps1` が `_app` に `companies`・`web`・`engine` をコピーするため、ソースを変えた内容は再ビルドで exe 配布物に反映されます。

### 起動・トラブル時

- **起動**: `TimeManagement.exe` をダブルクリック。しばらくするとブラウザで http://127.0.0.1:8000 が開きます。
- **接続できないとき**: exe と同じフォルダに `TimeManagement_error.log` ができていないか確認してください。

---

## 主なAPI（参考）

- `GET /api/companies` … 会社一覧  
- `GET /api/companies/{company}/devices` … 機種一覧  
- `POST /api/jobs` … ジョブ作成（PDF・対面・アルコールをアップロード）  
- `GET /api/jobs/{jobId}` … ジョブ状態・結果取得  
- `POST /api/jobs/{jobId}/revert-step` … 1つ前の画面に戻る  
- `POST /api/jobs/{jobId}/complete-merge` … 3時間未満のまとめ送信  
- `POST /api/jobs/{jobId}/complete-link-pairs` … 3時間以上の紐づけ送信  
- `POST /api/jobs/{jobId}/complete-codriver-link` … 同乗者紐づけ送信  
- `GET /api/jobs/{jobId}/download/excel` … Excelダウンロード  

---

## 注意事項

- **会社×機種のプリセット**は `backend/companies/<会社名>/<機種>.json` に配置します。  
- PDF抽出の本体は **`backend/engine/pipeline.py`** です。  
- ジョブの入出力は `backend/work/jobs/<jobId>/` に保存されます。

---

## ライセンス・利用

このリポジトリの利用条件はプロジェクト管理者に従ってください。
