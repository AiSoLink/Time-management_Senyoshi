# 時間管理システム（デジタコ・アルコール紐づけ）

デジタコのPDFとアルコール検知データをアップロードすると、**運行ごとの集計Excel**を作成するWebアプリです。

---

## 📌 このREADMEの見方

- **「とりあえず動かしたい」** → [クイックスタート](#-クイックスタート) だけ見ればOKです。
- **「中身をいじって開発したい」** → [開発の始め方](#-開発の始め方) と [フォルダの役割](#-フォルダの役割) を読んでください。
- **「exeにして配布したい」** → [exe化の手順](#-exe化して配布する) を参照してください。

---

## 🎯 このアプリでできること（1行で）

**デジタコPDF・対面データ・アルコールデータを入れると、運行単位で集計したExcelを出してくれる。**

---

## ⚡ クイックスタート（最短で動かす）

「とりあえず画面を開いてみたい」ときは、次の2ステップです。

### 1. リポジトリを手元に用意する

```powershell
git clone https://github.com/AiSoLink/Time-management_Senyoshi.git
cd Time-management_Senyoshi
```

（すでにクローン済みの場合は、上記の `cd Time-management_Senyoshi` まででフォルダに入れればOKです。）

### 2. バックエンドを起動する

**PowerShell** を開き、次を実行します。

```powershell
cd backend
.\run_backend.ps1
```

- 初回は自動で「仮想環境」を作り、必要なライブラリを入れます（数分かかることがあります）。
- 問題なく起動すると、**http://127.0.0.1:8000** と表示されます。

### 3. ブラウザで開く

ブラウザのアドレス欄に **http://127.0.0.1:8000** を入力して開きます。

- 会社を選ぶ → 機種を選ぶ → PDF等をアップロード → 画面の指示に従って進めると、最後にExcelをダウンロードできます。

**以上で「動かす」ところまで完了です。**

---

## 🛠 開発の始め方（コードを触る人向け）

### 必要なもの

- **Python 3.10 以上**（3.11 推奨）
- **PowerShell**（Windows）
- **Git**（リポジトリの取得・更新用）

### 手順（コピペでOK）

```powershell
# 1. リポジトリのフォルダに入る
cd <このリポジトリをクローンしたパス>

# 2. バックエンド用フォルダに入る
cd backend

# 3. 仮想環境を作る（初回だけ）
python -m venv .venv

# 4. 仮想環境を有効にする
.\.venv\Scripts\Activate.ps1

# 5. ライブラリを入れる
pip install -r requirements.txt

# 6. サーバーを起動する（開発時はこのコマンドでOK）
python -m uvicorn main:app --reload --port 8000
```

6のあと、ブラウザで **http://127.0.0.1:8000** を開けばアプリが動きます。  
コードを変更すると、`--reload` の効果で自動で再読み込みされます。

### 起動用スクリプトを使う場合

```powershell
cd backend
.\run_backend.ps1
```

中で「仮想環境の有効化」と「uvicorn の起動」をまとめてやってくれます。ポートは 8000 です。

---

## 📂 フォルダの役割（どこに何があるか）

```
Time-management_Senyoshi/
├── README.md          ← いま読んでいるファイル
├── backend/           ← サーバー・処理の本体（Python）
│   ├── main.py        ← APIと画面配信の入口。ここをいじると挙動が変わる
│   ├── run_app.py     ← exe 用の起動スクリプト（開発時は使わない）
│   ├── run_backend.ps1
│   ├── build_exe.ps1  ← exe を作るときに実行する
│   ├── requirements.txt
│   ├── engine/        ← PDF解析・Excel作成・アルコール突合のロジック
│   │   ├── pipeline.py      ← メインの処理の流れ
│   │   └── alcohol_integration.py
│   ├── companies/     ← 会社ごと・機種ごとの設定（JSON）
│   └── work/          ← ジョブの一時ファイル（アップロード結果など）
├── web/               ← 画面のHTML/CSS/JavaScript（静的ファイル）
│   ├── index.html     ← 最初の画面（会社選択）
│   ├── job.html       ← 結果画面（紐づけ・手入力・ダウンロード）
│   └── ...
└── frontend/          ← 旧Next.js版（通常は使わない）
```

- **画面を変えたい** → `web/` の HTML/JS を編集
- **集計ロジックを変えたい** → `backend/engine/pipeline.py` など
- **会社・機種の設定を変えたい** → `backend/companies/<会社名>/` の JSON

---

## 📋 画面の流れ（ユーザーがやること）

1. **会社・機種を選ぶ**
2. **PDF・対面データ・アルコールデータをアップロードして「実行」**
3. **結果画面で順番に選択・入力**
   - **3時間未満**の運行を「1本にまとめるか」選ぶ
   - **3時間以上**空いている運行を「1本にまとめる」グループを選ぶ（任意）
   - **同乗者**（デジタコがなくアルコールだけの人）を、どの運行に紐づけるか選ぶ（任意）
   - **出庫・帰庫が取れていない行**があれば、手で日時を入力
4. **「Excelダウンロード」** で集計結果を取得

---

## 📦 exe化して配布する

「Pythonを入れていない人にも渡したい」ときは、exe にまとめます。

### やること（3ステップ）

1. **backend フォルダに移動**
   ```powershell
   cd backend
   ```

2. **ビルドスクリプトを実行**
   ```powershell
   .\build_exe.ps1
   ```
   - 初回は PyInstaller のインストールなどで時間がかかることがあります。

3. **できあがったフォルダをそのまま配布**
   - 場所: `backend\dist\TimeManagement\`
   - 中にある **TimeManagement.exe** をダブルクリックすると起動します。
   - 同じフォルダに **起動方法.txt** があるので、それも一緒に渡すと親切です。

### コードを直したあと、exe をやり直すとき

```powershell
cd backend
Remove-Item -Recurse -Force .\dist, .\build -ErrorAction SilentlyContinue
.\build_exe.ps1
```

新しい `dist\TimeManagement\` を再度配布用として使います。

### 起動できないとき

- exe と同じフォルダに **TimeManagement_error.log** ができていないか確認する
- ログにエラー内容が書いてあるので、それを手がかりに原因を調べられる

---

## ❓ よくあること・困ったとき

| 状況 | 確認すること |
|------|----------------|
| 起動しない | Python のバージョン（3.10以上か）、`cd backend` できているか |
| 画面が開かない | ブラウザで **http://127.0.0.1:8000** を開いているか（ポート 8000 で起動しているか） |
| 会社や機種が選べない | `backend/companies/` に該当する会社名フォルダと JSON があるか |
| Excel の項目を変えたい | `backend/engine/excel_headers.json` や `pipeline.py` の出力部分 |
| ジョブの途中結果を見たい | `backend/work/jobs/<jobId>/` にファイルができる |

---

## 🔧 技術メモ（開発者向け）

- **フロント**: 静的 HTML/CSS/JS（`web/`）。バックエンドが `/` で配信し、`/api/*` でAPIを提供。
- **バックエンド**: FastAPI + Uvicorn。PDF解析は pdfplumber、Excelは openpyxl。
- **主なAPI**: `POST /api/jobs`（ジョブ作成）、`GET /api/jobs/{jobId}`（状態取得）、`GET /api/jobs/{jobId}/download/excel`（Excel取得）など。

---

## ライセンス・利用

このリポジトリの利用条件はプロジェクト管理者に従ってください。
