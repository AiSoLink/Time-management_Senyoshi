# GitHub へプッシュする手順

リポジトリ名: **timemanagement_senyoshi**（タイムマネジメント_センヨシのアルファベット）

## 1. Git のユーザー情報を設定（初回のみ）

まだ設定していない場合、PowerShell で実行してください。

```powershell
git config --global user.email "あなたのGitHub用メールアドレス"
git config --global user.name "あなたの名前またはGitHubユーザー名"
```

## 2. 初回コミット

```powershell
cd "c:\Users\sawak\OneDrive\ドキュメント\開発関連\アプリ"
git commit -m "Initial commit: タイムマネジメント センヨシ"
```

## 3. GitHub にリポジトリを作成してプッシュ（GitHub CLI を使う場合）

GitHub にログイン済みなら、次のコマンドでリポジトリ作成とプッシュが一度にできます。

```powershell
gh repo create timemanagement_senyoshi --public --source=. --remote=origin --push
```

- 初回で `gh auth login` を求められたら、ブラウザで GitHub にログインしてください。

## 4. GitHub CLI を使わない場合

1. ブラウザで https://github.com/new を開く
2. Repository name に **timemanagement_senyoshi** を入力
3. 「Create repository」をクリック（README 等は追加しない）
4. 次のコマンドを実行してリモートを追加し、プッシュ

```powershell
git remote add origin https://github.com/あなたのGitHubユーザー名/timemanagement_senyoshi.git
git branch -M main
git push -u origin main
```

---

※ すでに `git add -A` と `.gitignore` の作成・`git init` は済んでいます。  
※ `backend/work/` と `frontend/node_modules/`、`.next` は .gitignore で除外しているため、リポジトリには含まれません。
