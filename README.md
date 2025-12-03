# 📦 Parquet Split / Merge / Delete Pipeline (AWS Glue + Lambda)

本リポジトリは、**大規模な Parquet データを item_id 単位に分割 → マージ → 削除**するための  
AWS Glue（PythonShell / Ray）および AWS Lambda のサンプル実装となる.

Glue ジョブはすべて **CDK（TypeScript）で構築**し、処理ロジックは **Python** で記述.

---

## 機能概要

本リポジトリは次の 3 つの機能で構成されている

---

### 1. Parquet Splitter（Glue PythonShell / Ray）

**入力 Parquet（例: `YYYYMMDD.parquet`）を `item_id` ごとに分割し、  
分割後のファイルと “完了マーカー JSON” を S3 に出力する Glue ジョブ.**

- PythonShell と Ray の 2 種類のジョブが生成される（どちらも同じ処理）
- 出力構造例：

```text
data/
  input/pyshell/20250101.parquet
  split/pyshell/0000000001/20250101.parquet
  split/pyshell/0000000002/20250101.parquet
  ...
  markers/pyshell/20250101.json
```

---

### 2. Parquet Merger（Glue Ray）

複数の Parquet を Ray で並列に読み込み、DataFrame で結合して CSV を生成する Glue ジョブ.

---

### 3. Parquet Deleter（Lambda）

Splitter が生成した完了マーカー JSON を読み込み、記録されている Parquet を一括削除する Lambda 関数.

---

##  処理フロー

1. Splitter Glue → Parquet を item_id ごとに分割  
2. Merger Glue → 複数日付・複数 item_id の Parquet を CSV に統合  
3. Deleter Lambda → Splitter が生成した Parquet を削除（marker JSON の outputs を使用）
