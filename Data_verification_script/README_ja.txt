ターミナルで以下のコードでapp.py実行できます。

pkill -f "python .*app.py" 2>/dev/null || true
lsof -ti:5050 | xargs -I{} kill -9 {} 2>/dev/null || true
python app.py --host 0.0.0.0 --port 5050 --open

==============


Pythonコード別で実行する場合：

## ✅ まずは定番の実行例

### 1) データをダウンロード（DynamoDB + Timestream）
python Aws_integrated_downloader_organized.py   --user "azlm-prd-004@01ive.co.jp"   --company "azlm-prd"   --start "2025-09-04 23:00:00"   --end   "2025-09-04 23:30:00"

※ 片方だけ取得する場合
# DynamoDBのみ
python Aws_integrated_downloader_organized.py   --user "azlm-prd-004@01ive.co.jp"   --start "2025-09-04 23:00:00"   --end   "2025-09-04 23:30:00"

# Timestreamのみ
python Aws_integrated_downloader_organized.py   --company "azlm-prd"   --start "2025-09-04 23:00:00"   --end   "2025-09-04 23:30:00"

### 2) ダウンロードしたフォルダを解析（日本語レポートを作成）
python post_download_analysis.py   --input-folder "azlm-prd_20250904_requested_20250908_131751"   --config "analysis_config_ja.json"   --step 10

※ 必要に応じて少量の CSV も出したいとき：
python post_download_analysis.py   --input-folder "azlm-prd_20250904_requested_20250908_131751"   --config "analysis_config_ja.json"   --step 10   --emit-csv

※ 個別に CSV を指定したいとき：
python post_download_analysis.py   --timestream "azlm-prd_20250904_requested_20250908_131751/azlm-prd_timestream.csv"   --dynamodb  "azlm-prd_20250904_requested_20250908_131751/azlm-prd_dynamodb.csv"   --config    "analysis_config_ja.json"   --step 10

---

## 📄 解析の出力（最重要）
<対象フォルダ>/analysis_outputs/ に生成：
- analysis_report.txt … 日本語の総合レポート（これだけ見ればOK）
- analysis_summary.json … 機械可読なサマリ
（--emit-csv を付けた場合のみ、time gaps / four_types / 性別分布 / 年齢分布の CSV を最小限で追加）

---

## 🗂 ルート直下の主なファイル
- Aws_integrated_downloader_organized.py … AWS から DynamoDB / Timestream を取得し、<company>_<yyyymmdd>_requested_<時刻>/ に CSV を保存
- post_download_analysis.py … ダウンロード済み CSV を読み込み、analysis_report.txt（日本語）を出力。--config で data 内の属性（性別/眼鏡/髭/口の開閉…）も集計
- analysis_config_ja.json … 抽出属性の設定（name / keys / type / value_key）
- shibo-chen_accessKeys.csv … AWS 認証情報（列：Access key ID, Secret access key）
- azlm-prd_20250904_requested_20250908_131751/ … ダウンロードされた CSV と analysis_outputs/
- __pycache__/ … Python キャッシュ（無視可）

---

## 🔧 ワンポイント
- --config が認識されない → 解析スクリプトが旧版。最新版で上書き
- 連続性 --step は未指定でも自動推定可
- analysis_report.txt に 性別分布・年齢統計・設定属性の比率 まで全て展開
