#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import glob
import time
import csv
import socket
import platform
import subprocess
import re
import ast
import webbrowser
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify, send_file, render_template, abort

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADER = os.path.join(BASE_DIR, "Aws_integrated_downloader_organized.py")
ANALYZER = os.path.join(BASE_DIR, "post_download_analysis.py")

app = Flask(__name__, template_folder='templates', static_folder='static')

ARTIFACTS: Dict[str, str] = {}

HISTORY_PATH = os.path.join(BASE_DIR, "ui_history.json")

def _load_history():
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_history(data):
    try:
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

@app.get("/api/history")
def api_history_get():
    key = request.args.get("key") or ""
    data = _load_history()
    vals = data.get(key, [])
    return jsonify({"key": key, "values": vals})

@app.post("/api/history")
def api_history_put():
    body = request.get_json(force=True)
    key = body.get("key")
    val = (body.get("value") or "").strip()
    if not key or not val:
        return jsonify({"ok": False})
    data = _load_history()
    lst = data.get(key, [])
    lst = [v for v in lst if v != val]
    lst.insert(0, val)
    data[key] = lst[:12]
    _save_history(data)
    return jsonify({"ok": True})


def _print_hyperlink(url: str, label: str = None) -> str:
    if not label:
        label = url
    ESC = "\033"
    return f"{ESC}]8;;{url}{ESC}\\{label}{ESC}]8;;{ESC}\\"

def _shell(cmd: List[str], cwd: Optional[str] = None) -> Dict[str, Any]:
    proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = proc.communicate()
    return {"code": proc.returncode, "stdout": out, "stderr": err}

def _newest_folder_by_company(company: str) -> Optional[str]:
    if not company:
        return None
    pattern = os.path.join(BASE_DIR, f"{company}_*_requested_*")
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

def _newest_folder_any() -> Optional[str]:
    pattern = os.path.join(BASE_DIR, "*_requested_*")
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return os.path.basename(candidates[0])

def _register_artifact(path: str) -> str:
    path = os.path.abspath(path)
    if not os.path.exists(path):
        return ""
    token = f"{int(time.time()*1000)}_{len(ARTIFACTS)+1}"
    ARTIFACTS[token] = path
    return token

def _read_text(path: str, limit: int = 2_000_000) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read(limit)
    return data


def _try_read_gender_dist_csv(csv_path: str):
    if not os.path.exists(csv_path):
        return None
    try:
        out = {}
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            import csv as _csv
            r = _csv.DictReader(f)
            for row in r:
                key = (row.get("gender") or row.get("label") or row.get("key") or "").strip()
                val = row.get("count") or row.get("value") or row.get("freq") or row.get("数量") or "0"
                try:
                    cnt = int(float(str(val).strip()))
                except Exception:
                    cnt = 0
                if key:
                    out[key] = out.get(key, 0) + cnt
        return out or None
    except Exception:
        return None

def _try_read_four_types_csv(csv_path: str):
    if not os.path.exists(csv_path):
        return None
    try:
        out = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            import csv as _csv
            r = _csv.DictReader(f)
            for row in r:
                v = row.get("value") or row.get("label") or row.get("カテゴリ")
                c = row.get("count") or row.get("value_count") or row.get("freq") or row.get("数量")
                if v is None or c is None:
                    continue
                item = {"value": str(v).strip()}
                try: 
                    item["count"] = float(str(c).replace("%","").strip())
                    out.append(item)
                except: 
                    pass
        return out or None
    except Exception:
        return None

def _try_read_age_buckets_csv(csv_path: str):
    if not os.path.exists(csv_path):
        return None
    try:
        arr = []
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            import csv as _csv
            r = _csv.DictReader(f)
            for row in r:
                label = (row.get("bucket") or row.get("label") or "").strip()
                if not label:
                    low = row.get("low") or row.get("start") or row.get("from")
                    high = row.get("high") or row.get("end") or row.get("to")
                    if low or high:
                        label = f"{low}-{high}"
                val = row.get("count") or row.get("value") or row.get("freq") or "0"
                try: cnt = int(float(str(val).strip()))
                except: cnt = 0
                if label:
                    arr.append({"label": label, "count": cnt})
        return arr or None
    except Exception:
        return None

@app.get("/")
def index():
    now = datetime.now().replace(microsecond=0)
    start = (now - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    end = now.strftime("%Y-%m-%d %H:%M:%S")
    ctx = dict(
        rev="Test with charts v1 - 20250911",
        def_user="azlm-prd-004@01ive.co.jp",
        def_company="azlm-prd",
        def_start=start,
        def_end=end,
        def_timezone="9",
        def_region="ap-northeast-1",
        def_keypath="shibo-chen_accessKeys.csv",
        def_step="10",
        def_config="analysis_config_ja.json",
        def_folder=_newest_folder_any() or ""
    )
    return render_template("index.html", **ctx)

@app.post("/api/download_analyze")
def api_download_analyze():
    data = request.get_json(force=True)
    user = data.get("user") or ""
    company = data.get("company") or ""
    start = data.get("start") or ""
    end = data.get("end") or ""
    timezone = data.get("timezone") or "9"
    region = data.get("region") or "ap-northeast-1"
    keypath = data.get("keypath") or "shibo-chen_accessKeys.csv"
    step = data.get("step") or None
    config = data.get("config") or None
    emit_csv = bool(data.get("emit_csv"))

    if not os.path.exists(DOWNLOADER):
        return jsonify({"ok": False, "error": "Downloader script not found."})
    if not company and not user:
        return jsonify({"ok": False, "error": "company or user is required."})
    if not start or not end:
        return jsonify({"ok": False, "error": "start and end are required."})

    cmd = ["python", DOWNLOADER, "--start", start, "--end", end]
    if user: cmd += ["--user", user]
    if company: cmd += ["--company", company]
    if timezone: cmd += ["--timezone", str(timezone)]
    if region: cmd += ["--region", str(region)]
    if keypath: cmd += ["--csv-key-path", keypath]

    dres = _shell(cmd, cwd=BASE_DIR)
    if dres["code"] != 0:
        return jsonify({"ok": False, "error": "Download failed.", "stderr": dres["stderr"], "stdout": dres["stdout"]})

    target_folder = _newest_folder_by_company(company or (user.split("@")[0] if user else ""))
    if not target_folder or not os.path.exists(target_folder):
        return jsonify({"ok": False, "error": "Output folder not found.", "stdout": dres["stdout"]})

    return _run_analyzer_common(target_folder, step, config, emit_csv)

@app.post("/api/analyze_only")
def api_analyze_only():
    data = request.get_json(force=True)
    folder = data.get("folder") or ""
    step = data.get("step") or None
    config = data.get("config") or None
    emit_csv = bool(data.get("emit_csv"))

    if not folder:
        return jsonify({"ok": False, "error": "Folder is required."})
    abs_folder = os.path.join(BASE_DIR, folder) if not os.path.isabs(folder) else folder
    if not os.path.exists(abs_folder):
        return jsonify({"ok": False, "error": f"Folder not found: {abs_folder}"})
    return _run_analyzer_common(abs_folder, step, config, emit_csv)

def _augment_summary_from_csvs(out_dir: str, summary_obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    summary_obj = summary_obj or {}
    # four_types
    ft_csv = os.path.join(out_dir, "timestream_four_types_percentage.csv")
    if os.path.exists(ft_csv):
        dist = _try_read_four_types_csv(ft_csv)
        if dist: summary_obj["four_types_distribution"] = dist

    if "dynamodb_json" not in summary_obj: summary_obj["dynamodb_json"] = {}
    # gender
    gd_csv = os.path.join(out_dir, "dynamodb_gender_distribution.csv")
    if os.path.exists(gd_csv):
        gdist = _try_read_gender_dist_csv(gd_csv)
        if gdist: summary_obj["dynamodb_json"]["gender_distribution"] = gdist

    # age buckets
    ab_csv = os.path.join(out_dir, "dynamodb_age_buckets.csv")
    if os.path.exists(ab_csv):
        ab = _try_read_age_buckets_csv(ab_csv)
        if ab: summary_obj["dynamodb_json"]["age_buckets"] = ab
    return summary_obj
  
def _calculate_age_distribution(dynamodb_csv_path: str) -> Optional[Dict[str, List[Any]]]:
    if not os.path.exists(dynamodb_csv_path):
        return None
    print("reading:", dynamodb_csv_path)
    import csv, json, re, ast
    range_counts = {}   # 每岁计数
    low_counts  = {}    # 作为 Low 端点次数
    high_counts = {}    # 作为 High 端点次数
    min_age = float('inf')
    max_age = float('-inf')
    has_data = False

    try:
        with open(dynamodb_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data_str = row.get("data")
                if not data_str:
                    continue
                try:
                    # 1. 单引号→双引号
                    data_str = data_str.replace("'", '"')
                    # 2. 去掉 Decimal("42") → 42
                    data_str = re.sub(r'Decimal\(["\"](\d+)["\"]\)', r'\1', data_str)
                    # 3. JSON 解析
                    data_obj = json.loads(data_str)
                except Exception as e:
                    #print("data 解析失败:", data_str, e)
                    continue

                # 4. 提取 age_range
                age_range_obj = data_obj[0].get("age_range")
                if not age_range_obj:
                    continue
                low = int(age_range_obj.get("Low", 0))
                high = int(age_range_obj.get("High", 0))
                if low > high:
                    continue

                has_data = True
                min_age = min(min_age, low)
                max_age = max(max_age, high)

                # 5. 区间内每岁 +1
                for a in range(low, high + 1):
                    range_counts[a] = range_counts.get(a, 0) + 1
                # 6. 端点统计
                low_counts[low]  = low_counts.get(low, 0) + 1
                high_counts[high] = high_counts.get(high, 0) + 1

        if not has_data:
            print("没有提取到任何 age_range 数据")
            return None

        # 7. 组装返回
        ages = sorted(set(range_counts) | set(low_counts) | set(high_counts))
        return {
            "labels": ages,
            "data": [range_counts.get(a, 0) for a in ages],
            "low":  [low_counts.get(a, 0)  for a in ages],
            "high": [high_counts.get(a, 0) for a in ages],
        }

    except Exception as e:
        print("读取 CSV 异常:", e)
        return None


def _run_analyzer_common(target_folder: str, step: Optional[str], config: Optional[str], emit_csv: bool):
    if not os.path.exists(ANALYZER):
        return jsonify({"ok": False, "error": "Analyzer script not found."})
    
    cmd = ["python", ANALYZER, "--input-folder", target_folder]
    if step: cmd += ["--step", str(step)]
    if config: cmd += ["--config", config]
    if emit_csv: cmd += ["--emit-csv"]
    
    ares = _shell(cmd, cwd=BASE_DIR)
    if ares["code"] != 0:
        return jsonify({"ok": False, "error": "Analysis failed.", "stderr": ares["stderr"], "stdout": ares["stdout"]})

    out_dir = os.path.join(target_folder, "analysis_outputs")
    report_path = os.path.join(out_dir, "analysis_report.txt")
    summary_path = os.path.join(out_dir, "analysis_summary.json")

    report_text = ""
    if os.path.exists(report_path):
        try: report_text = _read_text(report_path)
        except Exception: report_text = "(Error reading report)"

    summary_obj = {}
    if os.path.exists(summary_path):
        try:
            with open(summary_path, "r", encoding="utf-8") as _f:
                summary_obj = json.load(_f)
        except Exception:
            pass

    summary_obj = _augment_summary_from_csvs(out_dir, summary_obj)

    dynamo_csv_files = glob.glob(os.path.join(target_folder, "*_dynamodb.csv"))
    print("↓↓↓ 进入 age_distribution 逻辑")          # ← 加这行
    print("↓↓↓ 扫描到的 DynamoDB CSV:", dynamo_csv_files)
    age_dist_curve = None
    if dynamo_csv_files:
        print("↓↓↓ 即将读取:", dynamo_csv_files[0])   # ← 加这行
        age_dist_curve = _calculate_age_distribution(dynamo_csv_files[0])
    
    if age_dist_curve:
        if "dynamodb_json" not in summary_obj:
             summary_obj["dynamodb_json"] = {}
        summary_obj["dynamodb_json"]["age_distribution_curve"] = age_dist_curve
    elif summary_obj.get("dynamodb_json", {}).get("age_buckets"):
        pass


    files = []
    for p in [report_path, summary_path]:
        if os.path.exists(p):
            files.append({"id": _register_artifact(p), "name": os.path.basename(p)})

    return jsonify({"ok": True, "report_text": report_text, "files": files, "summary": summary_obj})

@app.get("/download/<token>")
def download_file(token):
    path = ARTIFACTS.get(token)
    if not path or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=False, download_name=os.path.basename(path))

if __name__ == "__main__":
    import argparse, os, webbrowser

    # 读取 AWS 注入的 PORT 环境变量（若不存在则用 5050 本地调试）
    env_port = int(os.environ.get("PORT", 5050))

    parser = argparse.ArgumentParser(description="Local web UI for download & analysis")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default 0.0.0.0 for external access)")
    parser.add_argument("--port", type=int, default=env_port, help="Port to bind")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    parser.add_argument("--open", action="store_true", help="Open the URL in your browser")
    args = parser.parse_args()

    if args.open:
        host_for_browser = "localhost" if args.host in ("0.0.0.0", "127.0.0.1") else args.host
        webbrowser.open_new(f"http://{host_for_browser}:{args.port}")

    app.run(host=args.host, port=args.port, debug=args.debug)


