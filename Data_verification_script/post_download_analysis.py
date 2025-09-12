#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析用
■ 使い方
python post_download_analysis.py --input-folder "/path/to/folder" --step 10 --config "analysis_config_ja.json"
python post_download_analysis.py --timestream "..._timestream.csv" --dynamodb "..._dynamodb.csv" --config "analysis_config_ja.json"
"""

import os, re, json, ast, argparse
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
import pandas as pd

# ================== ユーティリティ ==================

def _ensure_output_dir(base_folder: str) -> str:
    out_dir = os.path.join(base_folder, "analysis_outputs")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def _fmt_ts(x) -> str:
    try:
        if pd.isna(x):
            return "NaT"
        return pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(x)

def _normalize_decimal_literals(s: str) -> str:
    return re.sub(r"Decimal\(\s*['\"]?([0-9eE+\-\.]+)['\"]?\s*\)", r"\1", s)

def _parse_json_cell(cell):
    """JSON / Python 風文字列の頑健パーサ"""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return None
    s = str(cell).strip()
    if not s:
        return None
    s = _normalize_decimal_literals(s)
    # strict json
    try:
        return json.loads(s)
    except Exception:
        pass
    # normalized json
    s2 = s.replace("'", '"')
    s2 = re.sub(r'\bTrue\b', 'true', s2)
    s2 = re.sub(r'\bFalse\b', 'false', s2)
    s2 = re.sub(r'\bNone\b', 'null', s2)
    try:
        return json.loads(s2)
    except Exception:
        pass
    # literal_eval
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, (dict, list)):
            return obj
    except Exception:
        pass
    return None

# ================== 1) time 連続性 ==================

def _infer_expected_step_seconds(times: pd.Series) -> Optional[int]:
    s = pd.to_datetime(times, errors="coerce").dropna().sort_values().reset_index(drop=True)
    if len(s) < 2: return None
    diffs = (s.diff().dropna().dt.total_seconds().round().astype("Int64"))
    if diffs.empty: return None
    vc = diffs[diffs > 0].value_counts()
    return int(vc.index[0]) if len(vc) > 0 else None

def analyze_time_continuity(ts_df: Optional[pd.DataFrame],
                            expected_step_seconds: Optional[int] = None) -> Dict[str, Any]:
    if ts_df is None or ts_df.empty or "time" not in ts_df.columns:
        return {"rows": 0, "message": "Timestream の time 列が見つかりません。"}
    tt = ts_df.copy()
    tt["time"] = pd.to_datetime(tt["time"], errors="coerce")
    tt = tt.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    step = expected_step_seconds or _infer_expected_step_seconds(tt["time"])
    result = {"rows": int(len(tt)), "expected_step_seconds": step}

    if step is None or len(tt) < 2:
        result["message"] = "サンプル数が不足 or 期待ステップの推定不可"
        return result

    diffs = tt["time"].diff().dt.total_seconds().fillna(0)
    gap_mask = diffs > step
    gaps: List[Dict[str, Any]] = []
    for i in range(1, len(tt)):
        if gap_mask.iloc[i]:
            prev_t = tt["time"].iloc[i-1]
            next_t = tt["time"].iloc[i]
            gap_sec = int(diffs.iloc[i])
            missing = max(int(round(gap_sec/step)) - 1, 0)
            gaps.append({
                "prev_time": _fmt_ts(prev_t),
                "next_time": _fmt_ts(next_t),
                "gap_seconds": gap_sec,
                "missing_points_est": missing
            })

    total_expected = 0
    if len(tt) >= 2:
        total_period_sec = int((tt["time"].iloc[-1] - tt["time"].iloc[0]).total_seconds())
        total_expected = max(int(round(total_period_sec/step)) + 1, len(tt))

    result.update({
        "first_time": _fmt_ts(tt["time"].iloc[0]),
        "last_time": _fmt_ts(tt["time"].iloc[-1]),
        "observed_points": int(len(tt)),
        "expected_points_est": int(total_expected),
        "gap_count": int(len(gaps)),
        "missing_points_total_est": int(sum(g["missing_points_est"] for g in gaps)),
        "continuity_ratio_est": float(len(tt) / total_expected) if total_expected > 0 else 1.0,
        "gaps": gaps,
    })
    return result

# ================== 2) four_types ==================

def analyze_four_types_percent(ts_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if ts_df is None or ts_df.empty or "four_types" not in ts_df.columns:
        return {"rows": 0, "message": "Timestream の four_types 列が見つかりません。"}
    tt = ts_df.copy()
    tt["four_types"] = pd.to_numeric(tt["four_types"], errors="coerce")
    total = len(tt)
    vc = tt["four_types"].value_counts(dropna=False).sort_index()
    pct = (vc / total * 100.0).round(2)
    dist = [{"four_types": int(idx) if pd.notna(idx) else None,
             "count": int(vc.loc[idx]),
             "percent": float(pct.loc[idx])} for idx in vc.index]
    return {"rows": int(total), "unique_values": int(vc.shape[0]), "distribution": dist}

# ================== 3) DynamoDB: 性別・年齢 ==================

def _recursive_collect_gender_age(obj) -> Tuple[Optional[str], Optional[float]]:
    gender = None
    ages: List[float] = []
    def rec(x):
        nonlocal gender, ages
        if isinstance(x, dict):
            for k,v in x.items():
                kl = str(k).lower()
                # gender
                if kl in ("gender", "sex", "性別", "性别"):
                    if isinstance(v, dict) and "Value" in v:
                        val = v.get("Value")
                        if val is not None: gender = str(val)
                    elif v is not None:
                        gender = str(v)
                if k == "Gender" and isinstance(v, dict):
                    val = v.get("Value")
                    if val is not None: gender = str(val)
                # age
                if kl in ("age", "age_years", "agey", "years", "年齢", "年龄"):
                    try:
                        ages.append(float(v))
                    except Exception:
                        m = re.search(r'(\d+(\.\d+)?)', str(v))
                        if m: ages.append(float(m.group(1)))
                if (k == "AgeRange" or k == "age_range") and isinstance(v, dict):
                    low = v.get("Low") if "Low" in v else v.get("low")
                    high = v.get("High") if "High" in v else v.get("high")
                    try:
                        if low is not None and high is not None:
                            ages.append((float(low) + float(high)) / 2.0)
                        elif low is not None:
                            ages.append(float(low))
                        elif high is not None:
                            ages.append(float(high))
                    except Exception:
                        pass
                rec(v)
        elif isinstance(x, list):
            for it in x: rec(it)
    rec(obj)
    if gender is not None:
        g = str(gender).strip().lower()
        gender = {"m":"Male","male":"Male","man":"Male","男性":"Male",
                  "f":"Female","female":"Female","woman":"Female","女性":"Female"}.get(g, gender)
    age = None
    if ages:
        try:
            s = sorted([a for a in ages if a is not None])
            mid = len(s)//2
            age = float(s[mid] if len(s)%2==1 else (s[mid-1]+s[mid])/2)
        except Exception:
            age = float(ages[0])
    return gender, age

def analyze_dynamodb_json(dynamo_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if dynamo_df is None or dynamo_df.empty or "data" not in dynamo_df.columns:
        return {"rows": 0, "message": "DynamoDB の data 列が見つかりません。"}
    dd = dynamo_df.copy()

    genders: List[Optional[str]] = []
    ages: List[Optional[float]] = []
    for cell in dd["data"]:
        obj = _parse_json_cell(cell)
        if obj is None:
            genders.append(None); ages.append(None)
            continue
        g, a = _recursive_collect_gender_age(obj)
        genders.append(g); ages.append(a)

    dd["parsed_gender"] = genders
    dd["parsed_age"] = ages

    total = len(dd)
    # gender distribution
    g_counts = dd["parsed_gender"].value_counts(dropna=False)
    gender_dist = []
    for val, cnt in g_counts.items():
        name = str(val) if pd.notna(val) else "NA"
        pct = round(cnt/total*100.0, 2) if total>0 else 0.0
        gender_dist.append({"gender": name, "count": int(cnt), "percent": pct})

    # age stats/buckets
    age_series = pd.to_numeric(dd["parsed_age"], errors="coerce").dropna()
    age_stats = {
        "rows": int(total),
        "parsed_ok_rows": int((dd["parsed_gender"].notna() | dd["parsed_age"].notna()).sum()),
        "age_non_null": int(age_series.shape[0]),
        "age_min": float(age_series.min()) if not age_series.empty else None,
        "age_max": float(age_series.max()) if not age_series.empty else None,
        "age_mean": float(age_series.mean()) if not age_series.empty else None,
        "age_median": float(age_series.median()) if not age_series.empty else None
    }
    age_buckets = []
    if not age_series.empty:
        ages_int = age_series.apply(lambda x: int(float(x)))
        ages_int = ages_int[(ages_int >= 0) & (ages_int <= 120)]
        vc = ages_int.value_counts().sort_index()
        for age, cnt in vc.items():
            age_buckets.append({
                "age_bucket": f"{int(age):02d}",
                "count": int(cnt),
                "percent": round(cnt/total*100.0, 2) if total > 0 else 0.0
            })


    return {
        "rows": int(total),
        "gender_distribution": gender_dist,
        "age_stats": age_stats,
        "age_buckets": age_buckets,
        # sample preview
        "preview": dd[["parsed_gender","parsed_age"]].head(5).to_dict(orient="records")
    }

# ================== 4) コンフィグ属性（レポート埋め込み） ==================

def _cfg_load(path: Optional[str]) -> Optional[dict]:
    if not path: return None
    if not os.path.exists(path): return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _get_by_path(obj, dotted_key: str):
    parts = [p for p in re.split(r'\.', dotted_key) if p]
    found = []
    def rec(x, seg_idx=0):
        if seg_idx >= len(parts):
            found.append(x); return
        key = parts[seg_idx].lower()
        if isinstance(x, dict):
            for k,v in x.items():
                if str(k).lower() == key:
                    rec(v, seg_idx+1)
            for v in x.values():
                rec(v, seg_idx)
        elif isinstance(x, list):
            for it in x:
                rec(it, seg_idx)
    rec(obj, 0)
    return None if not found else found[0]

def _find_any_key(obj, key_names):
    targets = [k.lower() for k in key_names]
    found = []
    def rec(x):
        if isinstance(x, dict):
            for k,v in x.items():
                if str(k).lower() in targets:
                    found.append(v)
                rec(v)
        elif isinstance(x, list):
            for it in x:
                rec(it)
    rec(obj)
    return None if not found else found[0]

def _cfg_extract_attr(obj, attr_cfg: dict):
    keys = attr_cfg.get("keys") or []
    typ = (attr_cfg.get("type") or "bool").lower()
    value_key = attr_cfg.get("value_key")
    value = None
    dotted = [k for k in keys if "." in k]
    normal = [k for k in keys if "." not in k]
    for dk in dotted:
        v = _get_by_path(obj, dk)
        if v is not None:
            value = v; break
    if value is None and normal:
        v = _find_any_key(obj, normal)
        if v is not None: value = v
    if isinstance(value, dict) and value_key and value_key in value:
        value = value[value_key]
    if typ == "bool":
        if isinstance(value, bool): return value
        if isinstance(value, (int, float)): return bool(value)
        if isinstance(value, str):
            vv = value.strip().lower()
            if vv in ("true","t","1","yes","y","on"): return True
            if vv in ("false","f","0","no","n","off"): return False
        return None
    elif typ == "number":
        try: return float(value)
        except Exception: return None
    else:
        return None if value is None else str(value)

def analyze_with_config(dynamo_df: Optional[pd.DataFrame], cfg: Optional[dict]) -> Dict[str, Any]:
    if dynamo_df is None or dynamo_df.empty or "data" not in dynamo_df.columns:
        return {"enabled": False, "message": "DynamoDB の data 列が見つかりません。"}
    if not cfg or "attributes" not in cfg or not isinstance(cfg["attributes"], list):
        return {"enabled": False}

    dd = dynamo_df.copy()
    total = len(dd)
    result = {"enabled": True, "attributes": []}

    for attr in cfg["attributes"]:
        name = attr.get("name")
        if not name: continue
        typ = (attr.get("type") or "bool").lower()
        vals = []
        for cell in dd["data"]:
            obj = _parse_json_cell(cell)
            vals.append(_cfg_extract_attr(obj, attr) if obj is not None else None)
        s = pd.Series(vals)

        if typ == "bool":
            vc = s.value_counts(dropna=False)
            rows = [{"value": str(k) if pd.notna(k) else "NA",
                     "count": int(v),
                     "percent": round(v/total*100.0, 2) if total>0 else 0.0}
                    for k,v in vc.items()]
            result["attributes"].append({"name": name, "type": "bool", "summary": rows})
        elif typ == "number":
            sn = pd.to_numeric(s, errors="coerce").dropna()
            stats = {
                "rows": int(total),
                "non_null": int(sn.shape[0]),
                "min": float(sn.min()) if not sn.empty else None,
                "max": float(sn.max()) if not sn.empty else None,
                "mean": float(sn.mean()) if not sn.empty else None,
                "median": float(sn.median()) if not sn.empty else None
            }
            result["attributes"].append({"name": name, "type": "number", "summary": stats})
        else:  # categorical
            sc = s.dropna().astype(str)
            vc = sc.value_counts()
            rows = [{"value": k, "count": int(v),
                     "percent": round(v/total*100.0, 2) if total>0 else 0.0}
                    for k,v in vc.items()]
            result["attributes"].append({"name": name, "type": "categorical", "summary": rows})

    return result

# ================== 実行器 ==================

def _resolve_paths(input_folder: Optional[str], timestream_csv: Optional[str], dynamodb_csv: Optional[str]):
    if input_folder:
        ts_path = None; dy_path = None
        for fn in os.listdir(input_folder):
            if fn.endswith("_timestream.csv") and ts_path is None: ts_path = os.path.join(input_folder, fn)
            if fn.endswith("_dynamodb.csv") and dy_path is None: dy_path = os.path.join(input_folder, fn)
        if timestream_csv: ts_path = timestream_csv
        if dynamodb_csv: dy_path = dynamodb_csv
        base_dir = input_folder
    else:
        ts_path = timestream_csv
        dy_path = dynamodb_csv
        base_dir = os.path.dirname(ts_path) if ts_path else os.path.dirname(dy_path)
    return ts_path, dy_path, base_dir

def run_analysis(input_folder: Optional[str] = None,
                 timestream_csv: Optional[str] = None,
                 dynamodb_csv: Optional[str] = None,
                 expected_step_seconds: Optional[int] = None,
                 config_path: Optional[str] = None,
                 emit_csv: bool = False) -> Dict[str, Any]:

    ts_path, dy_path, base_dir = _resolve_paths(input_folder, timestream_csv, dynamodb_csv)
    if not base_dir:
        raise ValueError("入力フォルダを特定できません。input-folder か CSV パスを指定してください。")
    out_dir = _ensure_output_dir(base_dir)

    ts_df = pd.read_csv(ts_path) if ts_path and os.path.exists(ts_path) else None
    dy_df = pd.read_csv(dy_path) if dy_path and os.path.exists(dy_path) else None
    cfg = _cfg_load(config_path)

    time_cont = analyze_time_continuity(ts_df, expected_step_seconds)
    four_types = analyze_four_types_percent(ts_df)
    dyn_main = analyze_dynamodb_json(dy_df)
    dyn_cfg = analyze_with_config(dy_df, cfg)

    report = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "output_dir": out_dir,
        "timestream_csv": ts_path,
        "dynamodb_csv": dy_path,
        "time_continuity": time_cont,
        "four_types": four_types,
        "dynamodb_json": dyn_main,
        "dynamodb_cfg": dyn_cfg
    }

    # ---- レポート作成（CSVに頼らず、中身を直接埋め込む） ----
    report_txt = os.path.join(out_dir, "analysis_report.txt")
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write("=== ダウンロード後解析レポート ===\n")
        f.write(f"生成日時: {report['generated_at']}\n\n")

        # [1] time
        tc = time_cont
        f.write("[1] Timestream データの時間連続性\n")
        f.write(f"- データ件数: {tc.get('rows')}\n")
        f.write(f"- 期待ステップ秒: {tc.get('expected_step_seconds')}\n")
        f.write(f"- 開始時刻: {tc.get('first_time')}\n")
        f.write(f"- 終了時刻: {tc.get('last_time')}\n")
        f.write(f"- 観測点数: {tc.get('observed_points')}\n")
        f.write(f"- 推定期待点数: {tc.get('expected_points_est')}\n")
        f.write(f"- ギャップ数: {tc.get('gap_count')}\n")
        f.write(f"- 推定欠損点数: {tc.get('missing_points_total_est')}\n")
        f.write(f"- 連続率(概算): {round(tc.get('continuity_ratio_est',0)*100,1)} %\n")
        gaps = tc.get("gaps") or []
        if gaps:
            f.write("  ・ギャップ一覧（先頭10件）：\n")
            for g in gaps[:10]:
                f.write(f"    - prev={g['prev_time']}, next={g['next_time']}, gap_s={g['gap_seconds']}, missing≈{g['missing_points_est']}\n")
        f.write("\n")

        # [2] four_types
        ft = four_types
        f.write("[2] four_types 列の分布\n")
        f.write(f"- 行数: {ft.get('rows')}, ユニーク値: {ft.get('unique_values')}\n")
        dist = ft.get("distribution") or []
        if dist:
            for row in dist:
                f.write(f"  - value={row['four_types']}, count={row['count']}, percent={row['percent']}%\n")
        f.write("\n")

        # [3] gender/age
        dj = dyn_main
        f.write("[3] DynamoDB data 列（性別・年齢）\n")
        f.write(f"- 全件数: {dj.get('rows')}\n")
        f.write("  ・性別分布：\n")
        for r in dj.get("gender_distribution") or []:
            f.write(f"    - {r['gender']}: {r['count']}件 ({r['percent']}%)\n")
        astats = dj.get("age_stats") or {}
        f.write("  ・年齢統計：\n")
        f.write(f"    - 最小: {astats.get('age_min')}, 最大: {astats.get('age_max')}, 平均: {astats.get('age_mean')}, 中央値: {astats.get('age_median')}\n")
        buckets = dj.get("age_buckets") or []
        if buckets:
            f.write("  ・年齢分布（1歳刻み）：\n")
            for b in buckets:
                f.write(f"    - {b['age_bucket']}: {b['count']}件 ({b['percent']}%)\n")
        prev = dj.get("preview") or []
        if prev:
            f.write("  ・解析プレビュー（先頭5行）：\n")
            for i, row in enumerate(prev, 1):
                f.write(f"    - #{i}: gender={row.get('parsed_gender')}, age={row.get('parsed_age')}\n")
        f.write("\n")

        # [4] config attributes
        dc = dyn_cfg
        f.write("[4] DynamoDB data 列（コンフィグ属性の集計）\n")
        if dc.get("enabled"):
            for a in dc.get("attributes") or []:
                name = a.get("name"); typ = a.get("type")
                f.write(f"- 属性: {name}（{typ}）\n")
                if typ in ("bool", "categorical"):
                    for r in a.get("summary") or []:
                        f.write(f"    - {r['value']}: {r['count']}件 ({r['percent']}%)\n")
                elif typ == "number":
                    s = a.get("summary") or {}
                    f.write(f"    - 非NULL: {s.get('non_null')}, 最小: {s.get('min')}, 最大: {s.get('max')}, 平均: {s.get('mean')}, 中央値: {s.get('median')}\n")
        else:
            f.write("- コンフィグなし\n")

    # ---- 付随の保存（必要最小限） ----
    summary_json = os.path.join(out_dir, "analysis_summary.json")
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # 任意で CSV を出したい場合だけ保存（デフォルトは出力しない）
    if emit_csv:
        # time gaps
        gaps = time_cont.get("gaps") or []
        if gaps:
            pd.DataFrame(gaps).to_csv(os.path.join(out_dir, "timestream_time_gaps.csv"),
                                      index=False, encoding="utf-8-sig")
        # four_types
        dist = four_types.get("distribution") or []
        if dist:
            pd.DataFrame(dist).to_csv(os.path.join(out_dir, "timestream_four_types_percentage.csv"),
                                      index=False, encoding="utf-8-sig")
        # gender
        gd = dyn_main.get("gender_distribution") or []
        if gd:
            pd.DataFrame(gd).to_csv(os.path.join(out_dir, "dynamodb_gender_distribution.csv"),
                                    index=False, encoding="utf-8-sig")
        # age buckets
        ab = dyn_main.get("age_buckets") or []
        if ab:
            pd.DataFrame(ab).to_csv(os.path.join(out_dir, "dynamodb_age_buckets.csv"),
                                    index=False, encoding="utf-8-sig")

    return {"report_txt": report_txt, "summary_json": summary_json, **report}

# ================== CLI ==================

def _build_parser():
    p = argparse.ArgumentParser(description="汎用・ダウンロード後分析ツール（レポート優先版）")
    p.add_argument("--input-folder", "-i", type=str, help="ダウンロードフォルダ（*_dynamodb.csv / *_timestream.csv を自動検出）")
    p.add_argument("--timestream", type=str, help="Timestream CSV のパス")
    p.add_argument("--dynamodb", type=str, help="DynamoDB CSV のパス")
    p.add_argument("--step", type=int, default=None, help="time 連続性の期待間隔（秒）。未指定時は自動推定")
    p.add_argument("--config", type=str, default=None, help="DynamoDB data 用コンフィグ(JSON)")
    p.add_argument("--emit-csv", action="store_true", help="CSV 出力を有効化（既定は出力しない）")
    return p

def main():
    print("post_download_analysis: v2.0（レポート優先・CSV最小化）")
    parser = _build_parser()
    args = parser.parse_args()
    report = run_analysis(input_folder=args.input_folder,
                          timestream_csv=args.timestream,
                          dynamodb_csv=args.dynamodb,
                          expected_step_seconds=args.step,
                          config_path=args.config,
                          emit_csv=args.emit_csv)
    print("=== 解析レポート ===")
    print(json.dumps({"report_txt": report["report_txt"], "summary_json": report["summary_json"]},
                     ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
