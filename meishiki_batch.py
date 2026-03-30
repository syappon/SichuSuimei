"""
四柱推命バッチ処理スクリプト
famous_people JSON に命式計算結果を付加して新しいJSONを出力する

使い方:
  python3 meishiki_batch.py input.json [output.json]

  output.json を省略した場合は input_meishiki.json として出力
"""

import json
import sys
import os
from datetime import date, datetime

# shichusuimei.py のロジック部分をインポート
# 同じディレクトリに shichusuimei.py があることが前提
sys.path.insert(0, os.path.dirname(__file__))

# shichusuimei.py から計算関数を直接取り込む
# StreamlitのUI部分は除外してロジックのみ実行
with open(os.path.join(os.path.dirname(__file__), "shichusuimei.py"), encoding="utf-8") as f:
    src = f.read()

# Streamlit UI部分を除いたロジック部分のみ実行
logic_src = src.split("# ===================== Streamlit UI =====================")[0]
logic_src = logic_src.replace("import streamlit as st", "")
logic_src = logic_src.replace("from datetime import date, datetime", "")

exec(logic_src, globals())


def calc_record(record: dict) -> dict:
    """
    1レコードに四柱推命の計算結果を付加して返す。
    birth_time が null の場合は時柱なし。
    """
    year  = record.get("birth_year")
    month = record.get("birth_month")
    day   = record.get("birth_day")
    birth_time = record.get("birth_time")  # "HH:MM" or null

    # 必須フィールドのチェック
    if not all([year, month, day]):
        record["meishiki_error"] = "birth_date が不完全です"
        return record

    # 時刻のパース
    hour = None
    if birth_time:
        try:
            h, m = birth_time.split(":")
            hour = int(h)
        except Exception:
            pass

    # 日付バリデーション
    try:
        date(int(year), int(month), int(day))
    except ValueError as e:
        record["meishiki_error"] = f"無効な日付: {e}"
        return record

    # 命式計算
    try:
        result = calc_meishiki(int(year), int(month), int(day), hour)
    except Exception as e:
        record["meishiki_error"] = f"計算エラー: {e}"
        return record

    pillars   = result["pillars"]
    zokan     = result["zokan"]
    sz        = result["strongest_zokan"]
    tsuhen    = result["tsuhensei"]
    nichi_kan = result["nichi_kan"]
    warnings  = result.get("warnings", [])

    # 天中殺
    nichi_shi = pillars["日柱"][1]
    tcs_label, tcs_s1, tcs_s2 = get_tchu_satsu(nichi_kan, nichi_shi)

    # 通変星（天干）・七殺→偏官変換済み
    def fmt(t):
        return "偏官" if t == "七殺" else t

    # 出力フィールドを構築
    record["meishiki"] = {
        "年柱": {"天干": pillars["年柱"][0], "地支": pillars["年柱"][1]},
        "月柱": {"天干": pillars["月柱"][0], "地支": pillars["月柱"][1]},
        "日柱": {"天干": pillars["日柱"][0], "地支": pillars["日柱"][1]},
        "時柱": {"天干": pillars["時柱"][0], "地支": pillars["時柱"][1]}
                if pillars["時柱"][0] else None,
    }

    record["tsuhensei_tenkan"] = {
        "年柱": fmt(tsuhen["年柱"]["天干"]),
        "月柱": fmt(tsuhen["月柱"]["天干"]),
        "日柱": "日主",
        "時柱": fmt(tsuhen["時柱"]["天干"]) if pillars["時柱"][0] else None,
    }

    record["strongest_zokan"] = {
        "年柱": sz["年柱"],
        "月柱": sz["月柱"],
        "日柱": sz["日柱"],
        "時柱": sz["時柱"] if pillars["時柱"][0] else None,
    }

    record["zokan_tsuhensei"] = {
        "年柱": fmt(get_tsuhensei(nichi_kan, sz["年柱"])) if sz["年柱"] else None,
        "月柱": fmt(get_tsuhensei(nichi_kan, sz["月柱"])) if sz["月柱"] else None,
        "日柱": fmt(get_tsuhensei(nichi_kan, sz["日柱"])) if sz["日柱"] else None,
        "時柱": fmt(get_tsuhensei(nichi_kan, sz["時柱"])) if sz["時柱"] else None,
    }

    record["tchu_satsu"] = tcs_label

    record["nichi_kan"] = nichi_kan

    # 流派差異の警告
    record["warnings"] = [
        {"pillar": w["pillar"], "reasons": w["reasons"]}
        for w in warnings
    ]

    return record


def process_json(input_path: str, output_path: str):
    """JSONファイルを読み込み、命式計算結果を付加して出力する"""
    print(f"読み込み: {input_path}")
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("data", [])
    total = len(records)
    print(f"総レコード数: {total}")

    ok_count    = 0
    error_count = 0
    warn_count  = 0

    for i, record in enumerate(records):
        records[i] = calc_record(record)
        if "meishiki_error" in records[i]:
            error_count += 1
        else:
            ok_count += 1
            if records[i].get("warnings"):
                warn_count += 1

        # 進捗表示
        if (i + 1) % 100 == 0 or (i + 1) == total:
            print(f"  処理中: {i+1}/{total} ({ok_count}件成功, {error_count}件エラー)", end="\r")

    print()

    # メタデータを更新
    data["metadata"]["meishiki_added_at"] = datetime.now().strftime("%Y%m%d_%H%M%S")
    data["metadata"]["meishiki_ok"]       = ok_count
    data["metadata"]["meishiki_error"]    = error_count
    data["metadata"]["meishiki_warnings"] = warn_count

    # 出力
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n完了: {output_path}")
    print(f"  成功: {ok_count}件  エラー: {error_count}件  流派警告あり: {warn_count}件")


def preview(input_path: str, n: int = 3):
    """計算結果のプレビュー（先頭n件）"""
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    for record in data["data"][:n]:
        r = calc_record(dict(record))  # コピーして計算
        name = r.get("name", "unknown")
        bd   = r.get("birth_date", "")
        if "meishiki_error" in r:
            print(f"{name} ({bd}): エラー - {r['meishiki_error']}")
            continue
        m = r["meishiki"]
        print(f"{name} ({bd})")
        print(f"  命式: 年={m['年柱']['天干']}{m['年柱']['地支']} "
              f"月={m['月柱']['天干']}{m['月柱']['地支']} "
              f"日={m['日柱']['天干']}{m['日柱']['地支']}")
        print(f"  通変星(天干): 年={r['tsuhensei_tenkan']['年柱']} "
              f"月={r['tsuhensei_tenkan']['月柱']}")
        print(f"  蔵干最強: 年={r['strongest_zokan']['年柱']} "
              f"月={r['strongest_zokan']['月柱']} "
              f"日={r['strongest_zokan']['日柱']}")
        print(f"  蔵干通変星: 年={r['zokan_tsuhensei']['年柱']} "
              f"月={r['zokan_tsuhensei']['月柱']} "
              f"日={r['zokan_tsuhensei']['日柱']}")
        print(f"  天中殺: {r['tchu_satsu']}")
        if r.get("warnings"):
            print(f"  ⚠ 流派警告あり: {len(r['warnings'])}件")
        print()


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        print("使い方: python3 meishiki_batch.py input.json [output.json]")
        print()
        print("プレビュー（先頭3件）:")
        preview("/mnt/user-data/uploads/famous_people_20260326_010445.json")
        sys.exit(0)

    input_path = args[0]
    if len(args) >= 2:
        output_path = args[1]
    else:
        output_dir = os.path.join(os.path.dirname(os.path.abspath(input_path)), "output")
        os.makedirs(output_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, base + "_meishiki.json")

    process_json(input_path, output_path)
