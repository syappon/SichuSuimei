"""
適職診断バッチ処理スクリプト
meishiki_batch.py で命式計算済みの JSON に
shokugyo_db.py の適職診断結果を付加する

使い方:
  python3 shokugyo_batch.py input_meishiki.json [output.json]

  output.json を省略した場合は input_meishiki_shokugyo.json として出力

前提:
  - input JSON に zokan_tsuhensei / tchu_satsu / nichi_kan が存在すること
  - shichusuimei.py / shokugyo_db.py と同じディレクトリに置くこと
"""

import json
import sys
import os
from datetime import datetime

# ===================== shokugyo_db のロード =====================
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "shokugyo_db.py"), encoding="utf-8") as f:
    shokugyo_src = f.read()

# 動作テストブロックを除外してexec
shokugyo_logic = shokugyo_src.split("# ===================== 動作テスト =====================")[0]
exec(shokugyo_logic, globals())


# ===================== 診断関数 =====================

def diagnose_record(record: dict) -> dict:
    """
    1レコードに適職診断結果を付加して返す。
    zokan_tsuhensei / tchu_satsu / nichi_kan が必須。
    """
    # 必須フィールドチェック
    zk  = record.get("zokan_tsuhensei")
    tcs = record.get("tchu_satsu")
    nk  = record.get("nichi_kan")

    if not zk or not tcs or not nk:
        record["shokugyo_error"] = "命式データが不足しています（zokan_tsuhensei / tchu_satsu / nichi_kan が必要）"
        return record

    ts_nen   = zk.get("年柱") or ""
    ts_tsuki = zk.get("月柱") or ""
    ts_nichi = zk.get("日柱") or ""
    ts_toki  = zk.get("時柱") or ""

    # 通変星が空白の場合はスキップ
    if not ts_tsuki or not ts_nichi:
        record["shokugyo_error"] = "月柱または日柱の蔵干通変星がありません"
        return record

    try:
        result = get_shokugyo_diagnosis(
            nichi_kan=nk,
            tsuhensei_nen=ts_nen,
            tsuhensei_tsuki=ts_tsuki,
            tsuhensei_nichi=ts_nichi,
            tchu_satsu=tcs,
            tsuhensei_toki=ts_toki,
        )
    except Exception as e:
        record["shokugyo_error"] = f"診断エラー: {e}"
        return record

    # 診断結果をフラットに整形して付加
    record["shokugyo"] = {
        # 使用した通変星・ウェイト（再現性のために記録）
        "input": {
            "tsuhensei": result["tsuhensei"],
            "weights": result["weights"],
            "tchu_satsu": result["tchu_satsu"],
        },
        # メイン出力
        "env":        result["env"],        # 向いている環境
        "env_avoid":  result["env_avoid"],  # 避けた方が良い環境
        "jobs":       result["jobs"],       # 職業例
        "seika_msg":  result["seika_msg"],  # 成果の出し方（時柱）
        "combo_msg":  result["combo_msg"],  # 通変星の組み合わせメッセージ
        "tagline":    result["tagline"],    # 一言まとめ
    }

    return record


# ===================== バッチ処理 =====================

def process_json(input_path: str, output_path: str):
    """JSONファイルを読み込み、適職診断を付加して出力する"""
    print(f"読み込み: {input_path}")
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("data", [])
    total = len(records)
    print(f"総レコード数: {total}")

    ok_count    = 0
    error_count = 0
    skip_count  = 0

    for i, record in enumerate(records):
        # すでに命式計算エラーがあるものはスキップ
        if "meishiki_error" in record:
            record["shokugyo_error"] = "命式計算エラーのためスキップ"
            skip_count += 1
            continue

        records[i] = diagnose_record(record)

        if "shokugyo_error" in records[i]:
            error_count += 1
        else:
            ok_count += 1

        if (i + 1) % 100 == 0 or (i + 1) == total:
            print(f"  処理中: {i+1}/{total} "
                  f"({ok_count}件成功, {error_count}件エラー, {skip_count}件スキップ)",
                  end="\r")

    print()

    # メタデータ更新
    data["metadata"]["shokugyo_added_at"] = datetime.now().strftime("%Y%m%d_%H%M%S")
    data["metadata"]["shokugyo_ok"]       = ok_count
    data["metadata"]["shokugyo_error"]    = error_count
    data["metadata"]["shokugyo_skip"]     = skip_count

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n完了: {output_path}")
    print(f"  成功: {ok_count}件  エラー: {error_count}件  スキップ: {skip_count}件")


# ===================== プレビュー =====================

def preview(input_path: str, n: int = 3):
    """先頭n件の診断結果をプレビュー表示"""
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    shown = 0
    for record in data["data"]:
        if "meishiki_error" in record:
            continue
        r = diagnose_record(dict(record))
        name = r.get("name", "unknown")
        bd   = r.get("birth_date", "")
        occ  = r.get("occupation_key", "")

        if "shokugyo_error" in r:
            print(f"{name} ({bd}) [{occ}]: エラー - {r['shokugyo_error']}")
        else:
            s = r["shokugyo"]
            ts = s["input"]["tsuhensei"]
            print(f"{name} ({bd}) [{occ}]")
            print(f"  通変星: 月={ts['月柱']} 日={ts['日柱']} 年={ts['年柱']}"
                  + (f" 時={ts['時柱']}" if ts['時柱'] else ""))
            print(f"  天中殺: {s['input']['tchu_satsu']}")
            print(f"  向いている環境: {' / '.join(s['env'][:3])}")
            print(f"  職業例: {' / '.join(s['jobs'][:5])}")
            if s["seika_msg"]:
                print(f"  成果の出し方: {s['seika_msg']}")
            print(f"  一言: {s['tagline'][:60]}...")
        print()
        shown += 1
        if shown >= n:
            break


# ===================== エントリポイント =====================

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        print("使い方: python3 shokugyo_batch.py input_meishiki.json [output.json]")
        print()
        print("プレビュー（先頭3件）:")
        preview("/home/claude/famous_people_meishiki.json")
        sys.exit(0)

    input_path = args[0]
    if len(args) >= 2:
        output_path = args[1]
    else:
        # input と同階層に output/ フォルダを作成（なければ）して出力
        output_dir = os.path.join(os.path.dirname(os.path.abspath(input_path)), "output")
        os.makedirs(output_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(output_dir, base + "_shokugyo.json")
 
    process_json(input_path, output_path)
