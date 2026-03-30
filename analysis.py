"""
四柱推命照合分析スクリプト
famous_people_full.json を受け取り、3軸で照合率を分析する

軸①: shichusuimei_category × 月柱・日柱通変星の照合
軸②: occupation_key × jobs（職業例）の一致率
軸③: occupation_key × env（環境キーワード）の照合

使い方:
  python3 analysis.py input_full.json [output_report.json]
"""

import json
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime

# occupation_label_map.py が同じディレクトリにある想定
try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) or ".")
    from occupation_label_map import OCCUPATION_LABEL_MAP
except ImportError:
    OCCUPATION_LABEL_MAP = {}


# ===================== マッピング定義 =====================

# shichusuimei_category → 対応する通変星
CATEGORY_TO_TSUHENSEI = {
    "比肩劫財系": ["比肩", "劫財"],
    "食神傷官系": ["食神", "傷官"],
    "偏財正財系": ["偏財", "正財"],
    "偏官正官系": ["偏官", "正官"],  # 偏官＝七殺の偏官変換済みを想定
    "偏印印綬系": ["偏印", "印綬"],
}

# ============================================================
# occupation_key → jobs リストの文字列との直接対応テーブル
# jobsリスト内の実際の文字列と完全一致で照合する（部分一致なし）
# 追加・修正は各リストに文字列を足すだけでOK
# ============================================================
OCCUPATION_TO_JOBS = {
    # スポーツ選手・武道家・格闘家・コーチ・審判など
    "athlete": [
        "スポーツ選手", "アスリート", "格闘家", "格闘家・武道家",
        "スポーツコーチ・監督", "競輪・競馬選手", "競技ゲーマー（eスポーツ）",
        "山岳救助隊", "登山家・冒険家", "ダンサー・振付師",
        "警察・消防・自衛隊",
    ],
    # 音楽家・作曲家・演奏家・DJなど
    "musician": [
        "ミュージシャン", "作曲家", "DJ・MC", "アーティスト",
        "ダンサー・振付師", "声優", "芸能・タレント",
    ],
    # 自然科学・工学・研究者
    "scientist": [
        "研究者", "研究者（学術）", "研究者（独立系）", "発明家",
        "AIリサーチャー", "天文学者", "考古学者", "民俗学者",
        "数学者", "エンジニア（新領域）", "プログラマー・エンジニア",
        "AIエンジニア",
    ],
    # 小説家・脚本家・ジャーナリスト・詩人など
    "writer": [
        "作家", "脚本家・劇作家", "ジャーナリスト", "批評家・評論家",
        "編集者", "出版・編集者", "詩人", "絵本作家", "漫画家",
        "ゲームシナリオライター", "翻訳者・通訳者", "ライター（会社員）",
        "校正者・校閲者",
    ],
    # コメディアン・お笑い芸人
    "comedian": [
        "お笑い芸人", "俳優・女優", "芸能・タレント",
        "ユーチューバー・インフルエンサー", "イベントプロデューサー",
        "舞台俳優",
    ],
    # 俳優・映画・舞台
    "actor": [
        "俳優・女優", "舞台俳優", "声優", "スタントマン",
        "映画監督", "芸能・タレント",
    ],
    # 裁判官・検察官・法曹
    "judge": [
        "裁判官・弁護士", "検察官", "官僚・行政", "国会議員",
        "外交官", "国連職員", "大企業管理職",
        "警察官（幹部）",
    ],
    # 起業家・スタートアップ創業者
    "entrepreneur": [
        "起業家", "経営者", "投資家", "ベンチャーキャピタリスト",
        "M&Aアドバイザー", "芸能プロデューサー", "イベンター",
        "コンテンツプロデューサー", "個人事業主",
        "スポーツエージェント",
    ],
    # 経営者・ビジネスパーソン・商社・金融
    "businessperson": [
        "経営者", "商社マン", "投資家", "マーケター", "不動産",
        "トレーダー・ディーラー", "ブローカー", "会社役員・取締役",
        "ホテル・観光業経営", "輸出入ビジネス", "保険営業",
        "大企業管理職",
    ],
    # 弁護士・法律家
    "lawyer": [
        "弁護士", "裁判官・弁護士", "検察官", "司法書士・行政書士",
        "官僚・行政", "外交官",
    ],
    # 発明家・特許・イノベーター
    "inventor": [
        "発明家", "研究者", "エンジニア（新領域）", "プログラマー・エンジニア",
        "AIリサーチャー", "AIエンジニア", "建築家",
        "ハッカー（ホワイトハット）", "暗号解読者",
    ],
    # 政治家・行政・外交
    "politician": [
        "政治家", "国会議員", "官僚・行政", "外交官",
        "国連職員", "宗教指導者",
    ],
    # 美術家・工芸家・デザイナー・写真家
    "artist": [
        "アーティスト", "デザイナー", "カメラマン", "イラストレーター",
        "漫画家", "陶芸家", "建築家", "映画監督", "アニメ監督",
        "アニメーター", "フォトジャーナリスト（紛争地域）",
        "伝統芸能継承者",
    ],
    # 哲学者・思想家・倫理学者
    "philosopher": [
        "哲学者・思想家", "哲学者", "研究者（学術）", "作家",
        "占い師・スピリチュアル", "心理学者", "宗教家・神職・住職",
        "僧侶・修道士",
    ],
    # 医師・医療従事者
    "physician": [
        "医師", "外科医", "医師・看護師", "精神科医・心療内科医",
        "救急救命士", "薬剤師", "カウンセラー・心理士",
        "臨床心理士", "言語聴覚士・作業療法士",
    ],
}

# env照合は削除（jobsとの直接照合に一本化）
OCCUPATION_TO_ENV_KEYWORDS = {}  # 後方互換のため残すが使用しない


# ===================== 照合ロジック =====================

def check_category_match(record: dict) -> dict:
    """軸①: shichusuimei_category × 通変星の照合"""
    cat = record.get("shichusuimei_category", "")
    expected = CATEGORY_TO_TSUHENSEI.get(cat, [])
    if not expected:
        return {"match": None, "reason": "カテゴリ未定義"}

    zk = record.get("zokan_tsuhensei", {})
    ts_tsuki = zk.get("月柱") or ""
    ts_nichi = zk.get("日柱") or ""
    ts_nen   = zk.get("年柱") or ""

    # 月柱・日柱・年柱のいずれかが期待通変星に含まれるか
    hit_tsuki = ts_tsuki in expected
    hit_nichi = ts_nichi in expected
    hit_nen   = ts_nen   in expected

    # 月柱OR日柱のどちらかがhitすれば「一致」とする（主要2柱基準）
    match = hit_tsuki or hit_nichi

    return {
        "match": match,
        "category": cat,
        "expected": expected,
        "月柱通変星": ts_tsuki,
        "日柱通変星": ts_nichi,
        "年柱通変星": ts_nen,
        "hit_月柱": hit_tsuki,
        "hit_日柱": hit_nichi,
        "hit_年柱": hit_nen,
    }


def check_jobs_match(record: dict) -> dict:
    """軸②: occupations（英語配列）→ OCCUPATION_LABEL_MAP で日本語変換 → jobs と照合

    旧データ（occupation_key あり）も後方互換で処理する。
    """
    actual_jobs = record.get("shokugyo", {}).get("jobs", [])
    jobs_set = set(actual_jobs)

    # ── 新データ: occupations（英語リスト） ──
    occupations = record.get("occupations", [])
    if occupations and OCCUPATION_LABEL_MAP:
        # 英語職業名 → 日本語ラベルに変換（マップにないものはスキップ）
        translated = list({
            OCCUPATION_LABEL_MAP[occ]
            for occ in occupations
            if occ in OCCUPATION_LABEL_MAP
        })
        if not translated:
            return {"match": None, "reason": "occupation未マッピング"}

        matched = [j for j in translated if j in jobs_set]
        return {
            "match": len(matched) > 0,
            "occupation": occupations[0] if occupations else "",
            "expected_jobs": translated,
            "matched_jobs": matched,
            "actual_jobs": actual_jobs,
        }

    # ── 旧データ: occupation_key（後方互換） ──
    occ = record.get("occupation_key", "")
    expected_jobs = OCCUPATION_TO_JOBS.get(occ, [])
    if not expected_jobs:
        return {"match": None, "reason": "occupation未定義"}

    matched = [j for j in expected_jobs if j in jobs_set]
    return {
        "match": len(matched) > 0,
        "occupation": occ,
        "expected_jobs": expected_jobs,
        "matched_jobs": matched,
        "actual_jobs": actual_jobs,
    }


def check_env_match(record: dict) -> dict:
    """軸③: 廃止（jobsとの直接照合に一本化）"""
    return {"match": None, "reason": "廃止"}


# ===================== 分析本体 =====================

def analyze(data: dict) -> dict:
    """
    birth_date → 命式計算 → jobs リスト生成 → occupation_key と照合
    OCCUPATION_TO_JOBS の対応表に基づき完全一致で判定する
    """
    records = data.get("data", [])
    total = len(records)

    results = []
    for r in records:
        if "shokugyo_error" in r or "meishiki_error" in r:
            continue
        entry = {
            "name":           r.get("name"),
            "birth_date":     r.get("birth_date"),
            "occupation_key": r.get("occupation_key"),
            "tchu_satsu":     r.get("tchu_satsu"),
            "has_warnings":   bool(r.get("warnings")),
            "jobs_match":     check_jobs_match(r),
        }
        results.append(entry)

    valid = len(results)

    # ---- 全体集計 ----
    valid_results = [r for r in results if r["jobs_match"]["match"] is not None]
    match_results = [r for r in valid_results if r["jobs_match"]["match"]]
    overall_rate  = len(match_results) / len(valid_results) if valid_results else 0

    # ---- 職種別集計 ----
    by_occ = defaultdict(lambda: {"total": 0, "match": 0, "miss_samples": []})
    for r in valid_results:
        occ = r["occupation_key"] or r["jobs_match"].get("occupation") or "不明"
        by_occ[occ]["total"] += 1
        if r["jobs_match"]["match"]:
            by_occ[occ]["match"] += 1
        elif len(by_occ[occ]["miss_samples"]) < 5:
            by_occ[occ]["miss_samples"].append({
                "name":        r["name"],
                "birth_date":  r["birth_date"],
                "actual_jobs": r["jobs_match"]["actual_jobs"],
                "expected":    r["jobs_match"]["expected_jobs"],
            })

    occ_rates = {
        occ: {
            "total":        v["total"],
            "match":        v["match"],
            "rate":         round(v["match"] / v["total"], 3) if v["total"] else 0,
            "miss_samples": v["miss_samples"],
        }
        for occ, v in sorted(by_occ.items(), key=lambda x: -x[1]["match"] / max(x[1]["total"], 1))
    }

    # ---- 天中殺別集計 ----
    by_tcs = defaultdict(lambda: {"total": 0, "match": 0})
    for r in valid_results:
        tcs = r["tchu_satsu"] or "不明"
        by_tcs[tcs]["total"] += 1
        if r["jobs_match"]["match"]:
            by_tcs[tcs]["match"] += 1

    tcs_rates = {
        tcs: {
            "total": v["total"],
            "match": v["match"],
            "rate":  round(v["match"] / v["total"], 3) if v["total"] else 0,
        }
        for tcs, v in sorted(by_tcs.items(), key=lambda x: -x[1]["match"] / max(x[1]["total"], 1))
    }

    # ---- 流派警告の影響 ----
    warn_yes = [r for r in valid_results if r["has_warnings"]]
    warn_no  = [r for r in valid_results if not r["has_warnings"]]
    rate_warn_yes = sum(1 for r in warn_yes if r["jobs_match"]["match"]) / len(warn_yes) if warn_yes else 0
    rate_warn_no  = sum(1 for r in warn_no  if r["jobs_match"]["match"]) / len(warn_no)  if warn_no  else 0

    # ---- サマリテキスト ----
    lines = [
        f"総レコード数: {total}件（有効: {valid}件）",
        "",
        "【birth_date → 命式 → jobs × occupation_key 照合結果】",
        f"  全体一致率: {overall_rate:.1%}  ({len(match_results)}/{len(valid_results)}件)",
        "",
        "  職種別（一致率順）:",
    ]
    for occ, v in occ_rates.items():
        lines.append(f"    {str(occ):20s}: {v['rate']:.1%}  ({v['match']}/{v['total']}件)")

    lines += [
        "",
        "  天中殺別（一致率順）:",
    ]
    for tcs, v in tcs_rates.items():
        lines.append(f"    {tcs}: {v['rate']:.1%}  ({v['match']}/{v['total']}件)")

    lines += [
        "",
        f"  流派警告あり: {rate_warn_yes:.1%}  ({len(warn_yes)}件中)",
        f"  流派警告なし: {rate_warn_no:.1%}  ({len(warn_no)}件中)",
    ]

    return {
        "metadata": {
            "analyzed_at":   datetime.now().strftime("%Y%m%d_%H%M%S"),
            "total_records": total,
            "valid_records": valid,
        },
        "summary":      "\n".join(lines),
        "overall_rate": round(overall_rate, 4),
        "match_count":  len(match_results),
        "valid_count":  len(valid_results),
        "by_occupation": occ_rates,
        "by_tchu_satsu": tcs_rates,
        "warnings_impact": {
            "警告あり一致率": round(rate_warn_yes, 4),
            "警告なし一致率": round(rate_warn_no,  4),
            "警告あり件数":   len(warn_yes),
            "警告なし件数":   len(warn_no),
        },
    }


# ===================== エントリポイント =====================

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        input_path = "/home/claude/famous_people_full.json"
        output_path = "/home/claude/analysis_report.json"
    else:
        input_path = args[0]
        base, ext = os.path.splitext(input_path)
        if len(args) >= 2:
            output_path = args[1]
        else:
            output_dir = os.path.join(os.path.dirname(os.path.abspath(input_path)), "output")
            os.makedirs(output_dir, exist_ok=True)
            base = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(output_dir, base + "_analysis.json")

    print(f"読み込み: {input_path}")
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    print("分析中...")
    report = analyze(data)

    # サマリをコンソール出力
    print()
    print("=" * 60)
    print(report["summary"])
    print("=" * 60)

    # JSONに保存
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nレポート保存: {output_path}")
