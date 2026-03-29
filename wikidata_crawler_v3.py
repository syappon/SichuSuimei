"""
Wikidata Famous People Crawler v3
Oracle Always Free サーバー運用版

特徴:
- レート制限: 5秒間隔 + 指数バックオフ (Wikidataの利用規約に準拠)
- チェックポイント: 途中停止→再開対応
- ログ: ファイル + コンソール同時出力
- 逐次保存: 大量データでもメモリを圧迫しない
- systemd対応: サービスとして常駐可能
"""

import requests
import json
import time
import logging
import signal
import sys
import argparse
from datetime import datetime
from pathlib import Path

# ─── 設定 ──────────────────────────────────────────────────────────────────────

BASE_DIR      = Path(__file__).parent
OUTPUT_DIR    = BASE_DIR / "output"
LOG_DIR       = BASE_DIR / "logs"
CHECKPOINT_FILE = BASE_DIR / "checkpoint.json"
OUTPUT_JSON   = OUTPUT_DIR / "famous_people.json"
OUTPUT_NDJSON = OUTPUT_DIR / "famous_people.ndjson"  # 逐次書き込み用

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "ShichusuimeiResearchBot/3.0 (academic research, non-commercial; github.com/yourname/shichusuimei)",
    "Accept": "application/sparql-results+json",
}

# Wikidata負荷対策: 1リクエストあたりの待機秒数
RATE_LIMIT_SECONDS  = 5    # 通常待機
RETRY_BASE_SECONDS  = 10   # リトライ初回待機（指数バックオフのベース）
MAX_RETRIES         = 5    # 最大リトライ回数
REQUEST_TIMEOUT     = 90   # タイムアウト秒（重めのクエリに対応）

# ─── 職業マスタ ────────────────────────────────────────────────────────────────

OCCUPATION_MASTER = {
    # QID: (occupation_key, shichusuimei_category, label)
    # 比肩劫財系
    "Q2066131":  ("athlete",        "比肩劫財系", "アスリート全般"),
    "Q937857":   ("athlete",        "比肩劫財系", "サッカー選手"),
    "Q10833314": ("athlete",        "比肩劫財系", "テニス選手"),
    "Q19204627": ("athlete",        "比肩劫財系", "バスケットボール選手"),
    "Q11338576": ("athlete",        "比肩劫財系", "ボクサー"),
    "Q131524":   ("entrepreneur",   "比肩劫財系", "起業家"),
    # 食神傷官系
    "Q639669":   ("musician",       "食神傷官系", "音楽家"),
    "Q177220":   ("musician",       "食神傷官系", "歌手"),
    "Q36834":    ("musician",       "食神傷官系", "作曲家"),
    "Q33999":    ("actor",          "食神傷官系", "俳優"),
    "Q36180":    ("writer",         "食神傷官系", "作家"),
    "Q482980":   ("writer",         "食神傷官系", "著者"),
    "Q1028181":  ("artist",         "食神傷官系", "画家"),
    "Q245068":   ("comedian",       "食神傷官系", "コメディアン"),
    "Q185351":   ("inventor",       "食神傷官系", "発明家"),
    "Q2526255":  ("director",       "食神傷官系", "映画監督"),
    # 偏財正財系
    "Q43845":    ("businessperson", "偏財正財系", "実業家"),
    "Q806798":   ("businessperson", "偏財正財系", "経営者"),
    # 偏官正官系
    "Q82955":    ("politician",     "偏官正官系", "政治家"),
    "Q40348":    ("lawyer",         "偏官正官系", "弁護士"),
    "Q16533":    ("judge",          "偏官正官系", "裁判官"),
    # 偏印印綬系
    "Q901":      ("scientist",      "偏印印綬系", "科学者"),
    "Q169470":   ("scientist",      "偏印印綬系", "物理学者"),
    "Q170790":   ("scientist",      "偏印印綬系", "数学者"),
    "Q4964182":  ("philosopher",    "偏印印綬系", "哲学者"),
    "Q49757":    ("physician",      "偏印印綬系", "医師"),
}

# ─── ロギング設定 ──────────────────────────────────────────────────────────────

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"crawler_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

log = setup_logging()

# ─── グレースフルシャットダウン ────────────────────────────────────────────────

shutdown_requested = False

def handle_signal(signum, frame):
    global shutdown_requested
    log.info(f"シグナル {signum} を受信。現在の処理完了後に終了します...")
    shutdown_requested = True

signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ─── チェックポイント ─────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            cp = json.load(f)
        log.info(f"チェックポイント読込: {cp['completed_qids']} 完了済み, {cp['total_records']}件取得済み")
        return cp
    return {"completed_qids": [], "total_records": 0, "seen_keys": []}

def save_checkpoint(completed_qids: list, total_records: int, seen_keys: list):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "completed_qids": completed_qids,
            "total_records":  total_records,
            "seen_keys":      seen_keys,
            "updated_at":     datetime.now().isoformat(),
        }, f, ensure_ascii=False, indent=2)

def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        log.info("チェックポイントをクリアしました")

# ─── SPARQL ──────────────────────────────────────────────────────────────────

def build_query(qid: str, limit: int, offset: int) -> str:
    return f"""
SELECT ?person ?personLabel ?birthDate ?genderLabel ?countryLabel WHERE {{
  ?person wdt:P31 wd:Q5 ;
          wdt:P106 wd:{qid} ;
          wdt:P569 ?birthDate .
  FILTER(YEAR(?birthDate) >= 1850 && YEAR(?birthDate) <= 2000)
  OPTIONAL {{ ?person wdt:P21 ?gender . }}
  OPTIONAL {{ ?person wdt:P27 ?country . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
ORDER BY ?birthDate
LIMIT {limit}
OFFSET {offset}
"""

def fetch_sparql(qid: str, limit: int = 50, offset: int = 0) -> list[dict] | None:
    """
    SPARQLクエリを実行。失敗時は指数バックオフでリトライ。
    None を返した場合は致命的エラー（スキップ推奨）。
    """
    query = build_query(qid, limit, offset)
    for attempt in range(MAX_RETRIES):
        if shutdown_requested:
            return None
        try:
            resp = requests.get(
                SPARQL_ENDPOINT,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 429:
                wait = RETRY_BASE_SECONDS * (2 ** attempt)
                log.warning(f"  Rate limit (429)。{wait}秒待機...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()["results"]["bindings"]

        except requests.exceptions.Timeout:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"  タイムアウト (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

        except requests.exceptions.HTTPError as e:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"  HTTPエラー {e} (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

        except Exception as e:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"  予期しないエラー: {e} (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

    log.error(f"  {MAX_RETRIES}回リトライ失敗。QID={qid} offset={offset} をスキップ")
    return None

# ─── パース ───────────────────────────────────────────────────────────────────

def parse_date(raw: str):
    try:
        clean = raw.lstrip("+").split("T")[0]
        dt = datetime.strptime(clean, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d"), dt.year, dt.month, dt.day
    except Exception:
        return None, None, None, None

def binding_to_record(b: dict, occ_key: str, category: str) -> dict | None:
    name     = b.get("personLabel", {}).get("value", "").strip()
    raw_date = b.get("birthDate",   {}).get("value", "")
    birth_date, yr, mo, dy = parse_date(raw_date)
    if not name or not birth_date:
        return None

    person_uri  = b.get("person", {}).get("value", "")
    wikidata_id = person_uri.rstrip("/").split("/")[-1]

    return {
        "name":                  name,
        "birth_date":            birth_date,
        "birth_year":            yr,
        "birth_month":           mo,
        "birth_day":             dy,
        "birth_time":            None,
        "occupation_key":        occ_key,
        "shichusuimei_category": category,
        "gender":                b.get("genderLabel",  {}).get("value", ""),
        "nationality":           b.get("countryLabel", {}).get("value", ""),
        "wikidata_id":           wikidata_id,
        "wikidata_url":          f"https://www.wikidata.org/wiki/{wikidata_id}",
        "fetched_at":            datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }

# ─── メインクロール ──────────────────────────────────────────────────────────

def crawl(per_occupation: int = 50, resume: bool = True):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # チェックポイント読込
    cp = load_checkpoint() if resume else {"completed_qids": [], "total_records": 0, "seen_keys": []}
    completed_qids = cp["completed_qids"]
    total_records  = cp["total_records"]
    seen_keys      = set(cp["seen_keys"])

    # ndjsonは追記モード（再開時に続きから書く）
    ndjson_mode = "a" if resume and total_records > 0 else "w"

    with open(OUTPUT_NDJSON, ndjson_mode, encoding="utf-8") as ndf:
        for qid, (occ_key, category, label) in OCCUPATION_MASTER.items():
            if shutdown_requested:
                log.info("シャットダウン要求を受信。チェックポイントを保存して終了します")
                break

            if qid in completed_qids:
                log.info(f"スキップ（完了済み）: {label} ({qid})")
                continue

            log.info(f"━━━ {label} [{category}] ({qid}) ━━━")
            added = 0
            offset = 0

            while added < per_occupation:
                if shutdown_requested:
                    break

                bindings = fetch_sparql(qid, limit=50, offset=offset)
                if bindings is None:
                    break  # 致命的エラー→次の職業へ

                if not bindings:
                    log.info(f"  {label}: 取得終了 (offset={offset})")
                    break

                for b in bindings:
                    rec = binding_to_record(b, occ_key, category)
                    if not rec:
                        continue
                    key = f"{rec['name']}|{rec['birth_date']}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    # ndjsonに逐次書き込み（メモリに溜めない）
                    ndf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    ndf.flush()
                    added += 1
                    total_records += 1

                    if added >= per_occupation:
                        break

                offset += 50

                # Wikidataへの礼儀: 5秒待機
                log.debug(f"  {RATE_LIMIT_SECONDS}秒待機...")
                time.sleep(RATE_LIMIT_SECONDS)

            log.info(f"  → {label}: {added}件追加 (累計 {total_records}件)")
            completed_qids.append(qid)

            # 職業1つ完了ごとにチェックポイント保存
            save_checkpoint(completed_qids, total_records, list(seen_keys))

    # ─── ndjson → まとめてJSONに変換 ─────────────────────────────────────
    log.info("最終JSONを生成中...")
    all_records = []
    if OUTPUT_NDJSON.exists():
        with open(OUTPUT_NDJSON, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    all_records.append(json.loads(line))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "created_at":    datetime.now().isoformat(),
                "total_records": len(all_records),
                "source":        "Wikidata (CC0 Public Domain)",
                "purpose":       "四柱推命統計分析用",
                "fields": {
                    "birth_date":            "YYYY-MM-DD（四柱推命計算に直接使用可）",
                    "birth_time":            "null（Wikidataに時刻情報なし）",
                    "shichusuimei_category": "通変星適職カテゴリとの照合用",
                },
            },
            "data": all_records,
        }, f, ensure_ascii=False, indent=2)

    log.info(f"JSON保存: {OUTPUT_JSON} ({len(all_records)}件)")

    # 正常完了時はチェックポイント削除
    if not shutdown_requested:
        clear_checkpoint()

    return all_records

# ─── サマリー ─────────────────────────────────────────────────────────────────

def print_summary(records: list[dict]):
    from collections import Counter
    cats = Counter(r["shichusuimei_category"] for r in records)
    print("\n" + "="*50)
    print(f"  収集完了: {len(records)}件")
    print("="*50)
    for cat, cnt in sorted(cats.items()):
        bar = "█" * (cnt // 5)
        print(f"  {cat:<12} {cnt:>4}件  {bar}")
    print("="*50)

# ─── エントリポイント ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="四柱推命統計用 Wikidataクローラー v3")
    parser.add_argument("--per-occupation", type=int, default=50,
                        help="職業ごとの最大取得件数（デフォルト: 50）")
    parser.add_argument("--no-resume", action="store_true",
                        help="チェックポイントを無視して最初から実行")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Wikidata Crawler v3 (Oracle Always Free 版) 起動")
    log.info(f"  職業数: {len(OCCUPATION_MASTER)}")
    log.info(f"  職業ごとの上限: {args.per_occupation}件")
    log.info(f"  レート制限: {RATE_LIMIT_SECONDS}秒/リクエスト")
    log.info(f"  チェックポイント再開: {'無効' if args.no_resume else '有効'}")
    log.info("=" * 60)

    records = crawl(
        per_occupation=args.per_occupation,
        resume=not args.no_resume,
    )

    if records:
        print_summary(records)
    else:
        log.error("データを取得できませんでした")
        sys.exit(1)
