"""
Wikidata Famous People Crawler v5
シンプル版 - 名前・生年月日・職業（複数）のみ取得

変更点 v5:
- 職業を GROUP_CONCAT で全件取得（1人複数職業対応）
- 月日が 01-01 のレコードを除外（年しか不明なデータを弾く）
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

# ─── 設定 ─────────────────────────────────────────────────────────────────────

BASE_DIR        = Path(__file__).parent
OUTPUT_DIR      = BASE_DIR / "output"
LOG_DIR         = BASE_DIR / "logs"
CHECKPOINT_FILE = BASE_DIR / "checkpoint.json"
OUTPUT_NDJSON   = OUTPUT_DIR / "famous_people.ndjson"
OUTPUT_JSON     = OUTPUT_DIR / "famous_people.json"

SPARQL_ENDPOINT    = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "ShichusuimeiResearchBot/5.0 (academic research, non-commercial; github.com/syappon/SichuSuimei; syappon.music@gmail.com)",
    "Accept":     "application/sparql-results+json",
}

RATE_LIMIT_SECONDS = 5    # リクエスト間隔（秒）
RETRY_BASE_SECONDS = 15   # リトライ初回待機（指数バックオフのベース）
MAX_RETRIES        = 5
REQUEST_TIMEOUT    = 120  # タイムアウト（秒）
WARMUP_WAIT        = 10   # 初回リクエスト前の待機（Wikidataウォームアップ）

# ─── ロギング ──────────────────────────────────────────────────────────────────

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"crawler_{datetime.now().strftime('%Y%m%d')}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for h in [logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)]:
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger

log = setup_logging()

# ─── グレースフルシャットダウン ────────────────────────────────────────────────

shutdown_requested = False

def handle_signal(signum, frame):
    global shutdown_requested
    log.info(f"シグナル {signum} 受信。現在の処理完了後に終了します...")
    shutdown_requested = True

signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ─── チェックポイント ─────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            cp = json.load(f)
        log.info(f"チェックポイント読込: offset={cp['offset']} / {cp['total_records']}件取得済み")
        return cp
    return {"offset": 0, "total_records": 0, "seen_keys": []}

def save_checkpoint(offset: int, total_records: int, seen_keys: list):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "offset":        offset,
            "total_records": total_records,
            "seen_keys":     seen_keys,
            "updated_at":    datetime.now().isoformat(),
        }, f, ensure_ascii=False)

def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

# ─── クエリ ───────────────────────────────────────────────────────────────────

def build_query(limit: int, offset: int) -> str:
    """
    GROUP BY を排除（Wikidataサーバーへの負荷が高すぎるため）
    同一人物の複数職業はPython側でまとめる
    """
    return f"""
SELECT ?person ?birthDate ?occupation WHERE {{
  ?person wdt:P31 wd:Q5 ;
          wdt:P569 ?birthDate ;
          wdt:P106 ?occupation .
  FILTER(?birthDate >= "1850-01-01"^^xsd:dateTime && ?birthDate < "2001-01-01"^^xsd:dateTime)
  FILTER(!(MONTH(?birthDate) = 1 && DAY(?birthDate) = 1))
}}
LIMIT {limit}
OFFSET {offset}
"""

def fetch_label(wikidata_id: str) -> str:
    """Wikidata REST APIで人物名を取得（SPARQLより軽量・安定）"""
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        entity = resp.json()["entities"][wikidata_id]
        labels = entity.get("labels", {})
        return (labels.get("ja") or labels.get("en") or {}).get("value", wikidata_id)
    except Exception:
        return wikidata_id

def fetch_occupation_label(occ_qid: str, cache: dict) -> str:
    """職業QIDのラベルをキャッシュしながら取得"""
    if occ_qid in cache:
        return cache[occ_qid]
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{occ_qid}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        entity = resp.json()["entities"][occ_qid]
        labels = entity.get("labels", {})
        label = (labels.get("ja") or labels.get("en") or {}).get("value", occ_qid)
    except Exception:
        label = occ_qid
    cache[occ_qid] = label
    time.sleep(1)  # ラベル取得間隔
    return label

def fetch_sparql(limit: int, offset: int) -> list[dict] | None:
    query = build_query(limit, offset)
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
                log.warning(f"Rate limit (429)。{wait}秒待機...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()["results"]["bindings"]

        except requests.exceptions.Timeout:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"タイムアウト (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

        except requests.exceptions.HTTPError as e:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"HTTPエラー {e} (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

        except Exception as e:
            wait = RETRY_BASE_SECONDS * (2 ** attempt)
            log.warning(f"エラー: {e} (attempt {attempt+1}/{MAX_RETRIES})。{wait}秒待機...")
            time.sleep(wait)

    log.error(f"{MAX_RETRIES}回リトライ失敗。offset={offset} をスキップ")
    return None

# ─── パース ───────────────────────────────────────────────────────────────────

def parse_date(raw: str):
    try:
        clean = raw.lstrip("+").split("T")[0]
        dt = datetime.strptime(clean, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d"), dt.year, dt.month, dt.day
    except Exception:
        return None, None, None, None

# ─── メイン ───────────────────────────────────────────────────────────────────

def crawl(target: int = 500, batch: int = 50, resume: bool = True):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cp = load_checkpoint() if resume else {"offset": 0, "total_records": 0, "seen_keys": []}
    offset        = cp["offset"]
    total_records = cp["total_records"]
    seen_keys     = set(cp["seen_keys"])
    occ_cache     = {}  # 職業ラベルのキャッシュ

    # ── ウォームアップ ──────────────────────────────────────────────────────
    # 初回リクエストは必ずタイムアウトしやすいので、
    # 軽いpingクエリで接続を確立してからメインクエリを投げる
    if total_records == 0 or not resume:
        log.info(f"Wikidataへの接続を確認中... ({WARMUP_WAIT}秒待機)")
        time.sleep(WARMUP_WAIT)
        ping_query = "SELECT ?s WHERE { wd:Q42 wdt:P31 ?s } LIMIT 1"
        for attempt in range(5):
            try:
                resp = requests.get(
                    SPARQL_ENDPOINT,
                    params={"query": ping_query, "format": "json"},
                    headers=HEADERS,
                    timeout=30,
                )
                if resp.status_code == 200:
                    log.info("接続OK。メインクロール開始")
                    break
            except Exception:
                pass
            log.info(f"接続待機中... ({attempt+1}/5)")
            time.sleep(10)

    ndjson_mode = "a" if resume and total_records > 0 else "w"

    with open(OUTPUT_NDJSON, ndjson_mode, encoding="utf-8") as ndf:
        while total_records < target:
            if shutdown_requested:
                log.info("シャットダウン。チェックポイント保存して終了")
                break

            log.info(f"取得中... offset={offset} / 累計={total_records}件")
            bindings = fetch_sparql(batch, offset)

            if bindings is None:
                log.error("致命的エラー。終了します")
                break
            if not bindings:
                log.info("これ以上データがありません")
                break

            # Python側で同一人物の職業をまとめる
            person_map: dict[str, dict] = {}  # wikidata_id → record
            for b in bindings:
                person_uri  = b.get("person",     {}).get("value", "")
                raw_date    = b.get("birthDate",   {}).get("value", "")
                occ_uri     = b.get("occupation",  {}).get("value", "")
                wikidata_id = person_uri.rstrip("/").split("/")[-1]
                occ_qid     = occ_uri.rstrip("/").split("/")[-1]

                birth_date, yr, mo, dy = parse_date(raw_date)
                if not birth_date or not wikidata_id:
                    continue

                if wikidata_id not in person_map:
                    person_map[wikidata_id] = {
                        "birth_date":  birth_date,
                        "birth_year":  yr,
                        "birth_month": mo,
                        "birth_day":   dy,
                        "occ_qids":    [],
                    }
                if occ_qid not in person_map[wikidata_id]["occ_qids"]:
                    person_map[wikidata_id]["occ_qids"].append(occ_qid)

            added = 0
            for wikidata_id, pdata in person_map.items():
                key = f"{wikidata_id}|{pdata['birth_date']}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                name = fetch_label(wikidata_id)
                time.sleep(1)

                occupations = [fetch_occupation_label(q, occ_cache) for q in pdata["occ_qids"]]

                rec = {
                    "name":        name,
                    "birth_date":  pdata["birth_date"],
                    "birth_year":  pdata["birth_year"],
                    "birth_month": pdata["birth_month"],
                    "birth_day":   pdata["birth_day"],
                    "birth_time":  None,
                    "occupations": occupations,
                    "wikidata_id": wikidata_id,
                    "fetched_at":  datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                }
                ndf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                ndf.flush()
                added += 1
                total_records += 1

                if total_records >= target:
                    break

            offset += batch
            save_checkpoint(offset, total_records, list(seen_keys))
            log.info(f"  +{added}件 (累計 {total_records}/{target}件)")

            time.sleep(RATE_LIMIT_SECONDS)

    # ndjson → JSON にまとめる
    log.info("最終JSON生成中...")
    records = []
    with open(OUTPUT_NDJSON, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "created_at":    datetime.now().isoformat(),
                "total_records": len(records),
                "source":        "Wikidata (CC0 Public Domain)",
                "purpose":       "四柱推命統計分析用",
            },
            "data": records,
        }, f, ensure_ascii=False, indent=2)

    log.info(f"完了: {len(records)}件 → {OUTPUT_JSON}")
    if not shutdown_requested:
        clear_checkpoint()
    return records

# ─── エントリポイント ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wikidata Crawler v4")
    parser.add_argument("--target",    type=int, default=500, help="収集目標件数（デフォルト: 500）")
    parser.add_argument("--batch",     type=int, default=50,  help="1回のSPARQLで取得する件数（デフォルト: 50）")
    parser.add_argument("--no-resume", action="store_true",   help="最初からやり直す")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("Wikidata Crawler v5 起動")
    log.info(f"  目標件数   : {args.target}件")
    log.info(f"  バッチサイズ: {args.batch}件")
    log.info(f"  レート制限 : {RATE_LIMIT_SECONDS}秒/リクエスト")
    log.info(f"  再開モード : {'無効' if args.no_resume else '有効'}")
    log.info("=" * 55)

    crawl(target=args.target, batch=args.batch, resume=not args.no_resume)
