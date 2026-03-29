#!/usr/bin/env python3
"""
Wikidata Birth Date & Occupation Crawler
=========================================
Wikidataから「人物 + 生年月日 + 職業」データを大量収集するクローラー。

戦略:
  - 年代別（10年刻み）× OFFSET/LIMIT でページネーション
  - 60秒クエリタイムアウト対策済み
  - 429/5xx時の指数バックオフ
  - 年代別JSONに逐次保存（クラッシュ耐性）
  - 最後に1ファイルに結合

使い方:
  python wikidata_crawler.py                    # 全年代を収集
  python wikidata_crawler.py --start 1900 --end 1950  # 範囲指定
  python wikidata_crawler.py --merge-only       # 既存ファイルの結合のみ
  python wikidata_crawler.py --status           # 進捗確認

レート制限:
  - デフォルト3秒間隔（--delay で変更可）
  - 429応答時は自動バックオフ（30秒→60秒→120秒→最大300秒）
  - User-Agentにプロジェクト名と連絡先を記載（Wikimedia推奨）
"""

import json
import os
import sys
import time
import argparse
import logging
from datetime import datetime, date
from pathlib import Path
from urllib.parse import quote

import requests

# ============================================================
# 設定
# ============================================================

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Wikimedia推奨: User-Agentにプロジェクト名と連絡先を含める
# ★ 自分のメールアドレスに書き換えてください
USER_AGENT = "ShichusuimeiCrawler/1.0 (https://github.com/your-project; syappon.music@gmail.com)"

# 出力ディレクトリ
OUTPUT_DIR = Path("wikidata_output")
MERGED_FILE = "wikidata_people.json"

# デフォルトのリクエスト間隔（秒）
DEFAULT_DELAY = 3.0

# ページネーション: 1回のクエリで取得する件数
PAGE_SIZE = 5000

# 年代の範囲（紀元前は扱いが特殊なので1500年以降をデフォルトに）
DEFAULT_START_YEAR = 1500
DEFAULT_END_YEAR = 2015

# 年代の刻み幅（10年単位）
DECADE_STEP = 10

# バックオフ設定
INITIAL_BACKOFF = 30
MAX_BACKOFF = 300
BACKOFF_MULTIPLIER = 2

# 最大リトライ回数（1クエリあたり）
MAX_RETRIES = 5

# ============================================================
# ロギング
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("wikidata_crawler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ============================================================
# SPARQLクエリテンプレート
# ============================================================

def build_query(year_start: int, year_end: int, limit: int, offset: int) -> str:
    """
    年範囲を指定して人物の生年月日・職業を取得するSPARQLクエリ。
    
    最適化ポイント:
    - ラベルサービスはサブクエリの外で呼ぶ
    - 年範囲フィルタで結果セットを絞る
    - LIMIT/OFFSETでページネーション
    - 職業は複数持つ場合GROUP_CONCATで1行にまとめる
    """
    return f"""
SELECT ?person ?personLabel ?dob 
       (GROUP_CONCAT(DISTINCT ?occLabel; SEPARATOR="|") AS ?occupations)
       (GROUP_CONCAT(DISTINCT ?occId; SEPARATOR="|") AS ?occupationIds)
WHERE {{
  {{
    SELECT ?person ?dob ?occ WHERE {{
      ?person wdt:P31 wd:Q5 ;          # 人物
              wdt:P569 ?dob ;           # 生年月日
              wdt:P106 ?occ .           # 職業
      FILTER(YEAR(?dob) >= {year_start} && YEAR(?dob) < {year_end})
    }}
    ORDER BY ?person
    LIMIT {limit}
    OFFSET {offset}
  }}
  ?occ rdfs:label ?occLabel .
  FILTER(LANG(?occLabel) = "en")
  BIND(REPLACE(STR(?occ), "http://www.wikidata.org/entity/", "") AS ?occId)
  SERVICE wikibase:label {{ 
    bd:serviceParam wikibase:language "en,ja" . 
  }}
}}
GROUP BY ?person ?personLabel ?dob
ORDER BY ?person
"""


def build_count_query(year_start: int, year_end: int) -> str:
    """年範囲内の人物数を概算するクエリ。"""
    return f"""
SELECT (COUNT(DISTINCT ?person) AS ?count) WHERE {{
  ?person wdt:P31 wd:Q5 ;
          wdt:P569 ?dob ;
          wdt:P106 ?occ .
  FILTER(YEAR(?dob) >= {year_start} && YEAR(?dob) < {year_end})
}}
"""

# ============================================================
# HTTPクライアント
# ============================================================

class WikidataClient:
    def __init__(self, delay: float = DEFAULT_DELAY):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
        })
        self.delay = delay
        self.last_request_time = 0
        self.total_requests = 0
        self.total_errors = 0
    
    def _wait(self):
        """レート制限: 前回リクエストから最低delay秒空ける"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
    
    def query(self, sparql: str) -> dict | None:
        """
        SPARQLクエリを実行。リトライ＋指数バックオフ付き。
        成功時はJSONレスポンスを返す。全リトライ失敗時はNone。
        """
        backoff = INITIAL_BACKOFF
        
        for attempt in range(1, MAX_RETRIES + 1):
            self._wait()
            self.last_request_time = time.time()
            self.total_requests += 1
            
            try:
                resp = self.session.get(
                    SPARQL_ENDPOINT,
                    params={"query": sparql},
                    timeout=90,  # 60秒タイムアウト + 余裕30秒
                )
                
                if resp.status_code == 200:
                    return resp.json()
                
                elif resp.status_code == 429:
                    self.total_errors += 1
                    retry_after = resp.headers.get("Retry-After")
                    wait = int(retry_after) if retry_after else backoff
                    logger.warning(
                        f"429 Rate Limited (attempt {attempt}/{MAX_RETRIES}). "
                        f"Waiting {wait}s..."
                    )
                    time.sleep(wait)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
                    
                elif resp.status_code == 403:
                    logger.error(
                        "403 Forbidden - IPがバンされた可能性があります。"
                        "しばらく待ってから再試行してください。"
                    )
                    return None
                    
                elif resp.status_code >= 500:
                    self.total_errors += 1
                    logger.warning(
                        f"{resp.status_code} Server Error (attempt {attempt}/{MAX_RETRIES}). "
                        f"Waiting {backoff}s..."
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
                    
                else:
                    self.total_errors += 1
                    logger.error(f"Unexpected status {resp.status_code}: {resp.text[:200]}")
                    return None
                    
            except requests.exceptions.Timeout:
                self.total_errors += 1
                logger.warning(
                    f"Request timeout (attempt {attempt}/{MAX_RETRIES}). "
                    f"Waiting {backoff}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
                
            except requests.exceptions.RequestException as e:
                self.total_errors += 1
                logger.error(f"Request error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
        
        logger.error(f"All {MAX_RETRIES} retries exhausted.")
        return None

# ============================================================
# データ変換
# ============================================================

def parse_results(data: dict) -> list[dict]:
    """SPARQLレスポンスをパースして既存形式互換のdictリストに変換"""
    results = []
    
    for binding in data.get("results", {}).get("bindings", []):
        qid = binding.get("person", {}).get("value", "")
        qid = qid.replace("http://www.wikidata.org/entity/", "")
        
        name = binding.get("personLabel", {}).get("value", "")
        dob_raw = binding.get("dob", {}).get("value", "")
        occupations_str = binding.get("occupations", {}).get("value", "")
        occupation_ids_str = binding.get("occupationIds", {}).get("value", "")
        
        # 生年月日をパース
        birth_date = None
        birth_year = None
        birth_month = None
        birth_day = None
        
        if dob_raw:
            # Wikidataは "YYYY-MM-DDT00:00:00Z" 形式
            date_part = dob_raw.split("T")[0]
            parts = date_part.split("-")
            # 紀元前は先頭に"-"がつく場合がある
            try:
                if date_part.startswith("-"):
                    # 紀元前: -YYYY-MM-DD
                    birth_year = -int(parts[1])
                    birth_month = int(parts[2]) if len(parts) > 2 else None
                    birth_day = int(parts[3]) if len(parts) > 3 else None
                else:
                    birth_year = int(parts[0])
                    birth_month = int(parts[1]) if len(parts) > 1 else None
                    birth_day = int(parts[2]) if len(parts) > 2 else None
                birth_date = date_part
            except (ValueError, IndexError):
                birth_date = date_part
        
        # 職業をリスト化
        occupations = []
        if occupations_str and occupation_ids_str:
            occ_names = occupations_str.split("|")
            occ_ids = occupation_ids_str.split("|")
            for occ_name, occ_id in zip(occ_names, occ_ids):
                occupations.append({
                    "id": occ_id,
                    "name": occ_name,
                })
        
        # 既存形式互換のレコード
        record = {
            "qid": qid,
            "name": name,
            "birth_date": birth_date,
            "birth_year": birth_year,
            "birth_month": birth_month,
            "birth_day": birth_day,
            "occupations": occupations,
        }
        
        results.append(record)
    
    return results


# ============================================================
# クローラー本体
# ============================================================

class WikidataCrawler:
    def __init__(self, client: WikidataClient, output_dir: Path):
        self.client = client
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {
            "total_records": 0,
            "decades_completed": 0,
            "decades_failed": [],
            "start_time": None,
        }
    
    def _decade_file(self, year_start: int) -> Path:
        """年代別の出力ファイルパス"""
        return self.output_dir / f"people_{year_start}_{year_start + DECADE_STEP}.json"
    
    def _progress_file(self) -> Path:
        """進捗管理ファイル"""
        return self.output_dir / "progress.json"
    
    def _load_progress(self) -> dict:
        """前回の進捗を読み込む"""
        pf = self._progress_file()
        if pf.exists():
            with open(pf, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"completed_decades": [], "last_updated": None}
    
    def _save_progress(self, progress: dict):
        """進捗を保存"""
        progress["last_updated"] = datetime.now().isoformat()
        with open(self._progress_file(), "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def get_decade_count(self, year_start: int, year_end: int) -> int | None:
        """年代の概算件数を取得"""
        query = build_count_query(year_start, year_end)
        data = self.client.query(query)
        if data:
            bindings = data.get("results", {}).get("bindings", [])
            if bindings:
                return int(bindings[0].get("count", {}).get("value", 0))
        return None
    
    def crawl_decade(self, year_start: int, year_end: int) -> list[dict]:
        """
        1年代（10年間）分のデータをページネーションで全件取得。
        """
        all_records = []
        offset = 0
        
        # まず概算件数を取得
        count = self.get_decade_count(year_start, year_end)
        if count is not None:
            logger.info(f"  {year_start}-{year_end}: 推定 {count:,} 件")
        
        while True:
            logger.info(
                f"  {year_start}-{year_end}: offset={offset}, "
                f"累計={len(all_records)} 件"
            )
            
            query = build_query(year_start, year_end, PAGE_SIZE, offset)
            data = self.client.query(query)
            
            if data is None:
                logger.error(
                    f"  {year_start}-{year_end}: offset={offset} でクエリ失敗。"
                    "このページをスキップします。"
                )
                # タイムアウトの場合、刻みを細かくする試みも可能だが
                # まずはスキップして次へ
                break
            
            records = parse_results(data)
            
            if not records:
                # 空 = この年代の全件取得完了
                break
            
            all_records.extend(records)
            offset += PAGE_SIZE
            
            # 安全弁: 100万件超えたら異常とみなす
            if len(all_records) > 1_000_000:
                logger.warning(
                    f"  {year_start}-{year_end}: 100万件超過。打ち切ります。"
                )
                break
        
        return all_records
    
    def save_decade(self, year_start: int, records: list[dict]):
        """年代別JSONファイルに保存"""
        filepath = self._decade_file(year_start)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        logger.info(f"  → {filepath} に {len(records):,} 件保存")
    
    def crawl(self, start_year: int, end_year: int):
        """
        メインのクロール処理。年代別に順次取得。
        前回の進捗から再開可能。
        """
        self.stats["start_time"] = datetime.now().isoformat()
        progress = self._load_progress()
        completed = set(progress.get("completed_decades", []))
        
        decades = list(range(start_year, end_year, DECADE_STEP))
        total_decades = len(decades)
        
        logger.info(f"=" * 60)
        logger.info(f"Wikidata Crawler 開始")
        logger.info(f"範囲: {start_year} - {end_year}")
        logger.info(f"年代数: {total_decades}")
        logger.info(f"完了済み: {len(completed)}")
        logger.info(f"リクエスト間隔: {self.client.delay}秒")
        logger.info(f"=" * 60)
        
        for i, decade_start in enumerate(decades):
            decade_end = decade_start + DECADE_STEP
            
            # 既に完了済みならスキップ
            if decade_start in completed:
                logger.info(
                    f"[{i+1}/{total_decades}] {decade_start}-{decade_end}: "
                    f"スキップ（完了済み）"
                )
                continue
            
            logger.info(
                f"[{i+1}/{total_decades}] {decade_start}-{decade_end} を取得中..."
            )
            
            try:
                records = self.crawl_decade(decade_start, decade_end)
                
                if records:
                    self.save_decade(decade_start, records)
                    self.stats["total_records"] += len(records)
                else:
                    logger.info(f"  {decade_start}-{decade_end}: 該当データなし")
                
                # 進捗を記録
                completed.add(decade_start)
                progress["completed_decades"] = sorted(completed)
                self._save_progress(progress)
                self.stats["decades_completed"] += 1
                
            except KeyboardInterrupt:
                logger.info("\n中断されました。進捗は保存済みです。")
                self._save_progress(progress)
                self._print_stats()
                sys.exit(0)
                
            except Exception as e:
                logger.error(f"  {decade_start}-{decade_end}: エラー: {e}")
                self.stats["decades_failed"].append(decade_start)
        
        self._print_stats()
        logger.info("クロール完了！ --merge-only で結合できます。")
    
    def _print_stats(self):
        """統計情報を表示"""
        logger.info(f"\n{'=' * 60}")
        logger.info(f"統計情報:")
        logger.info(f"  取得レコード数: {self.stats['total_records']:,}")
        logger.info(f"  完了年代数: {self.stats['decades_completed']}")
        logger.info(f"  失敗年代: {self.stats['decades_failed'] or 'なし'}")
        logger.info(f"  総リクエスト数: {self.client.total_requests}")
        logger.info(f"  エラー数: {self.client.total_errors}")
        logger.info(f"{'=' * 60}")
    
    def merge(self, output_file: str | None = None):
        """全年代ファイルを1つのJSONに結合"""
        output_file = output_file or MERGED_FILE
        all_records = []
        seen_qids = set()
        
        json_files = sorted(self.output_dir.glob("people_*.json"))
        
        if not json_files:
            logger.error("結合対象のファイルがありません。")
            return
        
        for filepath in json_files:
            with open(filepath, "r", encoding="utf-8") as f:
                records = json.load(f)
            
            # QIDで重複排除
            for rec in records:
                qid = rec.get("qid")
                if qid and qid not in seen_qids:
                    seen_qids.add(qid)
                    all_records.append(rec)
        
        output_path = self.output_dir / output_file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        
        logger.info(f"結合完了: {output_path}")
        logger.info(f"  ファイル数: {len(json_files)}")
        logger.info(f"  総レコード数: {len(all_records):,}")
        logger.info(f"  重複排除: {sum(len(json.load(open(fp))) for fp in json_files) - len(all_records):,} 件")
    
    def show_status(self):
        """現在の進捗状況を表示"""
        progress = self._load_progress()
        completed = progress.get("completed_decades", [])
        
        total_records = 0
        file_details = []
        
        for filepath in sorted(self.output_dir.glob("people_*.json")):
            with open(filepath, "r", encoding="utf-8") as f:
                records = json.load(f)
            total_records += len(records)
            file_details.append((filepath.name, len(records)))
        
        print(f"\n{'=' * 50}")
        print(f"進捗状況")
        print(f"{'=' * 50}")
        print(f"完了年代数: {len(completed)}")
        print(f"総レコード数: {total_records:,}")
        print(f"最終更新: {progress.get('last_updated', 'なし')}")
        print(f"\nファイル別:")
        for name, count in file_details:
            print(f"  {name}: {count:,} 件")
        print(f"{'=' * 50}")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Wikidata 生年月日・職業データ クローラー",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python wikidata_crawler.py                          # 全範囲を収集
  python wikidata_crawler.py --start 1900 --end 1960  # 1900-1960年
  python wikidata_crawler.py --delay 5                # 5秒間隔
  python wikidata_crawler.py --merge-only             # 結合のみ
  python wikidata_crawler.py --status                 # 進捗確認
        """,
    )
    
    parser.add_argument(
        "--start", type=int, default=DEFAULT_START_YEAR,
        help=f"開始年 (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end", type=int, default=DEFAULT_END_YEAR,
        help=f"終了年 (default: {DEFAULT_END_YEAR})",
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"リクエスト間隔（秒） (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help=f"出力ディレクトリ (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--merge-only", action="store_true",
        help="既存ファイルの結合のみ実行",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="進捗状況を表示",
    )
    parser.add_argument(
        "--user-agent", type=str, default=None,
        help="User-Agentを上書き（メールアドレス含め推奨）",
    )
    parser.add_argument(
        "--page-size", type=int, default=PAGE_SIZE,
        help=f"1クエリあたりの取得件数 (default: {PAGE_SIZE})",
    )
    
    args = parser.parse_args()
    
    if args.user_agent:
        global USER_AGENT
        USER_AGENT = args.user_agent
    
    page_size = args.page_size
    
    output_dir = Path(args.output_dir)
    client = WikidataClient(delay=args.delay)
    crawler = WikidataCrawler(client, output_dir)
    
    if args.status:
        crawler.show_status()
        return
    
    if args.merge_only:
        crawler.merge()
        return
    
    crawler.crawl(args.start, args.end)


if __name__ == "__main__":
    main()
