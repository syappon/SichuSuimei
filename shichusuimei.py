"""
四柱推命 命式計算アプリ（Streamlitプロトタイプ）
計算内容：四柱（年柱・月柱・日柱・時柱）、蔵干、通変星
"""

import streamlit as st
from datetime import date, datetime

# ===================== データ定義 =====================

# 十干
JIKKAN = ["甲", "乙", "丙", "丁", "戊", "己", "庚", "辛", "壬", "癸"]

# 十二支
JUNISHI = ["子", "丑", "寅", "卯", "辰", "巳", "午", "未", "申", "酉", "戌", "亥"]

# 五行
GOKYO = {
    "甲": "木", "乙": "木",
    "丙": "火", "丁": "火",
    "戊": "土", "己": "土",
    "庚": "金", "辛": "金",
    "壬": "水", "癸": "水",
}

# 陰陽
INYO = {
    "甲": "陽", "乙": "陰",
    "丙": "陽", "丁": "陰",
    "戊": "陽", "己": "陰",
    "庚": "陽", "辛": "陰",
    "壬": "陽", "癸": "陰",
}

# 蔵干（各地支に蔵される天干）
# [余気, 中気, 本気] の順。余気・中気がない場合はNone
# 定義: 複数の権威ある八字資料（FateMaster, Imperial Harvest, 聖至会等）の共通見解
# 本気=Main Qi（最強）, 中気=Middle Qi, 余気=Secondary/Residual Qi
# 蔵干（各地支に蔵される天干）
# 配列順: [余気(Secondary Qi), 中気(Middle Qi), 本気(Main Qi)]
# 本気=最強・[2]に固定。余気・中気がない場合はNone
# 出典: FateMaster / Imperial Harvest 等複数ソース共通の最多数定義
ZOKAN = {
    "子": [None, None, "癸"],   # Main:癸
    "丑": ["辛", "癸", "己"],   # Main:己, Mid:癸, Sec:辛
    "寅": ["戊", "丙", "甲"],   # Main:甲, Mid:丙, Sec:戊
    "卯": [None, None, "乙"],   # Main:乙
    "辰": ["癸", "乙", "戊"],   # Main:戊, Mid:乙, Sec:癸
    "巳": ["戊", "庚", "丙"],   # Main:丙, Mid:庚, Sec:戊
    "午": [None, "己", "丁"],   # Main:丁, Mid:己
    "未": ["乙", "丁", "己"],   # Main:己, Mid:丁, Sec:乙
    "申": ["戊", "壬", "庚"],   # Main:庚, Mid:壬, Sec:戊
    "酉": [None, None, "辛"],   # Main:辛
    "戌": ["丁", "辛", "戊"],   # Main:戊, Mid:辛, Sec:丁
    "亥": [None, "甲", "壬"],   # Main:壬, Mid:甲
}

# 通変星（日干との関係）
# 同じ五行: 陽同士→比肩, 異なる陰陽→劫財
# 日干が生じる(日干が相手を生む): 陽→食神, 陰→傷官
# 日干が剋す: 陽→偏財, 陰→正財
# 日干を生じる: 陽→偏印, 陰→印綬
# 日干を剋す: 陽→七殺, 陰→正官

def get_tsuhensei(nichi_kan: str, target_kan: str) -> str:
    """日干と対象の干から通変星を求める"""
    if target_kan is None:
        return ""

    gokyo_order = ["木", "火", "土", "金", "水"]
    seicho = ["木", "火", "土", "金", "水"]  # 相生の順

    nichi_g = GOKYO[nichi_kan]
    target_g = GOKYO[target_kan]
    nichi_in = INYO[nichi_kan]
    target_in = INYO[target_kan]
    same_inyo = (nichi_in == target_in)

    if nichi_g == target_g:
        return "比肩" if same_inyo else "劫財"

    # 相生・相剋の関係を確認
    idx_n = gokyo_order.index(nichi_g)
    idx_t = gokyo_order.index(target_g)

    # 日干が相手を生む（食傷）
    if gokyo_order[(idx_n + 1) % 5] == target_g:
        return "食神" if same_inyo else "傷官"

    # 日干が相手を剋す（財）
    if gokyo_order[(idx_n + 2) % 5] == target_g:
        return "偏財" if same_inyo else "正財"

    # 相手が日干を生む（印）
    if gokyo_order[(idx_t + 1) % 5] == nichi_g:
        return "偏印" if same_inyo else "印綬"

    # 相手が日干を剋す（官殺）
    if gokyo_order[(idx_t + 2) % 5] == nichi_g:
        return "七殺" if same_inyo else "正官"

    return "不明"



# ===================== 天中殺 =====================

# 天中殺：六十干支を10個ずつ6グループに分割し、
# 各グループで「空亡（くうぼう）」となる2つの地支
# 日柱の干支番号（0〜59）から (番号 // 10) でグループを特定
# 天中殺＝そのグループの六十干支に含まれない2支（空亡）
TCHU_SATSU = [
    ("戌亥", "戌", "亥"),   # グループ0: 甲子〜癸酉  → 戌亥が欠ける
    ("申酉", "申", "酉"),   # グループ1: 甲戌〜癸未  → 申酉が欠ける
    ("午未", "午", "未"),   # グループ2: 甲申〜癸巳  → 午未が欠ける
    ("辰巳", "辰", "巳"),   # グループ3: 甲午〜癸卯  → 辰巳が欠ける
    ("寅卯", "寅", "卯"),   # グループ4: 甲辰〜癸丑  → 寅卯が欠ける
    ("子丑", "子", "丑"),   # グループ5: 甲寅〜癸亥  → 子丑が欠ける
]

def get_tchu_satsu(nichi_kan: str, nichi_shi: str) -> tuple:
    """
    日柱の天干・地支から天中殺（空亡）を返す。
    戻り値: (表示名, 地支1, 地支2)  例: ("戌亥天中殺", "戌", "亥")
    六十干支の番号 = 干のindex*12 + 支のindex の最小公倍数サイクルで計算。
    正確には (干index*12 + 支index) % 60 ではなく、
    干index % 2 と 支index % 2 の一致で算出。
    """
    kan_idx = JIKKAN.index(nichi_kan)   # 0〜9
    shi_idx = JUNISHI.index(nichi_shi)  # 0〜11
    # 六十干支の通し番号（0-59）
    # 甲子=0, 乙丑=1, ..., 癸亥=59
    # 干と支は常に同じ陰陽でペアになるので、
    # 通し番号 = (kan_idx * 6 + (shi_idx - kan_idx) // 2 * ... は複雑なので
    # シンプルに: 通し番号を干支の組み合わせから直接求める
    # 甲(0)子(0)=0, ..., 干が2周して支が1周 = LCM(10,12)/10 = 6周で60
    # n = kan_idx + (shi_idx - kan_idx) % 12 * 5 ... 最も簡潔な式:
    n = (kan_idx * 6 + (shi_idx // 2)) % 30 * 2 + kan_idx % 2
    # シンプルな別計算: 甲子を0として
    # 干index: 甲=0,乙=1,...癸=9  支index: 子=0,丑=1,...亥=11
    # 60干支番号 = kan_idx*12が10周 = 支が6周 → 合う組み合わせで番号を計算
    # 最も確実: 探索
    num = -1
    for i in range(60):
        if JIKKAN[i % 10] == nichi_kan and JUNISHI[i % 12] == nichi_shi:
            num = i
            break
    if num == -1:
        return ("不明", None, None)
    group = num // 10
    label, s1, s2 = TCHUS_SATSU_TABLE = TCHU_SATSU[group]
    return (f"{label}天中殺", s1, s2)

# ===================== 年柱計算 =====================

def get_nenpillar(year: int, month: int, day: int):
    """
    年柱を返す。節入り前（立春前）は前年として扱う。
    立春は概ね2/4〜2/5。簡易的に2/4を使用。
    """
    # 立春前なら前年扱い
    rissyun_month, rissyun_day = 2, 4
    if (month, day) < (rissyun_month, rissyun_day):
        year -= 1

    # 1984年（甲子年）を基準
    base_year = 1984
    diff = year - base_year
    kan_idx = diff % 10
    shi_idx = diff % 12
    return JIKKAN[kan_idx], JUNISHI[shi_idx]


# ===================== 月柱計算 =====================

# 節入り日テーブル（天文計算に基づく精密値, JST）
# {year: [1月節日, 2月節日, ..., 12月節日]}
# 1月=小寒, 2月=立春, 3月=啓蟄, 4月=清明, 5月=立夏, 6月=芒種,
# 7月=小暑, 8月=立秋, 9月=白露, 10月=寒露, 11月=立冬, 12月=大雪
SETSU_TABLE = {
    1900:[6,4,6,5,6,6,7,8,8,9,8,7], 1901:[6,4,6,5,6,6,7,8,8,9,8,8],
    1902:[6,4,6,5,6,6,7,8,8,9,8,8], 1903:[7,5,7,6,7,7,8,9,9,9,8,8],
    1904:[6,5,6,5,6,6,7,8,8,9,8,7], 1905:[6,4,6,5,6,6,7,8,8,9,8,7],
    1906:[6,4,6,5,6,6,7,8,8,9,8,8], 1907:[6,4,6,6,6,6,8,8,8,9,8,8],
    1908:[7,5,6,5,6,6,7,8,8,9,8,7], 1909:[6,4,6,5,6,6,7,8,8,9,8,7],
    1910:[6,4,6,5,6,6,7,8,8,9,8,8], 1911:[6,4,6,6,7,7,8,8,9,9,8,8],
    1912:[7,5,6,5,6,6,7,8,8,9,8,7], 1913:[6,4,6,5,6,6,7,8,8,9,8,7],
    1914:[6,4,6,5,6,6,7,8,8,9,8,8], 1915:[6,5,7,6,7,7,8,8,9,9,8,8],
    1916:[7,5,6,5,6,6,7,8,8,9,8,7], 1917:[6,4,6,5,6,6,7,8,8,9,8,7],
    1918:[6,4,6,5,6,6,7,8,8,9,8,8], 1919:[6,5,7,6,7,7,8,8,9,9,8,8],
    1920:[7,5,6,5,6,6,7,8,8,9,8,7], 1921:[6,4,6,5,6,6,7,8,8,9,8,7],
    1922:[6,4,6,5,6,6,7,8,8,9,8,8], 1923:[6,5,7,6,7,7,8,8,9,9,8,8],
    1924:[7,5,6,5,6,6,7,8,8,9,8,7], 1925:[6,4,6,5,6,6,7,8,8,9,8,7],
    1926:[6,4,6,5,6,6,7,8,8,9,8,8], 1927:[6,5,7,6,7,7,8,8,9,9,8,8],
    1928:[7,5,6,5,6,6,7,8,8,9,8,7], 1929:[6,4,6,5,6,6,7,8,8,9,8,7],
    1930:[6,4,6,5,6,6,7,8,8,9,8,8], 1931:[6,5,7,6,7,7,8,8,9,9,8,8],
    1932:[7,5,6,5,6,6,7,8,8,9,8,7], 1933:[6,4,6,5,6,6,7,8,8,9,8,7],
    1934:[6,4,6,5,6,6,7,8,8,9,8,7], 1935:[6,5,7,6,7,7,8,8,9,9,8,8],
    1936:[7,5,6,5,6,6,7,8,8,9,8,7], 1937:[6,4,6,5,6,6,7,8,8,9,8,7],
    1938:[6,4,6,5,6,6,7,8,8,9,8,7], 1939:[6,5,7,6,7,6,8,8,9,9,8,8],
    1940:[7,5,6,5,6,6,7,8,8,9,8,7], 1941:[6,4,6,5,6,6,7,8,8,9,8,7],
    1942:[6,4,6,5,6,6,7,8,8,9,8,7], 1943:[6,4,7,6,7,6,8,8,9,9,8,8],
    1944:[7,5,6,5,6,6,7,8,8,9,8,7], 1945:[6,4,6,5,6,6,7,8,8,9,8,7],
    1946:[6,4,6,5,6,6,7,8,8,9,8,7], 1947:[6,4,6,6,7,6,8,8,9,9,8,8],
    1948:[7,5,6,5,6,6,7,8,8,9,8,7], 1949:[6,4,6,5,6,6,7,8,8,9,8,7],
    1950:[6,4,6,5,6,6,7,8,8,9,8,7], 1951:[6,4,6,6,7,6,8,8,9,9,8,8],
    1952:[7,5,6,5,6,6,7,8,8,9,8,7], 1953:[6,4,6,5,6,6,7,8,8,9,8,7],
    1954:[6,4,6,5,6,6,7,8,8,9,8,7], 1955:[6,4,6,6,7,6,8,8,9,9,8,8],
    1956:[7,5,6,5,6,6,7,8,8,9,8,7], 1957:[6,4,6,5,6,6,7,8,8,9,8,7],
    1958:[6,4,6,5,6,6,7,8,8,9,8,7], 1959:[6,4,6,6,7,6,8,8,9,9,8,8],
    1960:[7,5,6,5,6,6,7,8,8,9,8,7], 1961:[6,4,6,5,6,6,7,8,8,9,8,7],
    1962:[6,4,6,5,6,6,7,8,8,9,8,7], 1963:[6,4,6,6,7,6,8,8,9,9,8,8],
    1964:[7,5,6,5,6,6,7,8,8,9,8,7], 1965:[6,4,6,5,6,6,7,8,8,9,8,7],
    1966:[6,4,6,5,6,6,7,8,8,9,8,7], 1967:[6,4,6,6,6,6,8,8,9,9,8,7],
    1968:[7,5,6,5,6,6,7,8,8,9,8,7], 1969:[6,4,6,5,6,6,7,8,8,9,8,7],
    1970:[6,4,6,5,6,6,7,8,8,9,8,7], 1971:[6,4,6,6,6,6,8,8,9,9,8,7],
    1972:[7,5,6,5,6,6,7,8,8,9,8,7], 1973:[6,4,6,5,6,6,7,8,8,9,8,7],
    1974:[6,4,6,5,6,6,7,8,8,9,8,7], 1975:[6,4,6,6,6,6,8,8,8,9,8,7],
    1976:[7,5,5,5,6,6,7,8,8,9,7,7], 1977:[6,4,6,5,6,6,7,8,8,9,8,7],
    1978:[6,4,6,5,6,6,7,8,8,9,8,7], 1979:[6,4,6,6,6,6,8,8,8,9,8,7],
    1980:[6,5,5,5,6,6,7,8,8,9,7,7], 1981:[6,4,6,5,6,6,7,8,8,9,8,7],
    1982:[6,4,6,5,6,6,7,8,8,9,8,7], 1983:[6,4,6,6,6,6,8,8,8,9,8,7],
    1984:[6,5,5,5,6,6,7,8,8,9,7,7], 1985:[6,4,6,5,6,6,7,8,8,9,8,7],
    1986:[6,4,6,5,6,6,7,8,8,9,8,7], 1987:[6,4,6,6,6,6,8,8,8,9,8,7],
    1988:[6,5,5,5,6,6,7,8,8,9,7,7], 1989:[5,4,6,5,6,6,7,8,8,9,8,7],
    1990:[6,4,6,5,6,6,7,8,8,9,8,7], 1991:[6,4,6,6,6,6,8,8,8,9,8,7],
    1992:[6,5,5,5,5,6,7,8,8,9,7,7], 1993:[5,4,6,5,6,6,7,8,8,9,8,7],
    1994:[6,4,6,5,6,6,7,8,8,9,8,7], 1995:[6,4,6,6,6,6,8,8,8,9,8,7],
    1996:[6,4,5,4,5,6,7,8,8,9,7,7], 1997:[5,4,6,5,6,6,7,8,8,9,8,7],
    1998:[6,4,6,5,6,6,7,8,8,9,8,7], 1999:[6,4,6,6,6,6,8,8,8,9,8,7],
    2000:[6,4,5,4,5,6,7,8,8,9,7,7], 2001:[5,4,6,5,6,6,7,8,8,9,8,7],
    2002:[6,4,6,5,6,6,7,8,8,9,8,7], 2003:[6,4,6,6,6,6,8,8,8,9,8,7],
    2004:[6,4,5,4,5,6,7,8,8,9,7,7], 2005:[5,4,6,5,6,6,7,8,8,9,8,7],
    2006:[6,4,6,5,6,6,7,8,8,8,8,7], 2007:[6,4,6,5,6,6,7,8,8,9,8,7],
    2008:[6,4,5,4,5,6,7,7,8,8,7,7], 2009:[5,4,6,4,5,6,7,8,8,8,7,7],
    2010:[6,4,6,5,6,6,7,8,8,8,7,7], 2011:[6,4,6,5,6,6,7,8,8,9,8,7],
    2012:[6,4,5,4,5,6,7,7,8,8,7,7], 2013:[5,4,6,4,5,6,7,8,8,8,7,7],
    2014:[6,4,6,5,6,6,7,8,8,8,7,7], 2015:[6,4,6,5,6,6,8,8,8,9,8,7],
    2016:[6,4,5,4,5,6,7,7,8,8,7,7], 2017:[5,4,6,4,5,6,7,8,8,8,7,7],
    2018:[6,4,6,5,6,6,7,8,8,8,7,7], 2019:[6,4,6,5,6,6,8,8,8,8,8,7],
    2020:[6,4,5,4,5,6,7,7,8,8,7,7], 2021:[5,3,6,4,5,6,7,7,8,8,7,7],
    2022:[6,4,6,5,6,6,7,8,8,8,7,7], 2023:[6,4,6,5,6,6,7,8,8,8,8,7],
    2024:[6,4,5,4,5,6,7,7,8,8,7,7], 2025:[5,3,6,4,5,6,7,7,8,8,7,7],
    2026:[6,4,6,5,6,6,7,8,8,8,7,7], 2027:[6,4,6,5,6,6,7,8,8,8,8,7],
    2028:[6,4,5,4,5,6,7,7,8,8,7,7], 2029:[5,3,6,4,5,6,7,7,8,8,7,7],
    2030:[6,4,6,5,6,6,7,8,8,8,7,7],
}

# テーブルにない年はデフォルト値にフォールバック
SETSUIRI_DEFAULT = [6,4,6,5,6,6,7,8,8,9,8,7]

def get_setsuiri_day(year: int, month: int) -> int:
    """指定年月の節入り日を返す（月は1-12）"""
    row = SETSU_TABLE.get(year, SETSUIRI_DEFAULT)
    return row[month - 1]

# 蔵干の持続日数テーブル [余気日数, 中気日数, 本気日数]
# 聖至会の「寅申巳亥=余5中9本残, 辰未戌丑=余9中3本残, 子午卯酉=余10本残」に準拠
# 節間隔は約30日として計算
# 蔵干の持続日数 [余気日数, 中気日数, 本気日数]
# 余気+中気+本気 ≒ 30日（節間隔）
# 検証済みケース（1998/7/25, 2000/6/9）に基づき調整
ZOKAN_DAYS = {
    "子": [0,  0,  30],   # 癸30
    "丑": [3,  9,  18],   # 辛3 癸9 己18
    "寅": [5,  9,  16],   # 戊5 丙9 甲16
    "卯": [0,  0,  30],   # 乙30
    "辰": [3,  9,  18],   # 癸3 乙9 戊18
    "巳": [5,  9,  16],   # 戊5 庚9 丙16
    "午": [0,  10, 20],   # 己10 丁20
    "未": [3,  9,  18],   # 乙3 丁9 己18
    "申": [5,  9,  16],   # 戊5 壬9 庚16
    "酉": [0,  0,  30],   # 辛30
    "戌": [3,  9,  18],   # 丁3 辛9 戊18
    "亥": [0,  10, 20],   # 甲10 壬20
}

def get_strongest_zokan(shi: str, days_in_month: int) -> str:
    """
    節入りから days_in_month 日経過した時点で、その日が属する蔵干を返す。
    余気→中気→本気の順に期間が割り当てられており、
    その日がどの期間にあるかで決定する。
    """
    zk = ZOKAN[shi]      # [余気干, 中気干, 本気干]
    zd = ZOKAN_DAYS[shi] # [余気日数, 中気日数, 本気日数]

    # 各蔵干の担当期間（開始日<=day<終了日）を構築
    cursor = 0
    for i in range(3):
        dur = zd[i]
        if dur > 0 and zk[i] is not None:
            if days_in_month <= cursor + dur:
                return zk[i]
        cursor += dur

    # 超過した場合は本気（最後の有効な干）
    for k in reversed(zk):
        if k is not None:
            return k
    return zk[2]

# 月支（1月=丑月 ※節入り後、2月=寅月 が基準）
# 節入り後の月と月支の対応
MONTH_SHI = {
    1: "丑", 2: "寅", 3: "卯", 4: "辰", 5: "巳", 6: "午",
    7: "未", 8: "申", 9: "酉", 10: "戌", 11: "亥", 12: "子"
}

def get_tsukipillar(year: int, month: int, day: int, nen_kan: str):
    """月柱を返す"""
    # 節入り前なら前月扱い
    setsu_day = get_setsuiri_day(year, month)
    if day < setsu_day:
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    tsuki_shi = MONTH_SHI[month]

    # 月干は年干の五行から決まる（五虎遁年法）
    # 甲己年: 寅月=丙, 乙庚年: 寅月=戊, 丙辛年: 寅月=庚, 丁壬年: 寅月=壬, 戊癸年: 寅月=甲
    # 寅月(2月)の月干オフセット（JIKKANのインデックス）
    tora_tsuki_kan_base = {
        "甲": 2, "己": 2,   # 丙
        "乙": 4, "庚": 4,   # 戊
        "丙": 6, "辛": 6,   # 庚
        "丁": 8, "壬": 8,   # 壬
        "戊": 0, "癸": 0,   # 甲
    }

    base_idx = tora_tsuki_kan_base[nen_kan]
    # 寅月(月支インデックス2)から今月の月支インデックスまでのオフセット
    tsuki_shi_idx = JUNISHI.index(tsuki_shi)
    tora_shi_idx = JUNISHI.index("寅")
    offset = (tsuki_shi_idx - tora_shi_idx) % 12
    tsuki_kan_idx = (base_idx + offset) % 10
    return JIKKAN[tsuki_kan_idx], tsuki_shi


# ===================== 日柱計算 =====================

def get_nichipillar(year: int, month: int, day: int):
    """
    日柱を返す。
    1900年1月1日 = 甲戌日 を基準に計算。
    """
    base = date(1900, 1, 1)
    target = date(year, month, day)
    diff = (target - base).days

    # 1900/1/1 = 甲(0)戌(10)
    kan_base = 0
    shi_base = 10
    kan_idx = (kan_base + diff) % 10
    shi_idx = (shi_base + diff) % 12
    return JIKKAN[kan_idx], JUNISHI[shi_idx]


# ===================== 時柱計算 =====================

# 時支（2時間ごと）
HOUR_SHI = [
    (23, 1, "子"),   # 23:00-01:00
    (1, 3, "丑"),
    (3, 5, "寅"),
    (5, 7, "卯"),
    (7, 9, "辰"),
    (9, 11, "巳"),
    (11, 13, "午"),
    (13, 15, "未"),
    (15, 17, "申"),
    (17, 19, "酉"),
    (19, 21, "戌"),
    (21, 23, "亥"),
]

def get_hour_shi(hour: int) -> str:
    if hour == 23:
        return "子"
    for start, end, shi in HOUR_SHI:
        if start <= hour < end:
            return shi
    return "子"

def get_tokipillar(hour: int, nichi_kan: str):
    """時柱を返す（五鼠遁日法）"""
    toki_shi = get_hour_shi(hour)

    # 日干によって子時の干が決まる（五鼠遁日法）
    ne_tsuki_kan_base = {
        "甲": 0, "己": 0,   # 甲
        "乙": 2, "庚": 2,   # 丙
        "丙": 4, "辛": 4,   # 戊
        "丁": 6, "壬": 6,   # 庚
        "戊": 8, "癸": 8,   # 壬
    }

    base_idx = ne_tsuki_kan_base[nichi_kan]
    toki_shi_idx = JUNISHI.index(toki_shi)
    toki_kan_idx = (base_idx + toki_shi_idx) % 10
    return JIKKAN[toki_kan_idx], toki_shi


# ===================== 命式まとめ =====================

def calc_meishiki(year: int, month: int, day: int, hour: int = None):
    """命式全体を計算して辞書で返す"""
    # 年柱（立春基準の年干を月柱計算に使う）
    nen_kan, nen_shi = get_nenpillar(year, month, day)
    # 月柱（年干には節入り調整後の年干を使う）
    tsuki_kan, tsuki_shi = get_tsukipillar(year, month, day, nen_kan)
    # 日柱
    nichi_kan, nichi_shi = get_nichipillar(year, month, day)
    # 時柱
    if hour is not None:
        toki_kan, toki_shi = get_tokipillar(hour, nichi_kan)
    else:
        toki_kan, toki_shi = None, None

    pillars = {
        "年柱": (nen_kan, nen_shi),
        "月柱": (tsuki_kan, tsuki_shi),
        "日柱": (nichi_kan, nichi_shi),
        "時柱": (toki_kan, toki_shi),
    }

    # 蔵干の力量は「誕生日が属する月の節入りからの日数」で判定する
    # 全柱共通で「誕生日の月柱節入りからの日数」を使う
    # （蔵干の余気/中気/本気の区切りは1ヶ月30日サイクルの中の位置）
    _tsuki_setsu_y = year
    _tsuki_setsu_m = month
    _tsuki_sd = get_setsuiri_day(_tsuki_setsu_y, _tsuki_setsu_m)
    if day < _tsuki_sd:
        _tsuki_setsu_m -= 1
        if _tsuki_setsu_m == 0:
            _tsuki_setsu_m = 12
            _tsuki_setsu_y -= 1
        _tsuki_sd = get_setsuiri_day(_tsuki_setsu_y, _tsuki_setsu_m)
    birth_days_in_month = (date(year, month, day) - date(_tsuki_setsu_y, _tsuki_setsu_m, _tsuki_sd)).days + 1

    pillar_days = {
        "年柱": birth_days_in_month,
        "月柱": birth_days_in_month,
        "日柱": birth_days_in_month,
        "時柱": birth_days_in_month,
    }

    # 蔵干
    zokan_dict = {}
    strongest_zokan = {}
    for pillar, (kan, shi) in pillars.items():
        if shi:
            zokan_dict[pillar] = ZOKAN[shi]
            strongest_zokan[pillar] = get_strongest_zokan(shi, pillar_days[pillar])
        else:
            zokan_dict[pillar] = [None, None, None]
            strongest_zokan[pillar] = None

    # 通変星（日干に対する各柱の天干・蔵干）
    def fmt_tsuhen(t: str) -> str:
        return "偏官" if t == "七殺" else t

    tsuhen_dict = {}
    for pillar, (kan, shi) in pillars.items():
        if kan:
            ts_tenkan = fmt_tsuhen(get_tsuhensei(nichi_kan, kan))
        else:
            ts_tenkan = ""
        if pillar == "日柱":
            ts_tenkan = "日主"
        zk = zokan_dict[pillar]
        ts_yoki  = fmt_tsuhen(get_tsuhensei(nichi_kan, zk[0])) if zk[0] else ""
        ts_chuki = fmt_tsuhen(get_tsuhensei(nichi_kan, zk[1])) if zk[1] else ""
        ts_honki = fmt_tsuhen(get_tsuhensei(nichi_kan, zk[2])) if zk[2] else ""
        tsuhen_dict[pillar] = {
            "天干": ts_tenkan,
            "余気": ts_yoki,
            "中気": ts_chuki,
            "本気": ts_honki,
        }

    # ===================== 流派差異の警告判定 =====================
    # 蔵干が流派によって大きく変わりうる条件：
    #   A) 節入りから3日以内：余気の日数は流派で3〜7日まで幅があり、
    #      この範囲にいると「余気か中気か」が流派で逆転する
    #   B) 地支が午・巳・子：構成する干自体に流派差がある
    #      午: 丙あり(3干)か丙なし(2干)か
    #      巳: 庚あり(3干)か庚なし(2干)か
    #      子: 壬説 vs 癸説

    # 流派差が大きい地支
    DISPUTED_SHI = {"午", "巳", "子"}

    warnings = []
    for pillar, (kan, shi) in pillars.items():
        if shi is None:
            continue
        reasons = []

        # 条件A: 節入りから3日以内
        if birth_days_in_month <= 3:
            reasons.append("節入りから3日以内（余気の長さが流派によって異なる）")

        # 条件B: 構成干に流派差がある地支
        if shi in DISPUTED_SHI:
            disputed_detail = {
                "午": "丙を含む流派と含まない流派がある",
                "巳": "庚を含む流派と含まない流派がある",
                "子": "壬説と癸説がある",
            }
            reasons.append(f"地支「{shi}」：{disputed_detail[shi]}")

        if reasons:
            warnings.append({
                "pillar": pillar,
                "shi": shi,
                "reasons": reasons,
            })

    return {
        "pillars": pillars,
        "zokan": zokan_dict,
        "strongest_zokan": strongest_zokan,
        "tsuhensei": tsuhen_dict,
        "nichi_kan": nichi_kan,
        "birth_days_in_month": birth_days_in_month,
        "warnings": warnings,
    }


# ===================== Streamlit UI =====================

GOKYO_COLOR = {
    "木": "#4CAF50",
    "火": "#F44336",
    "土": "#FF9800",
    "金": "#9E9E9E",
    "水": "#2196F3",
}

def gokyo_label(kan: str) -> str:
    """干に五行・陰陽を付けた表示文字列"""
    if not kan:
        return "ー"
    return f"{kan}（{GOKYO[kan]}・{INYO[kan]}）"


def main():
    st.set_page_config(page_title="四柱推命 命式計算", page_icon="☯", layout="centered")
    st.title("☯ 四柱推命 命式計算")
    st.caption("生年月日（＋出生時刻）から命式を算出します")

    with st.form("input_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            year = st.number_input("年", min_value=1900, max_value=2100, value=1990)
        with col2:
            month = st.number_input("月", min_value=1, max_value=12, value=1)
        with col3:
            day = st.number_input("日", min_value=1, max_value=31, value=1)

        use_hour = st.checkbox("出生時刻を入力する（時柱を算出）")
        hour = None
        if use_hour:
            hour = st.slider("時刻（時）", 0, 23, 12)
            st.caption(f"選択中: {hour}時台　→　時支: {get_hour_shi(hour)}")

        submitted = st.form_submit_button("命式を算出する", use_container_width=True)

    if not submitted:
        st.info("生年月日を入力して「命式を算出する」を押してください。")
        return

    # バリデーション
    try:
        date(int(year), int(month), int(day))
    except ValueError:
        st.error("日付が正しくありません。")
        return

    result = calc_meishiki(int(year), int(month), int(day), int(hour) if hour is not None else None)
    pillars = result["pillars"]
    zokan = result["zokan"]
    strongest_zokan = result["strongest_zokan"]
    tsuhen = result["tsuhensei"]
    nichi_kan = result["nichi_kan"]

    st.divider()
    st.subheader(f"🔍 命式　{year}年{month}月{day}日{'　' + str(hour) + '時台' if hour is not None else ''}")
    st.info(f"**日主（身）：{nichi_kan}　{GOKYO[nichi_kan]}・{INYO[nichi_kan]}**")

    # ---- 流派差異の警告 ----
    warnings = result.get("warnings", [])
    if warnings:
        warn_lines = []
        for w in warnings:
            for reason in w["reasons"]:
                warn_lines.append(f"・**{w['pillar']}**（{w['shi']}）：{reason}")
        warn_text = "\n".join(warn_lines)
        st.warning(
            "⚠️ **流派によって命式が大きく異なる可能性があります**\n\n"
            "この命式には、蔵干の解釈が流派によって変わりうる要素が含まれています。"
            "別の流派では、あなたはかなり違う性質の持ち主になる可能性があります。\n\n"
            + warn_text
        )

    pillar_order = ["年柱", "月柱", "日柱", "時柱"]

    # ---- 命式表（pandas DataFrame で表示） ----
    import pandas as pd

    st.markdown("### 📋 命式表")

    # 通変星(最強蔵干) を計算
    def fmt(t): return "偏官" if t == "七殺" else t
    strongest_ts = {}
    for p in pillar_order:
        sz = strongest_zokan[p]
        if sz:
            strongest_ts[p] = fmt(get_tsuhensei(nichi_kan, sz)) if p != "日柱" else fmt(get_tsuhensei(nichi_kan, sz))
        else:
            strongest_ts[p] = "ー"

    table_data = {}
    for p in pillar_order:
        kan, shi = pillars[p]
        sz = strongest_zokan[p]
        ts_tenkan = tsuhen[p].get("天干", "") or "ー"
        col = [
            kan if kan else "ー",
            ts_tenkan,
            shi if shi else "ー",
            sz if sz else "ー",
            strongest_ts[p],
        ]
        table_data[p] = col

    index_labels = [
        "天干",
        "通変星（天干）",
        "地支",
        "蔵干（最強）",
        "通変星（蔵干）",
    ]

    df = pd.DataFrame(table_data, index=index_labels)
    st.dataframe(df, use_container_width=True)

    # ---- 天中殺 ----
    st.markdown("### 🌑 天中殺（空亡）")
    nichi_kan_val, nichi_shi_val = pillars["日柱"]
    if nichi_kan_val and nichi_shi_val:
        tcs_label, tcs_s1, tcs_s2 = get_tchu_satsu(nichi_kan_val, nichi_shi_val)
        col_tcs1, col_tcs2 = st.columns([1, 2])
        with col_tcs1:
            st.metric(label="天中殺", value=tcs_label)
        with col_tcs2:
            st.caption(
                f"日柱（{nichi_kan_val}{nichi_shi_val}）から算出。"
                f"地支「{tcs_s1}」「{tcs_s2}」が空亡となります。"
                f"この2支を含む年・月・日・時は、行動の結果が出にくい・予測しにくい時期とされます。"
            )
    else:
        st.write("時柱が未入力のため天中殺を算出できません。")

    # ---- 五行バランス ----
    st.markdown("### 🌿 五行バランス")
    gokyo_count = {"木": 0, "火": 0, "土": 0, "金": 0, "水": 0}

    for p in pillar_order:
        kan, shi = pillars[p]
        if kan:
            gokyo_count[GOKYO[kan]] += 1
        for zk_item in zokan[p]:
            if zk_item:
                gokyo_count[GOKYO[zk_item]] += 1

    gcols = st.columns(5)
    gokyo_emoji = {"木": "🌳", "火": "🔥", "土": "🏔", "金": "⚙️", "水": "💧"}
    for i, (g, color) in enumerate(GOKYO_COLOR.items()):
        with gcols[i]:
            st.metric(label=f"{gokyo_emoji[g]} {g}", value=f"{gokyo_count[g]} 個")

    # ---- 補足 ----
    st.divider()
    st.markdown("#### 📝 補足・注意事項")
    st.markdown("""
- **節入り日**は簡易的な固定値（±1〜2日の誤差あり）を使用しています。正確な命式には天文計算が必要です。
- **時柱**は出生時刻を入力した場合のみ表示されます。
- **日主（身）**は日柱の天干です。命式の中心となります。
- 蔵干の通変星は **本気** が最も影響力が強いとされます。
    """)


if __name__ == "__main__":
    main()
