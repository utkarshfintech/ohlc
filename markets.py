"""Official free symbol-list sources for HKEX, TSE, KRX and Europe.

Each loader downloads the official listed-securities directory, filters to
equities (stocks + ETFs + REITs; excludes warrants/CBBC/structured), and
returns {yahoo_symbol: {'FullName': ..., 'Exchange': ..., 'ISIN': ...}}.

Results are cached locally to CSV for fast re-runs.
Yahoo suffix: .HK (HKEX) / .T (TSE) / .KS (KOSPI) / .KQ (KOSDAQ) /
.PA .AS .BR .LS .IR .MI .OL (Euronext) / .L (LSE) / .F (Frankfurt FWB).
"""

from __future__ import annotations

import csv
import io
import re
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CACHE_DIR = Path(__file__).resolve().parent
_CACHE_FIELDS = ["symbol", "FullName", "Exchange", "ISIN", "Currency", "Country"]

# HKEX main securities list (with Category + ISIN columns).
HKEX_LIST_XLSX_URL = (
    "https://www.hkex.com.hk/eng/services/trading/securities/"
    "securitieslists/ListOfSecurities.xlsx"
)
JPX_LIST_PAGE = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html"
KRX_FINDER_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
_KRX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Referer": (
        "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd"
        "?menuId=MDC0201"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "http://data.krx.co.kr",
}
# Free Korea Investment master files: short code + ISIN (표준코드), no API key.
_KIS_MASTER_URL = "https://new.real.download.dws.co.kr/common/master/{name}"

# JASDEC handled-securities list (取扱銘柄一覧, monthly): authoritative ISINs
# for EVERY handled security — stocks + ETFs + ETNs + REITs — mirrored to
# tickers by JASDEC's 銘柄コード (= TSE code + trailing check char).
_JASDEC_YUCHO_URL = (
    "https://www.jasdec.com/assets/download/ds/meigara-yucho.xls"
)

# Free open mirror (auto-refreshed daily) of the JPX/TSE universe with ISINs.
# Covers ~86% of TSE (missing tail is mostly ETFs/ETNs without ISINs) — the
# remainder falls through to a Business Insider company-name lookup.
_TSE_ISIN_CSV_URL = (
    "https://raw.githubusercontent.com/adanos-software/"
    "free-ticker-database/main/data/listings.csv"
)
_TSE_ISIN_CACHE = CACHE_DIR / "jpx_isin_cache.csv"

# TSE-quoted foreign instruments that JASDEC's handled-securities list does
# not carry (physical gold share / physically-linked ETC). Their official
# issuer pages document the single ISIN used on every listing. Always applied
# on top of any source so cached maps stay correct too.
_TSE_ISIN_OVERRIDES: dict[str, str] = {
    "1326": "US78463V1070",  # SPDR Gold Shares — ssga.com/jp/ja/individual/etfs/spdr-gold-shares-gld
    "1694": "GB00B15KY211",  # WisdomTree Nickel — wisdomtree.eu/products/.../wisdomtree-nickel (TSE ticker 1694)
}

# Business Insider suggest endpoint (the engine behind yfinance get_isin()).
_BI_SUGGEST_URL = (
    "https://markets.businessinsider.com/ajax/"
    "SearchController_Suggest?max_results=25&query={q}"
)
_BI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# Europe (Euronext / LSE / Frankfurt FWB)
# ---------------------------------------------------------------------------

# Euronext 'Stocks all markets' page embeds a JS download gateway:
#   "jsongateway_download": "/product_directory/data/stocks-all-places/download?mics=..."
# which streams ONE semicolon CSV for every venue
# (Name;ISIN;Symbol;Market;Currency;...). The 'Market' label localises each
# row to a country, so a single table (EURONEXT_historic_ohlc) is
# differentiated per row by Country. Yahoo symbol = Symbol + venue suffix.
_EURONEXT_ALL_PAGE = "https://live.euronext.com/en/products/equities/list"
_EURONEXT_DATA_BASE = "https://live.euronext.com"

# Market-label keyword -> (country, Yahoo suffix). Labels can combine venues
# ('Euronext Paris, Brussels'); the first match wins so the primary quote sits
# on the right Yahoo suffix (Paris before Brussels, Amsterdam before Brussels).
_EURONEXT_VENUES: tuple[tuple[str, str, str], ...] = (
    ("paris", "France", ".PA"),
    ("amsterdam", "Netherlands", ".AS"),
    ("brussels", "Belgium", ".BR"),
    ("lisbon", "Portugal", ".LS"),
    ("dublin", "Ireland", ".IR"),
    ("milan", "Italy", ".MI"),
    ("oslo", "Norway", ".OL"),
)
# Cross-listing / alternate-book labels in the all-sites file: those rows are
# duplicates of the national-book rows or unplaceable (same ISIN, no venue).
_EURONEXT_DROP_LABELS = (
    "trading after hours",
    "global equity market",
    "eurtlx",
)

# LSE 'List of All Companies' (Main Market + AIM) mirrored on GitHub —
# LSEG's official bulk reference-data files are FTP/SFTP-only. The CSV
# carries ISIN + TIDM (Yahoo-style ticker) + LSE Market per security.
_LSE_LIST_CSV_URL = (
    "https://raw.githubusercontent.com/jessicayung/machine-learning-nd/"
    "master/p5-capstone/list-of-all-securities-ex-debt.csv"
)

# Frankfurt (FWB): official Deutsche Börse 'T7 (Frankfurt) All tradable
# instruments' CSV — all rows are MIC XFRA. The blob URL can change, so it is
# located through the downloads page on every load. Ordinary shares carry
# Instrument Type == 'CS'; symbol = Mnemonic + Yahoo suffix '.F'.
_FWB_INSTRUMENT_PAGE = (
    "https://www.cashmarket.deutsche-boerse.com/cash-en/trading/"
    "Tradable-Instruments-Xetra/Downloads/frankfurt-downloads"
)
_FWB_INSTRUMENT_RE = re.compile(
    r'href="([^"]*t7-xfra-BF-allTradableInstruments\.csv)"'
)

_ISIN_RE = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")

# HKEX securities list categories kept (equities only: stocks + ETFs + REITs).
_HK_KEEP_CATEGORIES = {
    "EQUITY",
    "EXCHANGE TRADED PRODUCTS",
    "REAL ESTATE INVESTMENT TRUSTS",
}

# Instrument / section filters (case-insensitive sets)
_JP_KEEP_SECTIONS = {
    "Prime Market (Domestic)",
    "Standard Market (Domestic)",
    "Growth Market (Domestic)",
    "ETFs/ ETNs",
    "REITs",
}

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _download(url: str, timeout: int = 90) -> bytes:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(5 << attempt)  # 5s, 10s, 20s backoff
    raise RuntimeError(f"symbol-list download failed after 4 attempts: {url}") from last_error


def _download_text(url: str, timeout: int = 90) -> str:
    return _download(url, timeout=timeout).decode("utf-8", errors="replace")


def _save_cache(path: Path, meta: dict[str, dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CACHE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for sym in sorted(meta):
            writer.writerow({"symbol": sym, **meta[sym]})


def _load_cache(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        out: dict[str, dict[str, str]] = {}
        for r in csv.DictReader(f):
            row = {**r, "ISIN": r.get("ISIN", "")}
            for key in ("Currency", "Country"):
                if not row.get(key):
                    row.pop(key, None)
            out[row["symbol"]] = row
        return out


# ---------------------------------------------------------------------------
# HKEX (Hang Seng / SEHK)
# ---------------------------------------------------------------------------


def _locate_header_row(df: pd.DataFrame, keywords: tuple[str, ...], limit: int = 40) -> int:
    """Find the row where a cell matches every required keyword (title rows like
    'By Stock Code Order' contain a keyword only as a substring and are skipped).
    The real header also carries at least three non-null cells."""
    wanted = [k.strip().lower() for k in keywords]
    for i in range(min(limit, len(df))):
        vals = [str(v).strip().lower() for v in df.iloc[i].values if pd.notna(v)]
        if len(vals) >= 3 and all(any(w in v for v in vals) for w in wanted):
            return i
    return 0


def _fetch_hk() -> dict[str, dict[str, str]]:
    raw = _download(HKEX_LIST_XLSX_URL)
    df_raw = pd.read_excel(io.BytesIO(raw), header=None, engine="calamine")

    # The sheet has a title block; the real header holds "Stock Code" + "ISIN".
    hdr = _locate_header_row(df_raw, ("Stock Code", "ISIN"))
    cols = [str(c).strip() for c in df_raw.iloc[hdr]]
    df = df_raw.iloc[hdr + 1 :].copy().reset_index(drop=True)
    df.columns = cols

    # Map columns by keyword (defensive against minor header changes)
    code_col = next((c for c in cols if "stock code" in c.lower()), None)
    name_col = next((c for c in cols if "name" in c.lower() and "of" in c.lower()), None)
    cat_col = next((c for c in cols if c.lower() == "category"), None)
    isin_col = next((c for c in cols if c.lower() == "isin"), None)

    if not all([code_col, name_col, cat_col, isin_col]):
        raise ValueError(f"HKEX column mapping failed — available columns: {cols}")

    df[code_col] = df[code_col].fillna("").astype(str)
    df[cat_col] = df[cat_col].fillna("").astype(str)
    df[isin_col] = df[isin_col].fillna("").astype(str)

    keep = df[cat_col].str.upper().str.strip().isin(_HK_KEEP_CATEGORIES)

    meta: dict[str, dict[str, str]] = {}
    for _, row in df[keep].iterrows():
        raw_code = row[code_col].strip()
        try:
            code = int(float(raw_code))
        except (ValueError, TypeError):
            continue
        if not (1 <= code <= 9999):
            continue
        symbol = f"{str(code).zfill(4)}.HK"
        name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        isin = row[isin_col].strip().upper()
        meta[symbol] = {
            "FullName": name,
            "Exchange": "HKEX",
            "ISIN": isin if len(isin) == 12 else "",
        }
    return meta


# ---------------------------------------------------------------------------
# TSE (Tokyo Stock Exchange)
# ---------------------------------------------------------------------------


def _fetch_jp() -> dict[str, dict[str, str]]:
    # Parse the JPX list page to locate the latest data_e.xlsx link
    page_html = _download_text(JPX_LIST_PAGE)
    match = re.search(r'href="([^"]*data_e\.xlsx)"', page_html)
    if not match:
        raise RuntimeError(
            "Could not locate data_e.xlsx link on JPX TSE-list page."
        )
    href = match.group(1)
    if href.startswith("/"):
        href = "https://www.jpx.co.jp" + href

    raw = _download(href)
    df = pd.read_excel(io.BytesIO(raw), engine="calamine")

    # Identify columns (exact names from Aug-2026 file: "Local Code",
    # "Name (English)", "Section/Products") — matched defensively
    code_col = next(
        (c for c in df.columns if "local" in c.lower() and "code" in c.lower()),
        None,
    )
    name_col = next(
        (c for c in df.columns if "name" in c.lower() and "english" in c.lower()),
        None,
    )
    section_col = next(
        (c for c in df.columns if "section" in c.lower() or "product" in c.lower()),
        None,
    )

    if not all([code_col, name_col, section_col]):
        raise ValueError(
            f"JPX column mapping failed — available columns: {list(df.columns)}"
        )

    df = df[df[section_col].astype(str).str.strip().isin(_JP_KEEP_SECTIONS)].copy()

    meta: dict[str, dict[str, str]] = {}
    code_names: dict[str, str] = {}
    for _, row in df.iterrows():
        try:
            code = str(int(row[code_col])).zfill(4)
        except (ValueError, TypeError):
            continue
        name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        symbol = f"{code}.T"
        code_names[code] = name
        meta[symbol] = {"FullName": name, "Exchange": "TSE", "ISIN": ""}

    isin_map = _fetch_jp_isin_map(code_names)
    for symbol in meta:
        meta[symbol]["ISIN"] = isin_map.get(symbol[:-2], "")
    return meta


def _fetch_jasdec_tse_isin_map() -> dict[str, str]:
    """TSE code -> ISIN from the JASDEC handled-securities list.

    The JASDEC 銘柄コード column is the TSE security code plus one trailing
    check character (e.g. 6902 -> '69020', 2031 -> '20310', 130A -> '130A0');
    ISIN is given for every handled instrument (stocks, ETFs, ETNs, REITs).
    Requires xlrd; returns an empty map when it is unavailable.
    """
    try:
        import xlrd  # noqa: PLC0415
    except ImportError:
        return {}
    raw = _download(_JASDEC_YUCHO_URL)
    book = xlrd.open_workbook(file_contents=raw, on_demand=True)
    sheet = book.sheet_by_index(0)

    isin_map: dict[str, str] = {}
    for row in range(sheet.nrows):
        code = sheet.cell_value(row, 0)
        isin = sheet.cell_value(row, 1)
        if isinstance(code, float):
            code = str(int(code))
        else:
            code = str(code).strip()
        isin = str(isin).strip().upper()
        if len(code) <= 1 or len(isin) != 12 or not isin.startswith("JP"):
            continue
        tse_code = code[:-1]
        isin_map[tse_code] = isin
    return isin_map


def _fetch_jp_isin_map(code_names: dict[str, str]) -> dict[str, str]:
    """TSE code -> ISIN, cached locally (refresh by deleting the cache file).

    Three tiers, in order of authority:
      1. JASDEC handled-securities list (official; covers stocks + ETFs +
         ETNs + REITs).
      2. free-ticker-database listings.csv (JPX official masterfiles +
         OpenFIGI/GLEIF enrichment).
      3. For codes neither carries, a Business Insider company-name lookup
         (yfinance's get_isin() uses BI too but matches the literal 'XXXX.T|'
         string, which never hits JP issues — BI indexes them by OTC ticker +
         real JP ISIN). Candidates are accepted only when JP-prefixed and the
         row name overlaps the queried name, rejecting BI's cross-market
         look-alikes (e.g. an HSI ETN matched to Henry Schein).
    """
    import csv
    import io as _io
    import time as _time
    import urllib.parse
    import urllib.request as _url

    if _TSE_ISIN_CACHE.exists():
        try:
            with _TSE_ISIN_CACHE.open(newline="", encoding="utf-8") as f:
                isin_map = {r["code"]: r["ISIN"] for r in csv.DictReader(f)}
                isin_map.update(_TSE_ISIN_OVERRIDES)
                return isin_map
        except (OSError, KeyError, ValueError):
            pass

    isin_map: dict[str, str] = {}

    isin_map.update(_fetch_jasdec_tse_isin_map())

    req = _url.Request(_TSE_ISIN_CSV_URL, headers=_BI_HEADERS)
    with _url.urlopen(req, timeout=180) as resp:
        text = resp.read().decode("utf-8", errors="replace")
    for row in csv.DictReader(_io.StringIO(text)):
        if (row.get("exchange") or "").strip() != "TSE":
            continue
        isin = (row.get("isin") or "").strip()
        code = (row.get("ticker") or "").strip()
        if len(isin) == 12 and code.isdigit():
            isin_map[code.zfill(4)] = isin

    for code, name in code_names.items():
        if code in isin_map or not name:
            continue
        isin = _tse_isin_by_name(name)
        if isin:
            isin_map[code] = isin
        _time.sleep(0.2)

    try:
        with _TSE_ISIN_CACHE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["code", "ISIN"])
            for code in sorted(isin_map):
                writer.writerow([code, isin_map[code]])
    except OSError:
        pass
    isin_map.update(_TSE_ISIN_OVERRIDES)
    return isin_map


def _norm_tokens(text: str) -> set[str]:
    words = re.sub(r"[^a-z0-9 ]", " ", text.lower()).split()
    return {w for w in words if len(w) >= 3}


_LEGAL_SUFFIX = re.compile(
    r"\b(?:co\.?|ltd\.?|limited|company|inc\.?|incorporated|"
    r"corp\.?|corporation|holdings?|group|kk|k\.k\.|plc)\b"
)


def _extract_bi_isin(data: str, query: str) -> str | None:
    """Parse a BI suggest payload; return the first trustworthy JP ISIN."""
    query_tokens = _norm_tokens(query)
    query_core = re.sub(r"[^a-z0-9]", "", query.lower())
    best: str | None = None
    for m in re.finditer(
        r'new Array\("([^"]{1,120})",\s*"([^"]{1,60})",\s*"([^"]{1,200})"', data
    ):
        row_name = m.group(1)
        category = m.group(2).lower()
        parts = m.group(3).split("|")
        if len(parts) < 2:
            continue
        isin = parts[1].strip().upper()
        if not re.fullmatch(r"[A-Z]{2}\d{10}", isin) or not isin.startswith("JP"):
            continue
        row_tokens = _norm_tokens(row_name)
        row_norm = re.sub(r"[^a-z0-9]", "", row_name.lower())
        tok = len(query_tokens & row_tokens)
        sub = bool(query_core) and query_core in row_norm
        if category == "stocks":
            if tok >= 2 or sub:
                return isin
        elif category in ("funds", "etf", "exotisch", "notes"):
            continue
        elif best is None and tok >= 2 and sub:
            best = isin
    return best


def _tse_isin_by_name(name: str) -> str | None:
    """Resolve a TSE issue's ISIN via a Business Insider name query."""
    import urllib.parse
    import urllib.request as _url

    variants: list[str] = []
    seen: set[str] = set()

    def add(variant: str) -> None:
        variant = re.sub(r"\s+", " ", variant or "").strip()
        if variant and variant not in seen:
            seen.add(variant)
            variants.append(variant)

    add(name)
    cur = name or ""
    prev: str | None = None
    while cur != prev:
        prev = cur
        nxt = re.sub(_LEGAL_SUFFIX.pattern + r"[,\s\.]*$", "", cur).strip()
        if nxt and nxt != cur:
            cur = nxt
            add(cur)
        else:
            break
    compact = re.sub(
        r"\s+",
        " ",
        re.sub(_LEGAL_SUFFIX.pattern, " ", re.sub(r"[^\w ]", " ", name.lower())),
    ).strip()
    add(compact)

    for q in variants:
        url = _BI_SUGGEST_URL.format(
            q=urllib.parse.quote(q.replace(" ", "+"))
        )
        try:
            with _url.urlopen(_url.Request(url, headers=_BI_HEADERS), timeout=45) as resp:
                data = resp.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - transient network errors
            continue
        isin = _extract_bi_isin(data, q)
        if isin:
            return isin
    return None


# ---------------------------------------------------------------------------
# KRX (Korea Exchange — KOSPI / KOSDAQ)
# ---------------------------------------------------------------------------


def _fetch_kr_isin_map(market: str) -> dict[str, str]:
    """short code -> ISIN from the free Korea Investment master file."""
    name = "kospi_code.mst.zip" if market == "kospi" else "kosdaq_code.mst.zip"
    raw = _download(_KIS_MASTER_URL.format(name=name))
    z = zipfile.ZipFile(io.BytesIO(raw))
    text = z.read(z.namelist()[0]).decode("cp949", errors="replace")

    isin_map: dict[str, str] = {}
    for line in text.splitlines():
        rf1 = line[: len(line) - 228]
        if len(rf1) < 21:
            continue
        short = rf1[:9].strip()
        isin = rf1[9:21].strip()
        # Include letter-suffix codes (preferred shares, REITs: e.g. 00104K)
        # and non-KR-prefixed ISINs (foreign REITs on KOSDAQ: e.g. HK/KY...).
        if short and len(isin) == 12:
            isin_map[short] = isin
    return isin_map


def _fetch_kr(market: str) -> dict[str, dict[str, str]]:
    import requests

    mkt_id = "STK" if market == "kospi" else "KSQ"
    exchange_label = "KOSPI" if market == "kospi" else "KOSDAQ"
    suffix = ".KS" if market == "kospi" else ".KQ"

    r = requests.post(
        KRX_FINDER_URL,
        headers=_KRX_HEADERS,
        data={
            "bld": "dbms/comm/finder/finder_stkisu",
            "locale": "en",
            "mktsel": mkt_id,
            "searchText": "",
            "typeNo": 0,
        },
        timeout=60,
    )
    r.raise_for_status()
    rows = (r.json().get("block1") or r.json().get("output") or [])

    meta: dict[str, dict[str, str]] = {}
    for row in rows:
        short_code = str(row.get("short_code", "")).strip()
        code_name = str(row.get("codeName", "")).strip()
        if not short_code or not code_name:
            continue
        meta[f"{short_code}{suffix}"] = {
            "FullName": code_name,
            "Exchange": exchange_label,
            "ISIN": "",
        }

    # Overlay official ISINs (표준코드) from the Korea Investment master file.
    for short_code, isin in _fetch_kr_isin_map(market).items():
        sym = f"{short_code}{suffix}"
        if sym in meta:
            meta[sym]["ISIN"] = isin
    return meta


# ---------------------------------------------------------------------------
# Europe — Euronext (ONE table, per-row Country)
# ---------------------------------------------------------------------------


def _euronext_all_text() -> str:
    page = _download_text(_EURONEXT_ALL_PAGE)
    match = re.search(r'"jsongateway_download":"(.*?)"', page)
    if not match:
        raise RuntimeError("Euronext: could not locate the all-equities download link")
    path = match.group(1).replace("\\/", "/")
    url = path if path.startswith("http") else _EURONEXT_DATA_BASE + path
    return _download_text(url, timeout=300)


def _fetch_euronext() -> dict[str, dict[str, str]]:
    meta: dict[str, dict[str, str]] = {}
    for row in csv.reader(io.StringIO(_euronext_all_text()), delimiter=";"):
        if len(row) < 5:
            continue
        isin = (row[1] or "").strip().upper()
        symbol = (row[2] or "").strip()
        name = (row[0] or "").strip().strip('"')
        market = (row[3] or "").strip().strip('"')
        currency = (row[4] or "").strip().strip('"')
        if not _ISIN_RE.fullmatch(isin) or not symbol or not market:
            continue
        label = market.lower()
        if any(d in label for d in _EURONEXT_DROP_LABELS):
            continue
        country = suffix = None
        for keyword, country, suffix in _EURONEXT_VENUES:
            if keyword in label:
                break
        else:
            continue
        key = f"{symbol}{suffix}"
        if key in meta:
            continue
        meta[key] = {
            "FullName": name,
            "Exchange": market,
            "ISIN": isin,
            "Currency": currency or "EUR",
            "Country": country,
        }
    return meta


# ---------------------------------------------------------------------------
# Europe — London Stock Exchange
# ---------------------------------------------------------------------------


def _fetch_lse() -> dict[str, dict[str, str]]:
    text = _download_text(_LSE_LIST_CSV_URL)
    rows = csv.reader(io.StringIO(text))
    header = next(rows, ())
    cols = {str(c).strip().lower(): i for i, c in enumerate(header)}
    isin_i = cols.get("isin")
    tidm_i = cols.get("tidm")
    market_i = cols.get("lse market")
    name_i = cols.get("company name")
    if isin_i is None or tidm_i is None or market_i is None:
        raise ValueError(f"LSE column mapping failed — available columns: {list(header)}")

    meta: dict[str, dict[str, str]] = {}
    for row in rows:
        if len(row) <= max(isin_i, tidm_i, market_i):
            continue
        market = (row[market_i] or "").strip().upper()
        if "AIM" not in market and "MAIN" not in market:
            continue
        isin = (row[isin_i] or "").strip().upper()
        if not _ISIN_RE.fullmatch(isin):
            continue
        tidm = re.sub(r"[^A-Z0-9]", "", (row[tidm_i] or "").strip().upper())
        if not tidm:
            continue
        name = (row[name_i] or "").strip() if name_i is not None else ""
        meta[f"{tidm}.L"] = {
            "FullName": name,
            "Exchange": "LSE" if "MAIN" in market else "LSE AIM",
            "ISIN": isin,
            "Currency": "GBP",
            "Country": "United Kingdom",
        }
    return meta


# ---------------------------------------------------------------------------
# Europe — Frankfurt (FWB, Börse Frankfurt, Yahoo suffix .F)
# ---------------------------------------------------------------------------


def _frankfurt_instruments_text() -> str:
    page = _download_text(_FWB_INSTRUMENT_PAGE)
    match = _FWB_INSTRUMENT_RE.search(page)
    if not match:
        raise RuntimeError("FWB: could not locate the tradable-instruments CSV link")
    url = match.group(1)
    if url.startswith("/"):
        url = "https://www.cashmarket.deutsche-boerse.com" + url
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=300) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _fetch_fwb() -> dict[str, dict[str, str]]:
    parsed = list(csv.reader(io.StringIO(_frankfurt_instruments_text()), delimiter=";"))
    hdr_i = next(
        (i for i, r in enumerate(parsed) if "ISIN" in r and "Mnemonic" in r),
        None,
    )
    if hdr_i is None:
        raise ValueError("FWB: header row not found")
    idx = {c: j for j, c in enumerate(parsed[hdr_i])}
    isin_i = idx.get("ISIN")
    mnem_i = idx.get("Mnemonic")
    type_i = idx.get("Instrument Type")
    name_i = idx.get("Instrument")
    cur_i = idx.get("Currency")
    mic_i = idx.get("MIC Code")
    if None in (isin_i, mnem_i, type_i, name_i):
        raise ValueError(f"FWB: column mapping failed — {list(idx)}")

    meta: dict[str, dict[str, str]] = {}
    for r in parsed[hdr_i + 1 :]:
        if len(r) <= max(isin_i, mnem_i, type_i, name_i):
            continue
        if mic_i is not None and (r[mic_i] or "").strip() != "XFRA":
            continue
        if (r[type_i] or "").strip() != "CS":
            continue
        mnem = (r[mnem_i] or "").strip()
        isin = (r[isin_i] or "").strip().upper()
        if not mnem or not _ISIN_RE.fullmatch(isin):
            continue
        meta[f"{mnem}.F"] = {
            "FullName": (r[name_i] or "").strip(),
            "Exchange": "Frankfurt (FWB)",
            "ISIN": isin,
            "Currency": (r[cur_i] or "").strip() or "EUR",
            "Country": "Germany",
        }
    return meta


# ---------------------------------------------------------------------------
# Public API — called by main.py
# ---------------------------------------------------------------------------


def get_hang_seng_symbols(refresh: bool = False) -> dict[str, dict[str, str]]:
    """HKEX: full universe of Main Board + GEM equities."""
    cache = CACHE_DIR / "symbols_hk_cache.csv"
    if cache.exists() and not refresh:
        return _load_cache(cache)
    meta = _fetch_hk()
    _save_cache(cache, meta)
    return meta


def get_tse_symbols(refresh: bool = False) -> dict[str, dict[str, str]]:
    """TSE: full universe (Prime / Standard / Growth + ETFs + REITs)."""
    cache = CACHE_DIR / "symbols_jp_cache.csv"
    if cache.exists() and not refresh:
        return _load_cache(cache)
    meta = _fetch_jp()
    _save_cache(cache, meta)
    return meta


def get_krx_symbols(
    market: str,
    refresh: bool = False,
) -> dict[str, dict[str, str]]:
    """KRX KOSPI or KOSDAQ full universe."""
    if market not in ("kospi", "kosdaq"):
        raise ValueError(f"market must be 'kospi' or 'kosdaq', got '{market}'")
    cache = CACHE_DIR / f"symbols_kr_{market}_cache.csv"
    if cache.exists() and not refresh:
        return _load_cache(cache)
    meta = _fetch_kr(market)
    _save_cache(cache, meta)
    return meta


def get_euronext_symbols(refresh: bool = False) -> dict[str, dict[str, str]]:
    """Euronext: all 7 markets in one map, differentiated by 'Country'."""
    return _cached(
        CACHE_DIR / "symbols_euronext_cache.csv", _fetch_euronext, refresh
    )


def get_lse_symbols(refresh: bool = False) -> dict[str, dict[str, str]]:
    """LSE: Main Market + AIM equities."""
    return _cached(CACHE_DIR / "symbols_lse_cache.csv", _fetch_lse, refresh)


def get_fwb_symbols(refresh: bool = False) -> dict[str, dict[str, str]]:
    """Frankfurt (FWB): ordinary shares (Instrument Type 'CS')."""
    return _cached(CACHE_DIR / "symbols_fwb_cache.csv", _fetch_fwb, refresh)


def _cached(
    path: Path, fetcher, refresh: bool
) -> dict[str, dict[str, str]]:
    if path.exists() and not refresh:
        return _load_cache(path)
    meta = fetcher()
    _save_cache(path, meta)
    return meta
