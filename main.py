"""CLI entry point: fetch historic OHLC + adjusted close and store to SQL Server.

The data timeframe is hardcoded below as Yahoo Unix-epoch timestamps (seconds).

Fetch a few symbols:
    python main.py AAPL MSFT NVDA

Fetch every US-listed symbol from the Nasdaq Trader directory
(or run bare, which defaults to --all):
    python main.py [--all]
    python main.py --all --limit 100 --delay 0.15
"""

from __future__ import annotations

import argparse
import sys
import time

import yfinance as yf

from fetcher import OHLCFetcher, OHLCFetcher as _OHLCFetcherAlias
from fetcher import OHLCFetcher
from markets import (
    get_euronext_symbols,
    get_fwb_symbols,
    get_hang_seng_symbols,
    get_krx_symbols,
    get_lse_symbols,
    get_tse_symbols,
)
from store import (
    DEFAULT_TABLE,
    EURONEXT_TABLE,
    FWB_TABLE,
    HKEX_TABLE,
    KRX_KOSDAQ_TABLE,
    KRX_KOSPI_TABLE,
    LSE_TABLE,
    NASDAQ_TABLE,
    SQLServerStore,
    TSE_TABLE,
)
from symbols import get_all_symbols, get_nasdaq_symbols, get_symbol_metadata

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5.0

# Yahoo Unix-epoch timestamps (seconds) for the backfill window.
START_DATE_EPOCH = 345427200  # 1980-12-12 00:00:00 UTC
END_DATE_EPOCH = 1789603200  # 2026-09-17 00:00:00 UTC

# International market -> its own table + its symbol directory getter.
MARKET_GETTERS = {
    "euronext": get_euronext_symbols,
    "lse": get_lse_symbols,
    "fwb": get_fwb_symbols,
    "hkex": get_hang_seng_symbols,
    "tse": get_tse_symbols,
    "kospi": lambda refresh=False: get_krx_symbols("kospi", refresh=refresh),
    "kosdaq": lambda refresh=False: get_krx_symbols("kosdaq", refresh=refresh),
}

# Natural short names / alternate spellings -> the canonical market key, so
# `--market hs` (Hang Seng), `--market krx`, `--market lon`, etc. all work.
MARKET_ALIASES = {
    "hs": "hkex",
    "hsi": "hkex",
    "hang_seng": "hkex",
    "hk": "hkex",
    "hkg": "hkex",
    "hong_kong": "hkex",
    "krx": "kospi",
    "seoul": "kospi",
    "kq": "kosdaq",
    "lon": "lse",
    "london": "lse",
    "fra": "fwb",
    "frankfurt": "fwb",
    "xetra": "fwb",
    "tokyo": "tse",
    "jpx": "tse",
    "epa": "euronext",
    "ams": "euronext",
    "brx": "euronext",
}

# Every legal value for --market, canonical + aliases (canonical names win on ties).
MARKET_CHOICES = sorted(set(MARKET_GETTERS) | set(MARKET_ALIASES))

MARKET_DISPATCH = {
    "euronext": (EURONEXT_TABLE, get_euronext_symbols),
    "lse": (LSE_TABLE, get_lse_symbols),
    "fwb": (FWB_TABLE, get_fwb_symbols),
    "hkex": (HKEX_TABLE, get_hang_seng_symbols),
    "tse": (TSE_TABLE, get_tse_symbols),
    "kospi": (KRX_KOSPI_TABLE, lambda refresh=False: get_krx_symbols("kospi", refresh=refresh)),
    "kosdaq": (KRX_KOSDAQ_TABLE, lambda refresh=False: get_krx_symbols("kosdaq", refresh=refresh)),
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="us-stock-ohlc",
        description="Fetch historic OHLC + adjusted close via Yahoo Finance and store to SQL Server.",
    )
    parser.add_argument("symbols", nargs="*", help="Stock tickers, e.g. AAPL MSFT")
    parser.add_argument("--all", action="store_true",
                        help="Fetch every US-listed symbol from the Nasdaq Trader directory")
    parser.add_argument("--nasdaq", action="store_true",
                        help="Fetch only Nasdaq-listed symbols into dbo.us_nasdaq_ohlc")
    parser.add_argument("--market", choices=MARKET_CHOICES,
                        help="Fetch an international market into its own table. "
                             "Canonical: euronext, lse, fwb, hkex, tse, kospi, kosdaq "
                             "(aliases like hs, hsi, krx, lon, fra, tokyo also work)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after processing N symbols (useful with --all/--nasdaq)")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Seconds to wait between Yahoo requests (rate limiting)")
    parser.add_argument("--refresh-symbols", action="store_true",
                        help="Re-download the Nasdaq symbol directory instead of using the cache")
    parser.add_argument("--replace", action="store_true",
                        help="Delete existing rows for the symbol before inserting")
    parser.add_argument("--resume", action="store_true",
                        help="Insert only missing rows per symbol instead of skipping "
                             "whole symbols already in the DB (recovers interrupted loads)")
    parser.add_argument("--no-bulk", action="store_true",
                        help="Disable fast_executemany bulk inserts")
    parser.add_argument("--init-schema", action="store_true",
                        help="Create the table/index if missing, then exit")
    args = parser.parse_args(argv)
    if args.market:
        args.market = MARKET_ALIASES.get(args.market, args.market)
    return args


def fetch_currency(symbol: str) -> str | None:
    try:
        return yf.Ticker(symbol).fast_info.get("currency")
    except Exception:  # noqa: BLE001
        return None


def fetch_isin(symbol: str) -> str | None:
    """Resolve ISIN via yfinance; ignore '-' (unknown) and empty results."""
    try:
        value = yf.Ticker(symbol).get_isin()
        value = (value or "").strip()
        if value and value != "-":
            return value
    except Exception:  # noqa: BLE001
        pass
    return None


def fetch_with_retry(fetcher: OHLCFetcher, symbol: str, start: int, end: int):
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            return fetcher.fetch_history(symbol, start=start, end=end)
        except Exception as exc:  # noqa: BLE001 - network/timeout errors are transient
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SECONDS)
    raise last_error


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.market:
        table, symbols_getter = MARKET_DISPATCH[args.market]
    else:
        table = NASDAQ_TABLE if args.nasdaq else DEFAULT_TABLE
    store = SQLServerStore(use_bulk=not args.no_bulk, table=table)

    if args.init_schema:
        store.init_schema()
        print("Schema ready.")
        return 0

    if args.market:
        meta_all = symbols_getter(refresh=args.refresh_symbols)
        symbols = list(meta_all)
        if not symbols:
            print(f"Symbol directory empty — could not load any {args.market} symbols.",
                  file=sys.stderr)
            return 2
    elif args.nasdaq:
        symbols = get_nasdaq_symbols(refresh=args.refresh_symbols)
        meta_all = get_symbol_metadata(refresh=args.refresh_symbols)
        if not symbols:
            print("Symbol directory empty — could not load any Nasdaq symbols.", file=sys.stderr)
            return 2
    elif args.all:
        symbols = get_all_symbols(refresh=args.refresh_symbols)
        meta_all = get_symbol_metadata(refresh=args.refresh_symbols)
        if not symbols:
            print("Symbol directory empty — could not load any symbols.", file=sys.stderr)
            return 2
    else:
        if not args.symbols:
            # Bare `python main.py` after a truncate means "fetch everything".
            args.all = True
            symbols = get_all_symbols(refresh=args.refresh_symbols)
            meta_all = get_symbol_metadata(refresh=args.refresh_symbols)
        else:
            symbols = args.symbols
            meta_all = {}

    if args.limit:
        symbols = symbols[: args.limit]

    existing = set() if args.replace else store.existing_symbols()
    if args.resume:
        existing = set()
    fetcher = OHLCFetcher()
    total = 0
    fetched = 0
    skipped = 0
    no_data = 0
    failed: list[str] = []

    print(
        f"Processing {len(symbols)} symbols for "
        f"{START_DATE_EPOCH} -> {END_DATE_EPOCH} (ts) into {store.table} ..."
    )
    for idx, symbol in enumerate(symbols, start=1):
        if symbol in existing:
            skipped += 1
            continue

        try:
            df = fetch_with_retry(fetcher, symbol, START_DATE_EPOCH, END_DATE_EPOCH)
        except Exception as exc:  # noqa: BLE001
            failed.append(symbol)
            print(f"[{idx}/{len(symbols)}] {symbol}: FAILED ({exc})", file=sys.stderr)
            continue

        if df.empty:
            no_data += 1
            print(f"[{idx}/{len(symbols)}] {symbol}: no data")
            continue

        if args.resume:
            present = store.existing_timestamps(symbol)
            if present:
                df = df[~df["Timestamp"].isin(present)]
                if df.empty:
                    skipped += 1
                    print(f"[{idx}/{len(symbols)}] {symbol}: already complete ({len(present)} rows)")
                    continue

        meta = dict(meta_all.get(symbol, {}))
        meta.setdefault("Currency", fetch_currency(symbol))
        isin = fetch_isin(symbol)
        if isin:
            meta["ISIN"] = isin
        written = store.save(symbol, df, meta=meta, replace=args.replace)
        fetched += 1
        total += written
        print(f"[{idx}/{len(symbols)}] {symbol}: {len(df)} rows -> {written}")

        if args.delay:
            time.sleep(args.delay)

    print(
        f"Done. Symbols processed: {fetched}, rows written: {total}, "
        f"skipped (already in DB): {skipped}, no data: {no_data}, failed: {len(failed)}"
    )
    if failed:
        print("Failed symbols: " + ", ".join(failed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())