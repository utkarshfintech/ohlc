"""Fill missing ISINs for TSE_historic_ohlc from the official JASDEC map.

Builds the TSE code -> ISIN map once (JASDEC handled-securities list, with
free-ticker-database + Business Insider fallbacks via markets.py), then
UPDATEs each row still missing an ISIN. Only touches TSE_historic_ohlc and
only the ISIN column. Idempotent.

Usage:  python fix_tse_isin.py
"""

from __future__ import annotations

import concurrent.futures as cf
import time

import pyodbc

import markets
from config import settings

TABLE = "TSE_historic_ohlc"
WORKERS = 8


def lookup(tse_map: dict[str, str], symbol: str, name: str) -> str | None:
    code = symbol[:-2] if symbol.endswith(".T") else symbol
    isin = tse_map.get(code)
    if isin:
        return isin
    if name:
        try:
            isin = markets._tse_isin_by_name(name)
        except Exception:  # noqa: BLE001 - never let a lookup kill the run
            isin = None
    return isin


def main() -> int:
    conn = pyodbc.connect(settings.connection_string)
    cur = conn.cursor()
    cur.execute(
        f"SELECT DISTINCT Symbol, FullName FROM {TABLE} "
        "WHERE ISIN IS NULL OR LTRIM(RTRIM(ISIN)) = '' "
        "ORDER BY Symbol"
    )
    rows = cur.fetchall()
    conn.close()
    print(f"Symbols missing ISIN: {len(rows)}", flush=True)

    tse_map = markets._fetch_jp_isin_map({})
    covered = sum(1 for symbol, _ in rows if tse_map.get(symbol[:-2] if symbol.endswith(".T") else symbol))
    print(f"JASDEC/map coverage of missing: {covered}/{len(rows)}", flush=True)

    fixed = 0
    unmatched: list[tuple[str, str]] = []

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(lookup, tse_map, symbol, name): (symbol, name)
            for symbol, name in rows
        }
        for done in cf.as_completed(futures):
            symbol, name = futures[done]
            isin = done.result()
            if isin:
                with pyodbc.connect(settings.connection_string) as conn:
                    with conn.cursor() as ucur:
                        ucur.execute(
                            f"UPDATE {TABLE} SET ISIN = ? "
                            "WHERE Symbol = ? "
                            "AND (ISIN IS NULL OR LTRIM(RTRIM(ISIN)) = '')",
                            isin,
                            symbol,
                        )
                    conn.commit()
                fixed += 1
                print(f"  {symbol} -> {isin}", flush=True)
            else:
                unmatched.append((symbol, name))
                print(f"  {symbol}: no match", flush=True)
            time.sleep(0.05)

    print(f"\nDone. Filled: {fixed}; unresolved: {len(unmatched)}", flush=True)
    for symbol, name in unmatched:
        print(f"  {symbol}  {name}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())