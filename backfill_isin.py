"""Backfill ISIN for symbols already stored without one.

Passes through every distinct Symbol in dbo.us_stock_historic_ohlc whose
ISIN is NULL, resolves it via yfinance.get_isin(), and updates the rows.
Ignore unknown results ('-' / empty) and only replaces NULL/empty ISINs.
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf

from store import SQLServerStore

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5.0


def resolve_isin(symbol: str) -> str | None:
    for attempt in range(MAX_RETRIES):
        try:
            value = yf.Ticker(symbol).get_isin()
            value = (value or "").strip()
            if value and value != "-":
                return value
            return None
        except Exception as exc:  # noqa: BLE001 - transient network errors
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                print(f"[{symbol}] resolve failed: {exc}", file=sys.stderr)
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill-isin",
        description="Fill missing ISIN values in us_stock_historic_ohlc.",
    )
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of concurrent lookups (default 4)")
    parser.add_argument("--delay", type=float, default=0.05,
                        help="Delay between batches to be gentle on the source")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process at most N symbols (useful for testing)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    store = SQLServerStore(use_bulk=True)

    with store._connect() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT Symbol FROM dbo.us_stock_historic_ohlc "
            "WHERE ISIN IS NULL OR ISIN = ''"
        )
        pending = [row[0] for row in cursor.fetchall()]

    if args.limit:
        pending = pending[: args.limit]

    if not pending:
        print("No symbols missing ISIN.")
        return 0

    print(f"Resolving ISIN for {len(pending)} symbols ...")
    updated = 0
    unknown = 0

    def work(symbol: str) -> tuple[str, str | None]:
        return symbol, resolve_isin(symbol)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(work, s) for s in pending]
        for idx, fut in enumerate(as_completed(futures), start=1):
            symbol, isin = fut.result()
            if isin:
                with store._connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE dbo.us_stock_historic_ohlc SET ISIN = ?, "
                        "UpdatedAt = SYSUTCDATETIME() "
                        "WHERE Symbol = ? AND (ISIN IS NULL OR ISIN = '')",
                        isin,
                        symbol,
                    )
                    conn.commit()
                    updated += 1
                print(f"[{idx}/{len(pending)}] {symbol}: {isin}")
            else:
                unknown += 1

            if args.delay and idx % (args.workers * 4) == 0:
                time.sleep(args.delay)

    print(f"Done. Updated: {updated}, unknown/no-ISIN: {unknown}, total: {len(pending)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())