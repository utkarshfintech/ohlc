"""Load the full universe of US-listed tickers from the Nasdaq Trader directory.

Downloads nasdaqlisted.txt (Nasdaq) and otherlisted.txt (NYSE/AMEX/Arca et al.),
caches the cleaned symbol list plus company name and exchange to a local CSV,
and exposes get_all_symbols() and get_symbol_metadata().
"""

from __future__ import annotations

import csv
import urllib.request
from pathlib import Path

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
CACHE_FILE = Path(__file__).resolve().parent / "symbols_cache.csv"

_EXCHANGE_CODES = {
    "N": "NYSE",
    "A": "NYSE American",
    "P": "NYSE Arca",
    "Z": "Nasdaq",
    "V": "IEX",
}


def _download(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_pipe(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    header: list[str] | None = None
    for line in text.strip().splitlines():
        line = line.split("#")[0].strip()
        if not line or line.startswith("File Creation Time"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if header is None:
            header = parts
        elif len(parts) == len(header):
            rows.append(dict(zip(header, parts)))
    return rows


def _clean(symbol: str) -> str:
    return symbol.strip().upper()


def _parse(meta: dict[str, dict[str, str]], text: str, nasdaq: bool) -> None:
    for row in _parse_pipe(text):
        if row.get("Test Issue", "N") == "Y" or row.get("Test Symbol", "N") == "Y":
            continue
        sym = _clean(row.get("Symbol") or row.get("ACT Symbol") or "")
        if not sym:
            continue
        name = row.get("Security Name") or ""
        if nasdaq:
            exchange = "NASDAQ"
        else:
            exchange = _EXCHANGE_CODES.get(row.get("Exchange", ""), row.get("Exchange", ""))
        meta[sym] = {"FullName": name, "Exchange": exchange}


def _load(refresh: bool = False) -> dict[str, dict[str, str]]:
    if CACHE_FILE.exists() and not refresh:
        with CACHE_FILE.open(newline="", encoding="utf-8") as f:
            return {r["symbol"]: r for r in csv.DictReader(f)}

    meta: dict[str, dict[str, str]] = {}
    _parse(meta, _download(NASDAQ_LISTED_URL), nasdaq=True)
    _parse(meta, _download(OTHER_LISTED_URL), nasdaq=False)

    with CACHE_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["symbol", "FullName", "Exchange"], extrasaction="ignore"
        )
        writer.writeheader()
        for sym in sorted(meta):
            writer.writerow({"symbol": sym, **meta[sym]})
    return meta


def get_all_symbols(refresh: bool = False) -> list[str]:
    """Return every US-listed ticker (stock/ETF/warrant/unit/preferred)."""
    return sorted(_load(refresh=refresh).keys())


def get_nasdaq_symbols(refresh: bool = False) -> list[str]:
    """Return only tickers listed on the Nasdaq exchange."""
    meta = _load(refresh=refresh)
    return sorted(
        sym for sym, info in meta.items()
        if info.get("Exchange", "").upper() == "NASDAQ"
    )


def get_symbol_metadata(refresh: bool = False) -> dict[str, dict[str, str]]:
    """Return {symbol: {'FullName': ..., 'Exchange': ...}} for all tickers."""
    return _load(refresh=refresh)