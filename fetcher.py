"""Fetch historic OHLC + adjusted close data from Yahoo Finance via yfinance."""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Callable

import pandas as pd
import yfinance as yf

MAX_CRUMB_RETRIES = 2
CRUMB_RETRY_BACKOFF_SECONDS = 3.0


def _is_crumb_failure(err: Exception) -> bool:
    """True for Yahoo 401 Invalid Crumb / Unauthorized (rate-limit or stale crumb)."""
    status = getattr(err, "status_code", None)
    if status == 401:
        return True
    text = str(err).lower()
    return "crumb" in text or "unauthorized" in text


def _as_utc_dt(value: int | float | str | date | datetime) -> datetime:
    """Convert a value (including a Yahoo Unix-epoch seconds timestamp) to a datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    return datetime.fromisoformat(text)


class OHLCFetcher:
    """Wraps yfinance to derive Open/High/Low/Close/AdjClose for a symbol."""

    def fetch_history(
        self,
        symbol: str,
        start: int | float | str | date | datetime | None = None,
        end: int | float | str | date | datetime | None = None,
        interval: str = "1d",
        period: str | None = None,
    ) -> pd.DataFrame:
        """Return a DataFrame of Timestamp(epoch s) + Date + OHLC + AdjClose.

        start/end may be Yahoo Unix-epoch second timestamps or datetimes.
        auto_adjust=False keeps the raw 'Close' and exposes a separate 'Adj Close'.
        """
        kwargs: dict = {
            "interval": interval,
            "auto_adjust": False,
        }
        if period:
            kwargs["period"] = period
        else:
            kwargs["start"] = _as_utc_dt(start) if start is not None else None
            kwargs["end"] = _as_utc_dt(end) if end is not None else None

        df = self._fetch_with_crumb_retry(symbol, kwargs)

        if df.empty:
            return df

        idx = pd.DatetimeIndex(df.index)
        utc = idx.tz_convert("UTC").tz_localize(None)
        epoch = utc.to_numpy().astype("datetime64[s]").astype("int64")
        df = df.copy()
        df["Timestamp"] = epoch

        cols = ["Timestamp", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
        df = df[[c for c in cols if c in df.columns]].copy()

        df.reset_index(inplace=True)
        if "Date" not in df.columns and df.iloc[:, 0].name is not None:
            df.rename(columns={df.columns[0]: "Date"}, inplace=True)
        if "Adj Close" in df.columns:
            df.rename(columns={"Adj Close": "AdjClose"}, inplace=True)

        return df

    def _fetch_with_crumb_retry(self, symbol: str, kwargs: dict) -> pd.DataFrame:
        """Call yf.Ticker(symbol).history(**kwargs), re-handshaking the crumb on 401.

        Bounded: up to MAX_CRUMB_RETRIES re-attempts after a stale-crumb/401
        failure, each attempt constructing a FRESH yf.Ticker (new cookie+crumb
        handshake ΓÇö the exact mechanism that already unblocks the next symbol in
        the running log). Re-raises the last error after exhaustion, so a symbol
        is NEVER silently skipped or half-saved.
        """
        last_err: Exception | None = None
        for attempt in range(MAX_CRUMB_RETRIES + 1):
            try:
                return yf.Ticker(symbol).history(**kwargs)
            except Exception as err:
                if not _is_crumb_failure(err) or attempt >= MAX_CRUMB_RETRIES:
                    raise
                last_err = err
                time.sleep(CRUMB_RETRY_BACKOFF_SECONDS * (2 ** attempt))
        assert last_err is not None
        raise last_err
