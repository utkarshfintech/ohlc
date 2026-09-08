"""Fetch historic OHLC + adjusted close data from Yahoo Finance via yfinance."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import yfinance as yf


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

        df = yf.Ticker(symbol).history(**kwargs)

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