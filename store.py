"""Persist OHLC history into a SQL Server database via pyodbc."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
import pyodbc

from config import settings

DEFAULT_TABLE = "us_stock_historic_ohlc"
NASDAQ_TABLE = "us_nasdaq_ohlc"
EURONEXT_TABLE = "EURONEXT_historic_ohlc"
LSE_TABLE = "LSE_historic_ohlc"
FWB_TABLE = "FWB_historic_ohlc"
HKEX_TABLE = "HKEX_historic_ohlc"
TSE_TABLE = "TSE_historic_ohlc"
KRX_KOSPI_TABLE = "KRX_KOSPI_historic_ohlc"
KRX_KOSDAQ_TABLE = "KRX_KOSDAQ_historic_ohlc"

CREATE_TABLE_SQL = """
IF OBJECT_ID('dbo.us_stock_historic_ohlc', 'U') IS NOT NULL
    DROP TABLE dbo.us_stock_historic_ohlc;

CREATE TABLE dbo.us_stock_historic_ohlc (
    Id        INT           IDENTITY(1,1) NOT NULL,
    Symbol    NVARCHAR(60)  NOT NULL,
    ISIN      NVARCHAR(50)  NULL,
    Currency  NVARCHAR(20)  NULL,
    FullName  NVARCHAR(1000) NULL,
    Exchange  NVARCHAR(20)  NULL,
    Timestamp BIGINT        NULL,
    [Date]    DATETIME2(7)  NULL,
    [Open]    DECIMAL(18,6) NULL,
    High      DECIMAL(18,6) NULL,
    Low       DECIMAL(18,6) NULL,
    [Close]   DECIMAL(18,6) NULL,
    AdjClose  DECIMAL(18,6) NULL,
    Volume    BIGINT        NULL,
    CreatedAt DATETIME2(7)  NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2(7)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_us_stock_historic_ohlc PRIMARY KEY (Id)
);

CREATE UNIQUE INDEX UX_us_stock_historic_ohlc_Symbol_Timestamp
    ON dbo.us_stock_historic_ohlc (Symbol, Timestamp);
"""

# Mirrors the live "UsStockOhlc" (NYSE) table so Nasdaq rows look identical.
# Created only if missing (never drops), with a unique (Symbol, Timestamp) guard.
CREATE_NASDAQ_TABLE_SQL = """
IF OBJECT_ID('dbo.us_nasdaq_ohlc', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.us_nasdaq_ohlc (
        Id        INT           IDENTITY(1,1) NOT NULL,
        Symbol    NVARCHAR(60)  NOT NULL,
        ISIN      NVARCHAR(50)  NULL,
        Currency  VARCHAR(20)   NULL,
        FullName  VARCHAR(1000) NULL,
        Exchange  VARCHAR(20)   NULL,
        Timestamp BIGINT        NULL,
        [Date]    DATETIME      NULL,
        [Open]    FLOAT         NULL,
        High      FLOAT         NULL,
        Low       FLOAT         NULL,
        [Close]   FLOAT         NULL,
        AdjClose  FLOAT         NULL,
        Volume    BIGINT        NULL,
        CreatedAt NVARCHAR(60)  NULL DEFAULT CONVERT(NVARCHAR(60), SYSUTCDATETIME(), 121),
        UpdatedAt NVARCHAR(60)  NULL DEFAULT CONVERT(NVARCHAR(60), SYSUTCDATETIME(), 121),
        CONSTRAINT PK_us_nasdaq_ohlc PRIMARY KEY (Id)
    );

    CREATE UNIQUE INDEX UX_us_nasdaq_ohlc_Symbol_Timestamp
        ON dbo.us_nasdaq_ohlc (Symbol, Timestamp);
END;
"""


class SQLServerStore:
    """Persists OHLC DataFrames into SQL Server."""

    def __init__(self, use_bulk: bool = True, table: str = DEFAULT_TABLE) -> None:
        self.use_bulk = use_bulk
        self.table = table

    @staticmethod
    def _tbl(table: str) -> str:
        return f"[dbo].[{table}]"

    def _connect(self) -> pyodbc.Connection:
        return pyodbc.connect(settings.connection_string, autocommit=False)

    def init_schema(self, table: str | None = None) -> None:
        table = table or self.table
        if table == DEFAULT_TABLE:
            sql = CREATE_TABLE_SQL
        elif table == NASDAQ_TABLE:
            sql = CREATE_NASDAQ_TABLE_SQL
        else:
            raise ValueError(f"Unsupported table name: {table}")
        with self._connect() as conn:
            conn.execute(sql)
            conn.commit()

    def existing_symbols(self) -> set[str]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT DISTINCT Symbol FROM {self._tbl(self.table)}"
            )
            return {row[0] for row in cursor.fetchall()}

    def existing_timestamps(self, symbol: str) -> set[int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT Timestamp FROM {self._tbl(self.table)} WHERE Symbol = ?",
                symbol,
            )
            return {int(row[0]) for row in cursor.fetchall()}

    def save(
        self,
        symbol: str,
        df: pd.DataFrame,
        meta: dict | None = None,
        replace: bool = False,
    ) -> int:
        """Insert OHLC rows. Returns the number of rows written.

        meta may carry {'ISIN', 'Currency', 'FullName', 'Exchange'}.
        If replace=True, existing rows for the symbol are deleted first.
        """
        if df is None or df.empty:
            return 0

        now = _timestamp_str()
        rows = [row for row in self._iter_rows(symbol, df, meta, now)]
        if not rows:
            return 0

        table = self._tbl(self.table)
        with self._connect() as conn:
            cursor = conn.cursor()
            if replace:
                cursor.execute(
                    f"DELETE FROM {table} WHERE Symbol = ?",
                    symbol,
                )

            if self.use_bulk:
                cursor.fast_executemany = True

            insert_sql = f"""
                INSERT INTO {table}
                    (Symbol, ISIN, Currency, FullName, Exchange,
                     Timestamp, [Date], [Open], High, Low, [Close], AdjClose, Volume,
                     CreatedAt, UpdatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.executemany(insert_sql, rows)
            conn.commit()
            return len(rows)

    @staticmethod
    def _iter_rows(
        symbol: str, df: pd.DataFrame, meta: dict | None, now: str
    ) -> Iterable[tuple]:
        meta = meta or {}
        timestamp_col = "Timestamp" if "Timestamp" in df.columns else None
        date_col = "Date" if "Date" in df.columns else df.columns[0]
        for _, r in df.iterrows():
            ts = _bigint(r.get("Timestamp")) if timestamp_col else None
            yield (
                symbol,
                _str(meta.get("ISIN")),
                _str(meta.get("Currency")),
                _str(meta.get("FullName")),
                _str(meta.get("Exchange")),
                ts,
                _as_naive_dt(r[date_col]),
                _decimal(r.get("Open")),
                _decimal(r.get("High")),
                _decimal(r.get("Low")),
                _decimal(r.get("Close")),
                _decimal(r.get("AdjClose")),
                _bigint(r.get("Volume")),
                now,
                now,
            )


def _timestamp_str() -> str:
    """UTC now as a 7-digit-fraction string, matching CONVERT(...,121)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + "0"


def _as_naive_dt(value) -> datetime | None:
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if value is None or value != value:  # NaN
            return None
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    except (TypeError, ValueError):
        return None


def _str(value) -> str | None:
    if value is None or value != value:  # NaN
        return None
    return str(value).strip() or None  # empty/whitespace -> SQL NULL (never "")


def _decimal(value) -> float | None:
    """Return a float (rounded to 6 dp to fit decimal(18,6))."""
    try:
        v = float(value)
        if v != v:  # NaN
            return None
        return round(v, 6)
    except (TypeError, ValueError):
        return None


def _bigint(value) -> int | None:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None