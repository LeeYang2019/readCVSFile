"""Normalization helpers for headers, dates, and numeric columns."""

from __future__ import annotations

import re
from typing import List, Optional

import pandas as pd


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).map(lambda col: re.sub(r"\s+", "_", col.strip().lower()))
    return out


def normalize_date_columns(df: pd.DataFrame, candidates: List[str]) -> pd.DataFrame:
    """Convert recognized date columns to YYYY-MM-DD when possible."""
    out = df.copy()
    for col in candidates:
        if col in out.columns:
            try:
                parsed = pd.to_datetime(out[col], errors="coerce", infer_datetime_format=True)
                out[col] = parsed.where(parsed.notna(), out[col])
                out[col] = pd.to_datetime(out[col], errors="ignore").dt.strftime("%Y-%m-%d")
            except Exception:
                continue
    return out


def drop_table_name_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove Numbers' leading 'Table X' row if present."""
    out = df.dropna(how="all")
    if len(out) and out.iloc[0].notna().sum() == 1:
        first_val = str(out.iloc[0].dropna().iloc[0]).strip().lower()
        if first_val.startswith("table "):
            out = out.iloc[1:]
    return out


def coerce_money(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\$", "", regex=True)
    s = s.str.replace(",", "", regex=False)
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(s, errors="coerce")


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


__all__ = [
    "normalize_headers",
    "normalize_date_columns",
    "drop_table_name_rows",
    "coerce_money",
    "pick_col",
]
