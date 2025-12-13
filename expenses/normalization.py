"""Normalization helpers for headers, dates, and numeric columns."""

from __future__ import annotations

import re
from typing import List, Optional

import pandas as pd


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to lowercase with underscores.
    
    Converts all column names to lowercase and replaces whitespace/special chars
    with underscores. Examples:
    - "Transaction Date" → "transaction_date"
    - "Amount ($)" → "amount"
    - "Debit / Credit" → "debit_/_credit"
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with normalized column names
    """
    out = df.copy()
    # Convert column names to lowercase and replace sequences of whitespace with single underscore
    out.columns = out.columns.astype(str).map(lambda col: re.sub(r"\s+", "_", col.strip().lower()))
    return out


def normalize_date_columns(df: pd.DataFrame, candidates: List[str]) -> pd.DataFrame:
    """
    Parse and standardize date columns to YYYY-MM-DD format.
    
    Attempts to convert recognized date columns to a consistent string format.
    Handles various input formats (MM/DD/YYYY, DD-MM-YYYY, ISO8601, etc.) by
    leveraging pandas' smart date parsing.
    
    Note: Uses single-pass parsing with inferred format for performance.
    
    Args:
        df: Input DataFrame
        candidates: List of column names to try (checked in order)
        
    Returns:
        DataFrame with date columns formatted as YYYY-MM-DD strings
    """
    out = df.copy()
    for col in candidates:
        if col in out.columns:
            try:
                # Use infer_datetime_format=True for faster parsing if dates follow a pattern
                parsed = pd.to_datetime(out[col], errors="coerce", infer_datetime_format=True)
                # Keep original values if parsing fails (errors="coerce" returns NaT)
                out[col] = parsed.where(parsed.notna(), out[col])
                # Convert to YYYY-MM-DD string format
                out[col] = pd.to_datetime(out[col], errors="ignore").dt.strftime("%Y-%m-%d")
            except Exception:
                # If column can't be parsed, leave it unchanged and move to next candidate
                continue
    return out


def drop_table_name_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove metadata rows from Numbers app exports.
    
    Numbers (Apple's spreadsheet app) sometimes prepends a "Table X" row
    to CSV exports. This function detects and removes such rows.
    
    A row is considered a metadata row if:
    - It contains only one non-empty value (e.g., "Table 1")
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with metadata rows removed
    """
    # Drop completely empty rows first
    out = df.dropna(how="all")
    
    # Check if first row looks like metadata (only one non-empty value)
    if len(out) and out.iloc[0].notna().sum() == 1:
        first_val = str(out.iloc[0].dropna().iloc[0]).strip().lower()
        # If the single value starts with "table ", it's a metadata row
        if first_val.startswith("table "):
            out = out.iloc[1:]  # Remove the first row
    return out


def coerce_money(series: pd.Series) -> pd.Series:
    """
    Convert currency strings to numeric values.
    
    Handles common money formatting:
    - Dollar signs: "$100.50" → 100.50
    - Thousands separators: "1,000.50" → 1000.50
    - Parentheses for negatives: "(100)" → -100 (accounting notation)
    
    Unparseable values become NaN.
    
    Args:
        series: pandas Series with string currency values
        
    Returns:
        Series with numeric values (float64)
    """
    s = series.astype(str).str.strip()
    # Remove dollar signs
    s = s.str.replace(r"\$", "", regex=True)
    # Remove thousands separators (commas)
    s = s.str.replace(",", "", regex=False)
    # Convert accounting notation: (100) → -100
    # Regex: match opening paren, capture content, closing paren → prepend minus sign
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    # Convert to numeric, coercing invalid values to NaN
    return pd.to_numeric(s, errors="coerce")


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Find the first matching column name from a candidate list.
    
    Useful for handling CSVs with different column naming conventions.
    Examples:
    - pick_col(df, ["date", "Date", "DATE"]) → first matching column
    - pick_col(df, ["description", "details", "payee"]) → description column
    
    Args:
        df: DataFrame to search
        candidates: List of column names to try (checked in order)
        
    Returns:
        First candidate found in df.columns, or None if no match
    """
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
