"""Robust CSV ingestion utilities."""

from __future__ import annotations

import csv
import io
import os
from typing import Tuple

import pandas as pd


def sniff_format(path: str) -> Tuple[str, str, str, bool]:
    """
    Auto-detect CSV format by analyzing file sample.
    
    Uses csv.Sniffer to determine:
    - Encoding: tries utf-8-sig (for BOM), utf-8, mac_roman, latin1 in order
    - Delimiter: detects between comma, semicolon, tab, or pipe
    - Quote character: usually double-quote
    - Has header: checks if first row contains column names
    
    Args:
        path: File path to analyze
        
    Returns:
        Tuple of (encoding, delimiter, quote_char, has_header)
    """
    # Try encodings in order of likelihood (utf-8 variants most common for modern files)
    encodings_to_try = ["utf-8-sig", "utf-8", "mac_roman", "latin1"]
    for encoding in encodings_to_try:
        try:
            # Read small sample (4KB) for performance; usually enough for dialect detection
            with io.open(path, "r", encoding=encoding, newline="") as fh:
                sample = fh.read(4096)
            
            # Use csv.Sniffer to detect dialect from sample
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
            has_header = sniffer.has_header(sample)
            return encoding, dialect.delimiter, dialect.quotechar, has_header
        except Exception:
            # If this encoding fails, try the next one
            continue
    
    # Fallback defaults (UTF-8 + comma-separated + typical quote char)
    return "utf-8-sig", ",", '"', True


def read_with_pandas(path: str, encoding: str, delimiter: str, quotechar: str, has_header: bool) -> pd.DataFrame:
    """
    Read CSV using pandas with detected format parameters.
    
    Primary reader that tries the fast C-engine first, then falls back to
    the slower but more robust Python engine if parsing fails.
    
    Args:
        path: File path to read
        encoding: File encoding (e.g., 'utf-8', 'latin1')
        delimiter: Column separator (comma, semicolon, tab, pipe, etc.)
        quotechar: Character used for quoted fields (typically double-quote)
        has_header: Whether first row contains column names (0) or not (None)
        
    Returns:
        pandas DataFrame with parsed CSV data
    """
    # If has_header is True, pandas reads row 0 as column names; None means auto-generate col_1, col_2, etc.
    header_arg = 0 if has_header else None
    try:
        # Try fast C-engine first (good for well-formed CSVs)
        return pd.read_csv(path, sep=delimiter, encoding=encoding, header=header_arg, quotechar=quotechar)
    except Exception:
        # Fall back to Python engine for quirky/malformed files
        # Slower but handles edge cases better (e.g., irregular quoting)
        return pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            header=header_arg,
            quotechar=quotechar,
            engine="python",
        )


def read_with_csv_module(path: str, encoding: str, delimiter: str, quotechar: str, has_header: bool) -> pd.DataFrame:
    """
    Fallback reader for edge-case CSVs using Python's csv module.
    
    More flexible than pandas for handling:
    - Irregular row lengths (auto-pads shorter rows)
    - Complex quoting edge cases
    - Non-standard delimiters
    
    Args:
        path: File path to read
        encoding: File encoding
        delimiter: Column separator
        quotechar: Quote character
        has_header: Whether first row is header
        
    Returns:
        pandas DataFrame with parsed CSV data
    """
    # Use csv.reader for fine-grained control over parsing
    with io.open(path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar)
        rows = list(reader)

    if not rows:
        raise ValueError("CSV appears to be empty.")

    # Extract header from first row or generate column names
    if has_header:
        header = rows[0]
        data = rows[1:]
    else:
        # No header in file: generate synthetic column names (col_1, col_2, ...)
        max_len = max(len(row) for row in rows)
        header = [f"col_{i+1}" for i in range(max_len)]
        data = rows

    # Pad rows to consistent width (some rows may be shorter than header)
    # This ensures all rows have the same number of columns
    width = len(header)
    padded = [row + [""] * (width - len(row)) if len(row) < width else row[:width] for row in data]
    return pd.DataFrame(padded, columns=header)


def read_csv_robust(path: str) -> pd.DataFrame:
    """
    Best-effort CSV reader with multiple fallback strategies.
    
    Handles encoding issues, unusual delimiters, and Mac-style line endings.
    Tries strategies in order:
    1. Auto-detect format and read with pandas (fast path)
    2. If pandas fails: try normalizing old Mac CR line endings
    3. If all else fails: use csv module parser (slowest but most robust)
    
    Args:
        path: File path to read
        
    Returns:
        pandas DataFrame with parsed CSV data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        IsADirectoryError: If path is a directory instead of file
    """
    # Validate input
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if os.path.isdir(path):
        raise IsADirectoryError("Directory passed where a file was expected.")

    # Step 1: Auto-detect format (encoding, delimiter, quotechar, header)
    encoding, delimiter, quote, has_header = sniff_format(path)
    
    # Step 2: Try pandas with detected format (fastest)
    try:
        return read_with_pandas(path, encoding, delimiter, quote, has_header)
    except Exception:
        pass

    # Step 3: Handle old Mac line endings (CR-only, no LF)
    # Some older Excel exports use carriage return (\r) instead of newline (\n)
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        if b"\r" in raw and b"\n" not in raw:
            # File has CR but no LF: needs normalization
            tmp_path = path + ".__norm__.csv"
            with open(tmp_path, "wb") as tmp:
                # Replace old Mac line endings with Unix line endings
                tmp.write(raw.replace(b"\r", b"\n"))
            try:
                # Try pandas again on normalized file
                return read_with_pandas(tmp_path, encoding, delimiter, quote, has_header)
            except Exception:
                # Still failing: fall back to csv module
                return read_with_csv_module(tmp_path, encoding, delimiter, quote, has_header)
    except Exception:
        pass

    # Step 4: Final fallback to csv module (slowest but most robust)
    return read_with_csv_module(path, encoding, delimiter, quote, has_header)


__all__ = [
    "sniff_format",
    "read_with_pandas",
    "read_with_csv_module",
    "read_csv_robust",
]
