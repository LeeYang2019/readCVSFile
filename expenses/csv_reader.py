"""Robust CSV ingestion utilities."""

from __future__ import annotations

import csv
import io
import os
from typing import Tuple

import pandas as pd


def sniff_format(path: str) -> Tuple[str, str, str, bool]:
    """Use csv.Sniffer to guess encoding, delimiter, quote char, and header."""
    encodings_to_try = ["utf-8-sig", "utf-8", "mac_roman", "latin1"]
    for encoding in encodings_to_try:
        try:
            with io.open(path, "r", encoding=encoding, newline="") as fh:
                sample = fh.read(4096)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
            has_header = sniffer.has_header(sample)
            return encoding, dialect.delimiter, dialect.quotechar, has_header
        except Exception:
            continue
    return "utf-8-sig", ",", '"', True


def read_with_pandas(path: str, encoding: str, delimiter: str, quotechar: str, has_header: bool) -> pd.DataFrame:
    """Read using pandas' fast engine, falling back to python engine."""
    header_arg = 0 if has_header else None
    try:
        return pd.read_csv(path, sep=delimiter, encoding=encoding, header=header_arg, quotechar=quotechar)
    except Exception:
        return pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            header=header_arg,
            quotechar=quotechar,
            engine="python",
        )


def read_with_csv_module(path: str, encoding: str, delimiter: str, quotechar: str, has_header: bool) -> pd.DataFrame:
    """Fallback reader using csv module for particularly messy files."""
    with io.open(path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar)
        rows = list(reader)

    if not rows:
        raise ValueError("CSV appears to be empty.")

    if has_header:
        header = rows[0]
        data = rows[1:]
    else:
        max_len = max(len(row) for row in rows)
        header = [f"col_{i+1}" for i in range(max_len)]
        data = rows

    width = len(header)
    padded = [row + [""] * (width - len(row)) if len(row) < width else row[:width] for row in data]
    return pd.DataFrame(padded, columns=header)


def read_csv_robust(path: str) -> pd.DataFrame:
    """Best-effort CSV reader that tries pandas, normalizes CR line endings, then csv module."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if os.path.isdir(path):
        raise IsADirectoryError("Directory passed where a file was expected.")

    encoding, delimiter, quote, has_header = sniff_format(path)
    try:
        return read_with_pandas(path, encoding, delimiter, quote, has_header)
    except Exception:
        pass

    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        if b"\r" in raw and b"\n" not in raw:
            tmp_path = path + ".__norm__.csv"
            with open(tmp_path, "wb") as tmp:
                tmp.write(raw.replace(b"\r", b"\n"))
            try:
                return read_with_pandas(tmp_path, encoding, delimiter, quote, has_header)
            except Exception:
                return read_with_csv_module(tmp_path, encoding, delimiter, quote, has_header)
    except Exception:
        pass

    return read_with_csv_module(path, encoding, delimiter, quote, has_header)


__all__ = [
    "sniff_format",
    "read_with_pandas",
    "read_with_csv_module",
    "read_csv_robust",
]
