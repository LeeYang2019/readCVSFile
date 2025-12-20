"""High-level orchestration for expense processing."""

from __future__ import annotations

import os
import traceback
from datetime import datetime
from typing import List, Optional, Sequence, Tuple

import pandas as pd

from .categories import DEFAULT_DOWNLOAD_FILENAME
from .categorizer import detect_or_build_category_with_debug
from .csv_reader import read_csv_robust
from .normalization import (
    coerce_money,
    drop_table_name_rows,
    normalize_date_columns,
    normalize_headers,
    pick_col,
)


def is_csv_filename(name: str) -> bool:
    """Check if filename has .csv extension (case-insensitive)."""
    return name.strip().lower().endswith(".csv")


def expand_inputs(paths: Sequence[str]) -> List[str]:
    """Convert file and folder paths to a deduplicated list of CSV file paths."""
    collected: List[str] = []
    for path in paths:
        expanded = os.path.expanduser(path)
        if os.path.isdir(expanded):
            for root, _, files in os.walk(expanded):
                for filename in files:
                    if is_csv_filename(filename):
                        collected.append(os.path.join(root, filename))
        elif is_csv_filename(expanded):
            collected.append(expanded)

    seen = set()
    deduped: List[str] = []
    for path in collected:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def ensure_raw_inputs(raw_inputs: Sequence[str], default_filename: str) -> List[str]:
    """Provide default input path if none specified."""
    if raw_inputs:
        return list(raw_inputs)
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    return [os.path.join(downloads_folder, default_filename)]


def compute_combined_dir(raw_inputs: Sequence[str], expanded_files: Sequence[str]) -> str:
    """Determine output directory for combined results."""
    if len(raw_inputs) == 1 and os.path.isdir(os.path.expanduser(raw_inputs[0])):
        return os.path.abspath(os.path.expanduser(raw_inputs[0]))

    dirs = [os.path.dirname(path) for path in expanded_files]
    try:
        common = os.path.commonpath(dirs)
        if os.path.isdir(common):
            return common
    except Exception:
        pass

    return dirs[0] if dirs else os.path.expanduser("~/Downloads")


def load_transactions(input_paths: Sequence[str]) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """Read multiple CSV files and combine them into single DataFrame."""
    frames: List[pd.DataFrame] = []
    failed: List[Tuple[str, str]] = []

    for path in input_paths:
        try:
            df_i = read_csv_robust(path)
            df_i["__source_file"] = os.path.basename(path)
            df_i["__source_dir"] = os.path.dirname(path)
            print(f"[OK] Loaded: {path}  rows={len(df_i)}")
            frames.append(df_i)
        except Exception as exc:
            failed.append((path, str(exc)))
            print(f"[WARN] Skipped (read error): {path}  reason={exc}")

    if not frames:
        raise RuntimeError("No inputs could be read. Check formats/permissions.")

    df_raw = pd.concat(frames, ignore_index=True)
    return df_raw, failed


def build_signed_amount_per_source(
    df_all: pd.DataFrame,
    debit_col: Optional[str],
    credit_col: Optional[str],
    amount_col: Optional[str],
) -> pd.Series:
    """Convert various amount column formats to signed values (negative = expense)."""
    output = pd.Series(index=df_all.index, dtype="float64")

    for src, group in df_all.groupby("__source_file"):
        if debit_col and (debit_col in group.columns) and group[debit_col].notna().any():
            debit = coerce_money(group[debit_col].fillna(0))
            signed = -debit
            print(f"[INFO] {src}: using Debit column ({debit_col}) → negatives=expenses")
        elif amount_col and (amount_col in group.columns) and group[amount_col].notna().any():
            amt = coerce_money(group[amount_col])
            neg_cnt = (amt < 0).sum()
            pos_cnt = (amt > 0).sum()

            if neg_cnt >= max(1, int(0.2 * max(1, pos_cnt))):
                signed = amt.where(amt < 0, 0)
                mode = "negatives=expenses"
            else:
                signed = -amt.where(amt > 0, 0)
                mode = "positives=expenses (flipped)"
            print(f"[INFO] {src}: using Amount column ({amount_col}) → {mode} (neg={neg_cnt}, pos={pos_cnt})")
        elif credit_col and (credit_col in group.columns) and group[credit_col].notna().any():
            signed = pd.Series(0, index=group.index)
            print(f"[INFO] {src}: only Credit present; zeroing out (no expenses inferred)")
        else:
            signed = pd.Series(float("nan"), index=group.index)
            print(f"[WARN] {src}: could not infer signed amount — leaving NaN")

        output.loc[group.index] = signed

    return output


def determine_detail_columns(df: pd.DataFrame, credit_col: Optional[str]) -> List[str]:
    """Select and order columns for final detail output."""
    omit_cols = set(col for col in [credit_col] if col)

    preferred = [
        "posted_date",
        "transaction_date",
        "date",
        "description",
        "details",
        "payee",
        "memo",
        "debit",
        "amount",
        "_signed_amount",
        "Category",
        "__source_file",
        "__source_dir",
    ]

    return [
        col
        for col in preferred
        if col not in omit_cols and (col in df.columns or col in {"_signed_amount", "Category", "__source_file", "__source_dir"})
    ]


def write_debug_outputs(
    df: pd.DataFrame,
    out_dir: str,
    rule_matches_df: pd.DataFrame,
    rule_misses_df: pd.DataFrame,
    rule_summary_df: pd.DataFrame,
) -> None:
    """Write categorization and source analysis CSVs for debugging."""
    try:
        per_src = (
            df.groupby(["__source_dir", "__source_file"], dropna=False)
            .agg(rows=("__source_file", "size"), total_amount=("_signed_amount", "sum"))
            .reset_index()
            .sort_values(["__source_dir", "rows"], ascending=[True, False])
        )
        per_src.to_csv(os.path.join(out_dir, "per_source_debug.csv"), index=False)
        rule_matches_df.to_csv(os.path.join(out_dir, "category_rule_matches.csv"), index=False)
        rule_misses_df.to_csv(os.path.join(out_dir, "category_rule_misses.csv"), index=False)
        rule_summary_df.to_csv(os.path.join(out_dir, "category_rule_summary.csv"), index=False)
        print(f"[DEBUG] Wrote debug files to: {out_dir}")
    except Exception as exc:
        print(f"[DEBUG] Debug write failed: {exc}")


def write_error_log(base_dir: str, base_name: str, exc: BaseException) -> str:
    """
    Persist an error log to expenses_outputs so failures are saved even when the pipeline crashes.
    Returns the log file path.
    """
    candidate_dirs = [base_dir, os.path.join(os.path.expanduser("~"), "Downloads")]
    last_err: Optional[BaseException] = None

    for root in candidate_dirs:
        try:
            out_root = os.path.join(root, "expenses_outputs")
            os.makedirs(out_root, exist_ok=True)

            log_path = os.path.join(out_root, f"{base_name}_error_log.txt")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("Expense pipeline failed.\n\n")
                f.write(f"Timestamp      : {datetime.now().isoformat()}\n")
                f.write(f"Base name      : {base_name}\n")
                f.write(f"Output base dir: {root}\n\n")
                f.write(f"Exception type : {type(exc).__name__}\n")
                f.write(f"Exception msg  : {exc}\n\n")
                f.write("Traceback:\n")
                f.write(traceback.format_exc())

            print(f"[ERROR] Wrote error log to: {log_path}")
            return log_path
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed to write error log. Last error: {last_err}") from last_err


def run_pipeline(
    raw_paths: Sequence[str],
    *,
    output_dir: Optional[str] = None,
    default_filename: str = DEFAULT_DOWNLOAD_FILENAME,
) -> str:
    """Complete expense processing pipeline (entry point)."""
    raw_inputs = ensure_raw_inputs(raw_paths, default_filename)
    input_paths = expand_inputs(raw_inputs)

    print(f"[INFO] Inputs after expansion ({len(input_paths)}):")
    for path in input_paths:
        print(f"  - {path}")
    if not input_paths:
        raise FileNotFoundError("No CSV inputs found. Drop files/folders containing CSVs.")

    combined_dir = output_dir or compute_combined_dir(raw_inputs, input_paths)
    if not os.access(combined_dir, os.W_OK):
        print("[WARN] Combined output dir not writable; falling back to ~/Downloads")
        combined_dir = os.path.join(os.path.expanduser("~"), "Downloads")

    if len(input_paths) == 1:
        base_name = os.path.splitext(os.path.basename(input_paths[0]))[0]
    else:
        base_name = f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        # Lazy import outputs so missing matplotlib doesn't crash module import time.
        from .outputs import write_grouped_category_outputs, write_monthly_categorygroup_charts

        df_raw, failed = load_transactions(input_paths)
        if failed:
            print(f"[INFO] {len(failed)} file(s) failed to load; continuing:")
            for path, msg in failed:
                print(f"       - {path}: {msg}")

        df = drop_table_name_rows(df_raw)
        df = normalize_headers(df)
        df = normalize_date_columns(df, candidates=["transaction_date", "posted_date", "date"])
        df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

        desc_col = pick_col(df, ["description", "details", "payee", "memo"])
        debit_col = pick_col(df, ["debit", "withdrawal", "outflow"])
        credit_col = pick_col(df, ["credit", "deposit", "inflow"])
        amount_col = pick_col(df, ["amount", "amt", "value", "transaction_amount"])
        if not desc_col:
            raise ValueError(f"Could not find a description-like column. Columns: {list(df.columns)}")

        df["_signed_amount"] = build_signed_amount_per_source(df, debit_col, credit_col, amount_col)
        df = df[df["_signed_amount"] < 0].copy()
        df = df.reset_index(drop=True)

        df["_description_clean"] = df[desc_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

        cat_series, rule_matches_df, rule_misses_df, rule_summary_df = detect_or_build_category_with_debug(df, desc_col)

        df["CategoryGroup"] = cat_series
        df["CategoryOriginal"] = df["CategoryGroup"].copy()

        if not rule_matches_df.empty and {"row_index", "matched_category"}.issubset(rule_matches_df.columns):
            for idx, matched in zip(rule_matches_df["row_index"].values, rule_matches_df["matched_category"].values):
                if idx in df.index:
                    df.at[idx, "CategoryOriginal"] = matched

        if not rule_misses_df.empty and {"row_index", "existing_category"}.issubset(rule_misses_df.columns):
            for idx, existing_cat in zip(rule_misses_df["row_index"].values, rule_misses_df["existing_category"].values):
                if idx in df.index and existing_cat is not None:
                    df.at[idx, "CategoryOriginal"] = existing_cat

        df["CategoryOriginal"] = df["CategoryOriginal"].fillna(df["CategoryGroup"])
        df["Category"] = df["CategoryOriginal"].copy()

        detail_cols = determine_detail_columns(df, credit_col)

        combined_out_root = write_grouped_category_outputs(combined_dir, base_name, df, detail_cols)
        write_monthly_categorygroup_charts(combined_dir, base_name, df)

        write_debug_outputs(df, combined_out_root, rule_matches_df, rule_misses_df, rule_summary_df)

        for (src_dir, src_file), df_src in df.groupby(["__source_dir", "__source_file"], dropna=False):
            src_base = os.path.splitext(src_file)[0]
            write_grouped_category_outputs(src_dir, src_base, df_src, detail_cols)
            write_monthly_categorygroup_charts(src_dir, src_base, df_src)

        return combined_out_root

    except Exception as exc:
        write_error_log(combined_dir, base_name, exc)
        raise


__all__ = [
    "run_pipeline",
    "expand_inputs",
    "compute_combined_dir",
]
