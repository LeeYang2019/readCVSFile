"""High-level orchestration for expense processing."""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Optional, Sequence, Tuple

import pandas as pd

from .categories import CATEGORY_CANON, DEFAULT_DOWNLOAD_FILENAME
from .categorizer import detect_or_build_category_with_debug
from .csv_reader import read_csv_robust
from .normalization import (
    coerce_money,
    drop_table_name_rows,
    normalize_date_columns,
    normalize_headers,
    pick_col,
)
from .outputs import write_outputs


def is_csv_filename(name: str) -> bool:
    """Check if filename has .csv extension (case-insensitive)."""
    return name.strip().lower().endswith(".csv")


def expand_inputs(paths: Sequence[str]) -> List[str]:
    """
    Convert file and folder paths to a deduplicated list of CSV file paths.
    
    Recursively walks directories to find all CSV files. Useful for accepting
    either individual files or entire folder structures as input.
    
    Args:
        paths: File paths, folder paths, or mix of both
        
    Returns:
        Deduplicated list of absolute CSV file paths
    """
    collected: List[str] = []
    for path in paths:
        expanded = os.path.expanduser(path)  # Handle ~/ shortcuts
        if os.path.isdir(expanded):
            # Recursively walk directory tree and collect all CSV files
            for root, _, files in os.walk(expanded):
                for filename in files:
                    if is_csv_filename(filename):
                        collected.append(os.path.join(root, filename))
        elif is_csv_filename(expanded):
            # Single CSV file provided
            collected.append(expanded)
    
    # Deduplicate (handles case where same file specified multiple times)
    seen = set()
    deduped = []
    for path in collected:
        if path not in seen:
            seen.add(path)
            deduped.append(path)
    return deduped


def ensure_raw_inputs(raw_inputs: Sequence[str], default_filename: str) -> List[str]:
    """
    Provide default input path if none specified.
    
    Allows headless/scripted operation: if no files provided, looks in ~/Downloads
    for the default CSV file (useful for recurring batch jobs).
    
    Args:
        raw_inputs: User-provided file/folder paths
        default_filename: Fallback filename to use in ~/Downloads (e.g., "japan_trip.csv")
        
    Returns:
        List with either raw_inputs or default path
    """
    if raw_inputs:
        return list(raw_inputs)
    # No inputs provided: use default file from Downloads folder
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    return [os.path.join(downloads_folder, default_filename)]


def compute_combined_dir(raw_inputs: Sequence[str], expanded_files: Sequence[str]) -> str:
    """
    Determine output directory for combined results.
    
    Uses these strategies in order:
    1. If single input is a directory, use that
    2. Find common parent directory of all input files
    3. Fall back to first file's directory
    4. Last resort: ~/Downloads
    
    Args:
        raw_inputs: Original user-provided paths
        expanded_files: Expanded list of CSV file paths
        
    Returns:
        Absolute path to output directory
    """
    # Strategy 1: Single directory input → use that directly
    if len(raw_inputs) == 1 and os.path.isdir(os.path.expanduser(raw_inputs[0])):
        return os.path.abspath(os.path.expanduser(raw_inputs[0]))
    
    # Strategy 2: Find common parent directory of all files
    dirs = [os.path.dirname(path) for path in expanded_files]
    try:
        common = os.path.commonpath(dirs)
        if os.path.isdir(common):
            return common
    except Exception:
        pass
    
    # Strategy 3-4: Use first file's dir or Downloads folder
    return dirs[0] if dirs else os.path.expanduser("~/Downloads")


def load_transactions(input_paths: Sequence[str]) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
    """
    Read multiple CSV files and combine them into single DataFrame.
    
    Gracefully handles read failures: skips problematic files and continues
    with valid ones. Adds source file/directory metadata for traceability.
    
    Args:
        input_paths: List of CSV file paths to read
        
    Returns:
        Tuple of (combined_df, list_of_failures)
        - combined_df: Concatenated DataFrame from all readable files
        - list_of_failures: List of (path, error_message) tuples for failed reads
        
    Raises:
        RuntimeError: If no files could be read at all
    """
    frames: List[pd.DataFrame] = []
    failed: List[Tuple[str, str]] = []
    
    # Try to read each file, skipping failures but recording them
    for path in input_paths:
        try:
            df_i = read_csv_robust(path)
            # Add metadata columns for tracking which file/directory data came from
            df_i["__source_file"] = os.path.basename(path)
            df_i["__source_dir"] = os.path.dirname(path)
            print(f"[OK] Loaded: {path}  rows={len(df_i)}")
            frames.append(df_i)
        except Exception as exc:
            failed.append((path, str(exc)))
            print(f"[WARN] Skipped (read error): {path}  reason={exc}")
    
    # Ensure at least one file was readable
    if not frames:
        raise RuntimeError("No inputs could be read. Check formats/permissions.")
    
    # Combine all DataFrames into one (resets index)
    df_raw = pd.concat(frames, ignore_index=True)
    return df_raw, failed


def build_signed_amount_per_source(
    df_all: pd.DataFrame,
    debit_col: Optional[str],
    credit_col: Optional[str],
    amount_col: Optional[str],
) -> pd.Series:
    """
    Convert various amount column formats to signed values (negative = expense).
    
    Different banks/sources use different conventions:
    - Debit/Credit columns: separate columns for debits and credits
    - Amount column: single column with positive/negative values or just one sign
    
    Intelligently detects which convention each source uses and converts accordingly.
    
    Priority order per file:
    1. Debit column (negated) if present and has data
    2. Amount column (if has both +/- or heuristically determines convention)
    3. Credit column (if only credit present, treated as no expenses)
    4. NaN (if none of the above)
    
    Args:
        df_all: DataFrame with all transactions and metadata
        debit_col: Name of debit/withdrawal column, or None
        credit_col: Name of credit/deposit column, or None
        amount_col: Name of amount column, or None
        
    Returns:
        Series of signed amounts (negative values = expenses to track)
    """
    output = pd.Series(index=df_all.index, dtype="float64")
    
    # Process each source file separately (different sources may have different conventions)
    for src, group in df_all.groupby("__source_file"):
        if debit_col and (debit_col in group.columns) and group[debit_col].notna().any():
            # Strategy 1: Debit column present and has data → negate debits (they're expenses)
            debit = coerce_money(group[debit_col].fillna(0))
            signed = -debit
            print(f"[INFO] {src}: using Debit column ({debit_col}) → negatives=expenses")
        elif amount_col and (amount_col in group.columns) and group[amount_col].notna().any():
            # Strategy 2: Amount column present
            # Try to auto-detect if column uses +/- notation or just one sign
            amt = coerce_money(group[amount_col])
            neg_cnt = (amt < 0).sum()
            pos_cnt = (amt > 0).sum()
            
            # If there are significant negatives (≥20% of positives), assume negatives=expenses
            # Otherwise, negate positives (assume positives=expenses)
            if neg_cnt >= max(1, int(0.2 * max(1, pos_cnt))):
                signed = amt.where(amt < 0, 0)
                mode = "negatives=expenses"
            else:
                signed = -amt.where(amt > 0, 0)
                mode = "positives=expenses (flipped)"
            print(f"[INFO] {src}: using Amount column ({amount_col}) → {mode} (neg={neg_cnt}, pos={pos_cnt})")
        elif credit_col and (credit_col in group.columns) and group[credit_col].notna().any():
            # Strategy 3: Only credit column (inflows/deposits) → no expenses inferred
            signed = pd.Series(0, index=group.index)
            print(f"[INFO] {src}: only Credit present; zeroing out (no expenses inferred)")
        else:
            # Strategy 4: No suitable column found → NaN (will be filtered out later)
            signed = pd.Series(float("nan"), index=group.index)
            print(f"[WARN] {src}: could not infer signed amount — leaving NaN")
        
        # Assign signed amounts for this source file's rows
        output.loc[group.index] = signed
    return output


def determine_detail_columns(df: pd.DataFrame, credit_col: Optional[str]) -> List[str]:
    """
    Select and order columns for final detail output.
    
    Filters to "important" columns in a sensible order, skipping the credit column
    (since we already extracted its data via signed amounts) and internal metadata.
    
    Args:
        df: Full transaction DataFrame
        credit_col: Credit column name to exclude (if any)
        
    Returns:
        List of column names to include in final output (in display order)
    """
    omit_cols = set(col for col in [credit_col] if col)  # Skip credit column from output
    
    # Preferred column order (checked in order, only includes what exists)
    preferred = [
        "transaction_date",
        "posted_date",
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
    # Return columns that exist in df (or are internal) in order, excluding omit_cols
    return [
        col
        for col in preferred
        if col not in omit_cols and (col in df.columns or col in {"_signed_amount", "Category", "__source_file", "__source_dir"})
    ]


def write_debug_outputs(df: pd.DataFrame, out_dir: str, rule_matches_df: pd.DataFrame, rule_misses_df: pd.DataFrame, rule_summary_df: pd.DataFrame) -> None:
    """
    Write categorization and source analysis CSVs for debugging.
    
    Outputs help with:
    - Identifying which files contributed most data
    - Understanding categorization hit rate by category
    - Finding transactions that couldn't be categorized (for adding new rules)
    
    Files written:
    - per_source_debug.csv: Row counts and total amounts by source file
    - category_rule_matches.csv: All successful matches with matched keywords
    - category_rule_misses.csv: All unmatched transactions (with reasons)
    - category_rule_summary.csv: Per-category hit counts and coverage %
    
    Args:
        df: Full transaction DataFrame
        out_dir: Output directory for debug files
        rule_matches_df: DataFrame of successful category matches
        rule_misses_df: DataFrame of failed category matches
        rule_summary_df: DataFrame of per-category statistics
    """
    try:
        # Aggregate data by source file for summary
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


def run_pipeline(
    raw_paths: Sequence[str],
    *,
    output_dir: Optional[str] = None,
    default_filename: str = DEFAULT_DOWNLOAD_FILENAME,
) -> str:
    """
    Complete expense processing pipeline (entry point).
    
    Orchestrates the full workflow:
    1. Input expansion: Resolve file/folder paths to CSV files
    2. Loading: Read all CSV files with robust format detection
    3. Cleaning: Normalize headers, dates, amounts, and descriptions
    4. Categorization: Match descriptions against keyword rules
    5. Output: Generate summary tables and per-category breakdowns
    6. Debug: Export categorization stats and misses for analysis
    
    Args:
        raw_paths: File paths, folder paths, or empty (for defaults)
        output_dir: Directory for combined results (auto-detected if None)
        default_filename: File to look for in ~/Downloads if no paths given
        
    Returns:
        Path to output directory containing summary and debug files
        
    Raises:
        FileNotFoundError: If no CSV files found
        ValueError: If no description column detected
    """
    # Step 1: Resolve input paths
    raw_inputs = ensure_raw_inputs(raw_paths, default_filename)
    input_paths = expand_inputs(raw_inputs)
    print(f"[INFO] Inputs after expansion ({len(input_paths)}):")
    for path in input_paths:
        print(f"  - {path}")
    if not input_paths:
        raise FileNotFoundError("No CSV inputs found. Drop files/folders containing CSVs.")

    # Step 2: Determine output directory and verify write access
    combined_dir = output_dir or compute_combined_dir(raw_inputs, input_paths)
    if not os.access(combined_dir, os.W_OK):
        print("[WARN] Combined output dir not writable; falling back to ~/Downloads")
        combined_dir = os.path.join(os.path.expanduser("~"), "Downloads")

    # Step 3: Load transactions from all CSVs
    df_raw, failed = load_transactions(input_paths)
    if failed:
        print(f"[INFO] {len(failed)} file(s) failed to load; continuing:")
        for path, msg in failed:
            print(f"       - {path}: {msg}")

    # Step 4: Clean and normalize data
    df = drop_table_name_rows(df_raw)  # Remove Numbers app metadata rows
    df = normalize_headers(df)  # Lowercase, underscores
    df = normalize_date_columns(df, candidates=["transaction_date", "posted_date", "date"])  # YYYY-MM-DD format
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)  # Trim whitespace

    # Step 5: Identify key columns (column names vary by source)
    desc_col = pick_col(df, ["description", "details", "payee", "memo"])
    debit_col = pick_col(df, ["debit", "withdrawal", "outflow"])
    credit_col = pick_col(df, ["credit", "deposit", "inflow"])
    amount_col = pick_col(df, ["amount", "amt", "value", "transaction_amount"])
    if not desc_col:
        raise ValueError(f"Could not find a description-like column. Columns: {list(df.columns)}")

    # Step 6: Convert amounts to signed values (negative = expense)
    df["_signed_amount"] = build_signed_amount_per_source(df, debit_col, credit_col, amount_col)
    df = df[df["_signed_amount"] < 0]  # Filter to only expenses
    df["_description_clean"] = df[desc_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

    # Step 7: Apply categorization rules
    cat_series, rule_matches_df, rule_misses_df, rule_summary_df = detect_or_build_category_with_debug(df, desc_col)
    df["Category"] = cat_series
    df["CategoryOriginal"] = df["Category"]  # Before remapping
    df["CategoryGroup"] = df["Category"].replace(CATEGORY_CANON, regex=True)  # After remapping
    df["CategoryGroup"] = df["CategoryGroup"].fillna(df["Category"])  # Fallback if remap didn't apply

    # Step 8: Select columns for output (remove internal/sensitive columns)
    # Step 8: Select columns for output (remove internal/sensitive columns)
    detail_cols = determine_detail_columns(df, credit_col)

    # Step 9: Generate output filename
    if len(input_paths) == 1:
        # Single file: use its name as base
        base_name = os.path.splitext(os.path.basename(input_paths[0]))[0]
    else:
        # Multiple files: use timestamp to differentiate combined runs
        base_name = f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Step 10: Write combined summary and per-category tables
    combined_out_root = write_outputs(combined_dir, base_name, df, detail_cols)
    # Write categorization analysis files (for debugging/refining rules)
    write_debug_outputs(df, combined_out_root, rule_matches_df, rule_misses_df, rule_summary_df)

    # Step 11: Write per-source outputs (helpful for comparing different bank CSVs)
    for (src_dir, src_file), df_src in df.groupby(["__source_dir", "__source_file"]):
        src_base = os.path.splitext(src_file)[0]
        write_outputs(src_dir, src_base, df_src, detail_cols)

    return combined_out_root


__all__ = [
    "run_pipeline",
    "expand_inputs",
    "compute_combined_dir",
]
