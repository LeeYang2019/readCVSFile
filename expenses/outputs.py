"""Output generation helpers."""

from __future__ import annotations

import os
from typing import List

import pandas as pd


def slugify(name: str) -> str:
    """
    Convert category name to safe filename.
    
    Removes/replaces special characters that are problematic in filenames.
    Example: "Food & Dining" → "food_dining"
    
    Args:
        name: Category name or any string
        
    Returns:
        Filename-safe string with alphanumerics, dots, hyphens, underscores only
    """
    sanitized = name.strip()
    # Keep only alphanumerics, dots, hyphens, underscores; replace others with underscore
    sanitized = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in sanitized)
    sanitized = sanitized.strip("_")  # Remove leading/trailing underscores
    return sanitized or "uncategorized"


def write_outputs(base_dir: str, base_name: str, df_subset: pd.DataFrame, detail_cols: List[str]) -> str:
    """
    Generate expense summary and detail CSV files.
    
    Creates two types of outputs:
    1. Master summary: All transactions grouped by (Category, Description) with counts and totals
    2. Per-category detail tables: Breakdown of each category with transaction details and summaries
    
    Structure:
    - base_dir/expenses_outputs/{base_name}_summary_expenses.csv (master)
    - base_dir/expenses_outputs/{base_name}_category_tables/{category}_detail.csv (per-file details)
    - base_dir/expenses_outputs/{base_name}_category_tables/{category}_summary.csv (per-category summary)
    
    Args:
        base_dir: Base output directory
        base_name: Base filename (e.g., "chase_checking" or "combined_20251212_150000")
        df_subset: Expense DataFrame to summarize
        detail_cols: Column names to include in detail tables
        
    Returns:
        Path to expenses_outputs directory (root of all generated files)
    """
    # Create main output directory
    out_root = os.path.join(base_dir, "expenses_outputs")
    os.makedirs(out_root, exist_ok=True)

    # Generate master summary: group by category and description
    summary = (
        df_subset.groupby(["Category", "_description_clean"], dropna=False)
        .agg(count=("_description_clean", "count"), total_amount=("_signed_amount", "sum"))
        .reset_index()
        .rename(columns={"_description_clean": "Description"})
        .sort_values(["Category", "total_amount", "Description"], ascending=[True, False, True])
    )
    # Add totals row
    totals = pd.DataFrame(
        {
            "Category": ["__ALL__"],
            "Description": ["__TOTAL__"],
            "count": [summary["count"].sum()],
            "total_amount": [summary["total_amount"].sum()],
        }
    )
    summary_with_total = pd.concat([summary, totals], ignore_index=True)
    master_out = os.path.join(out_root, f"{base_name}_summary_expenses.csv")
    summary_with_total.to_csv(master_out, index=False)
    print(f"[OK] Wrote master summary to: {master_out}")

    # Create per-category detail directory
    out_dir = os.path.join(out_root, f"{base_name}_category_tables")
    os.makedirs(out_dir, exist_ok=True)

    # Generate detail tables for each category
    categories = sorted(df_subset["Category"].dropna().unique().tolist())
    for category in categories:
        # Convert category name to safe filename
        category_slug = slugify(category)
        df_cat = df_subset[df_subset["Category"] == category].copy()
        cols_existing = [col for col in detail_cols if col in df_cat.columns]
        df_cat_detail = df_cat[cols_existing].copy()

        # Add category total row (for easy reference in Excel)
        total_row = {col: "" for col in cols_existing}
        if "description" in cols_existing:
            total_row["description"] = "__CATEGORY_TOTAL__"
        if "Category" in cols_existing:
            total_row["Category"] = category
        total_val = df_cat_detail["_signed_amount"].sum() if "_signed_amount" in cols_existing else 0
        if "_signed_amount" in cols_existing:
            total_row["_signed_amount"] = total_val
        elif "amount" in cols_existing:
            total_row["amount"] = total_val
        elif "debit" in cols_existing:
            total_row["debit"] = abs(total_val)  # Debit column usually positive
        df_cat_detail = pd.concat([df_cat_detail, pd.DataFrame([total_row])], ignore_index=True)
        df_cat_detail.to_csv(os.path.join(out_dir, f"{category_slug}_detail.csv"), index=False)

        # Per-category summary: group by description with count and total amount
        cat_summary = (
            df_cat.groupby("_description_clean", dropna=False)
            .agg(count=("_description_clean", "count"), total_amount=("_signed_amount", "sum"))
            .reset_index()
            .rename(columns={"_description_clean": "Description"})
            .sort_values(["total_amount", "Description"], ascending=[False, True])
        )
        # Add total row
        cat_total = {
            "Description": "__CATEGORY_TOTAL__",
            "count": cat_summary["count"].sum(),
            "total_amount": cat_summary["total_amount"].sum(),
        }
        cat_summary = pd.concat([cat_summary, pd.DataFrame([cat_total])], ignore_index=True)
        cat_summary.to_csv(os.path.join(out_dir, f"{category_slug}_summary.csv"), index=False)

    print(f"[OK] Wrote per-category CSVs (with totals) to: {out_dir}")
    return out_root


__all__ = ["slugify", "write_outputs"]
