"""Output generation helpers."""

from __future__ import annotations

import os
from typing import List

import pandas as pd


def slugify(name: str) -> str:
    sanitized = name.strip()
    sanitized = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in sanitized)
    sanitized = sanitized.strip("_")
    return sanitized or "uncategorized"


def write_outputs(base_dir: str, base_name: str, df_subset: pd.DataFrame, detail_cols: List[str]) -> str:
    """
    Write master summary + per-category detail tables to:
      base_dir/expenses_outputs/<base_name>_summary_expenses.csv
    Returns the `expenses_outputs` folder path.
    """
    out_root = os.path.join(base_dir, "expenses_outputs")
    os.makedirs(out_root, exist_ok=True)

    summary = (
        df_subset.groupby(["Category", "_description_clean"], dropna=False)
        .agg(count=("_description_clean", "count"), total_amount=("_signed_amount", "sum"))
        .reset_index()
        .rename(columns={"_description_clean": "Description"})
        .sort_values(["Category", "total_amount", "Description"], ascending=[True, False, True])
    )
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

    out_dir = os.path.join(out_root, f"{base_name}_category_tables")
    os.makedirs(out_dir, exist_ok=True)

    categories = sorted(df_subset["Category"].dropna().unique().tolist())
    for category in categories:
        category_slug = slugify(category)
        df_cat = df_subset[df_subset["Category"] == category].copy()
        cols_existing = [col for col in detail_cols if col in df_cat.columns]
        df_cat_detail = df_cat[cols_existing].copy()

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
            total_row["debit"] = abs(total_val)
        df_cat_detail = pd.concat([df_cat_detail, pd.DataFrame([total_row])], ignore_index=True)
        df_cat_detail.to_csv(os.path.join(out_dir, f"{category_slug}_detail.csv"), index=False)

        cat_summary = (
            df_cat.groupby("_description_clean", dropna=False)
            .agg(count=("_description_clean", "count"), total_amount=("_signed_amount", "sum"))
            .reset_index()
            .rename(columns={"_description_clean": "Description"})
            .sort_values(["total_amount", "Description"], ascending=[False, True])
        )
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
