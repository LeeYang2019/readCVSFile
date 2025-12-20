"""Output generation helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Sequence, Union

import pandas as pd

import matplotlib
matplotlib.use("Agg")  # headless PNG export (CI / servers)
import matplotlib.pyplot as plt


def slugify(name: str) -> str:
    """
    Convert category name to safe filename.
    Example: "Food & Dining" → "food_dining"
    """
    sanitized = name.strip()
    sanitized = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in sanitized)
    sanitized = sanitized.strip("_")
    return sanitized or "uncategorized"


def _ensure_dir(path: Union[str, os.PathLike]) -> str:
    p = str(path)
    os.makedirs(p, exist_ok=True)
    return p


def _infer_base_name_from_paths(paths: Sequence[Union[str, os.PathLike]]) -> str:
    # Single file -> file stem; many -> combined
    if len(paths) == 1:
        return Path(paths[0]).stem or "expenses"
    return "combined"


def _load_and_combine_csvs(
    paths: Sequence[Union[str, os.PathLike]],
    *,
    add_source_column: bool = True,
) -> pd.DataFrame:
    """Reads one or more CSVs and concatenates them."""
    frames: List[pd.DataFrame] = []
    for p in paths:
        pth = Path(p)
        df = pd.read_csv(pth)
        if add_source_column:
            df["__source_file"] = pth.name
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def write_outputs(
    base_dir: str,
    base_name: str,
    df_subset: pd.DataFrame,
    detail_cols: List[str],
) -> str:
    """Write a flat master summary CSV."""
    out_root = os.path.join(base_dir, "expenses_outputs")
    os.makedirs(out_root, exist_ok=True)

    if "CategoryGroup" in df_subset.columns:
        summary = (
            df_subset.groupby(["CategoryGroup", "Category", "_description_clean"], dropna=False)
            .agg(
                count=("_description_clean", "count"),
                total_amount=("_signed_amount", "sum"),
            )
            .reset_index()
            .rename(columns={"_description_clean": "Description"})
            .sort_values(
                ["CategoryGroup", "Category", "total_amount", "Description"],
                ascending=[True, True, False, True],
            )
        )

        totals = pd.DataFrame(
            {
                "CategoryGroup": ["__ALL__"],
                "Category": ["__ALL__"],
                "Description": ["__TOTAL__"],
                "count": [int(summary["count"].sum())],
                "total_amount": [float(summary["total_amount"].sum())],
            }
        )
    else:
        summary = (
            df_subset.groupby(["Category", "_description_clean"], dropna=False)
            .agg(
                count=("_description_clean", "count"),
                total_amount=("_signed_amount", "sum"),
            )
            .reset_index()
            .rename(columns={"_description_clean": "Description"})
            .sort_values(["Category", "total_amount", "Description"], ascending=[True, False, True])
        )

        totals = pd.DataFrame(
            {
                "Category": ["__ALL__"],
                "Description": ["__TOTAL__"],
                "count": [int(summary["count"].sum())],
                "total_amount": [float(summary["total_amount"].sum())],
            }
        )

    summary_with_total = pd.concat([summary, totals], ignore_index=True)
    master_out = os.path.join(out_root, f"{base_name}_summary_expenses.csv")
    summary_with_total.to_csv(master_out, index=False)
    print(f"[OK] Wrote master summary to: {master_out}")

    return out_root


def write_grouped_category_outputs(
    base_dir: str,
    base_name: str,
    df_subset: pd.DataFrame,
    detail_cols: List[str],
) -> str:
    """Generate summaries grouped by CategoryGroup."""
    out_root = os.path.join(base_dir, "expenses_outputs")
    os.makedirs(out_root, exist_ok=True)

    if "CategoryGroup" not in df_subset.columns:
        print("[WARN] CategoryGroup column not found; skipping grouped outputs")
        return out_root

    cat_src = "CategoryOriginal" if "CategoryOriginal" in df_subset.columns else "Category"

    summary = (
        df_subset.groupby(["CategoryGroup", cat_src, "_description_clean"], dropna=False)
        .agg(
            count=("_description_clean", "count"),
            total_amount=("_signed_amount", "sum"),
        )
        .reset_index()
        .rename(columns={cat_src: "Category", "_description_clean": "Description"})
        .sort_values(
            ["CategoryGroup", "Category", "total_amount", "Description"],
            ascending=[True, True, False, True],
        )
    )

    totals = pd.DataFrame(
        {
            "CategoryGroup": ["__ALL__"],
            "Category": ["__ALL__"],
            "Description": ["__TOTAL__"],
            "count": [int(summary["count"].sum())],
            "total_amount": [float(summary["total_amount"].sum())],
        }
    )

    summary_with_total = pd.concat([summary, totals], ignore_index=True)
    master_out = os.path.join(out_root, f"{base_name}_summary_expenses.csv")
    summary_with_total.to_csv(master_out, index=False)
    print(f"[OK] Wrote grouped master summary to: {master_out}")

    out_dir = os.path.join(out_root, f"{base_name}_grouped_tables")
    os.makedirs(out_dir, exist_ok=True)

    for group in sorted(df_subset["CategoryGroup"].dropna().unique()):
        group_slug = slugify(group)
        df_group = df_subset[df_subset["CategoryGroup"] == group].copy()

        cols_existing = [c for c in detail_cols if c in df_group.columns]
        df_group_detail = df_group[cols_existing].copy()

        if "Category" in df_group_detail.columns and "CategoryOriginal" in df_group.columns:
            df_group_detail.loc[:, "Category"] = df_group.loc[
                df_group_detail.index, "CategoryOriginal"
            ].values

        total_val = (
            df_group_detail["_signed_amount"].sum()
            if "_signed_amount" in df_group_detail.columns
            else 0
        )

        total_row = {c: "" for c in cols_existing}
        if "description" in cols_existing:
            total_row["description"] = "__GROUP_TOTAL__"
        if "_signed_amount" in cols_existing:
            total_row["_signed_amount"] = total_val

        df_group_detail = pd.concat(
            [df_group_detail, pd.DataFrame([total_row])],
            ignore_index=True,
        )

        df_group_detail.to_csv(
            os.path.join(out_dir, f"{group_slug}_detail.csv"),
            index=False,
        )

        cat_summary = (
            df_group.groupby(cat_src, dropna=False)
            .agg(
                count=("_signed_amount", "size"),
                total_amount=("_signed_amount", "sum"),
            )
            .reset_index()
            .rename(columns={cat_src: "Category"})
            .sort_values("total_amount", ascending=False)
        )

        subtotal = pd.DataFrame(
            {
                "Category": ["__GROUP_SUBTOTAL__"],
                "count": [int(cat_summary["count"].sum())],
                "total_amount": [float(cat_summary["total_amount"].sum())],
            }
        )

        cat_summary = pd.concat([cat_summary, subtotal], ignore_index=True)
        cat_summary.to_csv(
            os.path.join(out_dir, f"{group_slug}_category_summary.csv"),
            index=False,
        )

    print(f"[OK] Wrote grouped tables to: {out_dir}")
    return out_root


def write_monthly_categorygroup_charts(
    base_dir: str,
    base_name: str,
    df_subset: pd.DataFrame,
) -> str:
    """
    Export Month × CategoryGroup totals.

    Outputs:
      - *_monthly_categorygroup_totals_long.csv  (vertical, Numbers-friendly)
      - grouped bar chart PNG
    """
    out_root = os.path.join(base_dir, "expenses_outputs")
    os.makedirs(out_root, exist_ok=True)

    required = {"CategoryGroup", "posted_date", "_signed_amount"}
    missing = [c for c in required if c not in df_subset.columns]
    if missing:
        print(f"[WARN] Missing columns for charts: {missing}; skipping")
        return out_root

    df = df_subset.copy()
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    df = df.dropna(subset=["posted_date"])
    if df.empty:
        print("[WARN] No valid posted_date rows; skipping charts")
        return out_root

    df["month"] = df["posted_date"].dt.to_period("M").astype(str)

    monthly_long = (
        df.groupby(["month", "CategoryGroup"], dropna=False)
        .agg(total_amount=("_signed_amount", "sum"))
        .reset_index()
    )

    monthly_long["abs_total"] = monthly_long["total_amount"].abs()
    monthly_long = (
        monthly_long.sort_values(["month", "abs_total"], ascending=[True, False])
        .drop(columns=["abs_total"])
        .reset_index(drop=True)
    )

    long_csv_path = os.path.join(
        out_root, f"{base_name}_monthly_categorygroup_totals_long.csv"
    )
    monthly_long.to_csv(long_csv_path, index=False)
    print(f"[OK] Wrote monthly CategoryGroup totals (LONG) CSV to: {long_csv_path}")

    # Chart (pivot only in-memory)
    pivot = (
        monthly_long.pivot(index="month", columns="CategoryGroup", values="total_amount")
        .fillna(0)
        .sort_index()
    )

    charts_dir = os.path.join(out_root, f"{base_name}_charts")
    os.makedirs(charts_dir, exist_ok=True)

    plot_df = pivot.abs()
    ax = plot_df.plot(kind="bar", stacked=False, figsize=(12, 6))
    ax.set_title("Monthly Spend by Category Group (Grouped)")
    ax.set_xlabel("Month")
    ax.set_ylabel("Total Spend ($)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(
        os.path.join(charts_dir, f"{base_name}_monthly_categorygroup_grouped.png"),
        dpi=200,
    )
    plt.close()

    print(f"[OK] Wrote charts to: {charts_dir}")
    return charts_dir


def generate_outputs_for_files(
    *,
    base_dir: str,
    files: Sequence[Union[str, os.PathLike]],
    detail_cols: List[str],
    base_name: Optional[str] = None,
    add_source_column: bool = True,
) -> str:
    """Generate outputs once for single or multiple files (combined)."""
    files = list(files)
    if not files:
        raise ValueError("No files provided.")

    base_dir = _ensure_dir(base_dir)
    base_name = base_name or _infer_base_name_from_paths(files)

    df = _load_and_combine_csvs(files, add_source_column=add_source_column)
    if df.empty:
        raise ValueError("No rows found after reading the provided file(s).")

    out_root = write_outputs(base_dir, base_name, df, detail_cols)
    write_grouped_category_outputs(base_dir, base_name, df, detail_cols)
    write_monthly_categorygroup_charts(base_dir, base_name, df)

    return out_root


__all__ = [
    "slugify",
    "write_outputs",
    "write_grouped_category_outputs",
    "write_monthly_categorygroup_charts",
    "generate_outputs_for_files",
]
