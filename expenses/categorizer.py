"""Category detection utilities with debug outputs."""

from __future__ import annotations

from typing import List, Optional, Tuple

import pandas as pd

from .categories import CATEGORY_RULES, CATEGORY_CANON


def detect_or_build_category_with_debug(df: pd.DataFrame, desc_col: str) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns a tuple of:
      - Series of resolved categories
      - DataFrame of rule matches
      - DataFrame of rule misses
      - DataFrame summarizing rule hit counts/coverage
    """
    existing = None
    for candidate in ["category", "categories", "cat", "type"]:
        if candidate in df.columns:
            series = df[candidate]
            existing = series if existing is None else existing.combine_first(series)

    if existing is not None:
        existing_clean = (
            existing.astype(object)
            .where(pd.notna(existing), "")
            .astype(str)
            .str.strip()
            .replace({"": None})
        )
    else:
        existing_clean = None

    rule_hit_cat: List[Optional[str]] = []
    rule_hit_kw: List[Optional[str]] = []
    desc_series = df[desc_col].astype(str)
    for text in desc_series:
        normalized = (text or "").upper()
        found_cat = None
        found_kw = None
        if normalized:
            for category, keywords in CATEGORY_RULES.items():
                for keyword in keywords:
                    if keyword in normalized:
                        found_cat = category
                        found_kw = keyword
                        break
                if found_cat:
                    break
        rule_hit_cat.append(found_cat)
        rule_hit_kw.append(found_kw)

    rule_cat = pd.Series(rule_hit_cat, index=df.index, dtype="object")
    rule_kw = pd.Series(rule_hit_kw, index=df.index, dtype="object")

    if existing_clean is not None:
        resolved = rule_cat.where(rule_cat.notna(), existing_clean)
    else:
        resolved = rule_cat.copy()
    resolved = resolved.fillna("Uncategorized")
    resolved = resolved.replace(CATEGORY_CANON, regex=True)

    idx = df.index
    src_file = df["__source_file"] if "__source_file" in df.columns else pd.Series("", index=idx)
    src_dir = df["__source_dir"] if "__source_dir" in df.columns else pd.Series("", index=idx)

    match_mask = rule_cat.notna()
    matches_df = pd.DataFrame(
        {
            "__source_dir": src_dir[match_mask].values,
            "__source_file": src_file[match_mask].values,
            "row_index": idx[match_mask].values,
            "description": df[desc_col][match_mask].values,
            "matched_category": rule_cat[match_mask].values,
            "matched_keyword": rule_kw[match_mask].values,
            "final_category": resolved[match_mask].values,
        }
    )

    miss_mask = ~match_mask
    reasons = []
    for text in desc_series[miss_mask]:
        if not text or str(text).strip() == "":
            reasons.append("empty_description")
        else:
            reasons.append("no_keyword_match")
    misses_df = pd.DataFrame(
        {
            "__source_dir": src_dir[miss_mask].values,
            "__source_file": src_file[miss_mask].values,
            "row_index": idx[miss_mask].values,
            "description": df[desc_col][miss_mask].values,
            "existing_category": (existing_clean[miss_mask].values if existing_clean is not None else [None] * miss_mask.sum()),
            "final_category": resolved[miss_mask].values,
            "reason": reasons,
        }
    )

    rule_hits = rule_cat.value_counts(dropna=True).rename("rule_hits")
    total_rows = len(df)
    coverage = (len(matches_df) / total_rows * 100.0) if total_rows else 0.0
    all_rule_categories = pd.Index(list(CATEGORY_RULES.keys()), dtype="object")
    rule_hits = rule_hits.reindex(all_rule_categories, fill_value=0)
    summary_df = rule_hits.reset_index().rename(columns={"index": "category"})
    summary_df["total_rows"] = total_rows
    summary_df["rule_coverage_pct"] = round(coverage, 2)

    return resolved, matches_df, misses_df, summary_df


__all__ = ["detect_or_build_category_with_debug"]
