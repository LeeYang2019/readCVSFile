"""Category detection utilities with debug outputs."""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

import pandas as pd

from .categories import CATEGORY_RULES, CATEGORY_CANON


# Pre-compile category patterns for efficient matching
_COMPILED_CATEGORY_PATTERNS = {}
_PATTERN_TO_CATEGORY = {}

def _initialize_patterns():
    """Build pre-compiled regex patterns for all categories (called once on module load)."""
    global _COMPILED_CATEGORY_PATTERNS, _PATTERN_TO_CATEGORY
    if _COMPILED_CATEGORY_PATTERNS:  # Already initialized
        return
    
    for category, keywords in CATEGORY_RULES.items():
        # Create a single regex pattern with alternation: keyword1|keyword2|...
        pattern_str = "|".join(re.escape(kw) for kw in sorted(keywords, key=len, reverse=True))
        compiled = re.compile(pattern_str, re.IGNORECASE)
        _COMPILED_CATEGORY_PATTERNS[category] = (compiled, keywords)
        for kw in keywords:
            _PATTERN_TO_CATEGORY[kw] = category


def _match_category_and_keyword(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Match text against all category patterns and return (category, matched_keyword)."""
    if not text or not str(text).strip():
        return None, None
    
    normalized = str(text).upper()
    
    # Try each category pattern in order
    for category, (compiled_pattern, keywords) in _COMPILED_CATEGORY_PATTERNS.items():
        match = compiled_pattern.search(normalized)
        if match:
            matched_kw = match.group(0)
            return category, matched_kw
    
    return None, None


def detect_or_build_category_with_debug(df: pd.DataFrame, desc_col: str) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns a tuple of:
      - Series of resolved categories
      - DataFrame of rule matches
      - DataFrame of rule misses
      - DataFrame summarizing rule hit counts/coverage
    """
    _initialize_patterns()  # Ensure patterns are compiled
    
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

    # Vectorized matching using apply instead of for loop
    desc_series = df[desc_col].astype(str)
    matches = desc_series.apply(lambda text: _match_category_and_keyword(text))
    
    rule_hit_cat = pd.Series([m[0] for m in matches], index=df.index, dtype="object")
    rule_hit_kw = pd.Series([m[1] for m in matches], index=df.index, dtype="object")

    if existing_clean is not None:
        resolved = rule_hit_cat.where(rule_hit_cat.notna(), existing_clean)
    else:
        resolved = rule_hit_cat.copy()
    resolved = resolved.fillna("Uncategorized")
    resolved = resolved.replace(CATEGORY_CANON, regex=True)

    idx = df.index
    src_file = df["__source_file"] if "__source_file" in df.columns else pd.Series("", index=idx)
    src_dir = df["__source_dir"] if "__source_dir" in df.columns else pd.Series("", index=idx)

    match_mask = rule_hit_cat.notna()
    matches_df = pd.DataFrame(
        {
            "__source_dir": src_dir[match_mask].values,
            "__source_file": src_file[match_mask].values,
            "row_index": idx[match_mask].values,
            "description": df[desc_col][match_mask].values,
            "matched_category": rule_hit_cat[match_mask].values,
            "matched_keyword": rule_hit_kw[match_mask].values,
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

    rule_hits = rule_hit_cat.value_counts(dropna=True).rename("rule_hits")
    total_rows = len(df)
    coverage = (len(matches_df) / total_rows * 100.0) if total_rows else 0.0
    all_rule_categories = pd.Index(list(CATEGORY_RULES.keys()), dtype="object")
    rule_hits = rule_hits.reindex(all_rule_categories, fill_value=0)
    summary_df = rule_hits.reset_index().rename(columns={"index": "category"})
    summary_df["total_rows"] = total_rows
    summary_df["rule_coverage_pct"] = round(coverage, 2)

    return resolved, matches_df, misses_df, summary_df


__all__ = ["detect_or_build_category_with_debug"]
