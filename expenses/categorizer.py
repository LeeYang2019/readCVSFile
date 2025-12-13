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
    """
    Build pre-compiled regex patterns for all categories (called once on module load).
    
    Creates optimized regex patterns by combining keywords with alternation (|).
    Keywords are sorted by length (longest first) to match more specific patterns
    before generic ones.
    
    Example: For Coffee category with ["STARBUCKS", "COFFEE"], creates pattern:
        STARBUCKS|COFFEE (compiled with IGNORECASE flag)
        
    This initialization happens once per session for performance.
    """
    global _COMPILED_CATEGORY_PATTERNS, _PATTERN_TO_CATEGORY
    if _COMPILED_CATEGORY_PATTERNS:  # Already initialized
        return
    
    for category, keywords in CATEGORY_RULES.items():
        # Sort keywords by length (descending) to prioritize longer, more specific matches
        # Example: "WHOLE FOODS" should match before "FOODS"
        pattern_str = "|".join(re.escape(kw) for kw in sorted(keywords, key=len, reverse=True))
        compiled = re.compile(pattern_str, re.IGNORECASE)
        _COMPILED_CATEGORY_PATTERNS[category] = (compiled, keywords)
        for kw in keywords:
            _PATTERN_TO_CATEGORY[kw] = category


def _match_category_and_keyword(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Match transaction description against category patterns.
    
    Returns the first category whose pattern matches the text (case-insensitive).
    Also returns the specific keyword that triggered the match for debugging.
    
    Example:
        _match_category_and_keyword("Starbucks Coffee")
        → ("Coffee", "STARBUCKS")
    
    Args:
        text: Transaction description to match
        
    Returns:
        Tuple of (category_name, matched_keyword), or (None, None) if no match
    """
    if not text or not str(text).strip():
        return None, None
    
    normalized = str(text).upper()
    
    # Try each category pattern in order
    # Returns on first match (category order matters)
    for category, (compiled_pattern, keywords) in _COMPILED_CATEGORY_PATTERNS.items():
        match = compiled_pattern.search(normalized)
        if match:
            matched_kw = match.group(0)
            return category, matched_kw
    
    return None, None


def detect_or_build_category_with_debug(df: pd.DataFrame, desc_col: str) -> Tuple[pd.Series, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Categorize transactions and generate detailed debug reports.
    
    Three-tier categorization strategy:
    1. Rule-based: Match description against keyword patterns
    2. Existing: Use pre-existing category column if present (fallback)
    3. Fallback: Mark as "Uncategorized" if no match found
    
    Returns detailed DataFrames for analysis:
    - matches: Successfully categorized transactions with matched keywords
    - misses: Transactions that couldn't be categorized (useful for adding new rules)
    - summary: Hit counts per category and overall coverage percentage
    
    Args:
        df: Input DataFrame with transaction descriptions
        desc_col: Column name containing transaction descriptions
        
    Returns:
        Tuple of:
        - resolved (Series): Final category for each row
        - matches_df (DataFrame): Successful category matches with matched keywords
        - misses_df (DataFrame): Uncategorized transactions with reasons
        - summary_df (DataFrame): Per-category statistics and coverage metrics
    """
    _initialize_patterns()  # Ensure patterns are compiled
    
    # Step 1: Check for pre-existing category columns (case-insensitive fallback)
    # Useful for CSVs that already have categories we want to preserve
    existing = None
    for candidate in ["category", "categories", "cat", "type"]:
        if candidate in df.columns:
            series = df[candidate]
            existing = series if existing is None else existing.combine_first(series)

    # Clean up existing categories: strip whitespace, convert empty strings to None
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

    # Step 2: Apply rule-based pattern matching (primary categorization)
    # Vectorized approach: apply matching function to all descriptions at once
    desc_series = df[desc_col].astype(str)
    matches = desc_series.apply(lambda text: _match_category_and_keyword(text))
    
    # Extract results into separate Series for categories and matched keywords
    rule_hit_cat = pd.Series([m[0] for m in matches], index=df.index, dtype="object")
    rule_hit_kw = pd.Series([m[1] for m in matches], index=df.index, dtype="object")

    # Step 3: Resolve final categories with fallback strategy
    # Priority: rule match > existing category > "Uncategorized"
    if existing_clean is not None:
        # Use rule match if found, otherwise fall back to existing category
        resolved = rule_hit_cat.where(rule_hit_cat.notna(), existing_clean)
    else:
        # No existing categories, use rule matches only
        resolved = rule_hit_cat.copy()
    # Fill remaining NaN with "Uncategorized"
    resolved = resolved.fillna("Uncategorized")
    # Apply category name remapping (e.g., "Coffee" → "Household")
    resolved = resolved.replace(CATEGORY_CANON, regex=True)

    
    # Step 4: Build debug outputs (matches and misses DataFrames)
    # These help identify patterns in uncategorized transactions
    idx = df.index
    src_file = df["__source_file"] if "__source_file" in df.columns else pd.Series("", index=idx)
    src_dir = df["__source_dir"] if "__source_dir" in df.columns else pd.Series("", index=idx)

    # DataFrame of successful matches (for validation and analysis)
    match_mask = rule_hit_cat.notna()
    matches_df = pd.DataFrame(
        {
            "__source_dir": src_dir[match_mask].values,
            "__source_file": src_file[match_mask].values,
            "row_index": idx[match_mask].values,
            "description": df[desc_col][match_mask].values,
            "matched_category": rule_hit_cat[match_mask].values,
            "matched_keyword": rule_hit_kw[match_mask].values,  # Specific keyword that triggered match
            "final_category": resolved[match_mask].values,  # After category remapping
        }
    )

    # DataFrame of misses (for identifying new rules to add)
    miss_mask = ~match_mask
    reasons = []
    for text in desc_series[miss_mask]:
        # Classify why the transaction wasn't categorized
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
            "final_category": resolved[miss_mask].values,  # What it was categorized as (default: Uncategorized)
            "reason": reasons,  # Why it was missed
        }
    )

    # Step 5: Build summary statistics (useful for monitoring categorization quality)
    # Track which rules matched and overall coverage percentage
    rule_hits = rule_hit_cat.value_counts(dropna=True).rename("rule_hits")
    total_rows = len(df)
    coverage = (len(matches_df) / total_rows * 100.0) if total_rows else 0.0
    # Ensure all categories appear in summary (even with 0 hits)
    all_rule_categories = pd.Index(list(CATEGORY_RULES.keys()), dtype="object")
    rule_hits = rule_hits.reindex(all_rule_categories, fill_value=0)
    summary_df = rule_hits.reset_index().rename(columns={"index": "category"})
    summary_df["total_rows"] = total_rows
    summary_df["rule_coverage_pct"] = round(coverage, 2)

    return resolved, matches_df, misses_df, summary_df


__all__ = ["detect_or_build_category_with_debug"]
