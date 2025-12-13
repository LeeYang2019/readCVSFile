"""Category rules and normalization helpers."""

from __future__ import annotations

# Default filename to look for in ~/Downloads when no paths provided
# Users can override via --default-filename CLI argument
DEFAULT_DOWNLOAD_FILENAME = "japan_trip.csv"

# Category detection rules: keywords to match in transaction descriptions
# Matches are case-insensitive and use substring matching (any keyword triggers the category)
# Keywords are organized by intended expense category for grouping
CATEGORY_RULES = {
    "Coffee": ["STARBUCKS", "DUNKIN", "PEETS", "COFFEE", "BARRIQUES", "KAFE"],
    "Groceries": [
        "SAFEWAY",
        "KROGER",
        "WHOLE FOODS",
        "TRADER JOE",
        "GROCERY",
        "WOODMAN",
        "METRO",
        "SAMSCLUB",
        "SAMS CLUB",
    ],
    "Dining": ["MCDONALD", "CHIPOTLE", "SUBWAY", "GRUBHUB", "UBER EATS", "DOORDASH", "HMONG", "FOODS"],
    "Transit": [
        "UBER",
        "LYFT",
        "METRO",
        "TRANSIT",
        "PARKING",
        "TOLLS",
        "PRESTO",
        "TOLLWAY",
        "PAYGO",
        "BADGER COACHES",
    ],
    "Entertainment": ["NETFLIX", "HULU", "SPOTIFY", "DISNEY", "YOUTUBE", "PRIME"],
    "Shopping": ["AMAZON", "TARGET", "WALMART", "BEST BUY"],
    "Humanitarian": ["GOFUNDME", "DOCTORS W/O BORDER", "ACLU", "DOCTORSWITHOUTBORDERS"],
    "Thrifting": ["St. Vincent De Paul", "SVDP", "GOODWILL", "SUPERTHRIFT"],
    "Car Care": ["DON MILLER"],
    "Recreation": ["SMOKE", "MARIJUANA"],
    "Lodging": ["LODGING", "HOTEL", "RESORT"],
    "Travel": ["AIRLINE", "DELTA", "UNITED", "AA ", "AMERICAN AIRLINES", "SOUTHWEST", "TRAIN", "AMTRAK", "BUS"],
    "Other_Travel": ["OTHER TRAVEL", "TRAVEL MISC"],
    "Rent": ["SHILTS"],
    "Mobile Pay": ["VENMO", "PAYPAL", "eBAY", "ZELLE"],
    "Cash & ATM": ["WITHDRAWAL", "ATM"],
    "Gas / Automotive": ["ESSO", "SPEEDWAY", "KWIK", "SHELL", "CHEVRON", "BP", "MOBIL", "TEXACO", "SUNOCO", "CITGO", "OIL CHANGE", "CAR WASH", "REPAIR", "MAINTENANCE", "TIRE", "AUTO PARTS", "JIFFY LUBE", "VALVOLINE"],
}

# Category name remapping: consolidates detailed categories into broader groups
# Regex patterns (case-insensitive) → remapped category name
# Example: Detailed "Coffee" category gets remapped to "Household" for high-level grouping
CATEGORY_CANON = {
    r"(?i)^\s*lodging\s*$": "Travel",
    r"(?i)^\s*other[_\s]*travel\s*$": "Travel",
    r"(?i)^\s*Gas[_\s]*/[_\s]*Automotive\s*$": "Travel",
    r"(?i)^\s*coffee\s*$": "Household",
    r"(?i)^\s*dining[_\s]*out\s*$": "Household",
    r"(?i)^\s*dining\s*$": "Household",
    r"(?i)^\s*groceries\s*$": "Household",
    r"(?i)^\s*Food & Dining\s*$": "Household",
    r"(?i)^\s*shopping\s*$": "Household",
    r"(?i)^\s*pharmacy\s*$": "Household",
}

__all__ = ["CATEGORY_RULES", "CATEGORY_CANON", "DEFAULT_DOWNLOAD_FILENAME"]
