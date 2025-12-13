"""Category rules and normalization helpers."""

from __future__ import annotations

# Default filename to look for in ~/Downloads when no paths provided
# Users can override via --default-filename CLI argument
DEFAULT_DOWNLOAD_FILENAME = "japan_trip.csv"

# Category detection rules: keywords to match in transaction descriptions
# Matches are case-insensitive and use substring matching (any keyword triggers the category)
# Keywords are organized by intended expense category for grouping
CATEGORY_RULES = {
    "Car Care": ["DON MILLER", "MERMAID CAR WASH", "CAR WASH"],
    "Coffee": ["STARBUCKS", "DUNKIN", "PEETS", "COFFEE", "BARRIQUES", "KAFE"],
    "Cash & ATM": ["WITHDRAWAL", "ATM"],
    "Dining": ["MCDONALD", "CHIPOTLE", "SUBWAY", "GRUBHUB", "UBER EATS", "DOORDASH", "HMONG", "FOODS"],
    "Entertainment": ["NETFLIX", "HULU", "SPOTIFY", "DISNEY", "YOUTUBE", "PRIME"],
    "Gas/Automotive": ["ESSO", "SPEEDWAY", "KWIK", "SHELL", "CHEVRON", "BP", "MOBIL", "TEXACO", "SUNOCO", "CITGO", "OIL CHANGE", "CAR WASH", "REPAIR", "MAINTENANCE", "TIRE", "AUTO PARTS", "JIFFY LUBE", "VALVOLINE"],
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
    "Humanitarian": ["GOFUNDME", "DOCTORS W/O BORDER", "ACLU", "DOCTORSWITHOUTBORDERS"],
    "Lodging": ["LODGING", "HOTEL", "RESORT"],
    "Mobile Pay": ["VENMO", "PAYPAL", "eBAY", "ZELLE"],
    "Other_Travel": ["OTHER TRAVEL", "TRAVEL MISC"],
    "Recreation": ["SMOKE", "MARIJUANA"],
    "Rent": ["SHILTS"],
    "Shopping": ["AMAZON", "TARGET", "WALMART", "BEST BUY"],
    "Thrifting": ["St. Vincent De Paul", "SVDP", "GOODWILL", "SUPERTHRIFT"],
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
    "Airfare": ["AIRLINE", "DELTA", "UNITED", "AA ", "AMERICAN AIRLINES", "SOUTHWEST", "TRAIN", "AMTRAK", "BUS", "AMERICAN AIR"],
}

# Category name remapping: consolidates detailed categories into broader groups
# Regex patterns (case-insensitive) → remapped category name
# Example: Detailed "Coffee" category gets remapped to "Household" for high-level grouping
CATEGORY_CANON = {
    r"(?i)^\s*lodging\s*$": "Travel",
    r"(?i)^\s*airfare\s*$": "Travel",
    r"(?i)^\s*other[_\s]*travel\s*$": "Travel",
    r"(?i)^\s*Gas[_\s]*/[_\s]*Automotive\s*$": "Travel",
    r"(?i)^\s*coffee\s*$": "Household",
    r"(?i)^\s*dining[_\s]*out\s*$": "Household",
    r"(?i)^\s*dining\s*$": "Household",
    r"(?i)^\s*groceries\s*$": "Household",
    r"(?i)^\s*Food & Dining\s*$": "Household",
    r"(?i)^\s*shopping\s*$": "Household",
    r"(?i)^\s*pharmacy\s*$": "Household",
    r"(?i)^\s*merchandise\s*$": "Household",
}

__all__ = ["CATEGORY_RULES", "CATEGORY_CANON", "DEFAULT_DOWNLOAD_FILENAME"]
