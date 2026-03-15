"""Shared utility functions for Lead Scraper."""

import re

MOBILE_PREFIXES = (
    "010", "011", "012", "013", "014", "015", "016", "017", "018", "019",
)
PHONE_PATTERN = re.compile(r"(\+?6?0\d[\d\s\-]{7,12})")

_SAFE_COL = re.compile(r"^[a-z_]+$")


def classify_phone(phone: str) -> str:
    """Classify phone number as mobile or landline."""
    cleaned = re.sub(r"[\s\-\+]", "", phone)
    if cleaned.startswith("60"):
        cleaned = "0" + cleaned[2:]
    for prefix in MOBILE_PREFIXES:
        if cleaned.startswith(prefix):
            return "mobile"
    return "landline" if cleaned else ""


def clean_phone(phone: str) -> str:
    """Strip non-digit characters from phone (keep +)."""
    return re.sub(r"[^\d\+]", "", phone)


def normalize_name(name: str) -> str:
    """Normalize company name for deduplication."""
    name = name.lower().strip()
    for suffix in [
        "sdn. bhd.", "sdn bhd.", "sdn bhd",
        "bhd.", "bhd",
        "plt.", "plt",
        "llp.", "llp",
        "(m)", "(malaysia)",
    ]:
        name = name.replace(suffix, "")
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def is_safe_column_name(name: str) -> bool:
    """Check that a column name is safe for SQL interpolation."""
    return bool(_SAFE_COL.match(name))
