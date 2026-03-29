"""Shared utility functions for Lead Scraper."""

import re

MOBILE_PREFIXES = (
    "010", "011", "012", "013", "014", "015", "016", "017", "018", "019",
)
PHONE_PATTERN = re.compile(r"(\+?6?0\d[\d\s\-]{7,12})")

# Tighter pattern for website extraction (min 9 digits when cleaned)
MY_PHONE_PATTERN = re.compile(r"(\+?6?0[1-9][\d\s\-\.]{7,13})")

# Context words near phone numbers (increases confidence)
PHONE_CONTEXT_WORDS = re.compile(
    r"(phone|tel|call|contact|mobile|fax|whatsapp|hotline|office|"
    r"hubungi|telefon|talian)",
    re.IGNORECASE,
)

_SAFE_COL = re.compile(r"^[a-z_]+$")


def classify_phone(phone: str) -> str:
    """Classify phone number as mobile or landline."""
    cleaned = re.sub(r"[\s\-\+\.]", "", phone)
    if cleaned.startswith("60"):
        cleaned = "0" + cleaned[2:]
    for prefix in MOBILE_PREFIXES:
        if cleaned.startswith(prefix):
            return "mobile"
    return "landline" if cleaned else ""


def clean_phone(phone: str) -> str:
    """Strip non-digit characters from phone (keep +)."""
    return re.sub(r"[^\d\+]", "", phone)


def normalize_my_phone(phone: str) -> str:
    """Normalize to Malaysian local format: 0XX..."""
    cleaned = clean_phone(phone)
    # Fix +0XX (wrong format seen on some sites)
    if cleaned.startswith("+0"):
        cleaned = cleaned[1:]
    elif cleaned.startswith("+60"):
        cleaned = "0" + cleaned[3:]
    elif cleaned.startswith("60") and len(cleaned) > 9:
        cleaned = "0" + cleaned[2:]
    return cleaned


def is_valid_my_phone(phone: str) -> bool:
    """Check if a cleaned phone looks like a real Malaysian number."""
    digits = re.sub(r"[^\d]", "", phone)
    if digits.startswith("60"):
        digits = "0" + digits[2:]
    if not digits.startswith("0"):
        return False
    if len(digits) < 9 or len(digits) > 12:
        return False
    return True


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
