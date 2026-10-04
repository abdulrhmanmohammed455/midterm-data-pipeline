import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from quality_rules import (
    normalize_arabic_digits,
    normalize_currency,
    normalize_thousands_separator,
    normalize_phone,
    normalize_email,
    normalize_date,
    normalize_status,
    clean_record
)

def test_arabic_digits():
    assert normalize_arabic_digits("٥٠٠٠") == "5000"
    assert normalize_arabic_digits("١٢٣٤٥٦") == "123456"

def test_currency_normalization():
    assert normalize_currency("5000 ريال") == "5000"
    assert normalize_currency("2500 YER") == "2500"
    assert normalize_currency("1000 ريال يمني") == "1000"

def test_thousands_separator():
    assert normalize_thousands_separator("125,000.00") == "125000.00"

def test_phone_normalization():
    assert normalize_phone("+967 77 123 4567") == "+967771234567"

def test_email_normalization():
    assert normalize_email("user@@mail..com") == "user@mail.com"

def test_date_normalization():
    assert normalize_date("2025/01/31") == "2025-01-31"
    assert normalize_date("31-01-2025") == "2025-01-31"

def test_status_normalization():
    assert normalize_status("مدفوع") == "paid"
    assert normalize_status("معلق") == "pending"

def test_audit_trail_creation():
    raw = {
        "order_id": "1001",
        "price": "٥٠٠٠ ريال",
        "customer_email": "test@@mail..com"
    }
    cleaned = clean_record(raw)
    assert cleaned["quality_status"] == "corrected"
    assert len(cleaned["corrections"]) >= 2