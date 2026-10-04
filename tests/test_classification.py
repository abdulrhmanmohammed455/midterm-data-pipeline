import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from elt_pipeline import classify_record

def test_valid_record_classification():
    doc = {
        "record_raw": {
            "order_id": "ORD-1",
            "customer_id": "CUST-1",
            "order_date": "2025-02-01",
            "price": "5000",
            "items_json": '[{"sku":"A","qty":1,"price":5000}]'
        }
    }
    res = classify_record(doc)
    assert res["quality_status"] == "valid"
    assert len(res["codes_error"]) == 0

def test_quarantine_missing_id():
    doc = {
        "record_raw": {
            "order_id": "",
            "customer_id": "CUST-1",
            "order_date": "2025-02-01",
            "price": "5000"
        }
    }
    res = classify_record(doc)
    assert res["quality_status"] == "quarantined"
    assert "ID_ORDER_MISSING" in res["codes_error"]

def test_quarantine_corrupted_json():
    doc = {
        "record_raw": {
            "order_id": "ORD-2",
            "customer_id": "CUST-2",
            "order_date": "2025-02-01",
            "price": "5000",
            "items_json": 'invalid_json_format'
        }
    }
    res = classify_record(doc)
    assert res["quality_status"] == "quarantined"
    assert "JSON_ITEMS_CORRUPTED" in res["codes_error"]