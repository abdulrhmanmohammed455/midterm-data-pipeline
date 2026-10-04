import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from pymongo import MongoClient

# ============================================================
# Project Root & Path Setup
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    MONGO_URI,
    MONGO_DATABASE,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION,
    REPORTS_DIR,
)

RESULTS_FILE = REPORTS_DIR / "results.json"


# ============================================================
# MongoDB Connection
# ============================================================

def get_mongo_client():
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )
    client.admin.command("ping")
    return client


# ============================================================
# Generate Metrics from Database
# ============================================================

def generate_metrics(id_run: str) -> Dict[str, Any]:
    """
    Query the database to generate comprehensive metrics 
    for a specific pipeline run.
    """
    client = get_mongo_client()
    
    try:
        db = client[MONGO_DATABASE]
        
        # ----------------------------------------------------
        # Calculate Counters
        # ----------------------------------------------------
        
        raw_count = db[RAW_COLLECTION].count_documents(
            {"id_run": id_run}
        )
        
        valid_count = db[VALIDATED_COLLECTION].count_documents(
            {"id_run": id_run, "quality_status": "valid"}
        )
        
        corrected_count = db[VALIDATED_COLLECTION].count_documents(
            {"id_run": id_run, "quality_status": "corrected"}
        )
        
        quarantine_count = db[QUARANTINE_COLLECTION].count_documents(
            {"id_run": id_run}
        )
        
        # ----------------------------------------------------
        # Aggregate Top Errors from Quarantine
        # ----------------------------------------------------
        
        errors_pipeline = [
            {"$match": {"id_run": id_run}},
            {"$unwind": "$codes_error"},
            {"$group": {"_id": "$codes_error", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        errors_agg = list(db[QUARANTINE_COLLECTION].aggregate(errors_pipeline))
        errors_dict = {doc["_id"]: doc["count"] for doc in errors_agg}

        # ----------------------------------------------------
        # Build Metrics Dictionary
        # ----------------------------------------------------
        
        total_processed = valid_count + corrected_count + quarantine_count
        
        metrics = {
            "id_run": id_run,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "counters": {
                "raw_records": raw_count,
                "processed_records": total_processed,
                "valid_records": valid_count,
                "corrected_records": corrected_count,
                "quarantine_records": quarantine_count
            },
            "quality_percentages": {
                "valid_%": round((valid_count / raw_count * 100) if raw_count else 0, 2),
                "corrected_%": round((corrected_count / raw_count * 100) if raw_count else 0, 2),
                "quarantine_%": round((quarantine_count / raw_count * 100) if raw_count else 0, 2)
            },
            "top_errors": errors_dict
        }
        
        return metrics
        
    finally:
        client.close()


# ============================================================
# Save Metrics to JSON
# ============================================================

def save_metrics(metrics: Dict[str, Any], filepath: Path = RESULTS_FILE):
    """
    Save or append the metrics dictionary to the JSON file.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    data = []
    
    if filepath.exists():
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                data = json.load(file)
                if not isinstance(data, list):
                    data = [data]
        except Exception:
            data = []
            
    data.append(metrics)
    
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(
            data, 
            file, 
            indent=4, 
            ensure_ascii=False
        )
        
    print(f"\n[+] Metrics successfully saved to: {filepath}")


# ============================================================
# Command Line Interface
# ============================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Generate and save metrics for a specific run ID."
    )
    
    parser.add_argument(
        "--id-run", 
        required=True, 
        help="Pipeline Run ID to generate metrics for."
    )
    
    args = parser.parse_args()
    
    try:
        metrics = generate_metrics(args.id_run)
        
        print()
        print("=" * 65)
        print("PIPELINE METRICS GENERATED")
        print("=" * 65)
        
        print(json.dumps(metrics, indent=4, ensure_ascii=False))
        
        print("=" * 65)
        
        save_metrics(metrics)
        
    except Exception as error:
        print()
        print("=" * 65)
        print("ERROR")
        print("=" * 65)
        print(error)
        print("=" * 65)
        sys.exit(1)


if __name__ == "__main__":
    main()