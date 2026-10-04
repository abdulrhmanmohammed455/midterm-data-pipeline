import sys
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient

PROJECT_ROOT = Path(r"D:\The third level_Ai\Second semester\Level_2\Big Data Eng.Omar\Final-data-pipeline-main")
sys.path.append(str(PROJECT_ROOT))

from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION

def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]

def get_last_refresh_time(db, view_name):
    tracker = db["mv_refresh_tracker"].find_one({"view_name": view_name})
    if tracker and "last_refresh" in tracker:
        return tracker["last_refresh"]
    return None

def update_last_refresh_time(db, view_name, refresh_time):
    db["mv_refresh_tracker"].update_one(
        {"view_name": view_name},
        {"$set": {"last_refresh": refresh_time}},
        upsert=True
    )

def refresh_daily_sales_summary(db):
    view_name = "daily_sales_summary"
    last_refresh = get_last_refresh_time(db, view_name)
    current_time = datetime.now().isoformat()
    pipeline = []
    
    if last_refresh:
        pipeline.append({"$match": {"created_at": {"$gt": last_refresh}}})

    pipeline.extend([
        {"$group": {"_id": "$order_date", "total_sales": {"$sum": "$total_amount"}, "total_orders": {"$sum": 1}}},
        {"$merge": {
            "into": view_name,
            "on": "_id",
            "whenMatched": [
                {"$set": {
                    "total_sales": {"$add": ["$total_sales", "$$new.total_sales"]},
                    "total_orders": {"$add": ["$total_orders", "$$new.total_orders"]}
                }}
            ],
            "whenNotMatched": "insert"
        }}
    ])
    db[VALIDATED_COLLECTION].aggregate(pipeline)
    update_last_refresh_time(db, view_name, current_time)
    return f"تم تحديث العرض المادي '{view_name}' بنجاح."

def refresh_top_products_summary(db):
    view_name = "top_products_summary"
    last_refresh = get_last_refresh_time(db, view_name)
    current_time = datetime.now().isoformat()
    pipeline = []
    
    if last_refresh:
        pipeline.append({"$match": {"created_at": {"$gt": last_refresh}}})

    pipeline.extend([
        {"$unwind": "$items_json"},
        {"$group": {
            "_id": "$items_json.product_id", 
            "total_revenue": {"$sum": "$total_amount"}, 
            "times_ordered": {"$sum": 1}
        }},
        {"$merge": {
            "into": view_name,
            "on": "_id",
            "whenMatched": [
                {"$set": {
                    "total_revenue": {"$add": ["$total_revenue", "$$new.total_revenue"]},
                    "times_ordered": {"$add": ["$times_ordered", "$$new.times_ordered"]}
                }}
            ],
            "whenNotMatched": "insert"
        }}
    ])
    db[VALIDATED_COLLECTION].aggregate(pipeline)
    update_last_refresh_time(db, view_name, current_time)
    return f"تم تحديث العرض المادي '{view_name}' بنجاح."

if __name__ == "__main__":
    db = get_db()
    print(refresh_daily_sales_summary(db))
    print(refresh_top_products_summary(db))