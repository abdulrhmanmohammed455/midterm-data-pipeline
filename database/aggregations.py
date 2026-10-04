import sys
from pathlib import Path
from pymongo import MongoClient

PROJECT_ROOT = Path(r"D:\The third level_Ai\Second semester\Level_2\Big Data Eng.Omar\Final-data-pipeline-main")
sys.path.append(str(PROJECT_ROOT))

from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION

def get_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    return db[VALIDATED_COLLECTION]

def report_sales_by_city(collection):
    pipeline = [
        {"$group": {"_id": "$city", "total_sales": {"$sum": "$total_amount"}, "order_count": {"$sum": 1}}},
        {"$sort": {"total_sales": -1}}
    ]
    return list(collection.aggregate(pipeline))

def report_top_customers(collection):
    pipeline = [
        {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$total_amount"}, "orders_count": {"$sum": 1}}},
        {"$sort": {"total_spent": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def report_order_status_distribution(collection):
    pipeline = [
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return list(collection.aggregate(pipeline))

def report_top_products(collection):
    pipeline = [
        {"$group": {"_id": "$product_id", "total_revenue": {"$sum": "$total_amount"}, "times_ordered": {"$sum": 1}}},
        {"$sort": {"total_revenue": -1}},
        {"$limit": 10}
    ]
    return list(collection.aggregate(pipeline))

def report_daily_sales(collection):
    pipeline = [
        {"$group": {"_id": "$order_date", "daily_sales": {"$sum": "$total_amount"}, "daily_orders": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
        {"$limit": 30}
    ]
    return list(collection.aggregate(pipeline))

def run_all_reports():
    collection = get_collection()
    print("\n--- تقرير المبيعات حسب المدينة ---")
    print(report_sales_by_city(collection)[:3])
    print("\n--- تقرير أفضل المنتجات ---")
    print(report_top_products(collection)[:3])

if __name__ == "__main__":
    run_all_reports()