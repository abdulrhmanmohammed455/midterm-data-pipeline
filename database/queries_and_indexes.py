import sys
from pathlib import Path
from pymongo import MongoClient, ASCENDING, DESCENDING

# إضافة المسار الثابت للمشروع للوصول إلى ملف الإعدادات
PROJECT_ROOT = Path(r"D:\The third level_Ai\Second semester\Level_2\Big Data Eng.Omar\Final-data-pipeline-main")
sys.path.append(str(PROJECT_ROOT))

from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION

def get_collection():
    """الاتصال بقاعدة البيانات وإرجاع مجموعة البيانات المعتمدة"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    return db[VALIDATED_COLLECTION]

# ==========================================
# 1. الاستعلامات الخمسة (5 Queries)
# ==========================================
def get_completed_orders(collection):
    return list(collection.find({"status": "Completed"}).limit(10))

def get_high_value_orders(collection, min_amount=1000):
    return list(collection.find({"total_amount": {"$gt": min_amount}}).limit(10))

def get_customer_specific_orders(collection, customer_id, status):
    return list(collection.find({"customer_id": customer_id, "status": status}).limit(10))

def get_orders_by_date_range(collection, start_date, end_date):
    return list(collection.find({"order_date": {"$gte": start_date, "$lte": end_date}}).limit(10))

def get_orders_by_city(collection, city):
    return list(collection.find({"shipping_city": city}).limit(10))

# ==========================================
# 2. إنشاء الفهارس وتحليل الأداء (Indexes & Explain)
# ==========================================
def analyze_and_create_index(collection, query_name, query_filter, index_definition, index_name):
    print(f"\n--- تحليل الاستعلام: {query_name} ---")
    
    # قياس الأداء قبل الفهرس
    explain_before = collection.find(query_filter).explain()["executionStats"]
    docs_before = explain_before["totalDocsExamined"]
    time_before = explain_before["executionTimeMillis"]
    print(f"[قبل الفهرسة] المستندات المفحوصة: {docs_before} | الوقت: {time_before} ms")
    
    # إنشاء الفهرس
    collection.create_index(index_definition, name=index_name)
    print(f"تم إنشاء الفهرس: {index_name}")
    
    # قياس الأداء بعد الفهرس
    explain_after = collection.find(query_filter).explain()["executionStats"]
    docs_after = explain_after["totalDocsExamined"]
    time_after = explain_after["executionTimeMillis"]
    print(f"[بعد الفهرسة] المستندات المفحوصة: {docs_after} | الوقت: {time_after} ms")
    print("-" * 50)

def run_indexes_and_explain():
    collection = get_collection()
    
    analyze_and_create_index(
        collection, "البحث عن الطلبات المكتملة", {"status": "Completed"},
        [("status", ASCENDING)], "idx_status"
    )
    
    analyze_and_create_index(
        collection, "البحث في نطاق زمني", 
        {"order_date": {"$gte": "2023-01-01", "$lte": "2023-12-31"}},
        [("order_date", DESCENDING)], "idx_order_date"
    )
    
    analyze_and_create_index(
        collection, "البحث عن طلبات عميل محدد", 
        {"customer_id": "CUST-1001", "status": "Pending"},
        [("customer_id", ASCENDING), ("status", ASCENDING)], "idx_customer_status"
    )

if __name__ == "__main__":
    print("بدء تنفيذ الاستعلامات وإنشاء الفهارس...")
    run_indexes_and_explain()
