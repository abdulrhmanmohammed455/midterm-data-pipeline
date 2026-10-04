import sys
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo import MongoClient

# إضافة مسار المشروع الجذري
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

# الاستيراد من المسار الصحيح لإعداداتك
from config.settings import MONGO_URI, MONGO_DATABASE, VALIDATED_COLLECTION
from database.materialized_views import refresh_daily_sales_summary, refresh_top_products_summary
from database.aggregations import report_daily_sales

def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DATABASE]

# ==========================================
# نظام تسجيل المهام (Logging System)
# ==========================================
def log_job_execution(db, job_name, start_time, end_time, status, message):
    log_entry = {
        "job_name": job_name,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": round((end_time - start_time).total_seconds(), 2),
        "status": status,
        "message": message
    }
    db["scheduled_jobs_logs"].insert_one(log_entry)
    print(f"[{status}] المهمة '{job_name}' استغرقت {log_entry['duration_seconds']} ثانية.")

def run_job_with_logging(job_name, job_func):
    db = get_db()
    start_time = datetime.now()
    print(f"\n--- بدء تنفيذ المهمة: {job_name} في {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        result_message = job_func(db)
        end_time = datetime.now()
        log_job_execution(db, job_name, start_time, end_time, "Success", str(result_message))
        return {"status": "Success", "message": result_message}
    except Exception as e:
        end_time = datetime.now()
        error_msg = str(e)
        print(f"حدث خطأ: {error_msg}")
        log_job_execution(db, job_name, start_time, end_time, "Failed", error_msg)
        return {"status": "Failed", "message": error_msg}

# ==========================================
# تعريف المهام
# ==========================================
def task_refresh_mvs(db):
    msg1 = refresh_daily_sales_summary(db)
    msg2 = refresh_top_products_summary(db)
    return f"{msg1} | {msg2}"

def task_generate_daily_report(db):
    collection = db[VALIDATED_COLLECTION]
    report_data = report_daily_sales(collection)
    return f"تم توليد التقرير بنجاح. يحتوي على {len(report_data)} سجلات."

JOBS_MAP = {
    "refresh_mvs": task_refresh_mvs,
    "daily_report": task_generate_daily_report
}

def run_job_manually(job_name):
    if job_name in JOBS_MAP:
        return run_job_with_logging(job_name, JOBS_MAP[job_name])
    return {"status": "Failed", "message": "المهمة غير موجودة."}

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        lambda: run_job_with_logging("refresh_mvs", task_refresh_mvs),
        trigger='interval', hours=1, id="job_refresh_mvs", replace_existing=True
    )
    scheduler.add_job(
        lambda: run_job_with_logging("daily_report", task_generate_daily_report),
        trigger='interval', hours=24, id="job_daily_report", replace_existing=True
    )
    scheduler.start()
    print("تم تشغيل المجدول بنجاح. المهام تعمل الآن في الخلفية...")
    return scheduler