import sys
import subprocess
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pymongo import MongoClient

# إضافة المسار الثابت للمشروع
PROJECT_ROOT = Path(r"D:\The third level_Ai\Second semester\Level_2\Big Data Eng.Omar\Final-data-pipeline-main")
sys.path.append(str(PROJECT_ROOT))

from database.queries_and_indexes import get_collection, run_indexes_and_explain, get_completed_orders, get_high_value_orders, get_orders_by_city
from database.aggregations import report_sales_by_city, report_top_customers, report_order_status_distribution, report_top_products, report_daily_sales
from database.materialized_views import get_db, refresh_daily_sales_summary, refresh_top_products_summary
from scheduler_manager import start_scheduler, run_job_manually

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = start_scheduler()
    yield
    scheduler.shutdown()

app = FastAPI(title="Midterm Data Pipeline API", version="1.0.0", lifespan=lifespan)

QUERIES_MAP = {
    "completed_orders": lambda col: get_completed_orders(col),
    "high_value_orders": lambda col: get_high_value_orders(col, 1000),
    "orders_by_city_sanaa": lambda col: get_orders_by_city(col, "Sanaa"),
}

AGGREGATIONS_MAP = {
    "sales_by_city": report_sales_by_city,
    "top_customers": report_top_customers,
    "status_distribution": report_order_status_distribution,
    "top_products": report_top_products,
    "daily_sales": report_daily_sales
}

@app.get("/health")
def health_check():
    return {"status": "healthy", "message": "API is running."}

@app.post("/ingest")
def trigger_ingest(background_tasks: BackgroundTasks):
    def run_pipeline():
        main_script = PROJECT_ROOT / "main.py"  # افترضنا أن اسم ملف التشغيل القديم main.py
        if main_script.exists():
            subprocess.run(["python", str(main_script)], check=True)
    background_tasks.add_task(run_pipeline)
    return {"status": "Processing Started"}

@app.post("/indexes")
def create_indexes():
    try:
        run_indexes_and_explain()
        return {"status": "Success", "message": "تم إنشاء الفهارس وتحليلها."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/queries")
def list_queries():
    return {"available_queries": list(QUERIES_MAP.keys())}

@app.get("/queries/{name}")
def execute_query(name: str):
    if name not in QUERIES_MAP: raise HTTPException(status_code=404, detail="غير موجود")
    results = QUERIES_MAP[name](get_collection())
    for res in results: res["_id"] = str(res["_id"])
    return {"data": results}

@app.get("/aggregations")
def list_aggregations():
    return {"available_aggregations": list(AGGREGATIONS_MAP.keys())}

@app.get("/aggregations/{name}")
def execute_aggregation(name: str):
    if name not in AGGREGATIONS_MAP: raise HTTPException(status_code=404, detail="غير موجود")
    results = AGGREGATIONS_MAP[name](get_collection())
    return {"data": results}

@app.post("/refresh-mv")
def refresh_mvs():
    db = get_db()
    return {"messages": [refresh_daily_sales_summary(db), refresh_top_products_summary(db)]}

@app.get("/jobs")
def get_jobs_logs():
    logs = list(get_db()["scheduled_jobs_logs"].find().sort("start_time", -1).limit(20))
    for log in logs: log["_id"] = str(log["_id"])
    return {"recent_jobs": logs}

@app.post("/jobs/{name}/run")
def run_job(name: str):
    result = run_job_manually(name)
    if result["status"] == "Failed": raise HTTPException(status_code=404, detail=result["message"])
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
