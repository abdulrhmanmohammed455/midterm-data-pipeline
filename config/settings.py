import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# Project Root & Environment variables
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# تحميل المتغيرات البيئية
load_dotenv(PROJECT_ROOT / ".env")

# ============================================================
# Environment Setup for Windows (Java & Hadoop)
# ============================================================

# 1. ضبط مسار Java JDK 17
JAVA_HOME_PATH = os.getenv("JAVA_HOME_PATH", r"C:\Program Files\Java\jdk-17")
JAVA_BIN_PATH = os.path.join(JAVA_HOME_PATH, "bin")

if os.path.exists(JAVA_HOME_PATH):
    os.environ["JAVA_HOME"] = JAVA_HOME_PATH
    os.environ["PATH"] = JAVA_BIN_PATH + os.pathsep + os.environ.get("PATH", "")

# 2. ضبط مسار Hadoop و Winutils لحل خطأ ويندوز في Spark
HADOOP_HOME_PATH = os.getenv("HADOOP_HOME_PATH", r"C:\hadoop")
HADOOP_BIN_PATH = os.path.join(HADOOP_HOME_PATH, "bin")

if os.path.exists(HADOOP_HOME_PATH):
    os.environ["HADOOP_HOME"] = HADOOP_HOME_PATH
    os.environ["PATH"] = HADOOP_BIN_PATH + os.pathsep + os.environ.get("PATH", "")

# ============================================================
# Data Directories
# ============================================================
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
SCREENSHOTS_DIR = REPORTS_DIR / "screenshots"

# ============================================================
# Source Data & Router Configuration
# ============================================================
DEFAULT_INPUT_FILE = DATA_DIR / "orders_huge_mixed_quality.csv"
SMALL_SAMPLE_ROWS = 100_000
SMALL_SAMPLE_FILE = DATA_DIR / "orders_sample.csv"

SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", 200))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

# ============================================================
# MongoDB Configuration
# ============================================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_data_pipeline")

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ============================================================
# Utility Functions
# ============================================================
def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)