import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging
import time

# logging setup
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

def log_analytics_etl_event(engine, status, message, rows_affected=None, runtime_seconds=None):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO analytics.logs(status, message, rows_affected, runtime_seconds)
                    VALUES (:status, :message, :rows, :runtime)
                """),
                {
                    "status": status,
                    "message": message,
                    "rows": rows_affected,
                    "runtime": runtime_seconds
                }
            )
    except Exception as e:
        logger.error(f"Failed to log analytics ETL event: {e}")

def create_engine_from_env():
    load_dotenv(override=False)
    engine = create_engine(os.environ.get("database_url"), connect_args={"sslmode": "require"})
    return engine

def execute_sql(filepath, engine,):
    try:
        with engine.begin() as conn:
            with open(filepath) as file:
                query = text(file.read())
                conn.execute(query)

    except Exception as e:
        print(f"[ERROR] {e}")
        return 0

if __name__ == "__main__":
    start_time = time.time()
    load_dotenv(override=False)

    try:
        engine = create_engine_from_env()
        log_analytics_etl_event(engine, "START", "Analytics ETL started")

        filepath = "analytics/sql/fact_played_track_details.sql"
        execute_sql(filepath, engine)

        runtime = round(time.time() - start_time, 2)
        log_analytics_etl_event(engine, "SUCCESS", "Analytics ETL completed successfully", runtime_seconds=runtime)

        logger.info("Analytics ETL completed successfully.")

    except Exception as e:
        runtime = round(time.time() - start_time, 2)

        if "engine" in locals():
            log_analytics_etl_event(engine, "FAILURE", str(e), runtime_seconds=runtime)

        logger.error(f"Analytics ETL failed: {e}")
        raise