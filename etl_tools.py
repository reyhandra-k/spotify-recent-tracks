import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import traceback

def log_etl_event(engine, table_name, status, row_count=0, message=None):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_logs (table_name, status, row_count, message, created_at)
                    VALUES (:table_name, :status, :row_count, :message, NOW())
                """),
                {
                    "table_name": table_name,
                    "status": status,
                    "row_count": row_count,
                    "message": message,
                },
            )
    except Exception:
        print("[ERROR] Failed to write log entry:", traceback.format_exc())


def create_engine_from_env():
    load_dotenv(override=False)
    engine = create_engine(os.environ.get("database_url"), connect_args={"sslmode": "require"})
    return engine


def reflect_tables(engine, table_names):
    metadata = MetaData()
    metadata.reflect(bind=engine, only=table_names)
    return {name: Table(name, metadata, autoload_with=engine) for name in table_names}


def bulk_upsert_dataframe(df, table, engine, conflict_cols):
    if df.empty:
        print(f"[SKIP] No new rows for {table.name}")
        log_etl_event(engine, table.name, "SKIP", 0, "No new rows")
        return

    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    try:
        with engine.begin() as conn:
            conn.execute(upsert_stmt)
        print(f"[INSERTED] {len(records)} rows into {table.name}")
        log_etl_event(engine, table.name, "SUCCESS", len(records), "Insert completed")
    except Exception as e:
        print(f"[ERROR] Failed to insert into {table.name}: {e}")
        log_etl_event(engine, table.name, "FAILURE", 0, str(e))


def bulk_upsert_dataframe_update(df, table, engine, conflict_cols, update_cols):
    if df.empty:
        print(f"[SKIP] No new or updated rows for {table.name}")
        log_etl_event(engine, table.name, "SKIP", 0, "No new or updated rows")
        return

    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    update_dict = {col: insert_stmt.excluded[col] for col in update_cols}
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_dict
    )

    try:
        with engine.begin() as conn:
            conn.execute(upsert_stmt)
        print(f"[UPSERTED] {len(records)} rows into {table.name}")
        log_etl_event(engine, table.name, "SUCCESS", len(records), "Upsert/update completed")
    except Exception as e:
        print(f"[ERROR] Failed to upsert {table.name}: {e}")
        log_etl_event(engine, table.name, "FAILURE", 0, str(e))
