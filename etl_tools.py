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
        log_etl_event(engine, table.name, "SKIP", 0, "No new data.")
        return 0

    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_cols)

    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
        inserted_count = result.rowcount if result.rowcount is not None else len(records)
        print(f"[INSERTED] {inserted_count} rows into {table.name}")
        if inserted_count > 0:
            log_etl_event(engine, table.name, "SUCCESS", inserted_count, "Insert completed.")
        else:
            log_etl_event(engine, table.name, "SUCCESS", inserted_count, "No new data.")

        return inserted_count
    except Exception as e:
        print(f"[ERROR] Failed to insert into {table.name}: {e}")
        log_etl_event(engine, table.name, "FAILURE", 0, str(e))
        return 0

def bulk_upsert_dataframe_update(df, table, engine, conflict_cols, update_cols):
    if df.empty:
        print(f"[SKIP] No new or updated rows for {table.name}")
        log_etl_event(engine, table.name, "SKIP", 0, "No new or updated data.")
        return 0

    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    update_dict = {col: insert_stmt.excluded[col] for col in update_cols}
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_dict
    )

    try:
        with engine.begin() as conn:
            result = conn.execute(upsert_stmt)
        affected_count = result.rowcount if result.rowcount is not None else len(records)
        print(f"[UPSERTED] {affected_count} rows into {table.name}")
        if affected_count > 0:
            log_etl_event(engine, table.name, "SUCCESS", affected_count, "Upsert/update completed.")
        else:
            log_etl_event(engine, table.name, "SUCCESS", affected_count, "No new data.")
        return affected_count
    except Exception as e:
        print(f"[ERROR] Failed to upsert {table.name}: {e}")
        log_etl_event(engine, table.name, "FAILURE", 0, str(e))
        return 0