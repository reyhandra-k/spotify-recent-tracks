import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

def create_engine_from_env():
    load_dotenv()
    db_url = os.environ.get("database_url")
    return create_engine(db_url)

def reflect_tables(engine, table_names):
    metadata = MetaData()
    metadata.reflect(bind=engine, only=table_names)
    return {name: Table(name, metadata, autoload_with=engine) for name in table_names}

def bulk_upsert_dataframe(df, table, engine, conflict_cols):
    if df.empty:
        print(f"[SKIP] No new rows for {table.name}")
        return
    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=conflict_cols)
    with engine.begin() as conn:
        conn.execute(upsert_stmt)
        print(f"[INSERTED] {len(records)} rows into {table.name}")

def bulk_upsert_dataframe_update(df, table, engine, conflict_cols, update_cols):
    if df.empty:
        print(f"[SKIP] No new or updated rows for {table.name}")
        return
    records = df.to_dict(orient="records")
    insert_stmt = pg_insert(table).values(records)
    update_dict = {col: insert_stmt.excluded[col] for col in update_cols}
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=conflict_cols,
        set_=update_dict
    )
    with engine.begin() as conn:
        conn.execute(upsert_stmt)
        print(f"[UPSERTED] {len(records)} rows into {table.name}")
