import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import streamlit as st
import pandas as pd

def create_engine_from_env():
    load_dotenv(override=False)
    engine = create_engine(os.environ.get("database_url"), connect_args={"sslmode": "require"})
    return engine

def execute_sql(sqlquery, engine):
    try:
        with engine.begin() as conn:
            df = pd.read_sql(sqlquery, conn)
        return df

    except Exception as e:
        print(f"[ERROR] {e}")
        return 0

sqlquery = """
SELECT *
FROM analytics.fact_played_track_details
ORDER BY played_at DESC
"""

engine = create_engine_from_env()
df = execute_sql(sqlquery, engine)

st.dataframe(df)