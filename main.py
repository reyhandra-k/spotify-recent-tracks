import os
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from etl_tools import (
    create_engine_from_env,
    reflect_tables,
    bulk_upsert_dataframe,
    bulk_upsert_dataframe_update,
    log_etl_event
)

# logging setup
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

def authenticate_spotify(client_id, client_secret, redirect_uri, refresh_token=None, scopes="user-read-recently-played"):
    try:
        auth_manager = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=scopes,
            open_browser=False,
            cache_handler=None
        )

        if refresh_token:
            logging.info("Using pre-configured refresh token (headless mode).")
            token_info = auth_manager.refresh_access_token(refresh_token)
            sp = spotipy.Spotify(auth=token_info["access_token"], requests_timeout=15)
        else:
            sp = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=15)

        logging.info("Spotify authentication successful.")
        return sp

    except Exception as e:
        logging.error(f"Spotify authentication failed: {e}")
        raise SystemExit(1)


def get_last_fetch_time(engine, buffer_minutes=30, fallback_hours=72):
    try:
        last_fetch_dt = pd.read_sql("SELECT MAX(played_at) as last FROM plays", engine)['last'][0]
        if pd.isnull(last_fetch_dt):
            last_fetch_dt = datetime.now(timezone.utc) - timedelta(hours=fallback_hours)
        else:
            last_fetch_dt = pd.to_datetime(last_fetch_dt, utc=True) - timedelta(minutes=buffer_minutes)
        logging.info(f"Last fetch timestamp (with buffer): {last_fetch_dt}")
    except Exception as e:
        last_fetch_dt = datetime.now(timezone.utc) - timedelta(hours=fallback_hours)
        logging.warning(f"Could not fetch last fetch time from DB, using fallback: {e}")

    return int(last_fetch_dt.timestamp() * 1000)


def extract_recent_tracks(sp, after_ts):
    try:
        results = sp.current_user_recently_played(after=after_ts)
        items = results.get("items", [])
        logging.info(f"Fetched {len(items)} tracks from Spotify API.")
        return items
    except Exception as e:
        logging.error(f"Error fetching data from Spotify: {e}")
        raise


def transform_tracks(items, last_fetch_dt):
    try:
        df = pd.DataFrame([
            {
                "played_at": item["played_at"],
                "track_id": item["track"]["id"],
                "track_name": item["track"]["name"],
                "artist_id": item["track"]["artists"][0]["id"],
                "artist_name": item["track"]["artists"][0]["name"],
                "album_id": item["track"]["album"]["id"],
                "album_name": item["track"]["album"]["name"],
                "album_type": item["track"]["album"]["album_type"],
                "album_release_date": item["track"]["album"]["release_date"],
                "album_release_date_precision": item["track"]["album"]["release_date_precision"],
                "duration_ms": item["track"]["duration_ms"],
                "popularity": item["track"]["popularity"]
            }
            for item in items
        ])

        df['played_at'] = pd.to_datetime(df['played_at'], utc=True)
        df['album_release_date'] = pd.to_datetime(df['album_release_date'], errors='coerce')
        df = df[df['played_at'] > last_fetch_dt]

        logging.info(f"Transformed data: {len(df)} records after filtering.")

        # Normalize
        artists_df = df[['artist_id', 'artist_name']].drop_duplicates()
        albums_df = df[['album_id', 'album_name', 'album_type', 'album_release_date',
                        'album_release_date_precision', 'artist_id']].drop_duplicates()
        tracks_df = df[['track_id', 'track_name', 'album_id', 'duration_ms', 'popularity']].drop_duplicates()
        plays_df = df[['played_at', 'track_id']].drop_duplicates()

        return artists_df, albums_df, tracks_df, plays_df

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

def load_to_db(tables, engine, artists_df, albums_df, tracks_df, plays_df):
    try:
        bulk_upsert_dataframe(artists_df, tables['artists'], engine, conflict_cols=['artist_id'])
        bulk_upsert_dataframe(albums_df, tables['albums'], engine, conflict_cols=['album_id'])
        bulk_upsert_dataframe_update(tracks_df, tables['tracks'], engine, conflict_cols=['track_id'],
                                     update_cols=['track_name', 'popularity'])
        inserted_plays = bulk_upsert_dataframe(plays_df, tables['plays'], engine, conflict_cols=['played_at'])
        logging.info("Data successfully loaded to database.")
        return inserted_plays

    except Exception as e:
        logging.error(f"Error loading data to database: {e}")
        raise

def main():
    try:
        # DB connection
        engine = create_engine_from_env()
        log_etl_event(engine, "ALL", "START", 0, "ETL process has started.")

        tables = reflect_tables(engine, ["artists", "albums", "tracks", "plays"])
        logging.info("Database connection successful.")

        # Spotify auth
        load_dotenv(override=False)
        client_id = os.environ.get("client_id")
        client_secret = os.environ.get("client_secret")
        redirect_uri = os.environ.get("redirect_uri")
        refresh_token = os.environ.get("refresh_token")
        scopes = "user-read-recently-played"

        sp = authenticate_spotify(client_id, client_secret, redirect_uri, refresh_token, scopes)

        # ETL steps
        after_ts = get_last_fetch_time(engine)
        items = extract_recent_tracks(sp, after_ts)

        if not items:
            logging.info("No new tracks found. Exiting ETL process.")
            log_etl_event(engine, "ALL", "SUCCESS", 0, "No new recently played tracks found.")
            return

        artists_df, albums_df, tracks_df, plays_df = transform_tracks(items, pd.to_datetime(after_ts, unit='ms', utc=True))
        inserted_count = load_to_db(tables, engine, artists_df, albums_df, tracks_df, plays_df)

        if inserted_count > 0:
            log_etl_event(engine, "ALL", "SUCCESS", inserted_count, "ETL process completed successfully.")
            logging.info("=== Spotify ETL process completed successfully ===")
        else:
            log_etl_event(engine, "ALL", "SUCCESS", 0, "No new recently played tracks found.")
            logging.info("No new tracks found. Exiting ETL process.")


    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        try:
            if 'engine' in locals():
                log_etl_event(engine, "ALL", "FAILURE", 0, str(e))
        except Exception as inner_e:
            logging.error(f"Could not log failure to DB: {inner_e}")
        raise


if __name__ == "__main__":
    main()
