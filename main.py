import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta, timezone
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from etl_script import (
    create_engine_from_env,
    reflect_tables,
    bulk_upsert_dataframe,
    bulk_upsert_dataframe_update
)

# auth
load_dotenv()

client_id =  os.environ.get("client_id")
client_secret =  os.environ.get("client_secret")
redirect_uri = os.environ.get("redirect_uri")
scopes = "user-read-recently-played"

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id = client_id,
    client_secret = client_secret,
    redirect_uri = redirect_uri,
    scope = scopes,
    ),
    requests_timeout=15
)
# connect to db
engine = create_engine_from_env()
tables = reflect_tables(engine, ["artists", "albums", "tracks", "plays"])


try:
    last_fetch_dt = pd.read_sql("SELECT MAX(played_at) as last FROM plays", engine)['last'][0]
    if pd.isnull(last_fetch_dt):
        last_fetch_dt = datetime.now(timezone.utc) - timedelta(hours=72)
    else:
        last_fetch_dt = pd.to_datetime(last_fetch_dt, utc=True)
except:
    last_fetch_dt = datetime.now(timezone.utc) - timedelta(hours=72)

after_ts = int(last_fetch_dt.timestamp() * 1000)

# fetch data
results = sp.current_user_recently_played(after=after_ts)
items = results.get("items", [])

# transform to df
recent_tracks = pd.DataFrame([
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

if recent_tracks.empty:
    print("[INFO] No new tracks.")
    exit()

# Clean timestamps
recent_tracks['played_at'] = pd.to_datetime(recent_tracks['played_at'], utc=True)
recent_tracks['album_release_date'] = pd.to_datetime(recent_tracks['album_release_date'], errors='coerce')
recent_tracks = recent_tracks[recent_tracks['played_at'] > last_fetch_dt]

# Normalize
artists_df = recent_tracks[['artist_id', 'artist_name']].drop_duplicates()
albums_df = recent_tracks[['album_id', 'album_name', 'album_type', 'album_release_date', 'album_release_date_precision', 'artist_id']].drop_duplicates()
tracks_df = recent_tracks[['track_id', 'track_name', 'album_id', 'duration_ms', 'popularity']].drop_duplicates()
plays_df = recent_tracks[['played_at', 'track_id']].drop_duplicates()

# load to db
bulk_upsert_dataframe(artists_df, tables['artists'], engine, conflict_cols=['artist_id'])
bulk_upsert_dataframe(albums_df, tables['albums'], engine, conflict_cols=['album_id'])
bulk_upsert_dataframe_update(tracks_df, tables['tracks'], engine, conflict_cols=['track_id'], update_cols=['track_name', 'popularity'])
bulk_upsert_dataframe(plays_df, tables['plays'], engine, conflict_cols=['played_at'])

print("[SUCCESS] ETL Complete.")
