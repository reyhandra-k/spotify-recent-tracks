import os
import time
import datetime
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import spotipy
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()

client_id =  os.environ.get("client_id")
client_secret =  os.environ.get("client_secret")
redirect_uri = 'http://127.0.0.1:3000/callback'
scopes = [
    "user-read-private",
    "user-read-email",
    "user-top-read",
    "user-read-recently-played"
]

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id = client_id,
    client_secret = client_secret,
    redirect_uri = redirect_uri,
    scope = scopes,
    ),
    requests_timeout=15
)

engine = create_engine('postgresql://postgres:root@localhost:5432/spotify_stats')
engine.connect()

last_fetch = pd.read_sql("SELECT MAX(played_at) AS last_fetch FROM recent_tracks", engine)['last_fetch'][0]
if pd.isnull(last_fetch):
    last_fetch = datetime.datetime.utcnow() - datetime.timedelta(hours=24)

last_fetch = int(last_fetch.timestamp() * 1000)

def fetch_recently_played(sp, after_timestamp, retries=3, delay=5):
    for i in range(retries):
        try:
            return sp.current_user_recently_played() #(after=after_timestamp)
        except Exception as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise

results = fetch_recently_played(sp, last_fetch)

recent_track_df = pd.DataFrame([
    {
        "played_at": item["played_at"],
        "track_name": item["track"]["name"],
        "artist": ", ".join(artist["name"] for artist in item["track"]["artists"]),
        "album": item["track"]["album"]["name"],
        "album_type": item["track"]["album"]["album_type"],
        "album_release_date": item["track"]["album"]["release_date"],
        "album_release_date_precision": item["track"]["album"]["release_date_precision"],
        "duration_ms": item["track"]["duration_ms"],
        "track_id": item["track"]["id"],
        "popularity": item["track"]["popularity"]
    }
    for item in results["items"]
])

recent_track_df.album_release_date = pd.to_datetime(recent_track_df.album_release_date, format='ISO8601', utc=True)
recent_track_df.played_at = pd.to_datetime(recent_track_df.played_at, format='ISO8601', utc=True)
recent_track_df.played_at = recent_track_df['played_at'].dt.tz_convert("Asia/Jakarta")
# recent_track_df = recent_track_df[recent_track_df['played_at'] > last_fetch]
recent_track_df.drop_duplicates(subset=["track_id", "played_at"], inplace=True)

print(recent_track_df)

# recent_track_df.to_sql(name='recent_tracks', con=engine, if_exists='append')


