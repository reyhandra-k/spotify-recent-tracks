from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv, set_key
import os
import requests
import base64
import json
from pathlib import Path
import pandas as pd
import hashlib
from sqlalchemy import create_engine

# Refresh access token
def refresh_token():
    load_dotenv()
    env_file_path = Path(".env")

    client_id =  os.environ.get("client_id")
    client_secret =  os.environ.get("client_secret")

    auth_str = f"{client_id}:{client_secret}"
    b64_auth_str = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Content-Type": 'application/x-www-form-urlencoded',
        "Authorization": f"Basic {b64_auth_str}"
    }

    data = {
        "grant_type": 'refresh_token',
        "refresh_token": os.environ.get("refresh_token")
    }

    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)

    if response.status_code == 200:
        new_token_data = response.json()
        print(json.dumps(new_token_data, indent=3))    
        set_key(dotenv_path=env_file_path, key_to_set="access_token", value_to_set=new_token_data["access_token"])    
        os.environ["access_token"] = new_token_data["access_token"]
        if "refresh_token" in new_token_data:
            set_key(dotenv_path=env_file_path, key_to_set="refresh_token", value_to_set=new_token_data["refresh_token"])    
            os.environ["refresh_token"] = new_token_data["refresh_token"]
    else:
        print("Error: ", response.status_code, response.text)


#get last 50 tracks
load_dotenv()

access_token = os.environ.get("access_token")

headers = {
    'Authorization': f"Bearer {access_token}"
}

params = {
    "limit": 50
}

response = requests.get("https://api.spotify.com/v1/me/player/recently-played",headers=headers,params=params)

if response.status_code == 200:
    raw_recent_tracks = response.json()
    print(json.dumps(raw_recent_tracks, indent=4))
else:
    print("Error: ", response.status_code, response.text)


#convert api response to pandas dataframe
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
    for item in raw_recent_tracks["items"]
])

def gen_md5_id(id):
    return hashlib.md5(str(id).encode()).hexdigest()

recent_track_df.album_release_date = pd.to_datetime(recent_track_df.album_release_date, format='ISO8601', utc=True)
recent_track_df.played_at = pd.to_datetime(recent_track_df.played_at, format='ISO8601', utc=True)
recent_track_df.played_at = recent_track_df['played_at'].dt.tz_convert("Asia/Jakarta")
recent_track_df['str_id'] = recent_track_df['played_at'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ') + recent_track_df['track_id']
recent_track_df['md5_hash'] = recent_track_df['str_id'].apply(gen_md5_id)


#load dataframe to postgres
engine = create_engine('postgresql://postgres:root@localhost:5432/spotify_stats')
recent_track_df.to_sql(name='recent_tracks', con=engine, if_exists='append')


