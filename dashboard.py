import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import streamlit as st
import pandas as pd
from etl_tools import (
    create_engine_from_env,
    load_fact_played_tracks
) 

# Load data
engine = create_engine_from_env()
df = load_fact_played_tracks(engine)
# df['played_at'] = pd.to_datetime(df['played_at'])

# Dashboard
dt = st.date_input("Select date",(datetime.date.today().replace(day=1),datetime.date.today()), min_value = min(df['played_at'].dt.date), max_value = datetime.date.today())
if len(dt) != 2:
    st.stop()
datedf = df[(df['played_at'].dt.date >= dt[0]) & (df['played_at'].dt.date <= dt[1])]

number_of_tracks = datedf['track_id'].nunique()
number_of_artist = datedf['artist_id'].nunique()
minutes_listened = round(datedf['duration_ms'].sum() / 60000)
song_pivot = datedf[['track_id','track_name','played_at']].groupby(['track_id','track_name'])['played_at'].count().reset_index()
top_song = song_pivot[['track_name','played_at']][song_pivot['played_at'] == song_pivot['played_at'].max()]
artist_pivot = datedf[['artist_id','artist_name','played_at']].groupby(['artist_id','artist_name'])['played_at'].count().reset_index()
top_artist = artist_pivot[['artist_name','played_at']][artist_pivot['played_at'] == artist_pivot['played_at'].max()]


"Listening time" 
st.write(minutes_listened)

"Number of tracks"
st.write(number_of_tracks)

"Number of artists"
st.write(number_of_artist)

"Top Song"
st.write(top_song)

"Top Artist"
st.write(top_artist)

st.dataframe(datedf)