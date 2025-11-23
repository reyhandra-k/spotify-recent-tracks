import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import altair as alt
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

"Listening time" 
minutes_listened = round(datedf['duration_ms'].sum() / 60000)
st.write(minutes_listened)

"Number of tracks"
number_of_tracks = datedf['track_id'].nunique()
st.write(number_of_tracks)

"Number of artists"
number_of_artist = datedf['artist_id'].nunique()
st.write(number_of_artist)

"Top Song"
song_pivot = datedf[['track_id','track_name','played_at']].groupby(['track_id','track_name'])['played_at'].count().reset_index()
song_pivot = song_pivot.rename(columns={"played_at":"plays"})
top_song = song_pivot[['track_name','plays']][song_pivot['plays'] == song_pivot['plays'].max()]
st.write(top_song)

"Top 5 Songs"
top_5_song = song_pivot.sort_values(by=['plays'],ascending=False).head()
top_5_song_chart = (
    alt.Chart(top_5_song)
    .mark_bar()
    .encode(x=alt.X('track_name').sort('-y'), y='plays')
)
top_5_song_chart_label = (
    top_5_song_chart.mark_text(align='center', baseline='line-top')
    .encode(text='plays')
)
st.altair_chart(top_5_song_chart+top_5_song_chart_label)

"Top Artist"
artist_pivot = datedf[['artist_id','artist_name','played_at']].groupby(['artist_id','artist_name'])['played_at'].count().reset_index()
artist_pivot = artist_pivot.rename(columns={"played_at":"plays"})
top_artist = artist_pivot[['artist_name','plays']][artist_pivot['plays'] == artist_pivot['plays'].max()]
st.write(top_artist)

"Top 5 Artist"
top_5_artist = artist_pivot.sort_values(by=['plays'],ascending=False).head()
top_5_artist_chart = (
    alt.Chart(top_5_artist)
    .mark_bar()
    .encode(x=alt.X('artist_name').sort('-y'), y='plays')
)
top_5_artist_chart_label = (
    top_5_artist_chart.mark_text(align='center', baseline='line-top')
    .encode(text='plays')
)
st.altair_chart(top_5_artist_chart+top_5_artist_chart_label)

"Peak listening times"
day_type = st.multiselect(label="Choose daytype", options=["Weekday","Weekend"], default="Weekday")
hourly_pivot = datedf[['played_hour','played_at']][datedf['played_daytype'].isin(day_type)].groupby(['played_hour'])['played_at'].count().reset_index()
hourly_pivot = hourly_pivot.rename(columns={"played_at":"plays"})
hourly_trend_chart = (
    alt.Chart(hourly_pivot)
    .mark_bar()
    .encode(x=alt.X('played_hour:N').sort('x'), y='plays')
)
st.altair_chart(hourly_trend_chart)

"All Data"
st.dataframe(datedf)