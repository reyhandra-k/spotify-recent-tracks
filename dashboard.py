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
st.set_page_config(layout="wide")

## Date Filter
dt = st.date_input("Please select the period you would like to inspect",(datetime.date.today().replace(day=1),datetime.date.today()), min_value = min(df['played_at'].dt.date), max_value = datetime.date.today())
if len(dt) != 2:
    st.stop()
datedf = df[(df['played_at'].dt.date >= dt[0]) & (df['played_at'].dt.date <= dt[1])]
if datedf.empty:
    st.warning("You have no listening activity during this period.")
    st.stop()
prevdatestart = ( dt[0] - (dt[1]-dt[0]) - pd.DateOffset(1) ).date()
prevdf = df[(df['played_at'].dt.date >= prevdatestart) & (df['played_at'].dt.date < dt[0])]


## Summary Row
col1, col2, col3 = st.columns(3)
with col1:
    minutes_listened = round(datedf['duration_ms'].sum() / 60000)
    prev_minutes_listened = round(prevdf['duration_ms'].sum() / 60000)
    try:
        delta_minutes = (minutes_listened - prev_minutes_listened) / prev_minutes_listened
    except:
        delta_minutes = 0
    st.metric(label = "Minutes Listened", value = minutes_listened, delta=f"{delta_minutes:.1%}", border = True)
    st.info("Thats 18% of your life!")

with col2:
    number_of_tracks = datedf['track_id'].nunique()
    prev_number_of_tracks = prevdf['track_id'].nunique()
    try:
        delta_tracks = (number_of_tracks - prev_number_of_tracks) / prev_number_of_tracks
    except:
        delta_tracks = 0
    st.metric(label = "Tracks Listened", value = number_of_tracks, delta=f"{delta_tracks:.1%}", border = True)
    st.info("3 of those being new songs")

with col3:
    number_of_artist = datedf['artist_id'].nunique()
    prev_number_of_artist = prevdf['artist_id'].nunique()
    try:
        delta_artists = (number_of_artist - prev_number_of_artist) / prev_number_of_artist
    except:
        delta_artists = 0
    st.metric(label = "Artists Listened", value = number_of_artist, delta=f"{delta_artists:.1%}", border = True)
    st.info("Blabla")
st.divider()

## Top Track
### Text Config
st.markdown("""
<style>
.big-font {
    font-size: 52px !important;
    font-weight: bold;
}
.med-font {
    font-size: 26px !important;
    font-weight: bold;
}
</style>""", unsafe_allow_html=True)

### Top Track Extract 
song_pivot = datedf[['track_id','track_name','played_at']].groupby(['track_id','track_name'])['played_at'].count().reset_index()
song_pivot = song_pivot.rename(columns={"played_at":"plays"})
top_song = song_pivot[['track_name','plays']][song_pivot['plays'] == song_pivot['plays'].max()]
top_song_name = top_song.iat[0,0]
top_song_freq = top_song.iat[0,1]
st.markdown(f'''<p><span class="big-font">{top_song_name}</span> rises to the top as your 
            <span style="color: yellow">most listened to track </span>
            with a staggering<span class="med-font"> {top_song_freq} plays</span></p>''', unsafe_allow_html=True)




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


"Listening Activity Heatmap"
hourly_pivot = datedf[['played_dayofweek','played_hour','played_at']].groupby(['played_dayofweek','played_hour'])['played_at'].count().reset_index()
hourly_pivot = hourly_pivot.rename(columns={"played_at":"plays"})
hourly_trend_chart = (
    alt.Chart(hourly_pivot)
    .mark_rect()
    .encode(x=alt.X('played_dayofweek:N').sort('x'), y=alt.Y('played_hour:N').sort('y'), color='plays')
)
st.altair_chart(hourly_trend_chart)


"All Data"
st.dataframe(datedf)


"Prev"
st.write(prevdatestart)
st.dataframe(prevdf)
