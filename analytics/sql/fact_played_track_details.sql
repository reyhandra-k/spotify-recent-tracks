DROP TABLE analytics.fact_played_track_details;

CREATE TABLE analytics.fact_played_track_details AS 
with jkt_timezone as (
SELECT
p.played_at + interval '7 hours' as played_at,
t.track_id,
t.track_name,
t.duration_ms,
t.popularity,
alb.album_id,
alb.album_name,
alb.album_type,
alb.album_release_date,
art.artist_id,
art.artist_name
from plays p
left join tracks t
on p.track_id = t.track_id
left join albums alb
on t.album_id = alb.album_id
left join artists art
on alb.artist_id = art.artist_id
)

SELECT
*,
extract(hour from played_at) as played_hour,
extract(day from played_at) as played_day,
extract(dow from played_at) as played_dayofweek,
CASE WHEN extract(dow from played_at) in (0,6) THEN 'Weekend' ELSE 'Weekday' END as played_daytype,
extract(month from played_at) as played_month
from jkt_timezone