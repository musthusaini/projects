import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = " DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_events(
id bigint identity(0,1),
artist varchar(255),
auth varchar(255),
first_name varchar(255),
gender varchar(255),
items_in_session varchar(255),
last_name varchar(255),
length varchar(255),
level varchar(255),
location varchar(255),
method varchar(255),
page varchar(255),
registration varchar(255),
session_id varchar(255),
song varchar(255),
status varchar(255),
ts varchar(255),
user_Agent varchar(255),
user_id varchar(255)
)
""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs(
id bigint identity(0,1),
num_songs varchar(255),
artist_id varchar(255),
artist_latitude varchar(255),
artist_longitude varchar(255),
artist_location varchar(255),
artist_name varchar(255),
song_id varchar(255),
title varchar(255),
duration varchar(255),
year varchar(255)
)
""")

songplay_table_create = ("""
CREATE TABLE songplay(
songplay_id bigint identity(0,1),
start_time bigint sortkey, 
user_id integer, 
level varchar(255),
song_id varchar(255) distkey, 
artist_id varchar(255), 
session_id integer, 
location varchar(255), 
user_agent varchar(255)
)
""")

user_table_create = ("""
CREATE TABLE users(
user_id integer distkey sortkey, 
first_name varchar(255), 
last_name varchar(255), 
gender varchar(10), 
level varchar(50)

)
""")

song_table_create = ("""
CREATE TABLE song(
song_id varchar(255) distkey sortkey, 
title varchar(255), 
artist_id varchar(255), 
year integer, 
duration decimal(10,5))
""")

artist_table_create = ("""
CREATE TABLE artists(
artist_id varchar(255) sortkey distkey, 
name varchar(255),
location varchar(255), 
lattitude varchar(255), 
longitude varchar(255)
)
""")

time_table_create = ("""
CREATE TABLE time_table(
start_time bigint sortkey distkey, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday integer

)
""")

# STAGING TABLES

staging_events_copy = ("""copy stage_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' format as JSON {} ;
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy stage_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'  JSON 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay(
 start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
select 
ts::bigint as start_time, 
user_id::integer as user_id,
level, 
s.song_id as song_id,
s.artist_id as artist_id, 
session_id::integer AS session_id, 
location,
user_agent
FROM stage_events se
LEFT JOIN stage_songs s on se.song = s.title
where se.page='NextSong'
;
""")

user_table_insert = (""" INSERT INTO users(
user_id , 
first_name, 
last_name, 
gender,
level
) 
SELECT 
distinct cast(user_id as integer), 
first_name, 
last_name, 
gender,
level
FROM 
stage_events WHERE user_id!='';
""")

song_table_insert = ("""
INSERT INTO song(
song_id, 
title, 
artist_id, 
year, 
duration)
SELECT 
song_id, 
title, 
artist_id,
cast(year as integer),
cast(duration as float)
FROM 
stage_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(
artist_id, 
name, 
location, 
lattitude, 
longitude
)
SELECT 
artist_id, 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude
FROM 
stage_songs;
""")

time_table_insert = ("""
INSERT INTO time_table(
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday)
select distinct ts::bigint as start_time,
extract( hour from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') as hour,
extract( day from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') as day,
extract( week from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') AS week,
extract( month from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') as MONTH,
extract( year from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') as year,
extract( weekday from TIMESTAMP 'epoch' + ts::bigint/1000 * interval '1 second') As weekday
from stage_events;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,staging_songs_table_create,user_table_create,songplay_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy] 
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
