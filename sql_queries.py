# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY, 
start_time varchar(100) NOT NULL, 
user_id int NOT NULL, 
level varchar(10), 
song_id varchar(100), 
artist_id varchar(100), 
session_id int NOT NULL, 
location varchar(100), 
user_agent varchar(200));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY, 
firstName varchar(50) NOT NULL, 
lastName varchar(50) NOT NULL, 
gender char(1) NOT NULL, 
level varchar(10));
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar(100) PRIMARY KEY, 
title varchar(100) NOT NULL, 
artist_id varchar(100) NOT NULL, 
year int NOT NULL, 
duration decimal NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar(100) PRIMARY KEY, 
name varchar(100) NOT NULL, 
location varchar(100), 
latitude varchar(20), 
longitude varchar(20));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time varchar(100) PRIMARY KEY, 
hour varchar(50) NOT NULL, 
day varchar(50) NOT NULL, 
week varchar(50) NOT NULL, 
month varchar(50) NOT NULL, 
year varchar(50) NOT NULL, 
weekday varchar(50) NOT NULL);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, firstName, lastName, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING; """) 


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id from songs s JOIN artists a ON s.artist_id=a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s; 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]