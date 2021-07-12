import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads the song json data and convert into pandas dataframe.
    - Extract the columns required for songs and artists table created and populate the tables.
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = list(df.values[[6,7,1,9,8]])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df.values[[1,5,4,2,3]])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads the log json data and convert into pandas dataframe.
    - Convert the timestamp column into datetime values  and store in the pandas dataframe.
    - populate the columns on the time table and the songplay table
    """
    # open log file
    df = pd.read_json(filepath,  lines=True)

    # filter by NextSong action
    df=df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['datetime']=pd.to_datetime(df['ts'])
    df['time']=df.datetime.dt.time
    df['hour']=df.datetime.dt.hour
    df['day']=df.datetime.dt.day
    df['week']=df.datetime.dt.week
    df['year']=df.datetime.dt.year
    df['month']=df.datetime.dt.month
    df['weekday']=df.datetime.dt.weekday_name
    # insert time data records
    time_data = list(df.values[:,19:28])
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        songplay_data = [row.time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    - get all files matching extension from directory.
    - iterate over files and process.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - connect to sparkify database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()