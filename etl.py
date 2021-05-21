import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
#from psycopg2.extensions import register_adapter, AsIs
#import numpy as np
#psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def process_song_file(cur, filepath):
    """
    -Extract song data from a filepath
    -Select the columns needed and insert them to songs table and artists table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    -Extract user log data from a filepath
    -Filter and retrieve rows with page=NextSong
    -Convert timestamp to hour, date,week of year, month, year, and day of week
    -Insert these time data into time table, and user table
    -Retrieve song_id and artist_id from songs table and artists table and then put them into songplay table along with other columns
    """
    # open log file
    df = pd.read_json(filepath, lines=True)  

    # filter by NextSong action
    df = df.loc[df.page=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"])
    
    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.day_name()],axis=1) 
    column_labels = ["start time", "hour", "date", "week of year", "month", "year", "weekday"]
    time_data.columns=column_labels
    time_df = time_data

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert,row)

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

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

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    -Get all the absolute filepath
    -Loop through all the filepath and parse them to another function to further process these data
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
    -Connect to sparkifydb
    -Process files in the path 'data/song_data'
    -Process files in the path 'data/log_data'
    """
    conn = psycopg2.connect(os.environ["DATABASE_CONNECTION_SPARKIFYDB"])
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
