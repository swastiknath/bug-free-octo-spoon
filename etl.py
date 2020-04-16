import glob
import os

import numpy as np
import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    """
    get songs and artists data from JSON file and insert them into respective table.
    reads file and put its data in panda data frame.
    fetches song and artist's attributes from data frame and put them in insert query.
    :param cur: cursor instance.
    :param filepath: the path to the JSON file.
    :return: None.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = song_data[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    reads log file, filter the records by `NextSong` action.
    converts milliseconds into timestamp.
    fetches dimension specific attributes and insert them into respective table.
    also fills fact table `songplays`.
    :param cur: cursor instance.
    :param filepath: the path to the JSON file.
    :return: None.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['start_time'] = t.dt.to_pydatetime()

    # insert time data records
    time_data = (t.dt.to_pydatetime(), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(np.column_stack(list(time_data)), columns=list(column_labels))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data = [df['userId'], df['firstName'], df['lastName'], df['gender'], df['level']]
    column_labels = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = pd.DataFrame(np.column_stack(list(user_data)), columns=list(column_labels))

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
        songplay_data = (row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        print(songplay_data)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    iterates through data folder, append all files and process them one by one.
    :param cur: cursor instance.
    :param conn: connection instance.
    :param filepath: song/log data directory path.
    :param func: function to be called against each file.
    :return: None.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()