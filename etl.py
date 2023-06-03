import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


count = 0


def process_song_file(cur, filepath):
    """Load data from a song file to the song and the artist data tables"""
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[[
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
    ]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Load data from a log file into the time, user and songplay data tables"""
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])

    # insert time data records
    time_data = [
        (
            tt.value,
            tt.hour,
            tt.day,
            tt.week,
            tt.month,
            tt.year,
            tt.weekday()
        ) for tt in t
    ]
    column_labels = [
        'timestamp',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday'
    ]
    time_df = pd.DataFrame(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert song play records
    for i, row in df.iterrows():
        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert into songplay record
        global count
        count = count + 1
        songplay_data = (
            count,
            row['ts'],
            row['userId'],
            row['level'],
            song_id,
            artist_id,
            row['sessionId'],
            row['location'],
            row['userAgent']
        )

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iterate over all the files and populate data tables in sparkifydb"""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        file_lst = glob.glob(os.path.join(root, '*.json'))
        for f in file_lst:
            all_files.append(os.path.abspath(f))

    # get total number of files
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}.")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    conn = psycopg2.connect(
        database="sparkifydb",
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()
    process_data(cur, conn, "data/song_data", process_song_file)
    process_data(cur, conn, "data/log_data", process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
