import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function used to read all the files in the filepath folder (data/song_data)
    this will get the user and time data and used to populate in the SONGS and ARTISTS tables

    Arguments:
        cur: the cursor object. 
        filepath: file path for song_data API file (json). 
        
    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:This function used to read all the files in the filepath folder (data/log_data)
    this will get the user and time data and used to populate in the USERS and TIME tables

    Arguments:
        cur: the cursor object. 
        filepath: file path for song_data API file (json). 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.get('page') == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.get('ts'), unit='ms')
    
    # insert time data records
    time_data = ([t,t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday])
    column_labels = ('timestamp', 'hour','day','week of year','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame.from_dict(dict(zip(('userId','firstname','lastname','gender','level'),
                                              ([df['userId'],df['firstName'],df['lastName'],df['gender'],df['level']]))))

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
        songplay_data = (index, pd.Timestamp(row.ts, unit='ms'),row.userId,row.level, songid, artistid, row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is use to process the data. By calling function through func arguments and show the process message via console

    Arguments:
        cur: the cursor object. 
        conn: for connection to PostgreSQL in order to interacted with DB
        filepath: log data file path. 
        func: for input function to run/call inside this function

    Returns:
        None
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
    Description: Main function to run all of the code in this file. First it will start the connection with DATABASE.
    Then it will process the data in song_file, and log_file.
    Finally, it will close the connection.

    Arguments:
        None
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()