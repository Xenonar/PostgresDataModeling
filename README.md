# Instruction for SparkifyDB

## Create table
In the first step in order to create the table we need to check is there any table with the same name already existed or not
-  FIRST DROP the database (if exist) and then CREATE the database
-  After that create all tables

```
$ python3 create_tables.py
```
or
```
$ python create_tables.py
```
## Test/CHECKING all tables in Database
```
 test.ipnyp
```
So it **should be**:  

>>  postgresql://student:***@127.0.0.1/sparkifydb
0 rows affected.
songplay_id	start_time	user_id	level	song_id	artist_id	session_id	location	user_agent

For successfully create table

## ETL python notebook and python files
1) ETL.ipynb is for development process and understand how it work
2) ETL.py is the main python file to process data

## ER Diagram
Here is to illustrated the table idea
![ER Diagram](https://blog.artarawan.com/wp-content/uploads/2020/06/ERDiagram.png)

### Build `ETL.PY`
First it start connection to the sql server to be able to fetch and push data to the server.
Then, it start process the data from song_data by iterative run through all the files in song_data folder.
Here we will get the SONGS and ARTISTS tables into DB.

>> **Parameters**
df: extract API data into DataFrame format(by pandas library)
song_data: all the songs data from API file ready to be INSERT INTO SONGS TABLE
artist_data: all the artists data from API file ready to be INSERT INTO ARTISTS TABLE

> Open the json file from filepath and save into df
```
 df = pd.read_json(filepath, lines=True)
```
> **INSERT DATA to SONGS table and execute to DB**
**PUSH** data into **song_data** list contain all of the data
then execute the insert query from **sql_queies.py** with **song_data** values

> df.values[0].tolist()=> making it to list

>and for execute to DB is using cur.execute(insert_queries, data) insert_queries = query 
import from sql_queries.py
for SONGS table the query for do insert is song_table_insert

```
 song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
 cur.execute(song_table_insert, song_data)
```
> **INSERT DATA to ARTISTS table and execute to DB**
in df.get it will return 2 array values which is value and data type so we need only value
therefore we need to specify index = 0 
similar to SONGS table and here is artist_data and ARTISTS table

```
 artist_data = df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']].values[0].tolist()
 cur.execute(artist_table_insert, artist_data)
```

After that it process the data inside log_data in this step we will get USERS, TIME, and SONGPLAYS table into DB

> **Parameters and Variables**
df: extract API data into DataFrame format(by pandas library)
t: convert timestamp from df to datetime (in milisecond)
time_data: covert datetime into timestamp(in milisecond), hour, day, week of year, month, year, weekday 
column_labels: label in the column for time_data
time_df: combination of time_data and column_labels ready to be INSERT INTO TIME TABLE
user_df: all the users data from API file ready to be INSERT INTO USERS TABLE
results: After checking that the data we want to put in SONGPLAYS table is not already exists in DB
songid: song data from iterative through data
artistid: artist data from iterative through data
songplay_data: all data ready for INSERT INTO SONGPLAYS TABLE

> **Filter by 'NextSong' in PAGE column**
in this section we need to filter column page that NOT 'NextSong' out of process

```
 df = df[df.get('page') == 'NextSong']
```

> **Convert timestamp(ts) to DATETIME**

```
 t = pd.to_datetime(df.get('ts'))
```

> **INSERT time data**
>> convert time from DATETIME into hour, day, week of year, month, year, weekday 
with syntex of dt.hour, dt.day,dt.weekofyear

```
 time_data = ([df.get('ts'),t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday])
```
>> **MAKE column labels**

```
 column_labels = ('timestamp', 'hour','day','week of year','month','year','weekday')
```
>> **COMBINE with dict then convert to dataframe to show a proper table**
combile time and column using zip function, zip(key, values) which key = column name and value is time_data.
zip => use for create object that come from iterative list

```
 time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
```

> **INSERT data into USERS table and execute**
>> convert dict to DataFram with DataFrame.from_dict() inside pd library

```
 user_df = pd.DataFrame.from_dict(
     dict(
         zip(
             (('userId','firstname','lastname','gender','level'),
                   ([df['userId'],df['firstName'],df['lastName'],df['gender'],df['level']])))))

 cur.execute(user_table_insert, row)
```

> **SONGPLAYS table in sql_queries.py**

>> Create SELECT query for checking the duplication of data
WHERE is condition statement and %s mean input from the python file in `etl.py`
>>> cur.execute(song_select, (row.song, row.artist, row.length)) 
-> row.song is first input, row.artist and row.length accordingly
if title = row.song, name = row.artist, and duration = row.length is exist then return data
else return 'None'

```
    SELECT song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE title = %s AND name = %s AND duration = %s
```

>>> %s is input

>> Get the data from table SONGS and ARTISTS
>> row is the value from for loop df.iterrows()

> **EXECUTE to DATABASE**

```
 cur.execute(song_select, (row.song, row.artist, row.length))
```
>> **IF ELSE: Check statement**
if the result = exist then songid and artistid = results
else songid = None, artistid = None

```
  if results:
       songid, artistid = results
  else:
       songid, artistid = None, None
```

>> **INSERT and EXECUTE to SONGPLAYS table**
insert index to songplay_id
start_time = ts from data in row parameter come from iterative run
level, sessionId, location, userAgent they are all come from data from API
songid, artistid = come from the previous statement above

```
  songplay_data = (index, row.ts,row.userId,row.level, songid, artistid, row.sessionId,row.location, row.userAgent)
  cur.execute(songplay_table_insert, songplay_data)
```

**LAST close the connection with connection.close()**

```
  conn.close()
```

### RUN `ELT.py`
```
$ python3 elt.py
```
or
```
$ python elt.py
```