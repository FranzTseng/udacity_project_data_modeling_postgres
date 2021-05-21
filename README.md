# **Sparkifydb ETL project**

### Introduction
There are two sources of data. One is from the metadata of the sparkify app, and the other from the user log file in the app.<br>
These data are dessined such that data analysts are able to extract easily and get to know more about what songs users like to listen which provides more sights to the business and help optimize sparkify's service to the listeners.<br>

### Files in the repository
**create_tables.py:** Drops existing tables and creates all five new tables.<br>
**etl.ipynb:** Extracts and processes one single data file and loads data into tables.<br>
**etl.py:** Completely processes all data files and loads data into tables.<br>
**sql_queries.py:**  All queries needed to import to other python or notebook files.<br>

### How to run the scripts
- Fill in the environment variables in the .env file
	1. DATABASE_CONNECTION: connect to default database "postgres" on the database server.
	2. DATABASE_CONNECTION_SPARKIFYDB: connect to newly created database "sparkifydb".
- Run ***create_tables.py*** to drop any existing tables and create new tables.<br>
- Run ***etl.py*** to go through the pipeline of extracting data from two sources and put them into 5 tables.<br>
- Use ***test.ipynb*** to test if the data are successfully inserted into the tables in sparkifydb database.<br>

### Database Design
This database contains five tables: songplays, users, songs, time, and artists with the songplays being the the fact table.<br>
<br>
The databse uses a star schema design:<br>
<br> 
**songplays table** contain all instances of songs played by users and connects to all other tables.<br>
**songs table** has all the songs in the app.<br>
**users table** stores user information and their activities.<br>
**time table** has all the timestamp inofrmation documented by the app about when the song was played and who played it.<br>
**artist table** displays singers artists of the songs<br>
