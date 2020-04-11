### Project Objective :

The objective of this project is to create a data warehouse for user song listening events.
This data warehouse contains one fact table and four dim table. 

### Design :

#### stage tables :
There are two stage table stage_events and stage_songs. Both stores event and song data respectively. All the columns in stage table has varchar datatype so that it will load all the data from S3 without any error and type convversion will be taken care at later stage while creating database.

#### fact table :
Songplay table is primary fact table and it contains the data of all songs that are listned over the sparkify. 
Also stores the key of all dimension tables. song_id is chosen as dist_key so that it will help to join songs details and get all song related infomation.

#### dim table :
Data ware house contains 4 dimention tables . User stores user related data. Songs stores all the songs and song_id is chosen as distribute key to join with fact table. Artist stores artist related information. Time stores time re 

### ETL Strategy :
1. Configure dwh.cfg file. Provide all the access details ie ARN , databse host and password.
2. Run create_tables.py - It will run the DDL and create all the tables.
3. Run etl.py - It will populate all the stage table from s3 bucket and then it will load fact and dim tables.
