### Project Objective :

The objective of this project is to use spark for processing data.
It reads data from S3 bucket and processes using spark.It gemerates one fact table and four dim table. 
Finally, It uploads all fact and dim tables into S3 bucket using parquet file.

### Design :

#### fact table :
Songplay table is primary fact table and it contains the data of all songs that are listned over the sparkify. 
Also stores the key of all dimension tables. song_id is chosen as dist_key so that it will help to join songs details and get all song related infomation.

#### dim table :
Data ware house contains 4 dimention tables . User stores user related data. Songs stores all the songs and song_id is chosen as distribute key to join with fact table. Artist stores artist related information. Time stores time re 

### ETL Strategy :
1. Configure dl.cfg file. Provide all the access details ie Access key and secret.
2. Run etl.py - It will populate all the stage table from s3 bucket and then it will load fact and dim tables.
    a) process_song_data - will process song data sets.
    b) process_log_data - will process log data sets.
    
### Output :
Ouput will be stored in parquet file in seperate directory for each table.
