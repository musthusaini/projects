import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONFIG']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
        Function reads the song data from song data set available on S3.
        It generates songs and artists dim tables from song data set.
        Finally , It uploads dim table to S3 in parquet file format.
        
        Inputs :
            spark - Spark Session objects
            input_data - source S3 bucket path
            output_data - destination S3 bucket path
            
        Output :
            df - songs data frame . This will be later used for fact table.
    """
    # get filepath to song data file
    song_data = input_data+'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')\
                    .where("song_id is not null").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id')\
                .parquet(os.path.join(output_data,"songs.parquet"),"overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name',
                    'artist_location','artist_latitude','artist_longitude')\
                .withColumnRenamed('artist_name','name')\
                .withColumnRenamed('artist_location','location')\
                .withColumnRenamed('artist_latitude','latitude')\
                .withColumnRenamed('artist_longitude','longitude')\
                .where("artist_id is not null").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists.parquet"),"overwrite")
    return df

def process_log_data(spark, input_data, output_data, song_data):
        """
        Function reads the events data from events data set available on S3.
        It generates users and time dim tables from events data set.
        It also generates songplays fact table.
        Finally , It uploads dim and fact tables to S3 in parquet file format.
        
        Inputs :
            spark - Spark Session objects
            input_data - source S3 bucket path
            output_data - destination S3 bucket path
            song_data - spark data frame of song data set

    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    user_table = user_table = df.select('userId','firstName','lastName','gender','level')\
               .withColumnRenamed('userId','user_id')\
               .withColumnRenamed('firstName','first_name')\
               .withColumnRenamed('lastName','lastName')\
               .where(df.userId.isNotNull()).dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data,"users.parquet"),"overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:str(datetime.fromtimestamp(x/1000)))
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    
    # extract columns to create time table
    df.createOrReplaceTempView("time")
    time_table = spark.sql("""select timestamp as start_time,
                       extract(hour from timestamp) as hour,
                       extract(day from timestamp) as day,
                       extract(week from timestamp) as week,
                       extract(month from timestamp) as month,
                       extract(year from timestamp) as year,
                       extract(dayofweek from timestamp) as weekday
                       from time""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month')\
                .parquet(os.path.join(output_data,"time.parquet"),"overwrite")

    # read in song data to use for songplays table
    song_df = song_data

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.artist == song_df.artist_id),'left')\
                      .selectExpr("timestamp AS start_time", "userId AS user_id", "level", "song_id",
                                                "artist_id", "sessionId AS session_id", "location",
                                                "userAgent     AS user_agent",
                                                "EXTRACT(month FROM start_time)      AS month",
                                                "EXTRACT(year FROM start_time)       AS year")\
                       .withColumn('songplay_id', monotonically_increasing_id())
    #write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month')\
                .parquet(os.path.join(output_data,"songplays.parquet"),"overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://must-project-bucket/"
    
    song_data = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data ,song_data)


if __name__ == "__main__":
    main()
