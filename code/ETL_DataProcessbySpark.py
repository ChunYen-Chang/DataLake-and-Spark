# import the needed packages
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Doub, StringType as Str, LongType as Long, TimestampType


# define functions
def create_spark_session():
    """
    Description: This function create a spark session
    Parameters: None
    Return: None
    note: In config option, it loads "org.apache.hadoop:hadoop-aws:2.7.0". The function of this packages is
          helping us to load the data from AWS S3
    """
    print('create spark session....')    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function helps us to read the song_data from S3, put this data into spark dataframe
                 ,extract columns from this dataframe to form "songinf table" and "artist table", transform 
                 "songinf table data" and "artist table data" into a format that this project needs.
    Parameters: -spark: spark session
                -input_data: location of song_data json file (in S3 bucket)
                -output_data: location that the final table will be saved (in S3 bucket)
    Return: None
    """
    #--------------------read song data--------------------#
    print('Read song_data...')
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data_*.json")
    
    # define the song data schema for reading
    SongSchema = R([
                    Fld("artist_id",Str()),
                    Fld("artist_latitude",Doub()),
                    Fld("artist_location",Str()),
                    Fld("artist_longitude",Doub()),
                    Fld("artist_name",Str()),
                    Fld("duration",Doub()),
                    Fld("num_songs",Long()),
                    Fld("song_id",Str()),
                    Fld("title",Str()),
                    Fld("year",Long())
                    ])
    
    # read song data file
    song_df = spark.read.json(song_data, schema=SongSchema)
    
    #--------------------deal with song table--------------------#
    # extract columns to create songinf df
    songinf_df = song_df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songinf_df = songinf_df.dropDuplicates(['song_id'])
    songinf_df = songinf_df.dropna(how = "any", subset = ["song_id"])
    songinf_df = songinf_df.filter(songinf_df.song_id != "")
    
    print('Songs table: ')
    print(songinf_df.sort('song_id').show(5))
    
    # write songs table to parquet files partitioned by year and artist
    print('Save Songs table into S3...')
    songinf_df.write.partitionBy("year", "artist_id").parquet("{}/song_table.parquet".format(output_data))

    #--------------------deal with artists table--------------------#
    # extract columns to create artists df
    artist_df = song_df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    artist_df = artist_df.dropDuplicates(['artist_id'])
    artist_df = artist_df.dropna(how = "any", subset = ["artist_id"])
    artist_df = artist_df.filter(artist_df.artist_id != "")
    
    print('artists table: ')
    print(artist_df.sort('artist_id').show(5))
    
    # write artists table to parquet files
    print('Save artists table into S3...')
    artist_df.write.parquet("{}/artist_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Description: This function helps us to read the log_data from S3, put this data into spark dataframe
                 ,extract columns from this dataframe to form "user table" and "time table", transform 
                 "user table data" and "time table data" into a format that this project needs.
    Parameters: -spark: spark session
                -input_data: location of log_data json file (in S3 bucket)
                -output_data: location that the final table will be saved (in S3 bucket)
    Return: None
    """
    #--------------------read log data--------------------#
    print('Read log_data...')
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data_*.json")
    
    # define the log data schema for reading
    LogSchema = R([
                    Fld("artist",Str()),
                    Fld("auth",Str()),
                    Fld("firstName",Str()),
                    Fld("gender",Str()),
                    Fld("itemInSession",Long()),
                    Fld("lastName",Str()),
                    Fld("length",Doub()),
                    Fld("level",Str()),
                    Fld("location",Str()),
                    Fld("method",Str()),
                    Fld("page",Str()),
                    Fld("registration",Doub()),
                    Fld("sessionId",Long()),
                    Fld("song",Str()),
                    Fld("status",Long()),
                    Fld("ts",Long()),
                    Fld("userAgent",Str()),
                    Fld("userId",Str()),
                    ])
    
    # read log data file
    log_df = spark.read.json(log_data)

    #--------------------deal with user table--------------------#
    # extract columns for user df and drop the duplicated value and None value
    user_df = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    user_df = user_df.dropDuplicates(['userId'])
    user_df = user_df.dropna(how = "any", subset = ["userId"])
    user_df = user_df.filter(user_df.userId != "")
    
    print('User table: ')
    print(user_df.sort('userId').show(5))
        
    # write users table to parquet files
    print('Save User table into S3...')
    user_df.write.parquet("{}/user_table.parquet".format(output_data))

    #--------------------deal with time table--------------------#
    # define convert_timestamp function for convert timestamp format to datetime format
    def convert_timestamp(x):
        datetime_data = datetime.fromtimestamp(x/1000)
        return datetime_data
    
    # register convert_timestamp function for spark session by udf
    convert_timestamp_udf=udf(convert_timestamp, TimestampType())
    
    # extract columns for time df and drop the duplicated value and None value
    time_df = log_df.select(['ts'])
    time_df = time_df.dropDuplicates(['ts'])
    time_df = time_df.dropna(how = "any", subset = ["ts"])
    
    # use udf function defined above to convert the value in "ts column" into datetime format
    time_df = time_df.withColumn('start_time',convert_timestamp_udf('ts'))
    
    # use pyspark.sql.functions to extract hour, day, week, month, year, weekday
    time_df = time_df.withColumn('hour', F.hour('start_time'))
    time_df = time_df.withColumn('day', F.dayofmonth('start_time'))
    time_df = time_df.withColumn('week', F.weekofyear('start_time'))
    time_df = time_df.withColumn('month', F.month('start_time'))
    time_df = time_df.withColumn('year', F.year('start_time'))
    time_df = time_df.withColumn('weekday', F.dayofweek('start_time'))
    
    # select the necessary column for final time table
    time_df = time_df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])

    print('Time table: ')
    print(time_df.show(5))

    # write time table to parquet files partitioned by year and month
    print('Save Time table into S3...')
    time_df.write.partitionBy("year", "month").parquet("{}/time_table.parquet".format(output_data))
    
    #--------------------deal with song_play table--------------------#
    # since song_play table need the join result of song_data and log_data, we need to read song_data again.
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # define the song data schema for reading
    SongSchema = R([
                    Fld("artist_id",Str()),
                    Fld("artist_latitude",Doub()),
                    Fld("artist_location",Str()),
                    Fld("artist_longitude",Doub()),
                    Fld("artist_name",Str()),
                    Fld("duration",Doub()),
                    Fld("num_songs",Long()),
                    Fld("song_id",Str()),
                    Fld("title",Str()),
                    Fld("year",Long())
                    ])
    
    # read song data file
    song_df = spark.read.json(song_data, schema=SongSchema)
        
    # filter by actions for song plays
    log_df_filter = log_df.where(log_df.page == 'NextSong')
    
    # create start_time column by using udf function defined above
    log_df_filter = log_df_filter.withColumn('start_time',convert_timestamp_udf('ts'))
    
    # join song_df and log_df_filter
    cond = [log_df_filter.artist == song_df.artist_name, 
            log_df_filter.song == song_df.title,
            log_df_filter.length == song_df.duration]

    songplay_df = log_df_filter.join(song_df, cond) \
                            .select([F.monotonically_increasing_id().alias('songplay_id'),
                                      log_df_filter.start_time,
                                      log_df_filter.userId,
                                      log_df_filter.level,
                                      song_df.song_id,
                                      song_df.artist_id,
                                      log_df_filter.sessionId,
                                      log_df_filter.location,
                                      log_df_filter.userAgent])
    
    print('Song_play table: ')
    print(songplay_df.show(10))
    
    # write songplays table to parquet files partitioned by year and month
    songplay_df.write.partitionBy("year", "month").parquet("{}/songplay_table.parquet".format(output_data))

        
def main():
    """
    Description: This function helps us to create a spark session, use Spark to load song_data and log_data
                 from a data lake (AWS S3), transform song_data and log_data into a format which fits our 
                 data model (dimensional model: one fact table-songplay table, four dimensional table-user,
                 artist, time, song table), and save the result back to S3 bucket    
    Parameters: None
    Return: None
    """
    spark = create_spark_session()
    input_data = "s3a://musiccompany_raw_data/"
    output_data = "s3a://musiccompany_clean_data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
