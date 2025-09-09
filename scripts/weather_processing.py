import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType
from pyspark.sql.functions import col, lit, explode, to_date, from_unixtime, to_timestamp, max as spark_max
import boto3
from datetime import datetime, timedelta

## Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FULL_LOAD'])

weather_schema = StructType([
    StructField("id", LongType(), True),
    StructField("main", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
])

schema = StructType([
    StructField("base", StringType(), True),
    StructField("clouds", StructType([
        StructField("all", LongType(), True)
    ])),
    StructField("cod", LongType(), True),
    StructField("coord", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])),
    StructField("dt", LongType(), True),
    StructField("id", LongType(), True),
    StructField("main", StructType([
        StructField("feels_like", DoubleType(), True),
        StructField("grnd_level", LongType(), True),
        StructField("humidity", LongType(), True),
        StructField("pressure", LongType(), True),
        StructField("sea_level", LongType(), True),
        StructField("temp", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("temp_min", DoubleType(), True)
    ])),
    StructField("name", StringType(), True),
    StructField("rain", StructType([
        StructField("1h", DoubleType(), True)
    ])),
    StructField("sys", StructType([
        StructField("country", StringType(), True),
        StructField("id", LongType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True),
        StructField("type", LongType(), True)
    ])),
    StructField("timezone", LongType(), True),
    StructField("visibility", LongType(), True),
    StructField("weather", ArrayType(weather_schema, True)),
    StructField("wind", StructType([
        StructField("deg", LongType(), True),
        StructField("gust", DoubleType(), True),
        StructField("speed", DoubleType(), True)
    ]))
])

def normalize(df):
    cleaned_df = df.select(

    # direct string
    col("name").alias("CityName"),

    col("clouds.all").alias("Cloudiness"),
    col("coord.lat").alias("Latitude"),
    col("coord.lon").alias("Longitude"),
    to_timestamp(from_unixtime(col('dt'), 'yyyy-MM-dd HH:mm:ss')).alias('Timestamp'),
    col("id").alias("CityId"),
    
    # main struct
    col("main.feels_like").alias("FeelsLike"),
    col("main.grnd_level").alias("GroundLevelPressure"),
    col("main.humidity").alias("Humidity"),
    col("main.pressure").alias("Pressure"),
    col("main.sea_level").alias("SeaLevelPressure"),
    col("main.temp").alias("Temperature"),
    col("main.temp_max").alias("TempMax"),
    col("main.temp_min").alias("TempMin"),
    
    
    # rain (watch out: field name '1h' must be quoted using backticks)
    col("rain.`1h`").alias("Rain_1h"),
    
    # sys struct
    col("sys.country").alias("Country"),
    to_timestamp(from_unixtime(col('sys.sunrise'), 'yyyy-MM-dd HH:mm')).alias('Sunrise'),
    to_timestamp(from_unixtime(col('sys.sunset'), 'yyyy-MM-dd HH:mm')).alias('Sunset'),
    
    col("timezone").alias("Timezone"),
    col("visibility").alias("Visibility"),
    
    # weather array -> explode to get one row per weather description
    explode("weather").alias("Weather"),
    
    # wind struct
    col("wind.deg").alias("WindDegree"),
    col("wind.gust").alias("WindGust"),
    col("wind.speed").alias("WindSpeed"),
    
    col("date").alias("Date")
    )

    # now flatten the exploded weather struct
    final_df = cleaned_df.select(
        "*",
        col("Weather.description").alias("WeatherDescription"),
        col("Weather.icon").alias("WeatherIcon"),
        col("Weather.id").alias("WeatherId"),
        col("Weather.main").alias("WeatherMain")
    ).drop("Weather")
    
    print(f"Count of DF after normalization --> {final_df.count()}")
    return final_df

def get_last_processed_date():
    """Get last processed date from S3 state file"""
    try:
        s3_client = boto3.client('s3')
        bucket = 'weather-data-bucket-warehouse'
        key = 'state/last_processed_date.txt'
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        last_date = response['Body'].read().decode('utf-8').strip()
        print(f"Last processed date: {last_date}")
        return last_date
    except Exception as e:
        print(f"No previous state found or error reading state: {e}")
        print("Starting from default date: 2024-01-01")
        return "2024-01-01"

def get_new_partitions_to_process(spark):
    """Get list of date partitions that need processing"""
    last_processed = get_last_processed_date()
    
    try:
        # List all available date partitions in raw data
        s3_client = boto3.client('s3')
        bucket = 'weather-data-bucket-warehouse'
        prefix = 'raw/'
        
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        available_dates = []
        for obj in response.get('CommonPrefixes', []):
            folder_name = obj['Prefix'].replace(prefix, '').replace('/', '')
            if folder_name.startswith('date='):
                date_part = folder_name.replace('date=', '')
                if date_part > last_processed and date_part <= yesterday:
                    available_dates.append(date_part)
        
        available_dates.sort()
        print(f"New partitions to process: {available_dates}")
        return available_dates
        
    except Exception as e:
        print(f"Error getting partitions: {e}")
        return []

if __name__ == "__main__":
    ## Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    if args['FULL_LOAD'].lower() == 'y':
        load_type = 'FULL'
    else:
        load_type = 'DELTA'

    # Load data from S3

    if load_type == 'FULL':
        print("Performing FULL LOAD")
        weather_raw_df = spark.read.schema(schema).json("s3a://weather-data-bucket-warehouse/raw/")
        weather_normalized = normalize(weather_raw_df)
        s3_output_cleaned = "s3a://weather-data-bucket-warehouse/staged/"
        weather_normalized.write.format("parquet").partitionBy("Date").mode("overwrite").option("compression", "snappy").save(s3_output_cleaned)
        print(f"Data successfully written to S3: {s3_output_cleaned}")

        try:
            max_date = weather_normalized.select(spark_max("Date")).collect()[0][0]
            s3_client = boto3.client('s3')
            bucket = 'weather-data-bucket-warehouse'
            key = 'state/last_processed_date.txt'
            
            s3_client.put_object(Bucket=bucket, Key=key, Body=max_date)
            print(f"Updated last processed date to: {max_date}")
        except Exception as e:
            print(f"Error updating state file: {e}")

    else:
        print('Performing DELTA LOAD')
        new_dates = get_new_partitions_to_process(spark)

        if not new_dates:
            print("No new data to process. Exiting.")
            job.commit()
            sys.exit(0)
        
        paths_to_process = [f"s3a://weather-data-bucket-warehouse/raw/date={date}/" for date in new_dates]
        print(f"Processing paths: {paths_to_process}")
        
        # Load data from new partitions only
        weather_raw_df = spark.read.schema(schema).option("basePath", "s3a://weather-data-bucket-warehouse/raw/").json(paths_to_process)
        weather_normalized = normalize(weather_raw_df)

        s3_output_cleaned = "s3a://weather-data-bucket-warehouse/staged/"
    
        # Write with appropriate mode
        weather_normalized.write \
            .format("parquet") \
            .partitionBy("Date") \
            .mode('append') \
            .option("compression", "snappy") \
            .save(s3_output_cleaned)
        
        # Update last processed date in S3 state file
        last_date = new_dates[-1]
        try:
            s3_client = boto3.client('s3')
            bucket = 'weather-data-bucket-warehouse'
            key = 'state/last_processed_date.txt'
            
            s3_client.put_object(Bucket=bucket, Key=key, Body=last_date)
            print(f"Updated last processed date to: {last_date}")
        except Exception as e:
            print(f"Error updating state file: {e}")
        
    job.commit()
    print(f"Processing completed successfully! Mode: {load_type}")