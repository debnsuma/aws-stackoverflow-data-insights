import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_comments = f"{dataset_bucket}/Tags.xml"

rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(lambda row: (int(row.split('"')[1]), row.split('"')[3], int(row.split('"')[5]), int(row.split('"')[7]), int(row.split('"')[9])) if len(row) == 11 else (int(row.split('"')[1]), row.split('"')[3], int(row.split('"')[5]), None, None)) 
   
# Define the schema for the DataFrame
schema_tags = StructType([
    StructField("Id", LongType()),
    StructField("TagName", StringType()),
    StructField("Count", LongType()),
    StructField("ExcerptPostId", LongType()),
    StructField("WikiPostId", LongType())
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_tags)

# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed'
output_folder_name = f"{output_bucket}/Tags-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

## Verifying the data by reading from S3 
df = spark.read \
         .option("header", True) \
         .option("inferSchema", True) \
         .parquet(output_folder_name)
         
df.show()