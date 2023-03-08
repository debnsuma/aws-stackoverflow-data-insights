import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_comments = f"{dataset_bucket}/Votes.xml"

rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(lambda row: (int(row.split('"')[1]), int(row.split('"')[3]), int(row.split('"')[5]), F.to_str(row.split('"')[7]))) 

# Define the schema for the DataFrame
schema_votes = StructType([
    StructField("Id", LongType()),
    StructField("PostId", LongType()),
    StructField("VoteTypeId", LongType()),
    StructField("CreationDate", StringType())
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_votes)

# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed'
output_folder_name = f"{output_bucket}/Votes-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

