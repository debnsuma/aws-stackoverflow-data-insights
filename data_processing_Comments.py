import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_comments = f"{dataset_bucket}Comments.xml"

# Processed data path (OUTPUT)
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed/'
output_folder_name = f"{output_bucket}Comments-parquet"


# Load the data as an RDD
rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
                .filter(lambda row: row.startswith("<row")) \
                .filter(lambda row: "UserDisplayName=" not in row) \
                .map(lambda row: row[4:-3]) \
                .map(lambda row: row.strip()) \
                .filter(lambda row: len(row.split('"')) == 15) \
                .map(lambda row: (int(row.split('"')[1]), int(row.split('"')[3]), int(row.split('"')[5]), row.split('"')[7], datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), int(row.split('"')[11]), row.split('"')[13])) 

# Define the schema for the DataFrame
my_schema = StructType([
    StructField("Id", LongType()),
    StructField("PostId", LongType()),
    StructField("Score", LongType()),
    StructField("Text", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("UserId", LongType()),
    StructField("ContentLicense", StringType())
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(my_schema)

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(my_schema)

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)
  
# Verify the data by reading the data from S3 
df_comments = spark.read \
         .option("header", True) \
         .option("inferSchema", True) \
         .parquet(output_folder_name)
         
df.show()

