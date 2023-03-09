import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw'
dataset_comments = f"{dataset_bucket}/Users.xml"

rdd = spark.sparkContext.textFile(dataset_comments)

def row_parser(row):
    
    fields = [
                "Id=",
                "Reputation=",
                "CreationDate=",
                "DisplayName=",
                "LastAccessDate=",
                "WebsiteUrl=",
                "Location=",
                "AboutMe=",
                "Views=",
                "UpVotes=",
                "DownVotes=",
                "ProfileImageUrl=",
                "AccountId="
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] in ["LastAccessDate=", "CreationDate="]:
            row_field[row_list[i]] = datetime.strptime(row_list[i+1], "%Y-%m-%dT%H:%M:%S.%f")
        else:
            row_field[row_list[i]] = row_list[i+1]
        
    
    return tuple(row_field.values())


parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)

# Define the schema for the DataFrame
schema_users = StructType([
    StructField("Id", StringType()),
    StructField("Reputation", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("DisplayName", StringType()),
    StructField("LastAccessDate", TimestampType()),
    StructField("WebsiteUrl", StringType()),
    StructField("Location", StringType()),
    StructField("AboutMe", StringType()),
    StructField("Views", StringType()),
    StructField("UpVotes", StringType()),
    StructField("DownVotes", StringType()),
    StructField("ProfileImageUrl", StringType()),
    StructField("AccountId", StringType())
])


# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_users)

# Changing the data type 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('Reputation', F.col('Reputation').cast('int')) \
    .withColumn('Views', F.col('Views').cast('int')) \
    .withColumn('UpVotes', F.col('UpVotes').cast('int')) \
    .withColumn('DownVotes', F.col('DownVotes').cast('int')) \
    .withColumn('AccountId', F.col('AccountId').cast('int')) 


# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed'
output_folder_name = f"{output_bucket}/Users-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

df.show()

df.count()

