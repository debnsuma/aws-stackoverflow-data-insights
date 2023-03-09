import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw'
dataset_file = f"{dataset_bucket}/Votes.xml"

rdd = spark.sparkContext.textFile(dataset_file)

def row_parser(row):
    
    fields = [
                "Id=",
                "PostId=",
                "VoteTypeId=",
                "UserId=",
                "TagName=",
                "Count=",
                "CreationDate=",
                "ExcerptPostId=",
                "WikiPostId=",
                "BountyAmount=", 
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] == 'CreationDate=':
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
schema_votes = StructType([
    StructField("Id", StringType()),
    StructField("PostId", StringType()),
    StructField("VoteTypeId", StringType()),
    StructField("UserId", StringType()),
    StructField("TagName", StringType()),
    StructField("Count", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("ExcerptPostId", StringType()),
    StructField("WikiPostId", StringType()),
    StructField("BountyAmount", StringType()),
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_votes)

# Change the DF datatype/schema 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('PostId', F.col('PostId').cast('int')) \
    .withColumn('VoteTypeId', F.col('VoteTypeId').cast('int')) \
    .withColumn('Count', F.col('Count').cast('int')) \
    .withColumn('ExcerptPostId', F.col('ExcerptPostId').cast('int')) \
    .withColumn('WikiPostId', F.col('WikiPostId').cast('int')) \
    .withColumn('BountyAmount', F.col('BountyAmount').cast('int')) 
    

# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed'
output_folder_name = f"{output_bucket}/Votes-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

df.show()
print(df.count())
