import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_comments = f"{dataset_bucket}/PostHistory.xml"

def row_parser(row):
    
    fields = [
                "Id=",
                "PostHistoryTypeId=",
                "PostId=",
                "RevisionGUID=",
                "CreationDate=",
                "UserId=",
                "UserDisplayName=",
                "Comment=",
                "Text=",
                "ContentLicense=",
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] == 'CreationDate=':
            row_field[row_list[i]] = datetime.strptime(row_list[i+1], "%Y-%m-%dT%H:%M:%S.%f")
        
        else:
            row_field[row_list[i]] = row_list[i+1]
    
    return tuple(row_field.values())

rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)
   
# Define the schema for the DataFrame
schema_posthistory = StructType([
    StructField("Id", StringType()),
    StructField("PostHistoryTypeId", StringType()),
    StructField("PostId", StringType()),
    StructField("RevisionGUID", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("UserId", StringType()),
    StructField("UserDisplayName", StringType()),
    StructField("Comment", StringType()),
    StructField("Text", StringType()),
    StructField("ContentLicense", StringType())
])


# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_posthistory)

# Changing the DF datatype/schema 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('PostHistoryTypeId', F.col('PostHistoryTypeId').cast('int')) \
    .withColumn('PostId', F.col('PostId').cast('int')) \
    .withColumn('UserId', F.col('UserId').cast('int')) 
    
    
# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed'
output_folder_name = f"{output_bucket}/PostHistory-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

df.show()
print(df.count())
