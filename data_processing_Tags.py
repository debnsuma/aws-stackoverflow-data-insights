import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_file = f"{dataset_bucket}/Tags.xml"

rdd = spark.sparkContext.textFile(dataset_file)

def row_parser(row):
    
    fields = [
                "Id=",
                "TagName=",
                "Count=",
                "ExcerptPostId=",
                "WikiPostId="
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        row_field[row_list[i]] = row_list[i+1]
    
    
    return tuple(row_field.values())
  
parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)
   
    
# Define the schema for the DataFrame
schema_tags = StructType([
    StructField("Id", StringType()),
    StructField("TagName", StringType()),
    StructField("Count", StringType()),
    StructField("ExcerptPostId", StringType()),
    StructField("WikiPostId", StringType())
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
print(df.count())