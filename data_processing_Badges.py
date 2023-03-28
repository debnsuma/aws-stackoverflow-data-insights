from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType, BooleanType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/2023'
dataset_comments = f"{dataset_bucket}/Badges.xml"

def row_parser(row):
    
    fields = [
                "Id=",
                "UserId=",
                "Name=",
                "Date=",
                "Class=",
                "TagBased="
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] == 'Date=':
            row_field[row_list[i]] = datetime.strptime(row_list[i+1], "%Y-%m-%dT%H:%M:%S.%f")
        elif row_list[i] == 'TagBased=':
            row_field[row_list[i]] = True if row_list[i+1].lower() == 'true'  else False 
        
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
schema_badges = StructType([
    StructField("Id", StringType()),
    StructField("UserId", StringType()),
    StructField("Name", StringType()),
    StructField("Date", TimestampType()),
    StructField("Class", StringType()),
    StructField("TagBased", BooleanType()),
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_badges)

# Change the DF datatype 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('UserId', F.col('UserId').cast('int')) 
    
# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed/2023'
output_folder_name = f"{output_bucket}/Badges-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

df.show()
df.count()

