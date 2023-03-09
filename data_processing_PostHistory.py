import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/'
dataset_comments = f"{dataset_bucket}/PostHistory.xml"


def row_parser(row):
    row_len = len(row.split('"')) 
    result = [None] * 10

    if row_len == 11:
        result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  None,
                  None,
                  None,
                  None,
                  None
                )  
        
    elif row_len == 13:
        result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  int(row.split('"')[11]) if row.split('"')[11] and row.split('"')[10].strip() == 'UserId=' else None, 
                  None,
                  None,
                  None,
                  None
                )  
    
    elif row_len == 15:
        result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  int(row.split('"')[11]) if row.split('"')[11] and row.split('"')[10].strip() == 'UserId=' else None, 
                  None,
                  None,
                  None,
                  row.split('"')[13] if row.split('"')[13] else None
                )     
        
    elif row_len == 17:
         result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  int(row.split('"')[11]) if row.split('"')[11] and row.split('"')[10].strip() == 'UserId=' else None, 
                  None,
                  None,
                  row.split('"')[13] if row.split('"')[13] else None, 
                  row.split('"')[15] if row.split('"')[15] else None 
                )    

    elif row_len == 19:
         result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  int(row.split('"')[11]) if row.split('"')[11] and row.split('"')[10].strip() == 'UserId=' else None, 
                  row.split('"')[13] if row.split('"')[13] else None, 
                  None,
                  row.split('"')[15] if row.split('"')[15] else None, 
                  row.split('"')[17] if row.split('"')[17] else None
                ) 

    elif row_len == 21:
         result = (int(row.split('"')[1]) if row.split('"')[1] else None, 
                  int(row.split('"')[3]) if row.split('"')[3] else None, 
                  int(row.split('"')[5]) if row.split('"')[5] else None, 
                  row.split('"')[7] if row.split('"')[7] else None,
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  int(row.split('"')[11]) if row.split('"')[11] and row.split('"')[10].strip() == 'UserId=' else None, 
                  row.split('"')[13] if row.split('"')[13] else None, 
                  row.split('"')[15] if row.split('"')[15] else None, 
                  row.split('"')[17] if row.split('"')[17] else None,
                  row.split('"')[19] if row.split('"')[17] else None
                ) 
         
    return result

rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)
   
# Define the schema for the DataFrame
schema_posthistory = StructType([
    StructField("Id", LongType()),
    StructField("PostHistoryTypeId", LongType()),
    StructField("PostId", LongType()),
    StructField("RevisionGUID", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("UserId", LongType()),
    StructField("UserDisplayName", StringType()),
    StructField("Comment", StringType()),
    StructField("Text", StringType()),
    StructField("ContentLicense", StringType())
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_posthistory)

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
