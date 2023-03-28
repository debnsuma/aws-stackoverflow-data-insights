import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path (INPUT)
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/2023'
dataset_file = f"{dataset_bucket}Comments.xml"

# Processed data path (OUTPUT)
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed/2023'
output_folder_name = f"{output_bucket}/Comments-parquet"

def row_parser(row):
    
    fields = [
                "Id=",
                "PostId=",
                "Score=",
                "Text=",
                "CreationDate=",
                "UserDisplayName=",
                "UserId=",
                "ContentLicense="
            ]
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] == 'CreationDate=':
            row_field[row_list[i]] = datetime.strptime(row_list[i+1], "%Y-%m-%dT%H:%M:%S.%f")        
        else:
            row_field[row_list[i]] = row_list[i+1]
        
    
    return tuple(row_field.values())
  
# Load the data as an RDD
rdd = spark.sparkContext.textFile(dataset_file)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)
   
   
# Define the schema for the DataFrame
comments_schema = StructType([
    StructField("Id", StringType()),
    StructField("PostId", StringType()),
    StructField("Score", StringType()),
    StructField("Text", StringType()),
    StructField("CreationDate", TimestampType()),
    StructField("UserDisplayName", StringType()),
    StructField("UserId", StringType()),
    StructField("ContentLicense", StringType())
])

# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(comments_schema)

# Changing the DF DataType 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('PostId', F.col('PostId').cast('int')) \
    .withColumn('Score', F.col('Score').cast('int')) \
    .withColumn('UserId', F.col('UserId').cast('int')) 
    

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
print(df.count())


