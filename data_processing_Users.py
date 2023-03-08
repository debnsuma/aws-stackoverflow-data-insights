import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw'
dataset_comments = f"{dataset_bucket}/Users.xml"


def row_parser(row):
    row_len = len(row.split('"')) 
    result = [None] * 13
    
    
    if row_len == 27:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],  
                  row.split('"')[17],
                  row.split('"')[19], 
                  row.split('"')[21],  
                  row.split('"')[23],
                  row.split('"')[25], 
        ) 

    elif row_len == 25:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],  
                  row.split('"')[17],
                  row.split('"')[19], 
                  row.split('"')[21],  
                  row.split('"')[23],
                  None
        ) 

    elif row_len == 23:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],  
                  row.split('"')[17],
                  row.split('"')[19], 
                  row.split('"')[21],
                  None, 
                  None
        ) 

    elif row_len == 21:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],  
                  row.split('"')[17],
                  row.split('"')[19],
                  None,
                  None,
                  None
                  )      
        
    elif row_len == 19:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],  
                  row.split('"')[17],
                  None,
                  None,
                  None,
                  None
                  )      
        
    elif row_len == 17:
        result = (int(row.split('"')[1]), 
                  int(row.split('"')[3]),
                  datetime.strptime(row.split('"')[5], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[7], 
                  datetime.strptime(row.split('"')[9], "%Y-%m-%dT%H:%M:%S.%f"), 
                  row.split('"')[11], 
                  row.split('"')[13], 
                  row.split('"')[15],
                  None,
                  None,
                  None,
                  None, 
                  None
                  )          
    

                    
        
    return result

rdd = spark.sparkContext.textFile(dataset_comments)

parsed_rdd = rdd.map(lambda row: row.strip()) \
   .filter(lambda row: row.startswith("<row")) \
   .map(lambda row: row[4:-3]) \
   .map(lambda row: row.strip()) \
   .map(row_parser)
   
# Define the schema for the DataFrame
schema_users = StructType([
    StructField("Id", LongType()),
    StructField("Reputation", LongType()),
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
