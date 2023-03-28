import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime

spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Dataset path 
dataset_bucket = 's3://stackoverflow-dataset-2023/dataset/raw/2023/'
dataset_comments = f"{dataset_bucket}/Posts.xml"

def row_parser(row):
    
    fields = ['Id=',
             'PostTypeId=',
             'ParentId=',
             'AcceptedAnswerId=',
             'CreationDate=',
             'DeletionDate=', 
             'Score=',
             'ViewCount=',
             'Body=',
             'OwnerUserId=',
             'OwnerDisplayName=',
             'LastEditorUserId=',
             'LastEditorDisplayName=',
             'LastEditDate=',
             'LastActivityDate=',
             'Title=',
             'Tags=',
             'AnswerCount=',
             'CommentCount=',
             'FavoriteCount=',
             'ClosedDate=',
             'CommunityOwnedDate=', 
             'ContentLicense=']
    
    row_field = dict.fromkeys(fields, None)
    row_list = [ i.strip() for i in row.split('"')[:-1] ]
    
    for i in range(0, len(row_list), 2):
        if row_list[i] in ['ClosedDate=', 'CreationDate=', 'LastEditDate=', 'LastActivityDate=', 'CommunityOwnedDate=', 'DeletionDate=']:
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
schema_post = StructType([
    StructField('Id', StringType()),
    StructField('PostTypeId', StringType()),
    StructField('ParentId', StringType()),
    StructField('AcceptedAnswerId', StringType()),
    StructField('CreationDate', TimestampType()),
    StructField('DeletionDate', TimestampType()),
    StructField('Score', StringType()),
    StructField('ViewCount', StringType()),
    StructField('Body', StringType()),
    StructField('OwnerUserId', StringType()),
    StructField('OwnerDisplayName', StringType()),
    StructField('LastEditorUserId', StringType()),
    StructField('LastEditorDisplayName', StringType()),
    StructField('LastEditDate', TimestampType()),
    StructField('LastActivityDate', TimestampType()),
    StructField('Title', StringType()),
    StructField('Tags', StringType()),
    StructField('AnswerCount', StringType()),
    StructField('CommentCount', StringType()),
    StructField('FavoriteCount', StringType()),
    StructField('ClosedDate', TimestampType()),
    StructField('CommunityOwnedDate', TimestampType()),
    StructField('ContentLicense', StringType())
])


# Convert the RDD to a DataFrame
df = parsed_rdd.toDF(schema_post)

# Changing the DF datatype/schema 
df = df \
    .withColumn('Id', F.col('Id').cast('int')) \
    .withColumn('PostTypeId', F.col('PostTypeId').cast('int')) \
    .withColumn('ParentId', F.col('ParentId').cast('int')) \
    .withColumn('AcceptedAnswerId', F.col('AcceptedAnswerId').cast('int')) \
    .withColumn('Score', F.col('Score').cast('int')) \
    .withColumn('ViewCount', F.col('ViewCount').cast('int')) \
    .withColumn('OwnerUserId', F.col('OwnerUserId').cast('int')) \
    .withColumn('LastEditorUserId', F.col('LastEditorUserId').cast('int')) \
    .withColumn('AnswerCount', F.col('AnswerCount').cast('int')) \
    .withColumn('CommentCount', F.col('CommentCount').cast('int')) \
    .withColumn('FavoriteCount', F.col('FavoriteCount').cast('int')) 
    
    
# Dataset path 
output_bucket = 's3://stackoverflow-dataset-2023/dataset/raw-processed/2023'
output_folder_name = f"{output_bucket}/Post-parquet"

# save dataframe as csv
df.write \
  .format('parquet') \
  .option('header', True) \
  .mode('overwrite') \
  .save(output_folder_name)

df.show()
print(df.count())
