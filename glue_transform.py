import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

def get_latest_s3_object_key(bucket, prefix):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            print(f"No objects found in s3://{bucket}/{prefix}")
            return None
        latest_object = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)[0]
        print(f"Found latest file to process: {latest_object['Key']}")
        return latest_object['Key']
    except Exception as e:
        print(f"Error finding latest object in S3: {e}")
        return None

# Boilerplate for Glue and Spark contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# REPLACE WITH YOUR BUCKET PATH
s3_bucket = "tokopedia-reviews-matthewjl"
s3_prefix = "raw-reviews/"

latest_file_key = get_latest_s3_object_key(s3_bucket, s3_prefix)
if not latest_file_key:
    print("ETL process stopped: No new file to process.")
    job.commit()
    sys.exit(0)

s3_input_path = f"s3://{s3_bucket}/{latest_file_key}"
print(f"Reading raw JSON data from single file: {s3_input_path}")

dynamic_frame_raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [s3_input_path]
    },
    transformation_ctx="dynamic_frame_raw"
)

if dynamic_frame_raw.count() == 0:
    print("No data found in the source file. Exiting job.")
    job.commit()
    sys.exit(0)

print(f"Successfully read {dynamic_frame_raw.count()} records from S3.")

print("--- Inferred Schema from DynamicFrame ---")
dynamic_frame_raw.printSchema()

# Convert to Spark DataFrame for transformation
df_raw = dynamic_frame_raw.toDF()

print("\n--- Sample of Data in Spark DataFrame (Top 5 rows) ---")
df_raw.show(5, truncate=False)

print("Applying transformations: Casting data types and adding sentiment column.")
df_with_types = df_raw.withColumn("star_rating_int", col("star_rating").cast(IntegerType()))
df_with_sentiment = df_with_types.withColumn("sentiment",
    when(col("star_rating_int") > 3, "positive")
    .when(col("star_rating_int") == 3, "neutral")
    .otherwise("negative")
)
df_final = df_with_sentiment.select(
    col("review_text"),
    col("star_rating_int").alias("star_rating"), # Rename back to star_rating
    col("sentiment")
)
print("\n--- Final Transformed DataFrame Schema ---")
df_final.printSchema()
print("\n--- Sample of Final Data (Top 10 rows) ---")
df_final.show(10, truncate=False)

print("Preparing to write data to the clean zone...")
s3_output_path = f"s3://{s3_bucket}/clean-reviews-parquet/"
# convert the Spark DataFrame back to a Glue DynamicFrame
dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "dynamic_frame_final")

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_final,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": s3_output_path
    },
    transformation_ctx="datasink"
)
print(f"Successfully wrote clean data to: {s3_output_path}")

job.commit()