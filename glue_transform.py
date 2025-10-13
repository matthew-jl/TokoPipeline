import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# REPLACE WITH YOUR BUCKET
s3_input_path = "s3://tokopedia-reviews-matthewjl/raw-reviews/" 

print(f"Reading raw JSON data from: {s3_input_path}")

dynamic_frame_raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [s3_input_path],
        "recurse": True
    },
    transformation_ctx="dynamic_frame_raw"
)

if dynamic_frame_raw.count() == 0:
    print("No data found in the source directory. Exiting job.")
    sys.exit(0)

print(f"Successfully read {dynamic_frame_raw.count()} records from S3.")

print("--- Inferred Schema from DynamicFrame ---")
dynamic_frame_raw.printSchema()

df_raw = dynamic_frame_raw.toDF()

print("\n--- Sample of Data in Spark DataFrame (Top 5 rows) ---")
df_raw.show(5, truncate=False)

# TODO: transformation logic

job.commit()