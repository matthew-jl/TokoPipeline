import sys
import os
import boto3
import json
import google.generativeai as genai
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lower, regexp_replace, split, explode, collect_list
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from datetime import datetime

def get_gemini_api_key():
    secret_name = "prod/gemini/api_key"
    region_name = "ap-southeast-2"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)['GEMINI_API_KEY']
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

def generate_summary_with_gemini(api_key, keywords, sentiment_category):
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash-lite')

    keyword_str = ", ".join(keywords)

    prompt = f"""
    You are a helpful assistant who summarizes customer feedback.
    Based on the following highly relevant keywords extracted from many customer reviews, generate a bulleted list of the main {sentiment_category}.
    The summary should be concise, easy to read, and written in a natural tone.

    KEYWORDS:
    ---
    {keyword_str}
    ---
    
    SUMMARY OF {sentiment_category.upper()}:
    """

    try:
        print(f"Calling Gemini API to summarize {sentiment_category} with keywords: {keyword_str}")
        response = model.generate_content(prompt)
        summary = response.text.replace('*', '').strip()
        print("Successfully received summary from Gemini.")
        return summary.split('\n')
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return [f"Could not generate summary due to an API error: {e}"]

# Boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data Ingestion
s3_input_path = args['s3_input_path']
product_name = os.path.splitext(os.path.basename(s3_input_path))[0]
print(f"Starting ETL and Summarization job for product: {product_name}")

dynamic_frame_raw = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", format="json", connection_options={"paths": [s3_input_path]}
)
if dynamic_frame_raw.count() == 0:
    print("No data found. Exiting.")
    job.commit()
    sys.exit(0)

df_raw = dynamic_frame_raw.toDF()

# Data Transformation
print("Applying transformations...")
df_with_types = df_raw.withColumn("star_rating_int", col("star_rating").cast(IntegerType()))

df_with_sentiment = df_with_types.withColumn("sentiment",
    when(col("star_rating_int") > 3, "positive")
    .when(col("star_rating_int") == 3, "neutral")
    .otherwise("negative")
)
df_final = df_with_sentiment.select("review_text", "sentiment")

# Feature Engineering
print("Starting feature engineering: Word Frequency Analysis")
stop_words = [
    'a', 'an', 'and', 'the', 'is', 'it', 'in', 'on', 'for', 'with', 'to', 'of',
    'dan', 'di', 'ini', 'itu', 'yg', 'yang', 'ke', 'dari', 'dengan', 'saya', 'ga', 'gak',
    'tidak', 'tapi', 'sudah', 'belum', 'juga', 'sangat', 'sekali', 'banget', 'untuk'
]

df_words = df_final.withColumn("word", explode(split(
    lower(regexp_replace(col("review_text"), r'[^A-Za-z\s]', '')),
    " "
)))

df_filtered_words = df_words.filter(~col("word").isin(stop_words) & (col("word") != "") & (col("word").isNotNull()))

df_word_counts = df_filtered_words.groupBy("sentiment", "word").count()

window_spec = Window.partitionBy("sentiment").orderBy(col("count").desc())
df_top_words = df_word_counts.withColumn("rank", col("count")).where(col("rank") <= 10)

df_keywords = df_top_words.groupBy("sentiment").agg(collect_list("word").alias("keywords"))

print("Collecting top keywords to driver...")
collected_keywords = df_keywords.collect()
# Example result: [Row(sentiment='positive', keywords=[...]), ...]

# Summarization
keywords_by_sentiment = {row['sentiment']: row['keywords'] for row in collected_keywords}

api_key = get_gemini_api_key()

summary_pros = []
if 'positive' in keywords_by_sentiment:
    summary_pros = generate_summary_with_gemini(api_key, keywords_by_sentiment['positive'], "Pros")
else:
    print("No positive keywords found to summarize.")

summary_cons = []
if 'negative' in keywords_by_sentiment:
    summary_cons = generate_summary_with_gemini(api_key, keywords_by_sentiment['negative'], "Cons")
else:
    print("No negative keywords found to summarize.")

final_summary_json = {
    "product_name": product_name,
    "last_updated_utc": datetime.utcnow().isoformat(),
    "generation_inputs": {
        "positive_keywords": keywords_by_sentiment.get('positive', []),
        "negative_keywords": keywords_by_sentiment.get('negative', []),
        "neutral_keywords": keywords_by_sentiment.get('neutral', [])
    },
    "summary": {
        "pros": summary_pros,
        "cons": summary_cons
    }
}

print("Writing final JSON summary to S3...")
s3_client = boto3.client('s3')
s3_bucket = s3_input_path.split('/')[2]
summary_output_key = f"summaries/{product_name}.json"
s3_client.put_object(
    Bucket=s3_bucket,
    Key=summary_output_key,
    Body=json.dumps(final_summary_json, indent=4),
    ContentType='application/json'
)
print(f"Successfully wrote summary to s3://{s3_bucket}/{summary_output_key}")

job.commit()