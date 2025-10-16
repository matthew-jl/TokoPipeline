import json
import os
import boto3
import urllib.parse

glue_client = boto3.client('glue')

# triggered by an S3 'ObjectCreated' event
# extracts file etails and starts an AWS Glue job
def lambda_handler(event, context):
    print("Lambda function started. Received event:")
    print(json.dumps(event))

    # 1. Extract Bucket Name and File Key from the S3 Event
    try:
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        
        file_key = urllib.parse.unquote_plus(s3_record['object']['key'])
        
        # Prevents the Lambda from triggering on its own log files or other uploads
        if not file_key.startswith('raw-reviews/'):
            print(f"File {file_key} is not in the 'raw-reviews/' folder. Skipping trigger.")
            return {
                'statusCode': 200,
                'body': json.dumps('File is not a raw review, trigger skipped.')
            }

    except KeyError as e:
        print(f"Error: Could not parse S3 event data. Missing key: {e}")
        raise e

    # 2. Construct the Full S3 Path and Extract Product Name
    
    s3_input_path = f"s3://{bucket_name}/{file_key}"

    try:
        filename = os.path.basename(file_key)
        product_name = os.path.splitext(filename)[0]
    except Exception as e:
        print(f"Error: Could not extract product name from file key: {file_key}")
        raise e

    # 3. Start the AWS Glue Job
    
    glue_job_name = 'tokopedia_glue_transform' # <-- MUST MATCH job name

    job_arguments = {
        '--s3_input_path': s3_input_path,
    }

    print(f"Starting Glue job '{glue_job_name}' with arguments: {job_arguments}")

    try:
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments=job_arguments
        )
        
        job_run_id = response['JobRunId']
        print(f"Successfully started Glue job run. Run ID: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully started Glue job {glue_job_name} with Run ID: {job_run_id}")
        }

    except Exception as e:
        print(f"Error: Failed to start Glue job '{glue_job_name}'")
        print(e)
        raise e