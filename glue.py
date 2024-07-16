import boto3
import os

s3_client = boto3.client('s3')

def handler(event, context):
    glue = boto3.client('glue')
    job_name = os.environ['GLUE_JOB_NAME']
    processed_bucket_name = 'processed-json-youtube'

    # Extract the bucket name and object key from the event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Start Glue job
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--BUCKET_NAME': bucket,
                '--OBJECT_KEY': key,
                '--enable-continuous-cloudwatch-log': 'true'
            }
        )
        job_run_id = response["JobRunId"]
        print(f'Started Glue job: {job_run_id}')
        