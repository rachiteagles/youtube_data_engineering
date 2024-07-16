import boto3
import os

def handler(event, context):
    s3_client = boto3.client('s3')
    print("Glue Job State Change Event:", event)

    # Define the buckets
    source_bucket = os.environ['source_bucket']
    success_bucket = os.environ['success_bucket']
    failure_bucket = os.environ['failure_bucket']

    # Extract the state and object key from the event
    job_state = event['detail']['state']
    
    # List objects in the source bucket
    response = s3_client.list_objects_v2(Bucket=source_bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            object_key = obj['Key']
            print(f"Processing file: {object_key}")
    

            if job_state == 'SUCCEEDED':
                destination_bucket = success_bucket
            elif job_state == 'FAILED':
                destination_bucket = failure_bucket
            else:
                print("Unhandled job state:", job_state)
                return
        
            # Copy the object to the destination bucket
            copy_source = {'Bucket': source_bucket, 'Key': object_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=object_key)
        
            # Delete the object from the source bucket
            s3_client.delete_object(Bucket=source_bucket, Key=object_key)
        
            print(f"File moved from {source_bucket}/{object_key} to {destination_bucket}/{object_key}")
            
    else:
        print(f"No files found in the bucket {source_bucket}")
