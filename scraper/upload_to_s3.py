import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def create_bucket_if_not_exists(bucket_name, s3, region):
    """Create an S3 bucket if it doesn't exist"""
    
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If the bucket does not exist, create it
        if e.response['Error']['Code'] == '404':
            if region == 'us-east-1':
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Unexpected error: {e}")
            raise

def upload_file_to_s3(file_name, bucket_name, s3, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    """
    if object_name is None:
        object_name = file_name

    try:
        response = s3.upload_file(file_name, bucket_name, object_name)
        print(f"File '{file_name}' uploaded to '{bucket_name}/{object_name}'")
    except FileNotFoundError:
        print(f"The file '{file_name}' was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except ClientError as e:
        print(f"Unexpected error: {e}")