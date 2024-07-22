import json, boto3, time, tqdm, os
from scrape import *

# AWS credentials
aws_access_key_id = os.environ['aws_access_key_id']
aws_secret_access_key = os.environ['aws_secret_access_key']
region=os.environ['region']
bucket_name = os.environ['load_bucket_name']

# Set up YouTube API client
api_service_name = "youtube"
api_version = "v3"
api_key = os.environ['GOOGLE_API_KEY']

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# Scrape data
final_data = []

s3 = boto3.client(
    's3',
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
try:
    queries = suggest_queries(api_key).split(',')
    start_time = time.time()
    
    with tqdm.tqdm(total=len(queries)) as pbar:
        while queries:
            query = queries.pop()
            data = scrape_data(youtube, query)
            final_data += data
            pbar.update(1)

except Exception as e:
    print(f'error occured in scraping {e}')

finally:
    # Get the current date and format it as YYYY-MM-DD
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Create the filename with the current date
    filename = f'{current_date}.json'

    # Save data to a JSON file with the current date in the filename
    with open(filename, 'w') as f:
        json.dump(final_data, f, indent=4)

    
    file_name = f'{current_date}.json'

    create_bucket_if_not_exists(bucket_name, s3, region='us-east-1')
    if final_data:
        upload_file_to_s3(file_name, bucket_name, s3)

    