import boto3
import os

def lambda_handler(event, context):
    ec2 = boto3.resource('ec2', region_name=os.environ['REGION'])
    s3_bucket = os.environ['S3_BUCKET']
    s3_key = os.environ['S3_KEY']
    instance_type = os.environ['INSTANCE_TYPE']
    ami_id = os.environ['AMI_ID']
    key_name = os.environ['KEY_NAME']  # Ensure you have a key pair for SSH access if needed
    iam_role = os.environ['IAM_INSTANCE_PROFILE_NAME']
    ACCESS_KEY_ID = os.environ['ACCESS_KEY_ID']
    SECRET_ACCESS_KEY = os.environ['SECRET_ACCESS_KEY']
    DEFAULT_REGION = os.environ['DEFAULT_REGION']
    GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']


    # UserData script to download, unzip, and run the Python script
    user_data = f"""#!/bin/bash
    timestamp=$(date +"%Y%m%d_%H%M%S")
    
    LOG_FILE="/home/log_$timestamp.txt"

    echo "sudo apt-get update -y" >> $LOG_FILE
    sudo apt-get update -y >> "$LOG_FILE" 2>&1

    echo "sudo apt-get install -y unzip python3" >> $LOG_FILE
    sudo apt-get install -y unzip python3 >> "$LOG_FILE" 2>&1
    
    echo "sudo apt install python3-pip -y" >> $LOG_FILE
    sudo apt install python3-pip -y >> "$LOG_FILE" 2>&1

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" >> "$LOG_FILE" 2>&1

    echo "unzip awscliv2.zip" >> $LOG_FILE
    unzip awscliv2.zip >> "$LOG_FILE" 2>&1

    echo "sudo ./aws/install" >> $LOG_FILE
    sudo ./aws/install >> "$LOG_FILE" 2>&1

    echo "setting parameters" >> "$LOG_FILE" 2>&1
    sudo aws configure set aws_access_key_id {ACCESS_KEY_ID} >> "$LOG_FILE" 2>&1
    sudo aws configure set aws_secret_access_key {SECRET_ACCESS_KEY} >> "$LOG_FILE" 2>&1
    sudo aws configure set region {DEFAULT_REGION} >> "$LOG_FILE" 2>&1
    sudo aws configure set output json  # Optional: Set output format to JSON >> "$LOG_FILE" 2>&1

    echo "copying from s3 to local" >> $LOG_FILE
    sudo aws s3 cp s3://{s3_bucket}/{s3_key} /home/ec2-user/{s3_key} >> "$LOG_FILE" 2>&1

    echo "navigating to directory" >> $LOG_FILE
    cd /home/ec2-user >> "$LOG_FILE" 2>&1

    echo "unzipping" >> $LOG_FILE
    sudo unzip {s3_key} >> "$LOG_FILE" 2>&1

    echo "installing dependencies" >> "$LOG_FILE" 2>&1
    pip3 install -r scraper/requirements.txt --break-system-packages >> "$LOG_FILE" 2>&1
    
    echo "setting enviornment variables" >> "$LOG_FILE" 2>&1
    echo aws_access_key_id={ACCESS_KEY_ID} >> /etc/environment
    echo aws_secret_access_key={SECRET_ACCESS_KEY} >> /etc/environment
    echo region={DEFAULT_REGION} >> /etc/environment
    echo GOOGLE_API_KEY={GOOGLE_API_KEY} >> /etc/environment

    echo "running python script" >> $LOG_FILE
    sudo python3 scraper/app.py >> "$LOG_FILE" 2>&1
    
    sudo aws s3 cp $LOG_FILE s3://ec2-logs-youtube/log_$timestamp.txt >> "$LOG_FILE" 2>&1

    echo "Fetch metadata token" >> $LOG_FILE
    TOKEN=$(curl -sS http://169.254.169.254/latest/api/token -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 21600") >> "$LOG_FILE" 2>&1
    echo "token: $TOKEN" >> $LOG_FILE

    echo "Fetch instance ID using the token" >> $LOG_FILE
    INSTANCE_ID=$(curl -sS http://169.254.169.254/latest/meta-data/instance-id -H "X-aws-ec2-metadata-token: $TOKEN") >> "$LOG_FILE" 2>&1
    echo "INSTANCE_ID: $INSTANCE_ID" >> $LOG_FILE

    echo "Terminate instance using AWS CLI" >> $LOG_FILE
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >> "$LOG_FILE" 2>&1 

    """

    # Create a new EC2 instance
    instance = ec2.create_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        UserData=user_data,
        IamInstanceProfile={'Name': iam_role}
    )

    instance_id = instance[0].id
    print(f"Created EC2 instance: {instance_id}")

    return {
        'statusCode': 200,
        'body': f"EC2 instance {instance_id} created and running."
    }
