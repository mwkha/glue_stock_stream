import boto3
import pandas as pd
import io 
import json 
import zipfile
import os
## Visit http://localhost:3000/moto-api/ for dashboard
endpoint_url = 'http://localhost:3000'

s3 = boto3.client("s3", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")
lamb = boto3.client("lambda", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")
iam = boto3.client("iam", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")
assume_role_policy_document = json.dumps({
    "Version": "2012-10-17",
    "Statement": [
        {
        "Effect": "Allow",
        "Principal": {
            "Service": "greengrass.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
        }
    ]
})

create_role_response = iam.create_role(
    RoleName = "lambdaRole",
    AssumeRolePolicyDocument = assume_role_policy_document
)

s3.create_bucket(Bucket = "testing", CreateBucketConfiguration={
        'LocationConstraint':'ap-southeast-2'})

s3.upload_file('indexProcessed.csv', 'testing', 'indexProcessed.csv')





