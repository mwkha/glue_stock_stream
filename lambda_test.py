

import boto3
import io 
import json 
import zipfile
import time
import os
## Visit http://localhost:3000/moto-api/ for dashboard
endpoint_url = 'http://localhost:3000'

s3 = boto3.client("s3", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")
lamb = boto3.client("lambda", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")
iam = boto3.client("iam", endpoint_url = endpoint_url,aws_access_key_id = "ajajaj", aws_secret_access_key = "slslls", region_name = "ap-southeast-2")

archive = zipfile.ZipFile('function.zip', 'w')
archive.write('myFunc.py', 'myFunc.py')
archive.close()

s3.upload_file('function.zip', 'testing', 'function.zip')

lamb.create_function(FunctionName='myFunc8', Runtime = 'python3.9', Code={
                'S3Bucket': 'testing',
                'S3Key': 'function.zip', #how can i create or fetch this S3Key
            },
    Handler='myFunc.lambda_handler',
    Role = "arn:aws:iam::123456789012:role/lambdaRole",
    Publish=True)


resp = lamb.invoke(FunctionName='myFunc8',Payload=json.dumps({}))
print(resp['Payload'].read())
# payload = json.loads(resp['Payload'].read())

s3.create_bucket(Bucket = "testing", CreateBucketConfiguration={
        'LocationConstraint':'ap-southeast-2'})