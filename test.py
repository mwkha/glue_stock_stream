import boto3
import pandas as pd
import io 
import json 
endpoint_url = 'http://localhost:3000'
## Visit http://localhost:3000/moto-api/ for dashboard

import docker
client = docker.from_env()
res= client.containers.run("ubuntu", "echo hello world")
print("Running",res)

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

response = iam.get_role(
    RoleName='lambdaRole'
)
print(response)
file_contents = open("myFunc.py").read()
print(file_contents)
lamb.create_function(FunctionName='myFunc2', Runtime = 'python3.9', Code={
        'ZipFile': file_contents
    },
    Handler='myFunc.lambda_handler',
    Role = "arn:aws:iam::123456789012:role/lambdaRole")

s3.create_bucket(Bucket = "testing", CreateBucketConfiguration={
        'LocationConstraint':'ap-southeast-2'})

s3.upload_file('indexProcessed.csv', 'testing', 'indexProcessed.csv')

now = s3.get_object(Bucket = "testing", Key = "indexProcessed.csv")

df = pd.read_csv(io.BytesIO(now['Body'].read()), encoding='utf-8')
print(df.head())

resp = lamb.invoke(FunctionName='myFunc2',Payload=json.dumps({}))
print(resp['Payload'].read())
# payload = json.loads(resp['Payload'].read())




