
import boto3
import json

s3client = boto3.client('s3',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321")

onbj = s3client.get_object(Bucket='polygonStockData', Key = "stock_market_1725170381672116.json")

j = json.loads(onbj['Body'].read())
print(j)