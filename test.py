
import boto3
import json

s3client = boto3.client('s3',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321")
glue_client = boto3.client('glue',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321",region_name = "ap-southeast-2")

onbj = s3client.get_object(Bucket='polygonStockData', Key = "stock_market_1735642210020914.json")

j = json.loads(onbj['Body'].read())
print(j)

resp = glue_client.get_crawler(
    Name='kafkaCrawler'
)

print(resp)

resp = glue_client.stop_crawler(
    Name='kafkaCrawler'
)
print(resp)
# resp = glue_client.start_crawler(
#     Name='kafkaCrawler'
# )

# print(resp)