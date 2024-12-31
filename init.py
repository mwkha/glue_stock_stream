import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import os
from os.path import exists
from dotenv import load_dotenv
from botocore.client import ClientError
from json import dumps, loads
import json
import polygon
from datetime import datetime
import boto3
from s3fs import S3FileSystem

load_dotenv()

s3client = boto3.client('s3',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321")
glue_client = boto3.client('glue',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321",region_name = "ap-southeast-2")
iam_client = boto3.client('iam',endpoint_url = "http://localhost:3000", aws_access_key_id = "1234", aws_secret_access_key = "4321", region_name = "ap-southeast-2")
polygon_key = url = os.getenv("POLYGON_API_KEY")
ticker_csv = os.getenv("TICKER_CSV_FILE")
host = os.getenv("KAFKA_HOST")
port = os.getenv("KAFKA_PORT")
KAFKA_SERVER = host + ":" + port
client = polygon.StocksClient(polygon_key)
s3FS = S3FileSystem(key = "1234", secret = "1234", client_kwargs={'endpoint_url': 
    "http://localhost:3000"})

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    value_serializer = lambda v: dumps(v).encode('utf-8')
    )

consumer = KafkaConsumer(
    'stock_streamer',
    group_id='my-group',
    bootstrap_servers = KAFKA_SERVER,
    value_deserializer = lambda v: loads(v.decode('utf-8'))
    )

def getTickerData():
    # In order to reduce API calls check if the ticker data has already been written to local dir. 
    if exists(ticker_csv):
        print("Already pulled data please delete file to fetch data again")
    else:
        grouped_daily = client.get_grouped_daily_bars(
            "2023-02-16",
        )
        json_grouped = json.dumps(grouped_daily['results'])
        dataframe = pd.read_json(json_grouped)
        # Rename columns to full names for clearer representation
        final_df = dataframe.rename(columns={"T": "Ticker",
                                            "v": "volume",
                                            "vw": "volumeAvg",
                                            "o": "openPrice",
                                            "c": "closePrice",
                                            "h": "highPrice", 
                                            "l": "lowPrice", 
                                            "t": "unixTimestamp", 
                                            "n": "numTransactions"})
        # Convert timestamp to datetime.
        final_df["unixTimestamp"] = final_df["unixTimestamp"].apply(lambda x: datetime.datetime.utcfromtimestamp(x/1000).strftime("%Y-%m-%d"))

        print(final_df.head())
        print(final_df.shape[0])
        final_df.to_csv(ticker_csv, index=False) 

def fetchRecord(csv_file):
    df = pd.read_csv(csv_file)
    # Sample returns a random sample of items from an axis of object.
    # to_dict Convert the DataFrame to a dictionary, orient determines the type of the values of the dictionary.
    dict_record = df.sample(1).to_dict(orient = "records")
    print(dict_record)
    return dict_record

def fetchRecordPushToKafkaConsumer():
    record = fetchRecord(ticker_csv)
    producer.send('stock_streamer', value = record)

def createInfra(bucketName, crawlerName):
    # If head bucket returns error means bucket not create so we can go ahead and create
    try:
        s3client.head_bucket(Bucket = bucketName)
    except ClientError:
        s3client.create_bucket(Bucket = bucketName)
    try:
        glue_client.get_crawler(
    Name=crawlerName
)
    except ClientError:
        glue_client.create_crawler(Name=crawlerName,
    Role='testRole',
    Targets={
        'S3Targets': [
            {
                'Path': f's3://{bucketName}/',
            },
        ]
    })

        

def main():
    # Create the s3 bucket that we will push data to.
    createInfra("polygonStockData", "kafkaCrawler")

    # Fetch data using polygon api
    getTickerData()

    # Create some number of sample rows and push to producer
    for i in range(10):
        fetchRecordPushToKafkaConsumer()
        producer.flush()
    print("Pushed all messages to kafka consumer")
    # Get message and store in s3.
    print("Consuming messages from kafka and pushing to s3")

    while True:
    # Poll for messages
        messages = consumer.poll(timeout_ms=1000)  # 1-second timeout
        if not messages:
            print("No new messages. Exiting loop.")
            break

        for topic_partition, records in messages.items():
            for record in records:
                dt = datetime.now()
                ts_milli = int(dt.timestamp() * 1000000)
                print(f"Pushing file stock_market_{ts_milli}.json")
                with s3FS.open(f"s3://polygonStockData/stock_market_{ts_milli}.json", "w") as file:
                    # Use record.value directly
                    json.dump(record.value, file)
    
    producer.close()
    consumer.close()
    print("Pushed all records and closed producer + consumer")


main()

