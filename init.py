import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import os
from os.path import exists
from dotenv import load_dotenv
import json
import polygon
import datetime
import boto3

load_dotenv()

s3client = boto3.client('s3',endpoint_url = "http://localhost:9090", aws_access_key_id = "1234", aws_secret_access_key = "4321")
polygon_key = url = os.getenv("POLYGON_API_KEY")
ticker_csv = os.getenv("TICKER_CSV_FILE")
host = os.getenv("KAFKA_HOST")
port = os.getenv("KAFKA_PORT")
KAFKA_SERVER = host + ":" + port
client = polygon.StocksClient(polygon_key)

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    key_serializer = lambda v: json.dumps(v).encode("ascii"),
    value_serializer = lambda v: json.dumps(v).encode("ascii")
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


def pushToKafkaConsumer():
    record = fetchRecord(ticker_csv)
    producer.send('stock_streamer', value = record)

getTickerData()
for i in range(20):
    pushToKafkaConsumer()

producer.close()

s3client.put_object(Bucket = "polygon_stock_data", Key = "indexProcessed3.csv", Body = "indexProcessed.csv")




# producer.send('stock_streamer', value = dict_record)
# producer.close()

