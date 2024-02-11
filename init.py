import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json
import polygon
import datetime
import boto3

client = boto3.client('s3',endpoint_url = "http://localhost:9090",aws_access_key_id="1234", aws_secret_access_key="4321")

client.create_bucket(Bucket = "testbucket")
client.put_object(Bucket = "testbucket", Key = "indexProcessed3.csv", Body = "indexProcessed.csv")
response = client.list_objects_v2(Bucket = "testbucket")
print(response)
POLYGON_API_KEY = "hAQwGb5hFk4Tapp2pYjj9pr0863wFR8y"
client = polygon.StocksClient(POLYGON_API_KEY)
                    
HOST = "localhost"
PORT = "9092"
KAFKA_SERVER = HOST + ":" + PORT

# producer = KafkaProducer(
#     bootstrap_servers = KAFKA_SERVER,
#     key_serializer = lambda v: json.dumps(v).encode("ascii"),
#     value_serializer = lambda v: json.dumps(v).encode("ascii")
#     )

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

# df = pd.read_csv("indexProcessed.csv")
# print(df.head())
# Sample returns a random sample of items from an axis of object.
# to_dict Convert the DataFrame to a dictionary, orient determines the type of the values of the dictionary.
dict_record = final_df.sample(1).to_dict(orient = "records")
print(dict_record)





# producer.send('stock_streamer', value = dict_record)
# producer.close()

