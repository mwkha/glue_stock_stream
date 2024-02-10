import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from time import sleep
from json import dumps
import json


HOST = "localhost"
PORT = "9092"
KAFKA_SERVER = HOST + ":" + PORT

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    key_serializer = lambda v: json.dumps(v).encode("ascii"),
    value_serializer = lambda v: json.dumps(v).encode("ascii")
    )

df = pd.read_csv("indexProcessed.csv")
print(df.head())
# Sample returns a random sample of items from an axis of object.
# to_dict Convert the DataFrame to a dictionary, orient determines the type of the values of the dictionary.
dict_record = df.sample(1).to_dict(orient = "records")
print(dict_record)





producer.send('stock_streamer', value = dict_record)
producer.close()

