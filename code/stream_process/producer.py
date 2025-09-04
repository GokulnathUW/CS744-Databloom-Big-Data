import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from time import sleep
from typing import Dict
from confluent_kafka import Producer

import argparse

spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/Patron/.config/gcloud/application_default_credentials.json") \
    .getOrCreate()


spark.conf.set('temporaryGcsBucket', 'spark_temp_databloom')


# kafka_broker="pkc-n3603.us-central1.gcp.confluent.cloud:9092"
# kafka_APIKey="INW6BQKKWHISF6ZT"
# kafka_APISecret = "cbxWQf3NpTdATQ1+Ghtb1vd68gAAyLXF1okqGvpKLhi/xFtbO3yPWgAUl1570YZM"
# # checkpoint_GCSUri="gs://your-bucket/tmp/"

# kafka_Jaas_Config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + kafka_APIKey + "\" password=\"" + kafka_APISecret + "\";"


green_columns = [
    'VendorID',
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'trip_distance', # 'trip_miles'
    'fare_amount', # done
    'mta_tax', # done
    'tip_amount', # done
    'tolls_amount', #done
    'congestion_surcharge'
]

yellow_columns = [
    'VendorID',
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'trip_distance', # 'trip_miles'
    'fare_amount', # done
    'mta_tax', # done
    'tip_amount', # done
    'tolls_amount', #done
    'congestion_surcharge'
]

hvfhv_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'trip_miles', # 'trip_miles'
    'base_passenger_fee', # done
    'sales_tax', # done
    'tips', # done
    'tolls', #done
    'congestion_surcharge'
]

parser = argparse.ArgumentParser()

parser.add_argument('--input', required=True)
parser.add_argument('--service', required=True)

args = parser.parse_args()

INPUT_DATA_PATH = args.input
SERVICE = args.service
BOOTSTRAP_SERVERS = 'bootstrap.kafka-stream.us-central1.managedkafka.proverbial-will-455821-j9.cloud.goog:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'
PRODUCE_TOPIC_RIDES = CONSUME_TOPIC_RIDES = 'ridestream'
    

class RideParquetProducer:
    def __init__(self, props):
        self.producer = Producer(**props)

    @staticmethod
    def read_records(resource_path: str, columns):
        records, ride_keys = [], []
        
        df = spark.read.parquet(resource_path)
        for i, row in enumerate(df.collect()):
            record = ", ".join(str(row[col]) for col in columns)
            records.append(record)
            ride_keys.append(str(row['VendorID']))
            
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key, value in records:
            try:
                self.producer.produce(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)



if __name__ == "__main__":
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.token.endpoint.url': 'https://sts.googleapis.com/v1/token',
        'enable.idempotence': True,
        'queue.buffering.max.messages': 100000,
        'batch.size': 16384,
        'queue.buffering.max.kbytes': 1024,
    }
    
    
    if SERVICE == 'green':
        columns = green_columns
    elif SERVICE == 'yellow':
        columns = yellow_columns
    else:
        columns = hvfhv_columns
        
    
    producer = RideParquetProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH, columns = columns)
    producer.publish(topic=PRODUCE_TOPIC_RIDES, records=ride_records)
