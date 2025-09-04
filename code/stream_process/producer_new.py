import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from time import sleep
from typing import Dict
from kafka import KafkaProducer
import argparse

from settings import BOOTSTRAP_SERVERS, PRODUCE_TOPIC_RIDES

spark = SparkSession.builder \
    .appName('KafkaProducer') \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "6g") \
    .getOrCreate()
    # .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    # .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \


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
    'tpep_pickup_datetime',
    'tpep_pickup_datetime',
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

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideParquetProducer:
    def __init__(self, props):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str, columns):
        records, ride_keys = [], []
        
        df = spark.read.parquet(resource_path)
        df= df.repartition(24)
        for row in df.toLocalIterator():
            record = ", ".join(str(row[col]) for col in columns)
            print(record)
            records.append(record)
            ride_keys.append(str(row['VendorID']))
            
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key, value in records:
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")
        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8'),
        'max_block_ms': 120000  # 120 seconds
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


# python3 producer_new.py --input='./raw/yellow/2020/01/yellow_tripdata_2020_01.parquet' --service='yellow'
