#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType

parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--dataset', required=True)

args = parser.parse_args()

year = args.year
dataset = args.dataset
report_output = f"{dataset}.{year}_report"
data_output = f"{dataset}.trip_details"

input_green = f"gs://cs744-databloom/data/pq/green/{year}/*/"
input_yellow = f"gs://cs744-databloom/data/pq/yellow/{year}/*/"
input_high_volume = f"gs://cs744-databloom/data/pq/fhvhv/{year}/*/"

print(f"input_green path - {input_green}")

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'spark_temp_databloom')


green_schema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('lpep_pickup_datetime', TimestampType(), True),
    StructField('lpep_dropoff_datetime', TimestampType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('RatecodeID', LongType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('passenger_count', DoubleType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('ehail_fee', IntegerType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('trip_type', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
])

df_green = spark.read.schema(green_schema).parquet(input_green)

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.option("pathGlobFilter", "*.parquet").parquet(input_yellow)


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# High Volume 
df_high_volume = spark.read.option("pathGlobFilter", "*.parquet").parquet(input_high_volume)

df_high_volume = df_high_volume \
    .withColumnRenamed('hvfhs_license_num', 'VendorID') \
    .withColumnRenamed('trip_miles', 'trip_distance') \
    .withColumnRenamed('tips', 'tip_amount') \
    .withColumnRenamed('tolls', 'tolls_amount') \
    .withColumnRenamed('sales_tax', 'mta_tax') \
    .withColumnRenamed('base_passenger_fare', 'fare_amount')

df_high_volume = df_high_volume.withColumn("payment_type", F.lit("credit_card"))

df_high_volume = df_high_volume.withColumn(
    'total_amount',
    F.col('fare_amount') +
    F.col('tolls_amount') +
    F.col('mta_tax') +
    F.col('congestion_surcharge') +
    F.col('airport_fee') +
    F.col('tip_amount')
)

common_columns = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'trip_distance', # 'trip_miles'
    'fare_amount', # done
    'mta_tax', # done
    'tip_amount', # done
    'tolls_amount', #done
    'total_amount', # calculate
    'payment_type', # done -  mark as credit card
    'congestion_surcharge'
]

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_high_volume_sel = df_high_volume \
    .select(common_columns) \
    .withColumn('service_type', F.lit('high volume'))


df_trips_data = df_high_volume_sel.unionAll(df_yellow_sel)
df_trips_data = df_high_volume_sel.unionAll(df_green_sel)

df_trips_data.registerTempTable('trips_data')


df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


df_result.write.format('bigquery') \
    .option('table', report_output) \
    .save()
    
df_trips_data.write.mode('append') \
    .format('bigquery') \
    .option('table', data_output) \
    .save()
