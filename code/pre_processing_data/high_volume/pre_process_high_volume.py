#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import calendar



parser = argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--month', required=True)

args = parser.parse_args()

year = int(args.year)
month = int(args.month)
# dataset = args.dataset
# report_output = f"{dataset}.{year}_report"
# data_output = f"{dataset}.trip_details"

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'spark_temp_databloom')


def data_cleaning_green(df_hv):

    df_hv = df_hv.withColumn("hvfhs_license_num", F.substring("hvfhs_license_num", 6, 1))
    
    my_cols = ['hvfhs_license_num', 'PULocationID', 'DOLocationID']

    for c in my_cols:
        df_hv = df_hv.withColumn(c, F.col(c).cast('Integer'))
        
    start_ts = datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    end_ts = datetime(year, month, last_day, 23, 59, 59)

    # Filter for vendor_id in ['HV0002', 'HV0003', 'HV0004', 'HV0005'] AND timestamp within April 2024
    df_hv = df_hv.filter(
        (F.col("hvfhs_license_num").isin([2,3,4,5])) &
        (F.col("pickup_datetime") >= F.lit(start_ts)) &
        (F.col("dropoff_datetime") <= F.lit(end_ts))
    )

    columns_to_fill = [
        'base_passenger_fare',
        'tolls',
        'sales_tax',
        'congestion_surcharge',
        'airport_fee',
        'tips'
    ]

    for c in columns_to_fill:
        df_hv = df_hv.withColumn(c, F.col(c).cast('Double'))

    df_hv = df_hv.fillna(0, subset=columns_to_fill)
    
    return df_hv
    
input_high_volume = f"gs://cs744-databloom/data/raw/fhvhv/{year}/{month:02d}/"
output_path = f"gs://cs744-databloom/data/pq/fhvhv/{year}/{month:02d}/"

df_high_volume = spark.read.parquet(input_high_volume)
df_high_volume = data_cleaning_green(df_high_volume)
df_high_volume.repartition(4).write.mode('overwrite').parquet(output_path)
