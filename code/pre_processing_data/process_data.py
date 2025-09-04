import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types
import argparse

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True)

args = parser.parse_args()
year = args.year

def data_cleaning_green(df_green):

    my_cols = ['VendorID', 'PULocationID', 'DOLocationID', 'payment_type']

    for c in my_cols:
        df_green = df_green.withColumn(c, F.col(c).cast('Integer'))
        
    start_ts = F.lit(f"{year}-{month}-01 00:00:00")  # Start of the month
    end_ts = F.lit(f"{year}-{month}-31 23:59:59")    # End of the month

    # Filter for vendor_id in {1, 2, 6} AND timestamp within April 2024
    df_green = df_green.filter(
        (F.col("VendorID").isin([1, 2, 6])) &
        (F.col("lpep_pickup_datetime") >= start_ts) &
        (F.col("lpep_dropoff_datetime") <= end_ts)
    )
    
    df_green = df_green.withColumn('passenger_count', F.col('passenger_count').cast('Double')) \
        .withColumn('congestion_surcharge', F.col('congestion_surcharge').cast('Double'))
        
    return df_green

def data_cleaning_yellow(df_yellow):

    my_cols = ['VendorID', 'PULocationID', 'DOLocationID', 'payment_type']

    for c in my_cols:
        df_yellow = df_yellow.withColumn(c, F.col(c).cast('Integer'))
        
    start_ts = F.lit(f"{year}-{month}-01 00:00:00")  # Start of the month
    end_ts = F.lit(f"{year}-{month}-31 23:59:59")    # End of the month

    # Filter for vendor_id in {1, 2, 6} AND timestamp within April 2024
    df_yellow = df_yellow.filter(
        (F.col("VendorID").isin([1, 2, 6, 7])) &
        (F.col("tpep_pickup_datetime") >= start_ts) &
        (F.col("tpep_dropoff_datetime") <= end_ts)
    )
    
    df_yellow = df_yellow.withColumn('passenger_count', F.col('passenger_count').cast('Double')) \
    .withColumn('congestion_surcharge', F.col('congestion_surcharge').cast('Double'))

        
    return df_yellow

print("Started Processing Green")

# Process Green Data
for month in range(1, 13):
    print(f'processing data for {year}/{month}')

    input_path = f'./data/data_parquet/raw/green/{year}/{month:02d}/'
    output_path = f'./data/data_parquet/pq/green/{year}/{month:02d}/'

    df_green = spark.read \
        .parquet(input_path)
         # .schema(green_schema) \
        
    df_green = data_cleaning_green(df_green)
    

    df_green \
        .repartition(4) \
        .write.mode('overwrite').parquet(output_path)

print("Started Processing Yellow")
    
# Process Yellow Data
for month in range(1, 13):
    print(f'processing data for {year}/{month}')

    input_path = f'./data/data_parquet/raw/yellow/{year}/{month:02d}/'
    output_path = f'./data/data_parquet/pq/yellow/{year}/{month:02d}/'

    df_yellow = spark.read \
        .parquet(input_path)
        
    df_yellow = data_cleaning_yellow(df_yellow)
    

    df_yellow \
        .repartition(4) \
        .write.mode('overwrite').parquet(output_path)
