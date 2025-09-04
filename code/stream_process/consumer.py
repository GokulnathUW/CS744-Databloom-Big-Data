import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, avg, count, sum, expr, window, current_timestamp, expr, when, desc, lit

BOOTSTRAP_SERVERS = 'bootstrap.kafka-stream.us-central1.managedkafka.proverbial-will-455821-j9.cloud.goog:9092'
TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'
CONSUME_TOPIC_RIDES = 'ridestream'

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/Patron/.config/gcloud/application_default_credentials.json") \
    .getOrCreate()


spark.conf.set('temporaryGcsBucket', 'spark_temp_databloom')
spark.sparkContext.setLogLevel("DEBUG")





rides_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", CONSUME_TOPIC_RIDES) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "OAUTHBEARER") \
    .option("kafka.sasl.oauthbearer.token.endpoint.url", "https://sts.googleapis.com/v1/token") \
    .load()

# Load from Kafka are in binary
rides_df = rides_df.selectExpr("CAST(value AS STRING) as value")

columns = [
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
    'congestion_surcharge'
]
rides_df = rides_df.withColumn("split", split(col("value"), ",\s*"))  # comma + optional space

for idx, name in enumerate(columns):
    rides_df = rides_df.withColumn(name, col("split").getItem(idx))

rides_df = rides_df.drop("split")

print("rides_df created")

# TODO: Cast to correct types (depending on prev TODO)
rides_df = rides_df \
    .withColumn("trip_distance", col("trip_distance").cast("float")) \
    .withColumn("fare_amount", col("fare_amount").cast("float")) \
    .withColumn("tip_amount", col("tip_amount").cast("float")) \
    .withColumn("tolls_amount", col("tolls_amount").cast("float")) \
    .withColumn("mta_tax", col("mta_tax").cast("float")) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("float")) \
    .withColumn("PULocationID", col("PULocationID").cast("int")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
    .withColumn("pickup_datetime", col("pickup_datetime").cast("timestamp")) \
    .withColumn("dropoff_datetime", col("dropoff_datetime").cast("timestamp"))

    


# recent_rides = rides_df.filter(
#     (col("pickup_datetime") >= current_timestamp() - expr("INTERVAL 5 MINUTES"))
# )
print("recent_rides created")

## AGGREGATIONS
# Running total of all trips and revenue
running_totals = rides_df.groupBy(lit(1)).agg(
    count("*").alias("total_trips"),
    sum("fare_amount").alias("total_revenue")
)
print("running_totals created")


# Trips and revenue for last 30 days
aggregated_rides = rides_df.withWatermark("pickup_datetime", "5 MINUTES") \
    .groupBy(window("pickup_datetime", "5 minutes")) \
    .agg(
        count("*").alias("total_trips_last_month"),
        sum("fare_amount").alias("total_revenue_last5mins"), # base fare
        avg("fare_amount").alias("avg_revenue_last_5mins"),
        avg("tip_amount").alias("avg_tip"),
        avg("trip_distance").alias("avg_trip_distance_last_5mins"),
        avg(expr("unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)")).alias("avg_trip_duration_seconds_last_5mins")
    )
print("aggregated_rides created")


# Anomaly detection (high fare, long distance)
anomalies = rides_df.filter((col("trip_distance") > 1000) | (col("fare_amount") > 1000)) \
    .withColumn("Anomaly", 
                when(col("fare_amount") > 1000, "High amount")
                 .when(col("trip_distance") > 1000, "Long trip")
                 .otherwise("No anomaly"))
print("anomalies created")

# Trips affected by surcharge
trips_with_surcharge = rides_df \
    .withWatermark("pickup_datetime", "5 MINUTES") \
    .filter(col("congestion_surcharge") > 0) \
    .agg(count("*").alias("trips_with_surcharge_count"))
print("trips_with_surcharge created")


# Peak hours
traffic_stats = rides_df \
    .withWatermark("pickup_datetime", "30 minutes") \
    .groupBy(window(col("pickup_datetime"), "30 minutes").alias("pickup_window")) \
    .agg(count("*").alias("trip_count")) \
    .orderBy(desc("trip_count"))
    
print("traffic_stats created")
    
# TODO: Hotspots can be counted but location in IDs in HVFHV and latitudes and longitudes in green/yellow


# A) Running totals to console
query1 = running_totals.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query1.awaitTermination()
print("Query1 complete")

# B) Traffic stats to console
query2 = aggregated_rides.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query2.awaitTermination()
    
# C) Surcharged to console
query3 = trips_with_surcharge.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query3.awaitTermination()

# D) Anomalies to console
query4 = anomalies.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query4.awaitTermination()

# E) Peak Hours to console
query5 = traffic_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query5.awaitTermination() 
    
# print("Active streaming queries:")
# for q in spark.streams.active:
#     print(f"{q.name}: {q.status}, isActive={q.isActive}")

# 8. Wait for termination
spark.streams.awaitAnyTermination()