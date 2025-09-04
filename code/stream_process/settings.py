import pyspark.sql.types as T

INPUT_DATA_BUCKET = 'gs://'
BOOTSTRAP_SERVERS = 'broker:29092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES = CONSUME_TOPIC_RIDES = 'rides'
