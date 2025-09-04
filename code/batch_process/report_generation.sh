#!/bin/bash
for year in {2015..2024}; do
    ./run_spark_bigquery.sh ${year} 'tripdata'
done



