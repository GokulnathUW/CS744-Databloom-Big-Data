#!/bin/bash
year=$1

echo "year - ${year}"

for month in {1..12}; 
do
    echo ${month}

    gcloud dataproc jobs submit pyspark \
        --cluster=cs744-second-trial \
        --region=us-central1 \
        gs://cs744-databloom/code/pre_process_high_volume.py -- \
        --year=${year} \
        --month=${month}
    echo "Sleeping for 30s"
    sleep 30 # sleep for 1min
done
