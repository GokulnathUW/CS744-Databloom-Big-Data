dataset=$1
year=$2

prev_year=$((year - 1))

## archive existing report
# bq cp ${dataset}.${prev_year}_report \
#     archive.${prev_year}_report

# bq rm --force ${dataset}.${prev_year}_report

gcloud dataproc jobs submit pyspark \
    --cluster=cs744-second-trial \
    --region=us-central1 \
    gs://cs744-databloom/code/06_spark_sql_big_query.py \
    -- \
        --year=${year} \
        --dataset=${dataset}
