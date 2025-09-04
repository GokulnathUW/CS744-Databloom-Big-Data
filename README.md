# CS744-Databloom-Big-Data
Scalable Data Infrastructure for Real-Time and Batch Analytics
## Data cleaning

Read raw parquet data, clean it and write it as 4 partitioned parquet files.

```sh
python process_data.py --year=2016
```

## Copying files to GCP

```sh
gcloud storage cp -r pq gs://cs744-databloom/data
```
