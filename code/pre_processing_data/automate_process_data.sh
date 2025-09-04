#!/bin/bash
for year in $(seq 2015 2019); do
    echo ${year}
    python process_data.py --year=${year}
done