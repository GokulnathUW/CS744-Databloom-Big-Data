#! /bin/bash

for year in {2022..2024}; do 
    echo "Starting to process $year ------->"
    ./automate_process_data.sh ${year}
    echo "Completed processing $year -*-*-*-*-*-*->"
done
