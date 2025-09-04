for year in {2015..2024}; do
    ./download_data.sh "yellow" ${year} 
    ./download_data.sh "green" ${year}
done
