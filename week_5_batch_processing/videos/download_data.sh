# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz

# we want the script to stop on the first non-zero exit code
set -e

# pass variables through command line
TAXI_TYPE=$1 # "yellow"
YEAR=$2 #2020
URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/" # "https://s3.amazonaws.com/nyc-tlc/trip+data"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "donwloading ${URL} to ${LOCAL_PATH}"

    # create directory
    mkdir -p ${LOCAL_PREFIX}
    
    # download data
    wget ${URL} -O ${LOCAL_PATH}

    # gzip - compresses file and then removes original file
    #echo "compressing file ${LOCAL_PATH}"
    #gzip ${LOCAL_PATH}
done
