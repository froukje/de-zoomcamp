from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    #if randint(0, 1) > 0:
    #    raise Exception
    df = pd.read_csv(dataset_url, dtype={'passenger_count': 'Int64', \
            'payment_type': 'Int64', 'trip_type': 'Int64', \
            'VendorID': 'Int64', 'store_and_fwd_flag': 'object', \
            'trip_distance': 'float64', 'RatecodeID': 'Int64', \
            'PULoctionID': 'Int64', 'DOLocationID': 'Int64', \
            'fare_amount': 'float64', 'extra': 'float64', 'mta_tax': 'float64', \
            'tip_amount': 'float64', 'tolls_amount': 'float64', \
            'improvement_surcharge': 'float64', 'total_amount': 'float64', \
            'congestion_surcharge': 'float64'})
    print(df.dtypes)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write DataFrame out as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to Google Cloud Storage"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
        months: list[int] = [7, 8, 9, 10, 11, 12], year: int = 2021, color: str = "yellow"
        ):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color = "green"
    months = [1,2,3,4,5,6,7,8,9,10,11, 12]
    year = 2019
    etl_parent_flow(months, year, color)

