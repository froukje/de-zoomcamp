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
    df = pd.read_csv(dataset_url)
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write DataFrame out as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    gcp_path = Path(f"data/{color}/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path, gcp_path

@task()
def write_gcs(path: Path, gcp_path: Path) -> None:
    """Upload local parquet file to Google Cloud Storage"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=gcp_path
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path, gcp_path = write_local(df, color, dataset_file)
    write_gcs(path, gcp_path)

@flow()
def etl_parent_flow(
        months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], \
                year: int = 2019, color: str = "fhv"
        ):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color = "fhv"
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year, color)

