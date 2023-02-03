from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """download trip data from GCS"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="../data/")
    return Path(f"../data/{gcs_path}")

@task()
def read(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")
    df.to_gbq(
            destination_table="dezoomcamp.rides",
            project_id="stoked-mode-375206",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append"
            )

@flow()
def etl_gcs_to_bq(color, year, month):
    """main etl flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = read(path)
    print(df.columns)
    write_bq(df)
    return df

@flow()
def rows_processed(months=[2,3], year=2019, color="yellow"):
    """main flow prints total bumber of rows processed"""
    length = 0
    for month in months:
        df = etl_gcs_to_bq(color, year, month)
        length += len(df)
    print(f"Total number of rows processed: {length}")

if __name__ == "__main__":
    rows_processed()
