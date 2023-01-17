#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = "output.csv"

    # download the csv and write it to "csv_name"
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # Put this data to our postgres
    # create a connection to postgres

    # Read data in batches, all at the same time would be too much
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # pickup time and dropoff time is "TEXT", this needs to be changed to datetime
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # First create a table (using ```df.head(n=0)```)
    # The fill it with chunks of the data
    # With ```to_sql``` method the rows are inserted to the database
    # If a table with this name alreay exists, a new one will replace the old one (```if_exists="replace"```)

    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # add data
    df.to_sql(name=table_name, con=engine, if_exists='append')
   
    # load all data until there is no chunk left
    while True:
        t_start = time()
    
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists="append")
    
        t_end = time()
        print(f"inserted another chunk..., took {t_end - t_start:.3f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table, where we will write the results to")
    parser.add_argument("--url", help="url of the csv file")
    args = parser.parse_args()
    
    main(args)

