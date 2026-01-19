import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'green_tripdata_2025-11.csv.gz'

    # download the file
    import os
    os.system(f"wget {url} -O {csv_name}")

    # create database engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read and insert data in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')

    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # create table with headers
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # insert remaining chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'Inserted chunk... took {t_end - t_start:.3f} seconds')
        except StopIteration:
            print("All chunks inserted.")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')
    parser.add_argument('--user', required=True, help='PostgreSQL username')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', required=True, help='PostgreSQL host')
    parser.add_argument('--port', required=True, help='PostgreSQL port')
    parser.add_argument('--db', required=True, help='PostgreSQL database name')
    parser.add_argument('--table_name', required=True, help='Name of the table to insert data into')
    parser.add_argument('--url', required=True, help='URL of the CSV file to download')

    args = parser.parse_args()
    main(args)