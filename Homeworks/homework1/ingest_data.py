import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def ingest_green_taxi_data():
    """Ingest green taxi trip data into PostgreSQL"""
    
    # Configuration
    user = 'postgres'
    password = 'postgres'
    host = 'localhost'
    port = '5433'
    db = 'ny_taxi'
    table_name = 'green_taxi_trips'
    
    parquet_name = 'green_tripdata_2025-11.parquet'
    
    print("Connecting to database...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    print("Reading parquet file and inserting data in chunks...")
    df_iter = pd.read_parquet(parquet_name, engine='pyarrow')
    
    # Split into chunks and insert
    chunksize = 100000
    total_rows = len(df_iter)
    chunk_count = 0
    
    for i in range(0, total_rows, chunksize):
        df_chunk = df_iter.iloc[i:i+chunksize]
        
        if chunk_count == 0:
            # Create table with first chunk
            df_chunk.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f'Inserted first chunk')
        else:
            t_start = time()
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            t_end = time()
            print(f'Inserted chunk {chunk_count + 1}, took {t_end - t_start:.3f} seconds')
        
        chunk_count += 1
    
    print(f"Completed! Inserted {chunk_count} chunks total ({total_rows:,} rows)")

def ingest_zones_data():
    """Ingest taxi zones lookup data into PostgreSQL"""
    
    # Configuration
    user = 'postgres'
    password = 'postgres'
    host = 'localhost'
    port = '5433'
    db = 'ny_taxi'
    table_name = 'zones'
    
    csv_name = 'taxi_zone_lookup.csv'
    
    print("Connecting to database...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    print("Reading and inserting zones data...")
    df_zones = pd.read_csv(csv_name)
    df_zones.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    print(f"Inserted {len(df_zones)} zones")

if __name__ == '__main__':
    print("=" * 60)
    print("Starting data ingestion process")
    print("=" * 60)
    
    print("\n[1/2] Ingesting green taxi trip data...")
    ingest_green_taxi_data()
    
    print("\n[2/2] Ingesting taxi zones data...")
    ingest_zones_data()
    
    print("\n" + "=" * 60)
    print("Data ingestion completed successfully!")
    print("=" * 60)