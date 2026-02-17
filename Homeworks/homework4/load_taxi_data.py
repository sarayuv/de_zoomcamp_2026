import os
import sys
import urllib.request
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


# Change this to your bucket name
BUCKET_NAME = "sarayu-dezoomcamp-hw4"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/workspaces/de_zoomcamp_2026/gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TAXI_TYPES = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(taxi_type, year, month):
    filename = f"{taxi_type}_tripdata_{year}-{month}.csv"
    url = f"{BASE_URL}/{taxi_type}/{filename}.gz"
    gz_file_path = os.path.join(DOWNLOAD_DIR, f"{filename}.gz")
    csv_file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, gz_file_path)
        
        # Decompress the gzipped file
        print(f"Decompressing {gz_file_path}...")
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(csv_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove the compressed file
        os.remove(gz_file_path)
        
        print(f"Downloaded and decompressed: {csv_file_path}")
        return (taxi_type, year, month, csv_file_path)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    # Generate all combinations of taxi_type, year, month
    download_tasks = [(taxi_type, year, month) 
                      for taxi_type in TAXI_TYPES 
                      for year in YEARS 
                      for month in MONTHS]

    # Download files
    print(f"\nStarting download of {len(download_tasks)} files...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda x: download_file(*x), download_tasks))
    
    # Filter out None values (failed downloads)
    successful_downloads = [r for r in results if r is not None]
    print(f"\nSuccessfully downloaded {len(successful_downloads)} files.")

    # Upload to GCS
    print("\nUploading files to GCS...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda x: upload_to_gcs(x[3]), successful_downloads)

    print("\nAll files processed and uploaded to GCS.")