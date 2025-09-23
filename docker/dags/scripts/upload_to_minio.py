import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from etl.logger_config import logger 

load_dotenv("../docker/.env")

DATA_DIR = "../scripts/data"  # Directory where CSVs are stored

def upload_files():
    """Upload all CSV files from the local data directory to the MinIO bucket."""
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio-access"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio-secret"),
        secure=False, 
    )

    bucket_name = os.getenv("MINIO_BUCKET", "crypto-data")

    # Create bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")

    # Upload each CSV file in the data directory
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            filepath = os.path.join(DATA_DIR, filename)
            try:
                client.fput_object(bucket_name, filename, filepath)
                logger.info(f"Uploaded {filename} to bucket {bucket_name}")
            except S3Error as err:
                logger.error(f"Failed to upload {filename}: {err}")

if __name__ == "__main__":
    upload_files()
