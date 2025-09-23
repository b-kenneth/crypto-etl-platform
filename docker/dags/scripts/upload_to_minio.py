import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv("../docker/.env")

DATA_DIR = "../scripts/data"  # Directory where CSVs are stored

def upload_files():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        # access_key=os.getenv("MINIO_ACCESS_KEY"),
        # secret_key=os.getenv("MINIO_SECRET_KEY"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio-access"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio-secret"),
        secure=False,  # Set True if your MinIO uses TLS/SSL
    )

    bucket_name = os.getenv("MINIO_BUCKET", "crypto-data")

    # Create bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

    # Upload each CSV file in data/ directory
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            filepath = os.path.join(DATA_DIR, filename)
            try:
                client.fput_object(bucket_name, filename, filepath)
                print(f"Uploaded {filename} to bucket {bucket_name}")
            except S3Error as err:
                print(f"Failed to upload {filename}: {err}")

if __name__ == "__main__":
    upload_files()
