import os
from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

from .logger_config import logger  # use configured logger

load_dotenv("../docker/.env")

class MinioExtractor:
    def __init__(self):
        self.client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False,
        )
        self.bucket_name = os.getenv("MINIO_BUCKET", "crypto-data")

    def list_files(self, prefix=""):
        logger.info(f"Listing files from bucket {self.bucket_name} with prefix '{prefix}'")
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]
            logger.info(f"Found {len(files)} CSV files")
            return files
        except S3Error as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def read_csv(self, object_name):
        logger.info(f"Reading CSV {object_name} from bucket {self.bucket_name}")
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            df = pd.read_csv(BytesIO(data))
            logger.info(f"Read CSV {object_name} with {len(df)} rows")
            return df
        except S3Error as e:
            logger.error(f"Failed to read object {object_name}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading CSV {object_name}: {e}")
            return pd.DataFrame()
