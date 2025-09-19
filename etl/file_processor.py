import psycopg2
from typing import List, Dict, Optional
from .logger_config import logger
import os
from dotenv import load_dotenv

load_dotenv("../docker/.env")

class FileProcessingManager:
    
    def __init__(self):
        self.db_conn = os.getenv("POSTGRES_CONN")
    
    def get_unprocessed_files(self, available_files: List[str]) -> List[str]:
        """Return list of files that haven't been processed yet"""
        conn = psycopg2.connect(self.db_conn)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT file_path FROM processed_files WHERE status IN ('completed', 'processing')")
            processed_files = {row[0] for row in cursor.fetchall()}
            
            unprocessed = [f for f in available_files if f not in processed_files]
            logger.info(f"Found {len(unprocessed)} unprocessed files out of {len(available_files)} total")
            
            return unprocessed
            
        finally:
            cursor.close()
            conn.close()
    
    def mark_file_processing(self, file_path: str, file_size: int) -> None:
        """Mark file as currently being processed"""
        conn = psycopg2.connect(self.db_conn)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "INSERT INTO processed_files (file_path, file_size, status) VALUES (%s, %s, 'processing') ON CONFLICT (file_path) DO UPDATE SET status = 'processing'",
                (file_path, file_size)
            )
            conn.commit()
            logger.info(f"Marked {file_path} as processing")
            
        finally:
            cursor.close()
            conn.close()
    
    def mark_file_completed(self, file_path: str, record_count: int) -> None:
        """Mark file as successfully processed"""
        conn = psycopg2.connect(self.db_conn)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "UPDATE processed_files SET status = 'completed', record_count = %s, processed_at = CURRENT_TIMESTAMP WHERE file_path = %s",
                (record_count, file_path)
            )
            conn.commit()
            logger.info(f"Marked {file_path} as completed with {record_count} records")
            
        finally:
            cursor.close()
            conn.close()
    
    def mark_file_failed(self, file_path: str, error_message: str) -> None:
        """Mark file as failed with error details"""
        conn = psycopg2.connect(self.db_conn)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "UPDATE processed_files SET status = 'failed', error_message = %s WHERE file_path = %s",
                (error_message, file_path)
            )
            conn.commit()
            logger.error(f"Marked {file_path} as failed: {error_message}")
            
        finally:
            cursor.close()
            conn.close()
