import logging
from logging.handlers import RotatingFileHandler
import os

LOG_FILE = os.getenv("LOG_FILE_PATH", "logs/etl.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("crypto_etl")
logger.setLevel(logging.DEBUG)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)
