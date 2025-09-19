from extract import MinioExtractor
from transform import transform_data
from load import upsert_prices
from logger_config import logger

def run_etl_pipeline():
    logger.info("Starting ETL pipeline")
    
    try:
        # Extract
        extractor = MinioExtractor()
        files = extractor.list_files()
        
        for file_name in files:
            logger.info(f"Processing file: {file_name}")
            
            # Extract data from file
            raw_data = extractor.read_csv(file_name)
            
            if raw_data.empty:
                logger.warning(f"No data in file {file_name}, skipping")
                continue
            
            # Transform data
            processed_data = transform_data(raw_data)
            
            # Load data
            upsert_prices(processed_data)
            
            logger.info(f"Successfully processed {file_name}")
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()