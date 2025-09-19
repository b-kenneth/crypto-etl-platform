import pandas as pd
import numpy as np
from etl.logger_config import logger

def validate_data(df: pd.DataFrame) -> bool:
    required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'market_cap', 'volatility']
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        logger.error(f"Data validation failed! Missing columns: {missing_cols}")
        return False
    return True

def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding derived metrics...")
    df = df.copy()
    # Convert timestamp to pandas datetime if not already
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # Calculate price change %
    df['price_change_pct'] = df['close'].pct_change().fillna(0)
    # Calculate rolling 3-period volatility on closing price
    df['rolling_volatility'] = df.groupby('symbol')['close'].rolling(window=3).std().reset_index(0, drop=True).fillna(0)
    # Moving average (3 periods)
    df['moving_avg'] = df.groupby('symbol')['close'].rolling(window=3).mean().reset_index(0, drop=True).fillna(df['close'])
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting data transformation...")
    if not validate_data(df):
        raise ValueError("Data validation failed, missing columns!")
    df_clean = df.dropna().reset_index(drop=True)
    df_transformed = add_derived_metrics(df_clean)
    logger.info("Data transformation complete")
    return df_transformed

if __name__ == "__main__":
    # Basic standalone test example
    example_df = pd.DataFrame({
        'timestamp': ['2025-09-18T12:00:00', '2025-09-18T13:00:00', '2025-09-18T14:00:00'],
        'symbol': ['BTC', 'BTC', 'BTC'],
        'open': [30000, 30100, 30200],
        'high': [30500, 30400, 30600],
        'low': [29900, 30050, 30100],
        'close': [30400, 30300, 30500],
        'volume': [1000, 1100, 1050],
        'market_cap': [600000000, 610000000, 620000000],
        'volatility': [0.02, 0.015, 0.02]
    })
    result = transform_data(example_df)
    print(result)
    # print(result.columns)
