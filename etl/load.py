import os
import logging
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from dotenv import load_dotenv

from logger_config import logger  # use configured logger

load_dotenv("../docker/.env")
DB_CONN = os.getenv("POSTGRES_CONN")

def get_connection():
    conn = psycopg2.connect(DB_CONN)
    return conn

def upsert_prices(df: pd.DataFrame):
    if df.empty:
        logger.info("No data to upsert")
        return

    conn = get_connection()
    cursor = conn.cursor()
    sql = """
    INSERT INTO processed_prices (timestamp, symbol, open, high, low, close, volume, market_cap, volatility, price_change_pct, rolling_volatility, moving_avg)
    VALUES %s
    ON CONFLICT (timestamp, symbol) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      market_cap = EXCLUDED.market_cap,
      volatility = EXCLUDED.volatility,
      price_change_pct = EXCLUDED.price_change_pct,
      rolling_volatility = EXCLUDED.rolling_volatility,
      moving_avg = EXCLUDED.moving_avg;
    """
    tuples = [tuple(x) for x in df.to_numpy()]
    try:
        execute_values(cursor, sql, tuples)
        conn.commit()
        logger.info(f"Upserted {len(tuples)} rows into processed_prices")
    except Exception as e:
        logger.error(f"Failed to upsert data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Example usage with dummy data
    import pandas as pd
    data = {
        "timestamp": ["2025-09-18T12:00:00"], "symbol": ["BTC"],
        "open": [30000], "high": [30500], "low": [29900], "close": [30400],
        "volume": [1000], "market_cap": [600000000], "volatility": [0.02],
        "price_change_pct": [0.01], "rolling_volatility": [0.02], "moving_avg": [30100]
    }
    df_example = pd.DataFrame(data)
    upsert_prices(df_example)
