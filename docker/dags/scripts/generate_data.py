import csv
import os
import random
from datetime import datetime, timedelta

# Configurable coins and base prices (USD)
COINS = {
    'BTC': 30000,
    'ETH': 2000,
    'ADA': 0.5,
    'DOT': 6,
    'BNB': 350,
    'SOL': 20
}

OUTPUT_DIR = 'data'  # Output folder for generated CSVs

def simulate_hourly_data(base_price):
    """Simulate OHLCV data with volatility for 1 hour"""
    volatility = random.uniform(0.01, 0.06)  # 1% to 6%
    open_price = base_price * (1 + random.uniform(-volatility, volatility))
    close_price = open_price * (1 + random.uniform(-volatility, volatility))
    high_price = max(open_price, close_price) * (1 + random.uniform(0, volatility))
    low_price = min(open_price, close_price) * (1 - random.uniform(0, volatility))
    volume = random.uniform(100, 10000)
    market_cap = base_price * volume * 10000  # simplified
    return open_price, high_price, low_price, close_price, volume, market_cap, volatility

# def generate_csv_for_hour(dt):
#     """Generate CSV file for all coins for a single hour"""
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     filename = dt.strftime(f"{OUTPUT_DIR}/crypto_data_%Y%m%d_%H.csv")
#     with open(filename, 'w', newline='') as csvfile:
#         fieldnames = ['timestamp','symbol','open','high','low','close','volume','market_cap','volatility']
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
#         timestamp = dt.isoformat()
#         for symbol, base_price in COINS.items():
#             o,h,l,c,v,mc,vol = simulate_hourly_data(base_price)
#             writer.writerow({
#                 'timestamp': timestamp,
#                 'symbol': symbol,
#                 'open': round(o, 2),
#                 'high': round(h, 2),
#                 'low': round(l, 2),
#                 'close': round(c, 2),
#                 'volume': round(v, 2),
#                 'market_cap': round(mc, 2),
#                 'volatility': round(vol, 4)
#             })
#     print(f"Generated {filename}")

def generate_csv_for_hour(dt, output_path=None):
    """Generate CSV file for all coins for a single hour with custom output path"""
    import os
    
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        filename = output_path
    else:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = dt.strftime(f"{OUTPUT_DIR}/crypto_data_%Y%m%d_%H.csv")
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['timestamp','symbol','open','high','low','close','volume','market_cap','volatility']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        timestamp = dt.isoformat()
        for symbol, base_price in COINS.items():
            o,h,l,c,v,mc,vol = simulate_hourly_data(base_price)
            writer.writerow({
                'timestamp': timestamp,
                'symbol': symbol,
                'open': round(o, 2),
                'high': round(h, 2),
                'low': round(l, 2),
                'close': round(c, 2),
                'volume': round(v, 2),
                'market_cap': round(mc, 2),
                'volatility': round(vol, 4)
            })
    print(f"Generated {filename}")
    return filename


if __name__ == "__main__":
    # Generate last 24 hours of data for demo
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    for i in range(24):
        dt = now - timedelta(hours=i)
        generate_csv_for_hour(dt)
