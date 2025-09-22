CREATE TABLE IF NOT EXISTS processed_prices (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open NUMERIC(18, 8),
    high NUMERIC(18, 8),
    low NUMERIC(18, 8),
    close NUMERIC(18, 8),
    volume NUMERIC,
    market_cap NUMERIC,
    volatility NUMERIC,
    price_change_pct NUMERIC,
    rolling_volatility NUMERIC,
    moving_avg NUMERIC,
    PRIMARY KEY (timestamp, symbol)
);


CREATE INDEX IF NOT EXISTS idx_processed_prices_symbol ON processed_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_processed_prices_timestamp ON processed_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files(status);