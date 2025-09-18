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
