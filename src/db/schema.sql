-- QuestDB table schemas for trade data ingestion

-- Stock trades table
CREATE TABLE IF NOT EXISTS stock_trades (
    symbol SYMBOL,
    timestamp TIMESTAMP,
    price DOUBLE,
    size DOUBLE,
    conditions STRING,
    exchange LONG,
    tape LONG,
    trade_id STRING
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Stock aggregates (5-minute bars)
CREATE TABLE IF NOT EXISTS stock_aggregates (
    symbol SYMBOL,
    timestamp TIMESTAMP,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    vwap DOUBLE,
    transaction_count LONG
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Option contracts
CREATE TABLE IF NOT EXISTS option_contracts (
    ticker SYMBOL,
    contract_type SYMBOL,
    exercise_style SYMBOL,
    expiration_date STRING,
    shares_per_contract LONG,
    strike_price DOUBLE,
    underlying_ticker SYMBOL,
    created_at TIMESTAMP
) TIMESTAMP(created_at) PARTITION BY DAY;

-- Option trades
CREATE TABLE IF NOT EXISTS option_trades (
    ticker SYMBOL,
    timestamp TIMESTAMP,
    price DOUBLE,
    size DOUBLE,
    conditions STRING,
    exchange LONG,
    tape LONG,
    sequence_number LONG,
    received_at TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY;

-- Sync state tracking
CREATE TABLE IF NOT EXISTS sync_state (
    ticker SYMBOL,
    last_trade_timestamp TIMESTAMP,
    last_aggregate_timestamp TIMESTAMP,
    last_sync TIMESTAMP,
    is_streaming BOOLEAN
) TIMESTAMP(last_sync) PARTITION BY DAY;
