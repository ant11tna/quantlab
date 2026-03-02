-- DuckDB Schema for QuantLab
-- Minimal set of tables for research and backtesting

-- ============================================================================
-- Core price data
-- ============================================================================
CREATE TABLE IF NOT EXISTS prices (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    adj_factor DOUBLE DEFAULT 1.0,
    PRIMARY KEY (ts, symbol)
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);
CREATE INDEX IF NOT EXISTS idx_prices_ts ON prices(ts);

-- ============================================================================
-- Backtest run metadata
-- ============================================================================
CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    universe VARCHAR,
    start_date DATE,
    end_date DATE,
    rebalance_freq VARCHAR,
    threshold DOUBLE,
    fee_model VARCHAR,
    config_json VARCHAR,  -- JSON string for full config
    data_snapshot_id VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at);

-- ============================================================================
-- Target weights (research output)
-- ============================================================================
CREATE TABLE IF NOT EXISTS weights_target (
    run_id VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    target_weight DOUBLE NOT NULL,
    source VARCHAR,
    PRIMARY KEY (run_id, ts, symbol),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_weights_run ON weights_target(run_id);
CREATE INDEX IF NOT EXISTS idx_weights_ts ON weights_target(ts);

-- ============================================================================
-- Trades (backtest fills)
-- ============================================================================
CREATE TABLE IF NOT EXISTS trades (
    run_id VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    order_id VARCHAR,
    symbol VARCHAR NOT NULL,
    side VARCHAR,  -- 'BUY' or 'SELL'
    qty DOUBLE NOT NULL,
    price DOUBLE NOT NULL,
    fee DOUBLE DEFAULT 0.0,
    slippage DOUBLE DEFAULT 0.0,
    value DOUBLE GENERATED ALWAYS AS (qty * price) STORED,
    PRIMARY KEY (run_id, ts, order_id, symbol),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_run ON trades(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);

-- ============================================================================
-- Daily portfolio state
-- ============================================================================
CREATE TABLE IF NOT EXISTS portfolio_daily (
    run_id VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    nav DOUBLE NOT NULL,
    cash DOUBLE,
    positions_value DOUBLE,
    drawdown DOUBLE,
    turnover DOUBLE DEFAULT 0.0,
    PRIMARY KEY (run_id, ts),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_run ON portfolio_daily(run_id);

-- ============================================================================
-- Risk decomposition (optional)
-- ============================================================================
CREATE TABLE IF NOT EXISTS risk_decomp (
    run_id VARCHAR NOT NULL,
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    weight DOUBLE,
    vol_contrib DOUBLE,
    risk_contrib DOUBLE,
    corr_to_port DOUBLE,
    PRIMARY KEY (run_id, ts, symbol),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

-- ============================================================================
-- Data snapshots (for reproducibility)
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_snapshots (
    snapshot_id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_start DATE,
    data_end DATE,
    symbols ARRAY(VARCHAR),
    hash VARCHAR,
    manifest_path VARCHAR,
    row_count BIGINT
);

-- ============================================================================
-- Views
-- ============================================================================

-- Latest prices view
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT 
    symbol,
    arg_max(ts, ts) as latest_ts,
    arg_max(close, ts) as latest_price
FROM prices
GROUP BY symbol;

-- Run summary view
CREATE OR REPLACE VIEW v_run_summary AS
SELECT 
    r.run_id,
    r.universe,
    r.start_date,
    r.end_date,
    r.rebalance_freq,
    COUNT(DISTINCT t.symbol) as num_trades,
    SUM(t.fee) as total_fees,
    AVG(pd.nav) as avg_nav,
    MIN(pd.drawdown) as max_drawdown
FROM runs r
LEFT JOIN trades t ON r.run_id = t.run_id
LEFT JOIN portfolio_daily pd ON r.run_id = pd.run_id
GROUP BY r.run_id, r.universe, r.start_date, r.end_date, r.rebalance_freq;

-- Monthly returns view
CREATE OR REPLACE VIEW v_monthly_returns AS
SELECT 
    run_id,
    DATE_TRUNC('month', ts) as month,
    (MAX(nav) / MIN(nav) - 1) as monthly_return
FROM portfolio_daily
GROUP BY run_id, DATE_TRUNC('month', ts);
