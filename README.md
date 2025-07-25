# DataFusion Financial Functions

A Rust crate that provides financial and technical analysis functions for Apache DataFusion.

## Overview

This crate extends DataFusion with domain-specific functions for financial data analysis, including technical indicators commonly used in trading and investment analysis.

## Features

- **High-performance technical indicators** implemented as native DataFusion functions
- **SMA, EMA, RSI, MACD** with streaming window operations
- **Multi-source data loading** - S3, local files, or any DataFusion source
- **Polygon.io integration** with secure credential management
- **Multi-asset class support** - stocks, crypto, options, forex, futures, indices
- **Data validation and quality checks** with comprehensive reporting
- **Trading signal detection** based on technical indicators
- **Local development mode** - no cloud credentials required
- **Comprehensive benchmarks** and performance testing

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
datafusion-functions-financial = "0.1.0"
```

## Setup

### Secure Credentials for Polygon.io

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your actual Polygon.io S3 credentials:
```bash
POLYGON_ACCESS_KEY_ID=your_actual_access_key
POLYGON_SECRET_ACCESS_KEY=your_actual_secret_key
POLYGON_S3_REGION=us-east-1
POLYGON_S3_BUCKET=flatfiles
```

3. The `.env` file is ignored by git to keep your credentials secure.

## Quick Start (No Credentials Required)

Try the library immediately with local sample data:

```bash
git clone https://github.com/yourusername/datafusion-functions-financial
cd datafusion-functions-financial
cargo run --example local_demo
```

This will create sample data and demonstrate all features without needing cloud credentials.

## Usage

```rust
use datafusion_functions_financial::{PolygonClient, PolygonConfig, AssetClass, PolygonDataType};
use chrono::NaiveDate;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Load configuration from environment
    let config = PolygonConfig::from_env().unwrap();
    let client = PolygonClient::new(config)?;
    
    // Load any asset class data
    let df = client.load_data(
        AssetClass::Stocks,
        PolygonDataType::MinuteAggs,
        NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
        Some("AAPL")
    ).await?;
    
    // Register with financial functions and analyze
    client.register_table_with_indicators("stock_data", df).await?;
    
    let analysis = client.session_context().sql("
        SELECT 
            ticker,
            close,
            sma(close, 20) OVER (ORDER BY window_start) AS sma_20,
            ema(close, 12) OVER (ORDER BY window_start) AS ema_12,
            rsi(close, 14) OVER (ORDER BY window_start) AS rsi_14
        FROM stock_data
        ORDER BY window_start
        LIMIT 10
    ").await?;

    analysis.show().await?;
    Ok(())
}
```

## Available Functions

### Simple Moving Average (SMA)

Calculates the simple moving average over a specified window.

**Syntax:** `sma(value, window_size)`

**Parameters:**
- `value`: Float64 - The price or value column
- `window_size`: Int64 - Number of periods for the moving average

**Example:**
```sql
SELECT 
    date,
    close_price,
    sma(close_price, 20) OVER () AS sma_20,
    sma(close_price, 50) OVER () AS sma_50
FROM stock_prices
ORDER BY date;
```

### Exponential Moving Average (EMA)

Calculates the exponential moving average using a smoothing factor.

**Syntax:** `ema(value, window_size)`

**Parameters:**
- `value`: Float64 - The price or value column
- `window_size`: Int64 - Number of periods for calculating the smoothing factor (alpha = 2 / (N + 1))

**Formula:** EMA = α × current_value + (1 - α) × previous_EMA

**Example:**
```sql
SELECT 
    date,
    close_price,
    ema(close_price, 12) OVER () AS ema_12,
    ema(close_price, 26) OVER () AS ema_26
FROM stock_prices
ORDER BY date;
```

### Relative Strength Index (RSI)

Calculates the RSI momentum oscillator using Wilder's smoothing method.

**Syntax:** `rsi(value, window_size)`

**Parameters:**
- `value`: Float64 - The price or value column
- `window_size`: Int64 - Number of periods for RSI calculation (typically 14)

**Formula:** RSI = 100 - (100 / (1 + (Average Gain / Average Loss)))

**Example:**
```sql
SELECT 
    date,
    close_price,
    rsi(close_price, 14) OVER () AS rsi_14
FROM stock_prices
WHERE rsi(close_price, 14) OVER () > 70  -- Overbought condition
ORDER BY date;
```

### MACD (Moving Average Convergence Divergence)

Calculates the MACD line (EMA12 - EMA26) for trend analysis.

**Syntax:** `macd(value)`

**Parameters:**
- `value`: Float64 - The price or value column

**Formula:** MACD = EMA(12) - EMA(26)

**Example:**
```sql
SELECT 
    date,
    close_price,
    macd(close_price) OVER () AS macd_line
FROM stock_prices
ORDER BY date;
```

## Data Loading Examples

Load financial data from various sources:

```bash
# CSV and Parquet analysis
cargo run --example stock_data_analysis

# Basic usage with sample data
cargo run --example basic_usage

# Secure Polygon.io S3 integration with decompression (requires .env setup)
cargo run --example secure_polygon_integration

# Local development mode (no credentials required)
cargo run --example local_demo

# Data validation and quality checks
cargo run --example validate

# Multi-asset class support (stocks, crypto, options, forex, futures, indices)
cargo run --example multi_asset_class_demo

# Real Polygon.io data loading
cargo run --example polygon_real_data
```

## Performance Benchmarks

High-performance financial analysis with excellent throughput:

| Function | 10K rows/sec | 100K rows/sec | Notes |
|----------|-------------|---------------|-------|
| **SMA** | ~104K | ~91K | Simple Moving Average |
| **EMA** | ~105K | ~90K | Exponential Moving Average |
| **RSI** | ~92K | ~93K | Relative Strength Index |
| **MACD** | ~92K | ~93K | Moving Average Convergence Divergence |
| **Combined Analysis** | ~95K | ~85K | All 4 indicators together |

**Test Environment:** Apple Silicon M-series, 100,000 realistic price data points

Run benchmarks yourself:

```bash
cargo bench
```

This generates detailed HTML reports in `target/criterion/` with performance metrics across different dataset sizes.

## Running Examples

```bash
cargo run --example basic_usage
```

## Running Tests

```bash
cargo test
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0.
