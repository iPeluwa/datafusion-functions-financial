//! Trading signal detection for financial data

use datafusion::execution::context::SessionContext;
use datafusion::error::Result;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Trading signal types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalType {
    Buy,
    Sell,
    Hold,
}

/// A trading signal detected from financial data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSignal {
    pub signal_type: SignalType,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub confidence: f64, // 0.0 to 1.0
    pub reason: String,
}

/// Signal detection based on technical indicators
pub struct SignalDetector;

impl SignalDetector {
    /// Detect signals based on RSI thresholds
    pub async fn detect_rsi_signals(
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<Vec<TradingSignal>> {
        let df = ctx
            .sql(&format!(
                "SELECT 
                    ticker,
                    window_start,
                    close,
                    rsi(close, 14) OVER (PARTITION BY ticker ORDER BY window_start) as rsi_14
                FROM {}
                WHERE rsi(close, 14) OVER (PARTITION BY ticker ORDER BY window_start) IS NOT NULL
                ORDER BY ticker, window_start",
                table_name
            ))
            .await?;

        let batches = df.collect().await?;
        let mut signals = Vec::new();

        for batch in batches {
            let ticker_array = batch.column(0);
            let timestamp_array = batch.column(1);
            let price_array = batch.column(2);
            let rsi_array = batch.column(3);

            for row in 0..batch.num_rows() {
                if let (Some(ticker), Some(timestamp), Some(price), Some(rsi)) = (
                    ticker_array.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().and_then(|a| a.value(row).parse::<String>().ok()),
                    timestamp_array.as_any().downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>().map(|a| a.value(row)),
                    price_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().map(|a| a.value(row)),
                    rsi_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().map(|a| a.value(row)),
                ) {
                    let dt = DateTime::from_timestamp(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32)
                        .unwrap_or_else(|| Utc::now());

                    if rsi < 30.0 {
                        signals.push(TradingSignal {
                            signal_type: SignalType::Buy,
                            symbol: ticker,
                            timestamp: dt,
                            price,
                            confidence: (30.0 - rsi) / 30.0, // Higher confidence for lower RSI
                            reason: format!("RSI oversold: {:.2}", rsi),
                        });
                    } else if rsi > 70.0 {
                        signals.push(TradingSignal {
                            signal_type: SignalType::Sell,
                            symbol: ticker,
                            timestamp: dt,
                            price,
                            confidence: (rsi - 70.0) / 30.0, // Higher confidence for higher RSI
                            reason: format!("RSI overbought: {:.2}", rsi),
                        });
                    }
                }
            }
        }

        Ok(signals)
    }

    /// Detect moving average crossover signals
    pub async fn detect_ma_crossover_signals(
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<Vec<TradingSignal>> {
        let df = ctx
            .sql(&format!(
                "WITH ma_data AS (
                    SELECT 
                        ticker,
                        window_start,
                        close,
                        sma(close, 20) OVER (PARTITION BY ticker ORDER BY window_start) as sma_20,
                        sma(close, 50) OVER (PARTITION BY ticker ORDER BY window_start) as sma_50,
                        LAG(sma(close, 20), 1) OVER (PARTITION BY ticker ORDER BY window_start) as prev_sma_20,
                        LAG(sma(close, 50), 1) OVER (PARTITION BY ticker ORDER BY window_start) as prev_sma_50
                    FROM {}
                )
                SELECT *
                FROM ma_data
                WHERE sma_20 IS NOT NULL AND sma_50 IS NOT NULL 
                  AND prev_sma_20 IS NOT NULL AND prev_sma_50 IS NOT NULL
                  AND (
                    (prev_sma_20 <= prev_sma_50 AND sma_20 > sma_50) OR
                    (prev_sma_20 >= prev_sma_50 AND sma_20 < sma_50)
                  )
                ORDER BY ticker, window_start",
                table_name
            ))
            .await?;

        let batches = df.collect().await?;
        let mut signals = Vec::new();

        for batch in batches {
            let ticker_array = batch.column(0);
            let timestamp_array = batch.column(1);
            let price_array = batch.column(2);
            let sma_20_array = batch.column(3);
            let sma_50_array = batch.column(4);

            for row in 0..batch.num_rows() {
                if let (Some(ticker), Some(timestamp), Some(price), Some(sma_20), Some(sma_50)) = (
                    ticker_array.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().and_then(|a| a.value(row).parse::<String>().ok()),
                    timestamp_array.as_any().downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>().map(|a| a.value(row)),
                    price_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().map(|a| a.value(row)),
                    sma_20_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().map(|a| a.value(row)),
                    sma_50_array.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().map(|a| a.value(row)),
                ) {
                    let dt = DateTime::from_timestamp(timestamp / 1_000_000_000, (timestamp % 1_000_000_000) as u32)
                        .unwrap_or_else(|| Utc::now());

                    let signal_type = if sma_20 > sma_50 {
                        SignalType::Buy
                    } else {
                        SignalType::Sell
                    };

                    let spread = (sma_20 - sma_50).abs();
                    let confidence = (spread / price).min(1.0); // Confidence based on spread size

                    signals.push(TradingSignal {
                        signal_type,
                        symbol: ticker,
                        timestamp: dt,
                        price,
                        confidence,
                        reason: format!("MA crossover: SMA20={:.2}, SMA50={:.2}", sma_20, sma_50),
                    });
                }
            }
        }

        Ok(signals)
    }
}
