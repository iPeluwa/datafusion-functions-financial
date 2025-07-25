//! Data types for Polygon.io integration

use serde::{Deserialize, Serialize};

/// Supported Polygon.io data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PolygonDataType {
    Trades,
    Quotes,
    MinuteAggs,
    DayAggs,
    GroupedDaily,
}

/// Supported asset classes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AssetClass {
    Stocks,
    Options,
    Futures,
    Indices,
    Forex,
    Crypto,
}

impl AssetClass {
    /// Get the S3 directory prefix for this asset class
    pub fn s3_prefix(&self) -> &'static str {
        match self {
            AssetClass::Stocks => "us_stocks_sip",
            AssetClass::Options => "us_options_opra",
            AssetClass::Futures => "futures",
            AssetClass::Indices => "indices", 
            AssetClass::Forex => "forex",
            AssetClass::Crypto => "global_crypto",
        }
    }
}
