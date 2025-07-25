//! Configuration for Polygon.io data sources

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for Polygon.io S3 flat files access
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolygonConfig {
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
}

impl Default for PolygonConfig {
    fn default() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::demo())
    }
}

impl PolygonConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenv::dotenv().ok(); // Load .env file if it exists
        
        let access_key = std::env::var("POLYGON_ACCESS_KEY_ID")
            .map_err(|_| "POLYGON_ACCESS_KEY_ID not found in environment")?;
        let secret_key = std::env::var("POLYGON_SECRET_ACCESS_KEY")
            .map_err(|_| "POLYGON_SECRET_ACCESS_KEY not found in environment")?;
        let endpoint = std::env::var("POLYGON_S3_ENDPOINT")
            .unwrap_or_else(|_| "https://files.polygon.io".to_string());
        let bucket = std::env::var("POLYGON_S3_BUCKET")
            .unwrap_or_else(|_| "flatfiles".to_string());
            
        Ok(Self {
            access_key,
            secret_key,
            endpoint,
            bucket,
        })
    }
    
    /// Demo configuration with placeholder values
    pub fn demo() -> Self {
        Self {
            access_key: "your_access_key_here".to_string(),
            secret_key: "your_secret_key_here".to_string(),
            endpoint: "https://files.polygon.io".to_string(),
            bucket: "flatfiles".to_string(),
        }
    }
}

/// Data source configuration
#[derive(Debug, Clone)]
pub enum DataSource {
    /// S3-based data source with Polygon.io credentials
    S3(PolygonConfig),
    /// Local file system data source
    Local { root: PathBuf },
}

impl DataSource {
    /// Create S3 data source from configuration
    pub fn s3(config: PolygonConfig) -> Self {
        Self::S3(config)
    }
    
    /// Create local data source from root directory
    pub fn local<P: Into<PathBuf>>(root: P) -> Self {
        Self::Local { root: root.into() }
    }
    
    /// Create S3 data source from environment variables
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::S3(PolygonConfig::from_env()?))
    }
}
