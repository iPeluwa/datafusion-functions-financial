//! Polygon.io data client for flat files and APIs

use super::{DataSource, PolygonConfig, AssetClass, PolygonDataType};
use datafusion::execution::context::SessionContext;
use datafusion::error::Result;
use datafusion::prelude::CsvReadOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use std::sync::Arc;
use chrono::{NaiveDate, Datelike};
use object_store::{ObjectStore, path::Path as ObjectPath};
use futures::stream::StreamExt;

/// Polygon.io data client for flat files
pub struct PolygonClient {
    source: DataSource,
    ctx: SessionContext,
}

impl PolygonClient {
    /// Create a new Polygon.io client with S3 data source
    pub fn from_s3(config: PolygonConfig) -> Result<Self> {
        let source = DataSource::S3(config.clone());
        let ctx = SessionContext::new();
        
        // Register S3 object store for direct flat file access
        Self::register_s3_store(&ctx, &config)?;
        
        Ok(Self { source, ctx })
    }
    
    /// Create a new Polygon.io client with local file system data source
    pub fn from_local<P: Into<std::path::PathBuf>>(root: P) -> Result<Self> {
        let source = DataSource::Local { root: root.into() };
        let ctx = SessionContext::new();
        
        Ok(Self { source, ctx })
    }
    
    /// Create a new client from data source (preferred constructor)
    pub fn new(source: DataSource) -> Result<Self> {
        match source {
            DataSource::S3(config) => Self::from_s3(config),
            DataSource::Local { root } => Self::from_local(root),
        }
    }
    
    /// Register Polygon.io S3 object store with DataFusion
    fn register_s3_store(ctx: &SessionContext, config: &PolygonConfig) -> Result<()> {
        use object_store::aws::AmazonS3Builder;
        use url::Url;
        
        let s3 = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_access_key_id(&config.access_key)
            .with_secret_access_key(&config.secret_key)
            .with_bucket_name(&config.bucket)
            .with_region("us-east-1") // Polygon.io region
            .build()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        
        let url = Url::parse(&format!("s3://{}/", &config.bucket))
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            
        ctx.runtime_env()
            .register_object_store(&url, Arc::new(s3));
            
        Ok(())
    }

    /// Load minute aggregates from Polygon.io flat files  
    pub async fn load_minute_aggs(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<datafusion::dataframe::DataFrame> {
        self.load_data(AssetClass::Stocks, PolygonDataType::MinuteAggs, date, Some(symbol)).await
    }

    /// Load day aggregates from Polygon.io flat files
    pub async fn load_day_aggs(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<datafusion::dataframe::DataFrame> {
        self.load_data(AssetClass::Stocks, PolygonDataType::DayAggs, date, Some(symbol)).await
    }

    /// Load trades data from Polygon.io flat files
    pub async fn load_trades(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<datafusion::dataframe::DataFrame> {
        self.load_data(AssetClass::Stocks, PolygonDataType::Trades, date, Some(symbol)).await
    }

    /// Load CSV data from appropriate source with decompression
    async fn load_csv_from_source(
        &self,
        path: &str,
        symbol: &str,
    ) -> Result<datafusion::dataframe::DataFrame> {
        let df = match &self.source {
            DataSource::S3(_) => {
                // Read compressed CSV from S3
                let csv_options = CsvReadOptions::new()
                    .has_header(true)
                    .file_compression_type(FileCompressionType::GZIP);
                self.ctx.read_csv(path, csv_options).await?
            }
            DataSource::Local { root } => {
                // Convert to local file path and use uncompressed CSV
                let local_path = if path.starts_with("s3://") {
                    // Convert S3 path to local path, change .gz to regular .csv
                    let path_part = path.strip_prefix("s3://flatfiles/").unwrap_or(path);
                    let uncompressed_path = path_part.replace(".csv.gz", ".csv");
                    root.join(uncompressed_path)
                } else {
                    let uncompressed_path = path.replace(".csv.gz", ".csv");
                    root.join(uncompressed_path.strip_prefix("file://").unwrap_or(&uncompressed_path))
                };
                
                let csv_options = CsvReadOptions::new().has_header(true);
                self.ctx.read_csv(local_path.to_string_lossy().as_ref(), csv_options).await?
            }
        };
        
        // Filter by symbol if provided
        if !symbol.is_empty() {
            Ok(df.filter(datafusion::prelude::col("ticker").eq(datafusion::prelude::lit(symbol)))?)
        } else {
            Ok(df)
        }
    }

    /// Register the DataFrame as a table with financial functions available
    pub async fn register_table_with_indicators(
        &self,
        name: &str,
        df: datafusion::dataframe::DataFrame,
    ) -> Result<()> {
        // Register all financial functions
        crate::register_financial_functions(&self.ctx)?;
        
        // Register the table
        self.ctx.register_table(name, df.into_view())?;
        
        Ok(())
    }

    /// List available files in data source for discovery
    pub async fn list_available_files(&self, prefix: &str) -> Result<Vec<String>> {
        match &self.source {
            DataSource::S3(config) => {
                use object_store::aws::AmazonS3Builder;
                
                let s3 = AmazonS3Builder::new()
                    .with_endpoint(&config.endpoint)
                    .with_access_key_id(&config.access_key)
                    .with_secret_access_key(&config.secret_key)
                    .with_bucket_name(&config.bucket)
                    .with_region("us-east-1")
                    .build()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                
                let prefix_path = ObjectPath::from(prefix);
                let mut files = Vec::new();
                
                let mut stream = s3.list(Some(&prefix_path));
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(meta) => {
                            files.push(meta.location.to_string());
                            if files.len() >= 20 { // Limit results
                                break;
                            }
                        }
                        Err(e) => {
                            return Err(datafusion::error::DataFusionError::External(Box::new(e)));
                        }
                    }
                }
                
                Ok(files)
            }
            DataSource::Local { root } => {
                // List local files
                let search_path = root.join(prefix);
                let mut files = Vec::new();
                
                if let Ok(entries) = std::fs::read_dir(&search_path) {
                    for entry in entries.flatten().take(20) {
                        if let Ok(path) = entry.path().strip_prefix(root) {
                            files.push(path.to_string_lossy().to_string());
                        }
                    }
                }
                
                Ok(files)
            }
        }
    }
    
    /// Discover available asset classes in the data source
    pub async fn discover_asset_classes(&self) -> Result<Vec<String>> {
        let files = self.list_available_files("").await?;
        let mut asset_classes = std::collections::HashSet::new();
        
        for file in files {
            let parts: Vec<&str> = file.split('/').collect();
            if !parts.is_empty() {
                asset_classes.insert(parts[0].to_string());
            }
        }
        
        Ok(asset_classes.into_iter().collect())
    }
    
    /// Discover available data types for a specific asset class
    pub async fn discover_data_types(&self, asset_class: &str) -> Result<Vec<String>> {
        let files = self.list_available_files(&format!("{}/", asset_class)).await?;
        let mut data_types = std::collections::HashSet::new();
        
        for file in files {
            let parts: Vec<&str> = file.split('/').collect();
            if parts.len() >= 2 {
                data_types.insert(parts[1].to_string());
            }
        }
        
        Ok(data_types.into_iter().collect())
    }

    /// Load crypto day aggregates from Polygon.io flat files
    pub async fn load_crypto_day_aggs(
        &self,
        date: NaiveDate,
    ) -> Result<datafusion::dataframe::DataFrame> {
        self.load_data(AssetClass::Crypto, PolygonDataType::DayAggs, date, None).await
    }

    /// Load data for any asset class and data type
    pub async fn load_data(
        &self,
        asset_class: AssetClass,
        data_type: PolygonDataType,
        date: NaiveDate,
        symbol: Option<&str>,
    ) -> Result<datafusion::dataframe::DataFrame> {
        let data_type_str = match data_type {
            PolygonDataType::MinuteAggs => "minute_aggs_v1",
            PolygonDataType::DayAggs => "day_aggs_v1", 
            PolygonDataType::Trades => "trades_v1",
            PolygonDataType::Quotes => "quotes_v1",
            PolygonDataType::GroupedDaily => "grouped_daily_v1",
        };
        
        let file_path = match &self.source {
            DataSource::S3(config) => {
                format!(
                    "s3://{}/{}/{}/{}/{}-{:02}-{:02}.csv.gz",
                    &config.bucket,
                    asset_class.s3_prefix(),
                    data_type_str,
                    date.format("%Y"),
                    date.format("%Y"),
                    date.month(),
                    date.day()
                )
            }
            DataSource::Local { .. } => {
                format!(
                    "{}/{}/{}/{}-{:02}-{:02}.csv.gz",
                    asset_class.s3_prefix(),
                    data_type_str,
                    date.format("%Y"),
                    date.format("%Y"),
                    date.month(),
                    date.day()
                )
            }
        };
        
        self.load_csv_from_source(&file_path, symbol.unwrap_or("")).await
    }

    /// Get the session context for custom queries
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }
}
