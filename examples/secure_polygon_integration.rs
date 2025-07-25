use datafusion_functions_financial::{PolygonClient, PolygonConfig};
use chrono::NaiveDate;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("🔐 Secure Polygon.io Integration Demo\n");

    // Try to load credentials from environment/.env file
    match PolygonConfig::from_env() {
    Ok(config) => {
    println!("✅ Successfully loaded credentials from environment");
    let client = PolygonClient::from_s3(config)?;
            
            println!("\n🔍 Discovering available S3 structure...");
            
            // Discover the root structure
            match client.list_available_files("").await {
                Ok(files) => {
                    println!("📁 Root files/directories:");
                    for file in files.iter().take(10) {
                        println!("   {}", file);
                    }
                }
                Err(e) => {
                    println!("⚠️  Could not list root files: {}", e);
                }
            }
            
            // Try to discover stocks structure
            match client.list_available_files("stocks/").await {
                Ok(files) => {
                    println!("\n📁 Stocks directory structure:");
                    for file in files.iter().take(10) {
                        println!("   {}", file);
                    }
                }
                Err(e) => {
                    println!("⚠️  Could not list stocks files: {}", e);
                }
            }
            
            // Try to discover minute_aggs structure
            match client.list_available_files("stocks/minute_aggs/").await {
                Ok(files) => {
                    println!("\n📁 Minute aggregates structure:");
                    for file in files.iter().take(10) {
                        println!("   {}", file);
                    }
                }
                Err(e) => {
                    println!("⚠️  Could not list minute_aggs files: {}", e);
                }
            }
            
            // Discover available asset classes
            println!("\n🔍 Discovering available asset classes...");
            match client.discover_asset_classes().await {
                Ok(asset_classes) => {
                    println!("📁 Available asset classes:");
                    for asset_class in asset_classes.iter().take(10) {
                        println!("   📂 {}", asset_class);
                        
                        // Discover data types for this asset class
                        if let Ok(data_types) = client.discover_data_types(asset_class).await {
                            for data_type in data_types.iter().take(3) {
                                println!("      └── 📄 {}", data_type);
                            }
                        }
                    }
                }
                Err(e) => println!("⚠️  Could not discover asset classes: {}", e),
            }
            
            // Try to load real crypto data (which we know exists)
            println!("\n📊 Attempting to load real crypto data...");
            let crypto_test_date = NaiveDate::from_ymd_opt(2013, 11, 1).unwrap(); // Date we know exists
            
            match client.load_crypto_day_aggs(crypto_test_date).await {
                Ok(df) => {
                    println!("✅ Successfully loaded crypto data from {}!", crypto_test_date);
                    df.clone().limit(0, Some(5))?.show().await?;
                    
                    // Register and run analysis
                    client.register_table_with_indicators("real_crypto", df).await?;
                    
                    let analysis = client
                        .session_context()
                        .sql("
                            SELECT 
                                COUNT(*) as total_rows,
                                COUNT(DISTINCT ticker) as unique_symbols
                            FROM real_crypto
                            LIMIT 10
                        ")
                        .await?;
                    
                    println!("\n📈 Crypto Data Summary:");
                    analysis.show().await?;
                }
                Err(e) => {
                    println!("⚠️  Could not load crypto data: {}", e);
                }
            }
            
            // Also try stocks with different dates
            println!("\n📊 Attempting to load stock data...");
            let test_date = NaiveDate::from_ymd_opt(2024, 12, 20).unwrap();
            
            match client.load_minute_aggs("AAPL", test_date).await {
                Ok(df) => {
                    println!("✅ Successfully loaded AAPL data from {}!", test_date);
                    df.clone().limit(0, Some(3))?.show().await?;
                }
                Err(e) => {
                    println!("⚠️  Could not load stock data for {}: {}", test_date, e);
                    println!("   Stock data structure may be different or require different access");
                }
            }
        }
        Err(e) => {
            println!("⚠️  Could not load credentials from environment: {}", e);
            println!("\nTo use real Polygon.io data:");
            println!("1. Copy .env.example to .env");
            println!("2. Fill in your actual Polygon.io S3 credentials");
            println!("3. Run this example again");
            println!("\nUsing demo configuration for now...");
            
            let config = PolygonConfig::demo();
            println!("\n📋 Demo configuration:");
            println!("   Access Key: {}", config.access_key);
            println!("   Endpoint: {}", config.endpoint);
            println!("   Bucket: {}", config.bucket);
        }
    }

    Ok(())
}
