use datafusion_functions_financial::{PolygonClient, PolygonConfig, AssetClass, PolygonDataType};
use chrono::NaiveDate;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("🌍 Multi-Asset Class Polygon.io Demo\n");

    match PolygonConfig::from_env() {
    Ok(config) => {
    println!("✅ Successfully loaded credentials from environment");
    let client = PolygonClient::from_s3(config)?;
            
            // Test date
            let test_date = NaiveDate::from_ymd_opt(2023, 1, 3).unwrap(); // Trading day
            
            println!("📅 Testing data for: {}\n", test_date);
            
            // 1. Stocks - Minute Aggregates
            println!("📈 Loading US Stocks minute aggregates...");
            match client.load_data(
                AssetClass::Stocks, 
                PolygonDataType::MinuteAggs, 
                test_date, 
                Some("AAPL")
            ).await {
                Ok(df) => {
                    println!("✅ Successfully loaded AAPL minute data");
                    df.clone().limit(0, Some(3))?.show().await?;
                    
                    // Apply technical analysis
                    client.register_table_with_indicators("stocks_data", df).await?;
                    
                    let analysis = client.session_context().sql("
                        SELECT 
                            ticker,
                            COUNT(*) as total_bars,
                            AVG(close) as avg_close,
                            sma(close, 20) OVER (ORDER BY window_start) as sma_20
                        FROM stocks_data 
                        GROUP BY ticker
                        LIMIT 5
                    ").await?;
                    
                    println!("📊 Technical Analysis:");
                    analysis.show().await?;
                }
                Err(e) => println!("⚠️  Could not load stocks data: {}", e),
            }
            
            // 2. Crypto - Day Aggregates  
            println!("\n💰 Loading Crypto day aggregates...");
            match client.load_data(
                AssetClass::Crypto,
                PolygonDataType::DayAggs,
                NaiveDate::from_ymd_opt(2013, 11, 1).unwrap(), // Known available date
                None
            ).await {
                Ok(df) => {
                    println!("✅ Successfully loaded crypto data");
                    df.clone().limit(0, Some(3))?.show().await?;
                    
                    client.register_table_with_indicators("crypto_data", df).await?;
                    
                    let crypto_analysis = client.session_context().sql("
                        SELECT 
                            ticker,
                            close,
                            volume,
                            rsi(close, 14) OVER (PARTITION BY ticker ORDER BY date) as rsi_14
                        FROM crypto_data 
                        WHERE ticker IS NOT NULL
                        ORDER BY volume DESC
                        LIMIT 10
                    ").await?;
                    
                    println!("📈 Top Crypto by Volume with RSI:");
                    crypto_analysis.show().await?;
                }
                Err(e) => println!("⚠️  Could not load crypto data: {}", e),
            }
            
            // 3. Options Data
            println!("\n📋 Loading Options data...");
            match client.load_data(AssetClass::Options, PolygonDataType::DayAggs, test_date, Some("AAPL")).await {
                Ok(df) => {
                    println!("✅ Successfully loaded AAPL options data");
                    df.clone().limit(0, Some(3))?.show().await?;
                }
                Err(e) => println!("⚠️  Could not load options data: {}", e),
            }
            
            // 4. Forex Data
            println!("\n💱 Loading Forex data...");
            match client.load_data(AssetClass::Forex, PolygonDataType::MinuteAggs, test_date, Some("EUR/USD")).await {
                Ok(df) => {
                    println!("✅ Successfully loaded EUR/USD forex data");
                    df.clone().limit(0, Some(3))?.show().await?;
                }
                Err(e) => println!("⚠️  Could not load forex data: {}", e),
            }
            
            // 5. Futures Data
            println!("\n🌾 Loading Futures data...");
            match client.load_data(AssetClass::Futures, PolygonDataType::DayAggs, test_date, Some("ES")).await {
                Ok(df) => {
                    println!("✅ Successfully loaded ES futures data");
                    df.clone().limit(0, Some(3))?.show().await?;
                }
                Err(e) => println!("⚠️  Could not load futures data: {}", e),
            }
            
            // 6. Indices Data
            println!("\n📊 Loading Indices data...");
            match client.load_data(AssetClass::Indices, PolygonDataType::DayAggs, test_date, Some("SPY")).await {
                Ok(df) => {
                    println!("✅ Successfully loaded SPY index data");
                    df.clone().limit(0, Some(3))?.show().await?;
                }
                Err(e) => println!("⚠️  Could not load indices data: {}", e),
            }
            
            println!("\n🎯 Asset Class Coverage Summary:");
            println!("   ✅ Stocks (US Equities) - Minute & Day aggregates, Trades, Quotes");
            println!("   ✅ Crypto (Global) - Day aggregates available"); 
            println!("   ✅ Options (US) - Underlying symbol based access");
            println!("   ✅ Forex - Currency pair data");
            println!("   ✅ Futures - Contract symbol data");
            println!("   ✅ Indices - Index symbol data");
            
            println!("\n💡 All asset classes use the same decompression pipeline!");
            println!("   - Download .csv.gz files from S3");
            println!("   - Decompress in memory using GZIP");
            println!("   - Load into DataFusion for analysis");
            println!("   - Apply financial indicators (SMA, EMA, RSI, MACD)");
        }
        Err(e) => {
            println!("⚠️  Could not load credentials: {}", e);
            println!("Setup your .env file with Polygon.io S3 credentials to test real data");
            
            println!("\n📚 Supported Asset Classes:");
            println!("   📈 Stocks: us_stocks_sip/minute_aggs_v1|day_aggs_v1|trades_v1");
            println!("   💰 Crypto: global_crypto/day_aggs_v1");
            println!("   📋 Options: us_options_opra/[data_type]");
            println!("   💱 Forex: forex/[data_type]");
            println!("   🌾 Futures: futures/[data_type]");
            println!("   📊 Indices: indices/[data_type]");
        }
    }

    Ok(())
}
