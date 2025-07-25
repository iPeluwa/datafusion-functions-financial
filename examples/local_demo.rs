use datafusion_functions_financial::{PolygonClient, AssetClass, PolygonDataType};
use chrono::NaiveDate;
use std::fs;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("🏠 Local File Demo - No Cloud Credentials Required!\n");

    // Create sample data directory structure
    let sample_data_dir = "./sample_data";
    if let Err(e) = create_sample_data_structure(sample_data_dir) {
        println!("Warning: Could not create sample data: {}", e);
    }

    // Create client pointing to local files
    let client = PolygonClient::from_local(sample_data_dir)?;

    println!("📁 Using local data directory: {}", sample_data_dir);
    
    // Discover available asset classes
    println!("\n🔍 Discovering local asset classes...");
    match client.discover_asset_classes().await {
        Ok(asset_classes) => {
            println!("📂 Available asset classes:");
            for asset_class in asset_classes {
                println!("   📁 {}", asset_class);
            }
        }
        Err(e) => println!("⚠️  Could not discover asset classes: {}", e),
    }

    // Try to load sample data
    println!("\n📊 Loading sample crypto data...");
    let test_date = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
    
    match client.load_data(
        AssetClass::Crypto,
        PolygonDataType::DayAggs,
        test_date,
        None
    ).await {
        Ok(df) => {
            println!("✅ Successfully loaded local crypto data!");
            df.clone().limit(0, Some(5))?.show().await?;
            
            // Apply financial indicators
            client.register_table_with_indicators("crypto_data", df).await?;
            
            let analysis = client.session_context().sql("
                SELECT 
                    ticker,
                    close,
                    volume,
                    sma(close, 5) OVER (PARTITION BY ticker ORDER BY date) as sma_5,
                    rsi(close, 5) OVER (PARTITION BY ticker ORDER BY date) as rsi_5
                FROM crypto_data
                WHERE ticker IS NOT NULL
                ORDER BY ticker, date
                LIMIT 10
            ").await?;
            
            println!("\n📈 Technical Analysis on Local Data:");
            analysis.show().await?;
        }
        Err(e) => {
            println!("⚠️  Could not load local data: {}", e);
            println!("   Make sure the sample data exists in {}", sample_data_dir);
        }
    }

    println!("\n🎯 Local Mode Benefits:");
    println!("   ✅ No cloud credentials required");
    println!("   ✅ Fast loading from local filesystem");
    println!("   ✅ Perfect for development and testing");
    println!("   ✅ Same API as cloud mode");
    println!("   ✅ Full technical indicator support");
    
    println!("\n💡 To add your own data:");
    println!("   1. Place .csv.gz files in sample_data/[asset_class]/[data_type]/YYYY/");
    println!("   2. Follow Polygon.io naming: YYYY-MM-DD.csv.gz");
    println!("   3. Use same column structure as Polygon.io files");

    Ok(())
}

fn create_sample_data_structure(base_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create directory structure
    let crypto_dir = format!("{}/global_crypto/day_aggs_v1/2023", base_dir);
    fs::create_dir_all(&crypto_dir)?;
    
    // Create sample CSV data file (uncompressed for DataFusion compatibility)
    let sample_file = format!("{}/2023-01-15.csv", crypto_dir);
    
    if !std::path::Path::new(&sample_file).exists() {
        println!("📝 Creating sample data file: {}", sample_file);
        
        // Create sample CSV content
        let csv_content = r#"ticker,date,open,high,low,close,volume,vwap,transactions
BTC,2023-01-15,21000.50,21500.75,20800.25,21350.00,1500000,21300.25,12500
ETH,2023-01-15,1550.25,1580.50,1540.00,1575.75,800000,1565.50,8500
LTC,2023-01-15,85.50,87.25,84.75,86.50,150000,86.00,2500
ADA,2023-01-15,0.35,0.37,0.34,0.36,2000000,0.355,15000
DOT,2023-01-15,6.25,6.45,6.15,6.35,300000,6.30,3500
"#;

        // Write uncompressed CSV data
        fs::write(&sample_file, csv_content)?;
        
        println!("✅ Sample data created successfully!");
    } else {
        println!("📁 Sample data already exists: {}", sample_file);
    }
    
    Ok(())
}
