use datafusion_functions_financial::{PolygonClient, PolygonValidator, AssetClass, PolygonDataType};
use chrono::NaiveDate;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    println!("🔍 Data Validation Demo\n");

    // Use local data for this demo
    let client = PolygonClient::from_local("./sample_data")?;

    println!("📊 Loading data for validation...");
    let test_date = NaiveDate::from_ymd_opt(2023, 1, 15).unwrap();
    
    match client.load_data(
        AssetClass::Crypto,
        PolygonDataType::DayAggs,
        test_date,
        None
    ).await {
        Ok(df) => {
            println!("✅ Successfully loaded data for validation");
            
            // Register the table
            client.register_table_with_indicators("validation_data", df).await?;
            
            // Run validation
            println!("\n🔍 Running data quality validation...");
            
            match PolygonValidator::validate_day_aggs(
                client.session_context(),
                "validation_data"
            ).await {
                Ok(report) => {
                    println!("\n📋 Validation Results:");
                    println!("{}", report.summary());
                    
                    if report.passed {
                        println!("🎉 All validation checks passed!");
                    } else {
                        println!("⚠️  Some validation checks failed. Review the data quality issues above.");
                    }
                }
                Err(e) => {
                    println!("❌ Validation failed: {}", e);
                }
            }
            
            // Additional data exploration
            println!("\n📊 Data Overview:");
            let overview = client.session_context().sql("
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT ticker) as unique_symbols,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    AVG(close) as avg_close_price,
                    SUM(volume) as total_volume
                FROM validation_data
            ").await?;
            
            overview.show().await?;
        }
        Err(e) => {
            println!("⚠️  Could not load data: {}", e);
            println!("\n💡 To run this example:");
            println!("   1. First run: cargo run --example local_demo");
            println!("   2. This will create sample data in ./sample_data/");
            println!("   3. Then run this validation example again");
        }
    }

    println!("\n🔧 Validation Features:");
    println!("   ✅ Data completeness checks");
    println!("   ✅ Value range validation");
    println!("   ✅ Logic consistency checks");
    println!("   ✅ Timestamp gap detection");
    println!("   ✅ Weekend data filtering (for day aggregates)");
    println!("   ✅ Comprehensive reporting");

    Ok(())
}
