use datafusion::execution::context::SessionContext;
use datafusion_functions_financial::register_financial_functions;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    
    // Register all financial functions
    register_financial_functions(&ctx)?;

    // Create sample stock data
    let df = ctx
        .sql("SELECT * FROM (VALUES 
            ('2024-01-01', 100.0),
            ('2024-01-02', 102.0),
            ('2024-01-03', 98.0),
            ('2024-01-04', 105.0),
            ('2024-01-05', 107.0),
            ('2024-01-06', 103.0),
            ('2024-01-07', 110.0),
            ('2024-01-08', 108.0),
            ('2024-01-09', 112.0),
            ('2024-01-10', 115.0)
        ) AS stock_data(date, close_price)")
        .await?;

    println!("Original stock data:");
    df.clone().show().await?;

    // Calculate both SMA and EMA using SQL
    let df_with_indicators = ctx
        .sql("SELECT 
                date, 
                close_price, 
                sma(close_price, 3) OVER () AS sma_3,
                ema(close_price, 3) OVER () AS ema_3
              FROM (VALUES 
                  ('2024-01-01', 100.0),
                  ('2024-01-02', 102.0),
                  ('2024-01-03', 98.0),
                  ('2024-01-04', 105.0),
                  ('2024-01-05', 107.0),
                  ('2024-01-06', 103.0),
                  ('2024-01-07', 110.0),
                  ('2024-01-08', 108.0),
                  ('2024-01-09', 112.0),
                  ('2024-01-10', 115.0)
              ) AS stock_data(date, close_price)")
        .await?;

    println!("\nStock data with 3-day SMA and EMA:");
    df_with_indicators.show().await?;

    Ok(())
}
