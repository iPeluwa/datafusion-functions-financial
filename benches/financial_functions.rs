use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use datafusion_functions_financial::register_financial_functions;
use std::time::{Duration, Instant};

async fn benchmark_function(function_name: &str, size: usize, window: usize) -> datafusion::error::Result<f64> {
    let ctx = SessionContext::new();
    register_financial_functions(&ctx)?;

    // Generate realistic price data with volatility
    let mut price = 100.0;
    let values: Vec<String> = (0..size)
        .map(|i| {
            // Add some realistic price movement
            let change = (((i as f64 * 0.1).sin() + (i as f64 * 0.05).cos()) * 2.0) + 
                        ((i % 17) as f64 - 8.5) * 0.5; // Pseudo-random variation
            price += change;
            format!("({})", price)
        })
        .collect();
    let values_str = values.join(", ");

    let query = match function_name {
        "sma" => format!(
            "SELECT price, sma(price, {}) OVER (ORDER BY rownum) AS indicator 
             FROM (SELECT price, ROW_NUMBER() OVER () as rownum FROM (VALUES {}) AS t(price))",
            window, values_str
        ),
        "ema" => format!(
            "SELECT price, ema(price, {}) OVER (ORDER BY rownum) AS indicator 
             FROM (SELECT price, ROW_NUMBER() OVER () as rownum FROM (VALUES {}) AS t(price))",
            window, values_str
        ),
        "rsi" => format!(
            "SELECT price, rsi(price, {}) OVER (ORDER BY rownum) AS indicator 
             FROM (SELECT price, ROW_NUMBER() OVER () as rownum FROM (VALUES {}) AS t(price))",
            window, values_str
        ),
        "macd" => format!(
            "SELECT price, macd(price) OVER (ORDER BY rownum) AS indicator 
             FROM (SELECT price, ROW_NUMBER() OVER () as rownum FROM (VALUES {}) AS t(price))",
            values_str
        ),
        _ => panic!("Unknown function: {}", function_name),
    };

    let start = Instant::now();
    let _result = ctx.sql(&query).await?.collect().await?;
    let elapsed = start.elapsed().as_secs_f64();
    
    // Return throughput (rows per second)
    Ok(size as f64 / elapsed)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("financial_indicators");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);

    // Test different dataset sizes
    for &size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(size as u64));

        // SMA Benchmark
        group.bench_with_input(BenchmarkId::new("sma_20", size), &size, |b, &size| {
            b.iter(|| {
                let throughput = rt.block_on(benchmark_function("sma", black_box(size), 20)).unwrap();
                eprintln!("SMA {} rows: {:.0} rows/sec", size, throughput);
                throughput
            });
        });

        // EMA Benchmark
        group.bench_with_input(BenchmarkId::new("ema_12", size), &size, |b, &size| {
            b.iter(|| {
                let throughput = rt.block_on(benchmark_function("ema", black_box(size), 12)).unwrap();
                eprintln!("EMA {} rows: {:.0} rows/sec", size, throughput);
                throughput
            });
        });

        // RSI Benchmark
        group.bench_with_input(BenchmarkId::new("rsi_14", size), &size, |b, &size| {
            b.iter(|| {
                let throughput = rt.block_on(benchmark_function("rsi", black_box(size), 14)).unwrap();
                eprintln!("RSI {} rows: {:.0} rows/sec", size, throughput);
                throughput
            });
        });

        // MACD Benchmark
        group.bench_with_input(BenchmarkId::new("macd", size), &size, |b, &size| {
            b.iter(|| {
                let throughput = rt.block_on(benchmark_function("macd", black_box(size), 0)).unwrap();
                eprintln!("MACD {} rows: {:.0} rows/sec", size, throughput);
                throughput
            });
        });
    }

    group.finish();

    // Combined indicators benchmark
    let mut combined_group = c.benchmark_group("combined_analysis");
    combined_group.measurement_time(Duration::from_secs(20));
    combined_group.sample_size(10);

    for &size in [10_000, 100_000].iter() {
        combined_group.throughput(Throughput::Elements(size as u64));
        
        combined_group.bench_with_input(BenchmarkId::new("full_technical_analysis", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let ctx = SessionContext::new();
                    register_financial_functions(&ctx).unwrap();

                    // Generate test data
                    let mut price = 100.0;
                    let values: Vec<String> = (0..size)
                        .map(|i| {
                            let change = (((i as f64 * 0.1).sin() + (i as f64 * 0.05).cos()) * 2.0) + 
                                        ((i % 17) as f64 - 8.5) * 0.5;
                            price += change;
                            format!("({})", price)
                        })
                        .collect();
                    let values_str = values.join(", ");

                    let start = Instant::now();
                    let query = format!(
                        "SELECT 
                            price,
                            sma(price, 20) OVER (ORDER BY rownum) as sma_20,
                            ema(price, 12) OVER (ORDER BY rownum) as ema_12,
                            rsi(price, 14) OVER (ORDER BY rownum) as rsi_14,
                            macd(price) OVER (ORDER BY rownum) as macd_line
                         FROM (SELECT price, ROW_NUMBER() OVER () as rownum FROM (VALUES {}) AS t(price))",
                        values_str
                    );

                    let _result = black_box(ctx.sql(&query).await.unwrap().collect().await.unwrap());
                    let elapsed = start.elapsed().as_secs_f64();
                    let throughput = size as f64 / elapsed;
                    eprintln!("Full analysis {} rows: {:.0} rows/sec", size, throughput);
                    throughput
                });
            });
        });
    }

    combined_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
