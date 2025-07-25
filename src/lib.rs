use datafusion::execution::context::SessionContext;
use datafusion::error::Result;

pub mod functions;
pub mod polygon;
pub mod streaming;

pub use functions::*;
pub use polygon::*;
pub use streaming::{MarketTick, StreamingIndicators, StreamingProcessor};

/// Register all financial functions with the given SessionContext
pub fn register_financial_functions(ctx: &SessionContext) -> Result<()> {
    functions::sma::register_sma(ctx)?;
    functions::ema::register_ema(ctx)?;
    functions::rsi::register_rsi(ctx)?;
    functions::macd::register_macd(ctx)?;
    Ok(())
}
