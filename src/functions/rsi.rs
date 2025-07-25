use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl, PartitionEvaluator};

#[derive(Debug)]
pub struct RelativeStrengthIndex {
    name: String,
    signature: Signature,
}

impl RelativeStrengthIndex {
    pub fn new() -> Self {
        Self {
            name: "rsi".to_string(),
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64, DataType::Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for RelativeStrengthIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn partition_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RsiPartitionEvaluator::new()))
    }
}

#[derive(Debug)]
struct RsiPartitionEvaluator {
    window_size: usize,
    values: Vec<f64>,
    gains: Vec<f64>,
    losses: Vec<f64>,
    avg_gain: f64,
    avg_loss: f64,
}

impl RsiPartitionEvaluator {
    fn new() -> Self {
        Self {
            window_size: 0,
            values: Vec::new(),
            gains: Vec::new(),
            losses: Vec::new(),
            avg_gain: 0.0,
            avg_loss: 0.0,
        }
    }

    fn calculate_rsi(&self, avg_gain: f64, avg_loss: f64) -> f64 {
        if avg_loss == 0.0 {
            return 100.0;
        }
        let rs = avg_gain / avg_loss;
        100.0 - (100.0 / (1.0 + rs))
    }
}

impl PartitionEvaluator for RsiPartitionEvaluator {
    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "RSI function requires exactly 2 arguments: value and window_size".to_string(),
            ));
        }

        let value_array = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("First argument must be Float64".to_string())
            })?;

        let window_size_array = values[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Second argument must be Int64".to_string())
            })?;

        // Get window size from first non-null value
        self.window_size = window_size_array
            .iter()
            .find_map(|x| x)
            .ok_or_else(|| {
                DataFusionError::Execution("Window size cannot be null".to_string())
            })? as usize;

        let mut result = Vec::with_capacity(num_rows);
        self.values.clear();
        self.gains.clear();
        self.losses.clear();

        for i in 0..num_rows {
            if let Some(current_value) = value_array.value(i).into() {
                self.values.push(current_value);
                
                if self.values.len() == 1 {
                    // First value, no RSI calculation possible
                    result.push(None);
                    continue;
                }

                // Calculate price change
                let prev_value = self.values[self.values.len() - 2];
                let change = current_value - prev_value;
                
                let gain = if change > 0.0 { change } else { 0.0 };
                let loss = if change < 0.0 { -change } else { 0.0 };
                
                self.gains.push(gain);
                self.losses.push(loss);

                if self.gains.len() < self.window_size {
                    // Not enough data for RSI calculation
                    result.push(None);
                    continue;
                }

                if self.gains.len() == self.window_size {
                    // First RSI calculation - use simple average
                    self.avg_gain = self.gains.iter().sum::<f64>() / self.window_size as f64;
                    self.avg_loss = self.losses.iter().sum::<f64>() / self.window_size as f64;
                } else {
                    // Subsequent calculations - use Wilder's smoothing
                    let alpha = 1.0 / self.window_size as f64;
                    self.avg_gain = (self.avg_gain * (1.0 - alpha)) + (gain * alpha);
                    self.avg_loss = (self.avg_loss * (1.0 - alpha)) + (loss * alpha);
                }

                let rsi = self.calculate_rsi(self.avg_gain, self.avg_loss);
                result.push(Some(rsi));
            } else {
                result.push(None);
            }
        }

        Ok(Arc::new(Float64Array::from(result)))
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    fn include_rank(&self) -> bool {
        false
    }
}

pub fn register_rsi(ctx: &SessionContext) -> Result<()> {
    let rsi_udf = WindowUDF::from(RelativeStrengthIndex::new());
    ctx.register_udwf(rsi_udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_rsi() -> Result<()> {
        let ctx = SessionContext::new();
        register_rsi(&ctx)?;

        // Test RSI with window size 14 using SQL
        let result = ctx
            .sql("SELECT price, rsi(price, 14) OVER () AS rsi_14 FROM (VALUES 
                (44.34), (44.09), (44.15), (43.61), (44.33), (44.83), (45.85), (46.08),
                (45.89), (46.03), (46.83), (47.69), (46.49), (46.26), (47.09), (46.66),
                (46.80), (46.23), (46.38), (46.33), (46.51)
            ) AS t(price)")
            .await?
            .collect()
            .await?;

        println!("RSI Test Results:");
        datafusion::arrow::util::pretty::print_batches(&result)?;

        Ok(())
    }
}
