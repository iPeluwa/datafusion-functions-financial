use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl, PartitionEvaluator};

#[derive(Debug)]
pub struct ExponentialMovingAverage {
    name: String,
    signature: Signature,
}

impl ExponentialMovingAverage {
    pub fn new() -> Self {
        Self {
            name: "ema".to_string(),
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64, DataType::Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for ExponentialMovingAverage {
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
        Ok(Box::new(EmaPartitionEvaluator::new()))
    }
}

#[derive(Debug)]
struct EmaPartitionEvaluator {
    window_size: usize,
    alpha: f64,
    current_ema: Option<f64>,
}

impl EmaPartitionEvaluator {
    fn new() -> Self {
        Self {
            window_size: 0,
            alpha: 0.0,
            current_ema: None,
        }
    }
}

impl PartitionEvaluator for EmaPartitionEvaluator {
    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "EMA function requires exactly 2 arguments: value and window_size".to_string(),
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

        // Calculate alpha (smoothing factor): 2 / (N + 1)
        self.alpha = 2.0 / (self.window_size as f64 + 1.0);

        let mut result = Vec::with_capacity(num_rows);
        self.current_ema = None;

        for i in 0..num_rows {
            if let Some(value) = value_array.value(i).into() {
                match self.current_ema {
                    None => {
                        // First value becomes the initial EMA
                        self.current_ema = Some(value);
                        result.push(Some(value));
                    }
                    Some(prev_ema) => {
                        // EMA = alpha * current_value + (1 - alpha) * previous_ema
                        let new_ema = self.alpha * value + (1.0 - self.alpha) * prev_ema;
                        self.current_ema = Some(new_ema);
                        result.push(Some(new_ema));
                    }
                }
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

pub fn register_ema(ctx: &SessionContext) -> Result<()> {
    let ema_udf = WindowUDF::from(ExponentialMovingAverage::new());
    ctx.register_udwf(ema_udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_ema() -> Result<()> {
        let ctx = SessionContext::new();
        register_ema(&ctx)?;

        // Test EMA with window size 3 using SQL
        let result = ctx
            .sql("SELECT price, ema(price, 3) OVER () AS ema_3 FROM (VALUES 
                (10.0), (12.0), (13.0), (12.0), (15.0), (11.0), (16.0), (14.0), (18.0), (20.0)
            ) AS t(price)")
            .await?
            .collect()
            .await?;

        println!("EMA Test Results:");
        datafusion::arrow::util::pretty::print_batches(&result)?;

        Ok(())
    }
}
