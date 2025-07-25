use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl, PartitionEvaluator};

#[derive(Debug)]
pub struct SimpleMovingAverage {
    name: String,
    signature: Signature,
}

impl SimpleMovingAverage {
    pub fn new() -> Self {
        Self {
            name: "sma".to_string(),
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64, DataType::Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for SimpleMovingAverage {
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
        Ok(Box::new(SmaPartitionEvaluator::new()))
    }
}

#[derive(Debug)]
struct SmaPartitionEvaluator {
    values: Vec<f64>,
    window_size: usize,
}

impl SmaPartitionEvaluator {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            window_size: 0,
        }
    }
}

impl PartitionEvaluator for SmaPartitionEvaluator {
    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if values.len() != 2 {
            return Err(DataFusionError::Execution(
                "SMA function requires exactly 2 arguments: value and window_size".to_string(),
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

        for i in 0..num_rows {
            if let Some(value) = value_array.value(i).into() {
                self.values.push(value);
                
                if self.values.len() >= self.window_size {
                    let start_idx = self.values.len().saturating_sub(self.window_size);
                    let window_sum: f64 = self.values[start_idx..].iter().sum();
                    let sma = window_sum / self.window_size as f64;
                    result.push(Some(sma));
                } else {
                    result.push(None);
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

pub fn register_sma(ctx: &SessionContext) -> Result<()> {
    let sma_udf = WindowUDF::from(SimpleMovingAverage::new());
    ctx.register_udwf(sma_udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_sma() -> Result<()> {
        let ctx = SessionContext::new();
        register_sma(&ctx)?;

        // Create test data
        let _df = ctx
            .sql("SELECT * FROM (VALUES 
                (1.0), (2.0), (3.0), (4.0), (5.0), (6.0), (7.0), (8.0), (9.0), (10.0)
            ) AS t(price)")
            .await?;

        // Test SMA with window size 3 using SQL
        let result = ctx
            .sql("SELECT price, sma(price, 3) OVER () AS sma_3 FROM (VALUES 
                (1.0), (2.0), (3.0), (4.0), (5.0), (6.0), (7.0), (8.0), (9.0), (10.0)
            ) AS t(price)")
            .await?
            .collect()
            .await?;

        println!("SMA Test Results:");
        datafusion::arrow::util::pretty::print_batches(&result)?;

        Ok(())
    }
}
