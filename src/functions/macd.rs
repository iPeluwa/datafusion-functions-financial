use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl, PartitionEvaluator};

#[derive(Debug)]
pub struct MacdIndicator {
    name: String,
    signature: Signature,
}

impl MacdIndicator {
    pub fn new() -> Self {
        Self {
            name: "macd".to_string(),
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for MacdIndicator {
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
        Ok(Box::new(MacdPartitionEvaluator::new()))
    }
}

#[derive(Debug)]
struct MacdPartitionEvaluator {
    ema12: Option<f64>,
    ema26: Option<f64>,
    alpha12: f64,
    alpha26: f64,
}

impl MacdPartitionEvaluator {
    fn new() -> Self {
        Self {
            ema12: None,
            ema26: None,
            alpha12: 2.0 / 13.0, // 2 / (12 + 1)
            alpha26: 2.0 / 27.0, // 2 / (26 + 1)
        }
    }

    fn update_ema(&mut self, value: f64) -> Option<f64> {
        // Update EMA12
        self.ema12 = match self.ema12 {
            None => Some(value),
            Some(prev_ema) => Some(self.alpha12 * value + (1.0 - self.alpha12) * prev_ema),
        };

        // Update EMA26
        self.ema26 = match self.ema26 {
            None => Some(value),
            Some(prev_ema) => Some(self.alpha26 * value + (1.0 - self.alpha26) * prev_ema),
        };

        // Calculate MACD (EMA12 - EMA26)
        match (self.ema12, self.ema26) {
            (Some(ema12), Some(ema26)) => Some(ema12 - ema26),
            _ => None,
        }
    }
}

impl PartitionEvaluator for MacdPartitionEvaluator {
    fn evaluate_all(
        &mut self,
        values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if values.len() != 1 {
            return Err(DataFusionError::Execution(
                "MACD function requires exactly 1 argument: value".to_string(),
            ));
        }

        let value_array = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Argument must be Float64".to_string())
            })?;

        let mut result = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            if let Some(value) = value_array.value(i).into() {
                let macd = self.update_ema(value);
                result.push(macd);
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

pub fn register_macd(ctx: &SessionContext) -> Result<()> {
    let macd_udf = WindowUDF::from(MacdIndicator::new());
    ctx.register_udwf(macd_udf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::context::SessionContext;

    #[tokio::test]
    async fn test_macd() -> Result<()> {
        let ctx = SessionContext::new();
        register_macd(&ctx)?;

        // Test MACD using SQL
        let result = ctx
            .sql("SELECT price, macd(price) OVER () AS macd_line FROM (VALUES 
                (100.0), (102.0), (98.0), (105.0), (107.0), (103.0), (110.0), (108.0),
                (112.0), (115.0), (113.0), (118.0), (120.0), (116.0), (122.0), (119.0),
                (125.0), (123.0), (127.0), (130.0), (128.0), (132.0), (135.0), (133.0),
                (138.0), (140.0), (136.0), (142.0), (145.0), (143.0)
            ) AS t(price)")
            .await?
            .collect()
            .await?;

        println!("MACD Test Results:");
        datafusion::arrow::util::pretty::print_batches(&result)?;

        Ok(())
    }
}
