//! Data validation utilities for Polygon.io datasets

use datafusion::execution::context::SessionContext;
use datafusion::error::Result;

use std::collections::HashMap;

/// Data quality validation report
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub checks: HashMap<String, usize>,
    pub total_rows: usize,
    pub passed: bool,
}

impl ValidationReport {
    pub fn new() -> Self {
        Self {
            checks: HashMap::new(),
            total_rows: 0,
            passed: true,
        }
    }
    
    pub fn add_check(&mut self, name: &str, failed_rows: usize) {
        self.checks.insert(name.to_string(), failed_rows);
        if failed_rows > 0 {
            self.passed = false;
        }
    }
    
    pub fn set_total_rows(&mut self, count: usize) {
        self.total_rows = count;
    }
    
    pub fn summary(&self) -> String {
        let mut report = format!("Validation Report:\n");
        report.push_str(&format!("Total rows: {}\n", self.total_rows));
        report.push_str(&format!("Overall status: {}\n\n", 
            if self.passed { "✅ PASSED" } else { "❌ FAILED" }));
        
        for (check, failed_count) in &self.checks {
            let status = if *failed_count == 0 { "✅" } else { "❌" };
            report.push_str(&format!("{} {}: {} failed rows\n", status, check, failed_count));
        }
        
        report
    }
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Polygon.io data validation utilities
pub struct PolygonValidator;

impl PolygonValidator {

    
    /// Validate minute aggregates data quality
    pub async fn validate_minute_aggs(
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<ValidationReport> {
        let mut report = ValidationReport::new();

        // Get total row count
        let total_count = ctx
            .sql(&format!("SELECT COUNT(*) as total FROM {}", table_name))
            .await?
            .collect()
            .await?;
            
        if let Some(batch) = total_count.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                if let Some(count) = array.value(0).try_into().ok() {
                    report.set_total_rows(count);
                }
            }
        }

        // Check for gaps in timestamps
        let gap_check = ctx
            .sql(&format!(
                "WITH time_gaps AS (
                    SELECT window_start,
                           LAG(window_start) OVER (ORDER BY window_start) as prev_time,
                           window_start - LAG(window_start) OVER (ORDER BY window_start) as gap_ns
                    FROM {}
                )
                SELECT COUNT(*) as gap_count
                FROM time_gaps 
                WHERE gap_ns > 60000000000", // More than 1 minute gap
                table_name
            ))
            .await?
            .collect()
            .await?;

        let gap_rows = if let Some(batch) = gap_check.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                array.value(0) as usize
            } else { 0 }
        } else { 0 };

        // Check for negative values
        let negative_check = ctx
            .sql(&format!(
                "SELECT 
                    COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume,
                    COUNT(CASE WHEN open <= 0 THEN 1 END) as invalid_open,
                    COUNT(CASE WHEN close <= 0 THEN 1 END) as invalid_close,
                    COUNT(CASE WHEN high <= 0 THEN 1 END) as invalid_high,
                    COUNT(CASE WHEN low <= 0 THEN 1 END) as invalid_low
                FROM {}",
                table_name
            ))
            .await?
            .collect()
            .await?;

        if let Some(batch) = negative_check.first() {
            let arrays = batch.columns();
            if arrays.len() >= 5 {
                if let (Some(vol), Some(open), Some(close), Some(high), Some(low)) = (
                    arrays[0].as_any().downcast_ref::<datafusion::arrow::array::Int64Array>(),
                    arrays[1].as_any().downcast_ref::<datafusion::arrow::array::Int64Array>(),
                    arrays[2].as_any().downcast_ref::<datafusion::arrow::array::Int64Array>(),
                    arrays[3].as_any().downcast_ref::<datafusion::arrow::array::Int64Array>(),
                    arrays[4].as_any().downcast_ref::<datafusion::arrow::array::Int64Array>(),
                ) {
                    let total_negative = vol.value(0) + open.value(0) + close.value(0) + high.value(0) + low.value(0);
                    report.add_check("Negative Values", total_negative as usize);
                }
            }
        }

        // Check for logic errors (high < low, etc.)
        let logic_check = ctx
            .sql(&format!(
                "SELECT COUNT(*) as logic_errors
                FROM {} 
                WHERE high < low OR high < open OR high < close OR low > open OR low > close",
                table_name
            ))
            .await?
            .collect()
            .await?;

        let logic_rows = if let Some(batch) = logic_check.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                array.value(0) as usize
            } else { 0 }
        } else { 0 };

        report.add_check("Time Gaps", gap_rows);
        report.add_check("Logic Errors", logic_rows);

        Ok(report)
    }

    /// Validate day aggregates data quality
    pub async fn validate_day_aggs(
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<ValidationReport> {
        let mut report = ValidationReport::new();

        // Get total row count
        let total_count = ctx
            .sql(&format!("SELECT COUNT(*) as total FROM {}", table_name))
            .await?
            .collect()
            .await?;
            
        if let Some(batch) = total_count.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                if let Some(count) = array.value(0).try_into().ok() {
                    report.set_total_rows(count);
                }
            }
        }

        // Check for missing weekend filtering (should not have Saturday/Sunday data)
        let weekend_check = ctx
            .sql(&format!(
                "SELECT COUNT(*) as weekend_count
                FROM {}
                WHERE EXTRACT(DOW FROM date) IN (0, 6)", // Sunday = 0, Saturday = 6
                table_name
            ))
            .await?
            .collect()
            .await?;

        let weekend_rows = if let Some(batch) = weekend_check.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>() {
                array.value(0) as usize
            } else { 0 }
        } else { 0 };

        report.add_check("Weekend Data", weekend_rows);

        Ok(report)
    }
}
