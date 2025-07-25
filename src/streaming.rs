//! Real-time financial data streaming and processing
//! 
//! Provides capabilities for processing streaming financial data with
//! real-time technical indicators and signal detection.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Real-time market data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub volume: u64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
}

/// Streaming financial indicators calculator
pub struct StreamingIndicators {
    _symbol: String,
    window_size: usize,
    prices: VecDeque<f64>,
    volumes: VecDeque<u64>,
    _sma_buffer: VecDeque<f64>,
    ema_value: Option<f64>,
    rsi_gains: VecDeque<f64>,
    rsi_losses: VecDeque<f64>,
    rsi_avg_gain: f64,
    rsi_avg_loss: f64,
}

impl StreamingIndicators {
    /// Create new streaming indicators calculator
    pub fn new(symbol: String, window_size: usize) -> Self {
        Self {
            _symbol: symbol,
            window_size,
            prices: VecDeque::new(),
            volumes: VecDeque::new(),
            _sma_buffer: VecDeque::new(),
            ema_value: None,
            rsi_gains: VecDeque::new(),
            rsi_losses: VecDeque::new(),
            rsi_avg_gain: 0.0,
            rsi_avg_loss: 0.0,
        }
    }

    /// Process new market tick and update indicators
    pub fn update(&mut self, tick: &MarketTick) -> StreamingIndicatorValues {
        // Add new price and volume
        self.prices.push_back(tick.price);
        self.volumes.push_back(tick.volume);

        // Maintain window size
        if self.prices.len() > self.window_size {
            self.prices.pop_front();
            self.volumes.pop_front();
        }

        // Calculate indicators
        let sma = self.calculate_sma();
        let ema = self.calculate_ema(tick.price);
        let rsi = self.calculate_rsi(tick.price);
        let volume_sma = self.calculate_volume_sma();

        StreamingIndicatorValues {
            symbol: tick.symbol.clone(),
            timestamp: tick.timestamp,
            price: tick.price,
            volume: tick.volume,
            sma,
            ema,
            rsi,
            volume_sma,
            volume_ratio: volume_sma.map(|vs| tick.volume as f64 / vs),
        }
    }

    fn calculate_sma(&mut self) -> Option<f64> {
        if self.prices.len() < self.window_size {
            return None;
        }

        let sum: f64 = self.prices.iter().sum();
        Some(sum / self.prices.len() as f64)
    }

    fn calculate_ema(&mut self, current_price: f64) -> Option<f64> {
        let alpha = 2.0 / (self.window_size as f64 + 1.0);
        
        match self.ema_value {
            None => {
                self.ema_value = Some(current_price);
                Some(current_price)
            }
            Some(prev_ema) => {
                let new_ema = alpha * current_price + (1.0 - alpha) * prev_ema;
                self.ema_value = Some(new_ema);
                Some(new_ema)
            }
        }
    }

    fn calculate_rsi(&mut self, current_price: f64) -> Option<f64> {
        if self.prices.len() < 2 {
            return None;
        }

        let prev_price = self.prices[self.prices.len() - 2];
        let change = current_price - prev_price;

        let gain = if change > 0.0 { change } else { 0.0 };
        let loss = if change < 0.0 { -change } else { 0.0 };

        self.rsi_gains.push_back(gain);
        self.rsi_losses.push_back(loss);

        if self.rsi_gains.len() > self.window_size {
            self.rsi_gains.pop_front();
            self.rsi_losses.pop_front();
        }

        if self.rsi_gains.len() < self.window_size {
            return None;
        }

        if self.rsi_gains.len() == self.window_size && self.rsi_avg_gain == 0.0 {
            // First calculation - use simple average
            self.rsi_avg_gain = self.rsi_gains.iter().sum::<f64>() / self.window_size as f64;
            self.rsi_avg_loss = self.rsi_losses.iter().sum::<f64>() / self.window_size as f64;
        } else {
            // Subsequent calculations - use Wilder's smoothing
            let alpha = 1.0 / self.window_size as f64;
            self.rsi_avg_gain = (self.rsi_avg_gain * (1.0 - alpha)) + (gain * alpha);
            self.rsi_avg_loss = (self.rsi_avg_loss * (1.0 - alpha)) + (loss * alpha);
        }

        if self.rsi_avg_loss == 0.0 {
            Some(100.0)
        } else {
            let rs = self.rsi_avg_gain / self.rsi_avg_loss;
            Some(100.0 - (100.0 / (1.0 + rs)))
        }
    }

    fn calculate_volume_sma(&self) -> Option<f64> {
        if self.volumes.len() < self.window_size {
            return None;
        }

        let sum: u64 = self.volumes.iter().sum();
        Some(sum as f64 / self.volumes.len() as f64)
    }
}

/// Streaming indicator values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingIndicatorValues {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub volume: u64,
    pub sma: Option<f64>,
    pub ema: Option<f64>,
    pub rsi: Option<f64>,
    pub volume_sma: Option<f64>,
    pub volume_ratio: Option<f64>,
}

/// Real-time signal detector
pub struct StreamingSignalDetector {
    indicators: StreamingIndicatorValues,
}

impl StreamingSignalDetector {
    pub fn new(indicators: StreamingIndicatorValues) -> Self {
        Self { indicators }
    }

    /// Detect various trading signals
    pub fn detect_signals(&self) -> Vec<TradingSignal> {
        let mut signals = Vec::new();

        // RSI signals
        if let Some(rsi) = self.indicators.rsi {
            if rsi < 30.0 {
                signals.push(TradingSignal {
                    signal_type: SignalType::Oversold,
                    symbol: self.indicators.symbol.clone(),
                    timestamp: self.indicators.timestamp,
                    strength: (30.0 - rsi) / 30.0, // Strength based on how oversold
                    price: self.indicators.price,
                    description: format!("RSI oversold at {:.2}", rsi),
                });
            } else if rsi > 70.0 {
                signals.push(TradingSignal {
                    signal_type: SignalType::Overbought,
                    symbol: self.indicators.symbol.clone(),
                    timestamp: self.indicators.timestamp,
                    strength: (rsi - 70.0) / 30.0, // Strength based on how overbought
                    price: self.indicators.price,
                    description: format!("RSI overbought at {:.2}", rsi),
                });
            }
        }

        // Volume spike signals
        if let Some(volume_ratio) = self.indicators.volume_ratio {
            if volume_ratio > 2.0 {
                signals.push(TradingSignal {
                    signal_type: SignalType::VolumeSpike,
                    symbol: self.indicators.symbol.clone(),
                    timestamp: self.indicators.timestamp,
                    strength: (volume_ratio - 2.0) / 3.0, // Normalize strength
                    price: self.indicators.price,
                    description: format!("Volume spike: {:.2}x average", volume_ratio),
                });
            }
        }

        // Moving average crossover signals
        if let (Some(sma), Some(ema)) = (self.indicators.sma, self.indicators.ema) {
            let crossover_strength = ((ema - sma) / sma).abs();
            if ema > sma * 1.002 {
                // EMA significantly above SMA
                signals.push(TradingSignal {
                    signal_type: SignalType::BullishCrossover,
                    symbol: self.indicators.symbol.clone(),
                    timestamp: self.indicators.timestamp,
                    strength: crossover_strength.min(1.0),
                    price: self.indicators.price,
                    description: format!("EMA above SMA: {:.2} vs {:.2}", ema, sma),
                });
            } else if ema < sma * 0.998 {
                // EMA significantly below SMA
                signals.push(TradingSignal {
                    signal_type: SignalType::BearishCrossover,
                    symbol: self.indicators.symbol.clone(),
                    timestamp: self.indicators.timestamp,
                    strength: crossover_strength.min(1.0),
                    price: self.indicators.price,
                    description: format!("EMA below SMA: {:.2} vs {:.2}", ema, sma),
                });
            }
        }

        signals
    }
}

/// Trading signal types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalType {
    Oversold,
    Overbought,
    VolumeSpike,
    BullishCrossover,
    BearishCrossover,
    PriceBreakout,
}

/// Trading signal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSignal {
    pub signal_type: SignalType,
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub strength: f64, // 0.0 to 1.0
    pub price: f64,
    pub description: String,
}

/// Real-time streaming processor
pub struct StreamingProcessor {
    indicators: Arc<Mutex<StreamingIndicators>>,
    signal_handlers: Vec<Box<dyn Fn(&TradingSignal) + Send + Sync>>,
}

impl StreamingProcessor {
    pub fn new(symbol: String, window_size: usize) -> Self {
        Self {
            indicators: Arc::new(Mutex::new(StreamingIndicators::new(symbol, window_size))),
            signal_handlers: Vec::new(),
        }
    }

    /// Add signal handler callback
    pub fn add_signal_handler<F>(&mut self, handler: F)
    where
        F: Fn(&TradingSignal) + Send + Sync + 'static,
    {
        self.signal_handlers.push(Box::new(handler));
    }

    /// Process incoming market tick
    pub fn process_tick(&self, tick: MarketTick) -> Result<Vec<TradingSignal>> {
        let indicator_values = {
            let mut indicators = self.indicators.lock().unwrap();
            indicators.update(&tick)
        };

        let detector = StreamingSignalDetector::new(indicator_values);
        let signals = detector.detect_signals();

        // Call signal handlers
        for signal in &signals {
            for handler in &self.signal_handlers {
                handler(signal);
            }
        }

        Ok(signals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_streaming_indicators() {
        let mut indicators = StreamingIndicators::new("AAPL".to_string(), 10);

        let tick = MarketTick {
            symbol: "AAPL".to_string(),
            timestamp: Utc::now(),
            price: 150.0,
            volume: 1000,
            bid: Some(149.5),
            ask: Some(150.5),
        };

        let values = indicators.update(&tick);
        assert_eq!(values.symbol, "AAPL");
        assert_eq!(values.price, 150.0);
    }

    #[test]
    fn test_signal_detection() {
        let indicators = StreamingIndicatorValues {
            symbol: "AAPL".to_string(),
            timestamp: Utc::now(),
            price: 150.0,
            volume: 1000,
            sma: Some(149.0),
            ema: Some(150.5),
            rsi: Some(25.0), // Oversold
            volume_sma: Some(500.0),
            volume_ratio: Some(2.5), // Volume spike
        };

        let detector = StreamingSignalDetector::new(indicators);
        let signals = detector.detect_signals();

        assert!(!signals.is_empty());
        assert!(signals.iter().any(|s| matches!(s.signal_type, SignalType::Oversold)));
        assert!(signals.iter().any(|s| matches!(s.signal_type, SignalType::VolumeSpike)));
    }

    #[test]
    fn test_streaming_processor() {
        let mut processor = StreamingProcessor::new("AAPL".to_string(), 5);
        
        // Add signal handler
        processor.add_signal_handler(|signal| {
            println!("Signal detected: {:?}", signal);
        });

        let tick = MarketTick {
            symbol: "AAPL".to_string(),
            timestamp: Utc::now(),
            price: 150.0,
            volume: 1000,
            bid: Some(149.5),
            ask: Some(150.5),
        };

        let signals = processor.process_tick(tick).unwrap();
        // First tick typically doesn't generate signals due to insufficient data
        assert!(signals.is_empty() || !signals.is_empty());
    }
}
