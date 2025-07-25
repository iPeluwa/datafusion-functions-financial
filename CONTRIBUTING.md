# Contributing to DataFusion Financial Functions

Thank you for your interest in contributing! This project extends Apache DataFusion with financial analysis functions.

## Development Setup

1. **Prerequisites:**
   - Rust 1.70+ 
   - Git

2. **Clone and build:**
   ```bash
   git clone <repository-url>
   cd datafusion-functions-financial
   cargo build
   ```

3. **Run tests:**
   ```bash
   cargo test
   ```

4. **Run examples:**
   ```bash
   # Local development (no credentials required)
   cargo run --example local_demo
   
   # Data validation
   cargo run --example validate
   
   # Multi-asset class demo
   cargo run --example multi_asset_class_demo
   ```

## Contributing New Functions

When adding new financial functions:

1. Create a new module in `src/functions/`
2. Implement the `WindowUDFImpl` trait
3. Add registration function
4. Update `src/functions/mod.rs` and `src/lib.rs`
5. Add comprehensive tests
6. Update documentation

## Contributing to Data Integration

When working with data loading and validation:

1. **Polygon.io integration:** Add new methods to `src/polygon/client.rs`
2. **Data validation:** Extend `src/polygon/validator.rs`
3. **Signal detection:** Add algorithms to `src/polygon/signals.rs`
4. **New data sources:** Extend the `DataSource` enum in `src/polygon/config.rs`

## Module Structure

The codebase is organized as follows:

- `src/functions/` - Technical indicator implementations
- `src/polygon/` - Data loading and Polygon.io integration
  - `config.rs` - Configuration and data source definitions
  - `types.rs` - Asset classes and data types
  - `client.rs` - Main client and data loading logic
  - `validator.rs` - Data quality validation
  - `signals.rs` - Trading signal detection
- `src/streaming.rs` - Real-time data processing

### Function Guidelines

- Use `WindowUDFImpl` for time-series functions
- Handle null values gracefully
- Include mathematical formulas in documentation
- Add both unit tests and integration tests
- Follow existing naming conventions

### Testing

- Unit tests for individual functions
- Integration tests for SQL usage
- Performance tests for large datasets
- Test edge cases (null values, empty datasets, etc.)

## Code Style

- Use `cargo fmt` for formatting
- Run `cargo clippy` for linting
- Follow Rust naming conventions
- Add documentation comments

## Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Ensure all tests pass
6. Submit a pull request

## Questions?

Open an issue for discussion before implementing major features.
