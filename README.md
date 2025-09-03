# Sravz Backend Rust Codebase

## Overview

This repository contains the Rust backend for Sravz, designed for scalable, high-performance data processing, NSQ messaging, MongoDB integration, and advanced analytics using Polars and Python interoperability.

## Features

- **NSQ Messaging**: Consumes and publishes messages using NSQ for distributed processing.
- **MongoDB Integration**: Stores and retrieves message data in MongoDB.
- **DataFrame Analytics**: Uses Polars for fast DataFrame operations and analytics.
- **Python Interop**: Executes Python code via PyO3 for advanced analytics and LLM queries.
- **Modular Services**: Includes services for leveraged funds, earnings, S3 storage, and more.
- **Configurable**: Uses TOML config files for environment-specific settings.
- **Logging**: Integrated logging for debugging and monitoring.

## Directory Structure

- `src/` - Main Rust source code, including services, models, router, and Python interop.
- `config.production.toml` - Example configuration file for production.
- `Cargo.toml` - Rust package and dependency configuration.
- `Dockerfile` - Containerization support for deployment.
- `tests/` - Test cases and message samples.

## Getting Started

### Prerequisites

- Rust (edition 2021)
- MongoDB
- NSQ
- Python (for PyO3 interop)
- S3-compatible storage (optional, for some services)

### Build Instructions

```bash
cargo build --release
```

### Running

Edit `config.production.toml` for your environment. Then run:

```bash
cargo run
```

### NSQ Usage Example

```bash
# Publish message
curl -d "@tests/message.json" http://nsqd-1:4151/pub?topic=production_backend-rust

# Get messages
nsq_tail --lookupd-http-address=nsqlookupd-1:4161 --topic=production_backend-rust
```

## License
Sravz LLC
