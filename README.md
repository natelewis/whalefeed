# WhaleFeed - Trade Data Ingestion System

A TypeScript-based system for ingesting real-time and historical trade data from Polygon.io into QuestDB.

## Features

- **Real-time Data Ingestion**: WebSocket-based streaming of stock trades and 5-minute aggregates
- **Historical Backfill**: Comprehensive backfill system for missing data with crash recovery
- **Option Contracts**: Support for option contract data ingestion
- **Hot Reloading**: Development mode with automatic restart on code changes
- **Resilient Design**: Crash recovery and gap-free data ingestion
- **TypeScript**: Fully typed codebase without `any` or `unknown` types

## Prerequisites

- Node.js 18+ 
- QuestDB running on localhost:9000
- Polygon.io API key

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   make install
   ```
3. Copy the environment template and add your API key:
   ```bash
   cp env.example .env
   # Edit .env and add your POLYGON_API_KEY
   ```

## Usage

### Available Commands

- `make install` - Install all dependencies
- `make ingest` - Start real-time data ingestion with hot reloading
- `make backfill` - Backfill historical data for all tickers
- `make backfill TICKER` - Backfill historical data for a specific ticker
- `make reset` - Reset all data (with confirmation prompt)
- `make dev` - Development mode with hot reloading (same as ingest)

### Data Types

#### Stock Data
- **Trades**: Individual trade records with price, size, conditions, exchange
- **Aggregates**: 5-minute OHLCV bars for charting applications

#### Option Data
- **Contracts**: Option contract specifications
- **Trades**: Option trade records with receipt timestamps

## Configuration

The system is configured via environment variables in `.env`:

```env
POLYGON_API_KEY=your_api_key_here
QUESTDB_HOST=127.0.0.1
QUESTDB_PORT=9000
QUESTDB_USER=admin
QUESTDB_PASSWORD=quest
LOG_LEVEL=info
MAX_RETRIES=3
RETRY_DELAY_MS=1000
```

## Architecture

- **Database Layer**: QuestDB connection and schema management
- **API Layer**: Polygon.io REST API client
- **WebSocket Layer**: Real-time data streaming
- **Ingestion Layer**: Data processing and storage
- **Command Layer**: CLI commands for different operations

## Development

The system supports hot reloading during development. When you run `make ingest` or `make dev`, the system will automatically restart when code changes are detected.

## Data Recovery

The system is designed to be resilient:
- Sync states are tracked for each ticker
- Backfill operations can resume from the last known position
- WebSocket connections automatically reconnect on failure
- No data gaps occur during restarts

## License

MIT
# whalefeed
