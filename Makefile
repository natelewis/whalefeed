.PHONY: install ingest backfill reset dev clean

# Install dependencies
install:
	@echo "Installing dependencies..."
	npm install

# Start real-time data ingestion
ingest:
	@echo "Starting real-time data ingestion..."
	npm run ingest

# Backfill historical data
backfill:
	@echo "Backfilling historical data..."
	npm run backfill

# Reset all data (with confirmation)
reset:
	@echo "This will delete ALL data in QuestDB. Are you sure? (y/N)"
	@read -r confirm && [ "$$confirm" = "y" ] || exit 1
	npm run reset

# Development mode with hot reload
dev:
	@echo "Starting development mode with hot reload..."
	npm run dev

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf dist/
	rm -rf node_modules/

# Build the project
build:
	@echo "Building project..."
	npm run build
