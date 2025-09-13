#!/usr/bin/env tsx

import { db } from './db/connection';
import chalk from 'chalk';

async function main() {
  console.log(chalk.blue('WhaleFeed - Trade Data Ingestion System'));
  console.log(chalk.yellow('Use make commands to run specific operations:'));
  console.log(chalk.gray('  make ingest    - Start real-time data ingestion'));
  console.log(chalk.gray('  make backfill  - Backfill historical data'));
  console.log(chalk.gray('  make reset     - Reset all data'));
  console.log(chalk.gray('  make install   - Install dependencies'));
  console.log(chalk.gray('  make dev       - Development mode with hot reload'));
  
  // Initialize database connection
  try {
    await db.connect();
    await db.executeSchema();
    console.log(chalk.green('Database initialized successfully'));
  } catch (error) {
    console.error(chalk.red('Failed to initialize database:'), error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error(chalk.red('Unhandled error:'), error);
  process.exit(1);
});
