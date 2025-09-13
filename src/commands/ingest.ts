#!/usr/bin/env tsx

import { DataIngestionService } from '../services/data-ingestion';
import chalk from 'chalk';

async function main() {
  console.log(chalk.blue('Starting real-time data ingestion...'));
  
  const ingestionService = new DataIngestionService();
  
  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log(chalk.yellow('\nReceived SIGINT, shutting down gracefully...'));
    await ingestionService.stopIngestion();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log(chalk.yellow('\nReceived SIGTERM, shutting down gracefully...'));
    await ingestionService.stopIngestion();
    process.exit(0);
  });

  try {
    await ingestionService.startIngestion();
    
    // Keep the process running
    console.log(chalk.green('Data ingestion is running. Press Ctrl+C to stop.'));
    
    // Keep alive
    setInterval(() => {
      // Heartbeat to keep process alive
    }, 60000);
    
  } catch (error) {
    console.error(chalk.red('Failed to start ingestion:'), error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error(chalk.red('Unhandled error:'), error);
  process.exit(1);
});
