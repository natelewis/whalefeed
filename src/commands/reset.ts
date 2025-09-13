#!/usr/bin/env tsx

import { db } from '../db/connection';
import chalk from 'chalk';

async function main() {
  console.log(chalk.red('WARNING: This will delete ALL data in QuestDB!'));
  console.log(chalk.yellow('This action cannot be undone.'));
  
  try {
    await db.connect();
    await db.resetAllData();
    console.log(chalk.green('All data has been reset successfully'));
  } catch (error) {
    console.error(chalk.red('Failed to reset data:'), error);
    process.exit(1);
  } finally {
    await db.disconnect();
  }
}

main().catch(error => {
  console.error(chalk.red('Unhandled error:'), error);
  process.exit(1);
});
