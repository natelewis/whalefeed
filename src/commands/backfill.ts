#!/usr/bin/env tsx

import { BackfillService } from '../services/backfill';
import chalk from 'chalk';

function parseDate(dateStr: string): Date {
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid date format: ${dateStr}. Please use YYYY-MM-DD format.`);
  }
  return date;
}

function showUsage() {
  console.log(chalk.blue('Usage:'));
  console.log('  npm run backfill                    # Backfill all tickers from last sync');
  console.log('  npm run backfill <ticker>           # Backfill specific ticker from last sync');
  console.log(
    '  npm run backfill <ticker> <date>    # Backfill specific ticker from specific date (replaces existing data)'
  );
  console.log('  npm run backfill <ticker> <date> --skip-replace  # Skip data replacement (may create duplicates)');
  console.log(
    '  npm run backfill <date>             # Backfill all tickers from specific date (replaces existing data)'
  );
  console.log('');
  console.log(chalk.blue('Date format: YYYY-MM-DD (e.g., 2025-09-01)'));
  console.log('');
  console.log(chalk.blue('Examples:'));
  console.log('  npm run backfill AAPL 2025-09-01');
  console.log('  npm run backfill AAPL 2025-09-01 --skip-replace');
  console.log('  npm run backfill 2025-09-01         # Backfill all tickers from date');
  console.log('');
  console.log(chalk.yellow('Note: Data replacement is enabled by default to prevent duplicates.'));
  console.log(chalk.yellow('Use --skip-replace only if you are certain no duplicates will occur.'));
}

async function main() {
  const args = process.argv.slice(2);

  // Check for help flag
  if (args.includes('--help') || args.includes('-h')) {
    showUsage();
    return;
  }

  let ticker: string | undefined;
  let startDate: Date | undefined;
  let skipReplace = false;

  // Parse arguments
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    if (arg === '--skip-replace') {
      skipReplace = true;
    } else if (!ticker && !startDate) {
      // First argument could be ticker or date
      if (arg.match(/^\d{4}-\d{2}-\d{2}$/)) {
        // It's a date
        startDate = parseDate(arg);
      } else {
        // It's a ticker
        ticker = arg;
      }
    } else if (!startDate) {
      // Second argument should be date
      startDate = parseDate(arg);
    }
  }

  console.log(chalk.blue('Starting data backfill...'));

  if (ticker && startDate) {
    console.log(
      chalk.yellow(
        `Backfilling data for ${ticker} from ${startDate.toISOString().split('T')[0]}${
          skipReplace ? ' (skipping data replacement)' : ' (replacing existing data)'
        }`
      )
    );
  } else if (ticker) {
    console.log(chalk.yellow(`Backfilling data for specific ticker: ${ticker}`));
  } else if (startDate) {
    console.log(
      chalk.yellow(
        `Backfilling data for all tickers from ${startDate.toISOString().split('T')[0]} (replacing existing data)`
      )
    );
  } else {
    console.log(chalk.yellow('Backfilling data for all configured tickers'));
  }

  const backfillService = new BackfillService();

  try {
    if (ticker && startDate) {
      await backfillService.backfillTickerFromDate(ticker, startDate, skipReplace);
    } else if (ticker) {
      await backfillService.backfillTicker(ticker);
    } else if (startDate) {
      await backfillService.backfillAllFromDate(startDate);
    } else {
      await backfillService.backfillAll();
    }

    console.log(chalk.green('Backfill completed successfully'));
  } catch (error) {
    console.error(chalk.red('Backfill failed:'), error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error(chalk.red('Unhandled error:'), error);
  process.exit(1);
});
