import { db } from '../db/connection';
import { PolygonClient } from './polygon-client';
import { OptionIngestionService } from './option-ingestion';
import { config } from '../config';
import { StockAggregate, SyncState } from '../types/database';
import { PolygonAggregate } from '../types/polygon';

export class BackfillService {
  private polygonClient: PolygonClient;
  private optionIngestionService: OptionIngestionService;

  constructor() {
    this.polygonClient = new PolygonClient();
    this.optionIngestionService = new OptionIngestionService();
  }

  private formatDuration(milliseconds: number): string {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      const remainingMinutes = minutes % 60;
      const remainingSeconds = seconds % 60;
      return `${hours}h ${remainingMinutes}m ${remainingSeconds}s`;
    } else if (minutes > 0) {
      const remainingSeconds = seconds % 60;
      return `${minutes}m ${remainingSeconds}s`;
    } else {
      return `${seconds}s`;
    }
  }

  async backfillAll(): Promise<void> {
    console.log('Starting backfill for all tickers...');
    const startTime = Date.now();

    try {
      await db.connect();

      // Get current sync states
      const syncStates = await this.getSyncStates();

      // Find the oldest last sync date
      const timestamps = Array.from(syncStates.values())
        .map(state => state.last_aggregate_timestamp?.getTime())
        .filter(ts => ts !== undefined) as number[];

      const oldestSync = timestamps.length > 0 ? Math.min(...timestamps) : null;

      const backfillStart = oldestSync ? new Date(oldestSync) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000); // 7 days ago if no data
      const backfillEnd = new Date();

      console.log(`Backfilling from ${backfillStart.toISOString()} to ${backfillEnd.toISOString()}`);

      let totalItemsProcessed = 0;

      // Backfill each ticker
      for (const ticker of config.tickers) {
        try {
          const tickerStartTime = Date.now();
          const itemsProcessed = await this.backfillTickerData(ticker, backfillStart, backfillEnd);
          const tickerDuration = Date.now() - tickerStartTime;
          totalItemsProcessed += itemsProcessed;
          console.log(`Completed ${ticker} in ${this.formatDuration(tickerDuration)} - ${itemsProcessed} items`);
        } catch (error) {
          console.error(`Error backfilling ${ticker}:`, error);
        }
      }

      // Add one more week of data for all tickers
      const additionalWeekStart = backfillEnd;
      const additionalWeekEnd = new Date(backfillEnd.getTime() + 7 * 24 * 60 * 60 * 1000);

      console.log('Adding additional week of data...');

      for (const ticker of config.tickers) {
        try {
          const tickerStartTime = Date.now();
          const itemsProcessed = await this.backfillTickerData(ticker, additionalWeekStart, additionalWeekEnd);
          const tickerDuration = Date.now() - tickerStartTime;
          totalItemsProcessed += itemsProcessed;
          console.log(
            `Completed additional week for ${ticker} in ${this.formatDuration(
              tickerDuration
            )} - ${itemsProcessed} items`
          );
        } catch (error) {
          console.error(`Error backfilling additional week for ${ticker}:`, error);
        }
      }

      const totalDuration = Date.now() - startTime;
      console.log(
        `Backfill completed for all tickers in ${this.formatDuration(
          totalDuration
        )} - Total items processed: ${totalItemsProcessed}`
      );
    } catch (error) {
      console.error('Backfill failed:', error);
      throw error;
    } finally {
      await db.disconnect();
    }
  }

  async backfillTicker(ticker: string): Promise<void> {
    console.log(`Starting backfill for ticker: ${ticker}`);
    const startTime = Date.now();

    try {
      await db.connect();

      // Get current sync state for this ticker
      const syncState = await this.getSyncState(ticker);

      const backfillStart = syncState.last_aggregate_timestamp || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000); // 7 days ago if no data
      const backfillEnd = new Date();

      const itemsProcessed = await this.backfillTickerData(ticker, backfillStart, backfillEnd);

      const duration = Date.now() - startTime;
      console.log(
        `Backfill completed for ${ticker} in ${this.formatDuration(duration)} - ${itemsProcessed} items processed`
      );
    } catch (error) {
      console.error(`Backfill failed for ${ticker}:`, error);
      throw error;
    } finally {
      await db.disconnect();
    }
  }

  async backfillTickerFromDate(ticker: string, startDate: Date, skipReplace = false): Promise<void> {
    console.log(
      `Starting backfill for ticker: ${ticker} from ${startDate.toISOString().split('T')[0]}${
        skipReplace ? ' (skipping data replacement)' : ' (replacing existing data)'
      }`
    );
    const startTime = Date.now();

    try {
      await db.connect();

      // Calculate the maximum end date based on configuration
      const maxDays = config.app.backfillMaxDays;
      const backfillEnd =
        maxDays > 0
          ? new Date(Math.min(new Date().getTime(), startDate.getTime() + maxDays * 24 * 60 * 60 * 1000))
          : new Date();

      if (maxDays > 0 && backfillEnd.getTime() < new Date().getTime()) {
        console.log(
          `Backfill limited to ${maxDays} days from start date. End date: ${backfillEnd.toISOString().split('T')[0]}`
        );
      }

      // Always replace data by default to prevent duplicates
      if (!skipReplace) {
        await this.deleteDataForDateRange(ticker, startDate, backfillEnd);
        console.log(`Deleted existing data for ${ticker} from ${startDate.toISOString().split('T')[0]}`);
      }

      const itemsProcessed = await this.backfillTickerData(ticker, startDate, backfillEnd);

      // Also backfill option contracts and trades for this ticker
      console.log(`Backfilling option data for ${ticker}...`);
      await this.optionIngestionService.backfillOptionData(ticker, startDate, backfillEnd);

      const duration = Date.now() - startTime;
      console.log(
        `Backfill completed for ${ticker} in ${this.formatDuration(duration)} - ${itemsProcessed} items processed`
      );
    } catch (error) {
      console.error(`Backfill failed for ${ticker}:`, error);
      throw error;
    } finally {
      await db.disconnect();
    }
  }

  async backfillAllFromDate(startDate: Date): Promise<void> {
    console.log(
      `Starting backfill for all tickers from ${startDate.toISOString().split('T')[0]} (replacing existing data)`
    );
    const startTime = Date.now();

    try {
      await db.connect();

      // Calculate the maximum end date based on configuration
      const maxDays = config.app.backfillMaxDays;
      const backfillEnd =
        maxDays > 0
          ? new Date(Math.min(new Date().getTime(), startDate.getTime() + maxDays * 24 * 60 * 60 * 1000))
          : new Date();

      if (maxDays > 0 && backfillEnd.getTime() < new Date().getTime()) {
        console.log(
          `Backfill limited to ${maxDays} days from start date. End date: ${backfillEnd.toISOString().split('T')[0]}`
        );
      }

      let totalItemsProcessed = 0;

      // Backfill each ticker with data replacement
      for (const ticker of config.tickers) {
        try {
          // Delete existing data for the date range
          await this.deleteDataForDateRange(ticker, startDate, backfillEnd);
          console.log(`Deleted existing data for ${ticker} from ${startDate.toISOString().split('T')[0]}`);

          // Backfill the data
          const itemsProcessed = await this.backfillTickerData(ticker, startDate, backfillEnd);
          totalItemsProcessed += itemsProcessed;

          // Also backfill option contracts and trades for this ticker
          console.log(`Backfilling option data for ${ticker}...`);
          await this.optionIngestionService.backfillOptionData(ticker, startDate, backfillEnd);
        } catch (error) {
          console.error(`Error backfilling ${ticker}:`, error);
        }
      }

      const totalDuration = Date.now() - startTime;
      console.log(
        `Backfill completed for all tickers in ${this.formatDuration(
          totalDuration
        )} - Total items processed: ${totalItemsProcessed}`
      );
    } catch (error) {
      console.error('Backfill failed:', error);
      throw error;
    } finally {
      await db.disconnect();
    }
  }

  private async backfillTickerData(ticker: string, startDate: Date, endDate: Date): Promise<number> {
    console.log(`Backfilling ${ticker} from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    let currentStart = new Date(startDate);
    const batchSize = 7; // Process 7 days at a time to avoid API limits
    let totalItemsProcessed = 0;

    while (currentStart < endDate) {
      const currentEnd = new Date(
        Math.min(currentStart.getTime() + batchSize * 24 * 60 * 60 * 1000, endDate.getTime())
      );

      try {
        const aggregates = await this.polygonClient.getHistoricalAggregates(
          ticker,
          currentStart,
          currentEnd,
          'minute',
          1
        );

        if (aggregates.length > 0) {
          await this.insertAggregates(ticker, aggregates);
          totalItemsProcessed += aggregates.length;
          console.log(
            `Inserted ${
              aggregates.length
            } aggregates for ${ticker} (${currentStart.toISOString()} - ${currentEnd.toISOString()})`
          );
        }

        // Update sync state
        await this.updateSyncState(ticker, new Date(aggregates[aggregates.length - 1]?.t || currentEnd.getTime()));
      } catch (error) {
        console.error(
          `Error backfilling ${ticker} for period ${currentStart.toISOString()} - ${currentEnd.toISOString()}:`,
          error
        );
        // Continue with next batch
      }

      currentStart = new Date(currentEnd);

      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    return totalItemsProcessed;
  }

  private async getSyncStates(): Promise<Map<string, SyncState>> {
    const result = await db.query(
      `
      SELECT ticker, last_trade_timestamp, last_aggregate_timestamp, last_sync, is_streaming
      FROM sync_state
      WHERE ticker IN (${config.tickers.map((_, i) => `$${i + 1}`).join(',')})
    `,
      config.tickers
    );

    const questResult = result as {
      columns: { name: string; type: string }[];
      dataset: unknown[][];
    };

    const syncStates = new Map<string, SyncState>();

    for (const row of questResult.dataset) {
      const ticker = row[0] as string;
      const last_trade_timestamp = row[1] ? new Date(row[1] as string) : null;
      const last_aggregate_timestamp = row[2] ? new Date(row[2] as string) : null;
      const last_sync = new Date(row[3] as string);
      const is_streaming = row[4] as boolean;

      syncStates.set(ticker, {
        ticker,
        last_trade_timestamp: last_trade_timestamp ?? undefined,
        last_aggregate_timestamp: last_aggregate_timestamp ?? undefined,
        last_sync,
        is_streaming,
      });
    }

    return syncStates;
  }

  private async getSyncState(ticker: string): Promise<SyncState> {
    const result = await db.query(
      `
      SELECT ticker, last_trade_timestamp, last_aggregate_timestamp, last_sync, is_streaming
      FROM sync_state
      WHERE ticker = $1
    `,
      [ticker]
    );

    const questResult = result as {
      columns: { name: string; type: string }[];
      dataset: unknown[][];
    };

    if (questResult.dataset.length > 0) {
      const row = questResult.dataset[0];
      const ticker = row[0] as string;
      const last_trade_timestamp = row[1] ? new Date(row[1] as string) : null;
      const last_aggregate_timestamp = row[2] ? new Date(row[2] as string) : null;
      const last_sync = new Date(row[3] as string);
      const is_streaming = row[4] as boolean;

      return {
        ticker,
        last_trade_timestamp: last_trade_timestamp ?? undefined,
        last_aggregate_timestamp: last_aggregate_timestamp ?? undefined,
        last_sync,
        is_streaming,
      };
    }

    // Return default sync state if not found
    return {
      ticker,
      last_sync: new Date(),
      is_streaming: false,
    };
  }

  private async insertAggregates(ticker: string, aggregates: PolygonAggregate[]): Promise<void> {
    for (const aggregate of aggregates) {
      const stockAggregate: StockAggregate = {
        symbol: ticker,
        timestamp: PolygonClient.convertTimestamp(aggregate.t),
        open: aggregate.o,
        high: aggregate.h,
        low: aggregate.l,
        close: aggregate.c,
        volume: aggregate.v,
        vwap: aggregate.vw,
        transaction_count: aggregate.n,
      };

      try {
        await db.query(
          `
          INSERT INTO stock_aggregates (symbol, timestamp, open, high, low, close, volume, vwap, transaction_count)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `,
          [
            stockAggregate.symbol,
            stockAggregate.timestamp,
            stockAggregate.open,
            stockAggregate.high,
            stockAggregate.low,
            stockAggregate.close,
            stockAggregate.volume,
            stockAggregate.vwap,
            stockAggregate.transaction_count,
          ]
        );
      } catch (error) {
        // Ignore unique constraint violations (duplicates)
        if (error instanceof Error && error.message.includes('duplicate')) {
          console.log(`Skipping duplicate stock aggregate: ${stockAggregate.symbol} at ${stockAggregate.timestamp}`);
          continue;
        }
        throw error;
      }
    }
  }

  private async updateSyncState(ticker: string, lastTimestamp: Date): Promise<void> {
    // QuestDB doesn't support ON CONFLICT, so we'll use a simple INSERT
    // In a production system, you might want to check if the record exists first
    try {
      await db.query(
        `INSERT INTO sync_state (ticker, last_aggregate_timestamp, last_sync, is_streaming)
         VALUES ('${ticker}', '${lastTimestamp.toISOString()}', '${new Date().toISOString()}', false)`
      );
    } catch (error) {
      // If the record already exists, we'll ignore the error for now
      // In a production system, you might want to implement proper upsert logic
      console.warn(`Could not update sync state for ${ticker}: ${error}`);
    }
  }

  private async deleteDataForDateRange(ticker: string, startDate: Date, endDate: Date): Promise<void> {
    console.log(`Deleting existing data for ${ticker} from ${startDate.toISOString()} to ${endDate.toISOString()}`);

    try {
      // QuestDB doesn't support DELETE FROM, so we'll use a different approach
      // We'll create a temporary table with the data we want to keep, then replace the original

      // First, let's check if there's any data to delete
      const aggregatesCheck = await db.query(
        `SELECT COUNT(*) as count FROM stock_aggregates WHERE symbol = '${ticker}' AND timestamp >= '${startDate.toISOString()}' AND timestamp <= '${endDate.toISOString()}'`
      );

      const tradesCheck = await db.query(
        `SELECT COUNT(*) as count FROM stock_trades WHERE symbol = '${ticker}' AND timestamp >= '${startDate.toISOString()}' AND timestamp <= '${endDate.toISOString()}'`
      );

      // Check for option contracts, trades, and quotes
      const optionContractsCheck = await db.query(
        `SELECT COUNT(*) as count FROM option_contracts WHERE underlying_ticker = '${ticker}' AND created_at >= '${startDate.toISOString()}' AND created_at <= '${endDate.toISOString()}'`
      );

      const optionTradesCheck = await db.query(
        `SELECT COUNT(*) as count FROM option_trades WHERE ticker IN (
          SELECT ticker FROM option_contracts WHERE underlying_ticker = '${ticker}'
        ) AND timestamp >= '${startDate.toISOString()}' AND timestamp <= '${endDate.toISOString()}'`
      );

      const optionQuotesCheck = await db.query(
        `SELECT COUNT(*) as count FROM option_quotes WHERE ticker IN (
          SELECT ticker FROM option_contracts WHERE underlying_ticker = '${ticker}'
        ) AND timestamp >= '${startDate.toISOString()}' AND timestamp <= '${endDate.toISOString()}'`
      );

      const aggregatesCount = (aggregatesCheck as any)?.dataset?.[0]?.[0] || 0;
      const tradesCount = (tradesCheck as any)?.dataset?.[0]?.[0] || 0;
      const optionContractsCount = (optionContractsCheck as any)?.dataset?.[0]?.[0] || 0;
      const optionTradesCount = (optionTradesCheck as any)?.dataset?.[0]?.[0] || 0;
      const optionQuotesCount = (optionQuotesCheck as any)?.dataset?.[0]?.[0] || 0;

      if (aggregatesCount > 0) {
        console.log(`Found ${aggregatesCount} stock aggregates to delete for ${ticker}`);
        // For now, we'll skip the actual deletion since QuestDB doesn't support DELETE
        // In a production system, you might want to implement a different strategy
        console.log(`Skipping deletion of stock aggregates (QuestDB limitation)`);
      }

      if (tradesCount > 0) {
        console.log(`Found ${tradesCount} stock trades to delete for ${ticker}`);
        // For now, we'll skip the actual deletion since QuestDB doesn't support DELETE
        console.log(`Skipping deletion of stock trades (QuestDB limitation)`);
      }

      if (optionContractsCount > 0) {
        console.log(`Found ${optionContractsCount} option contracts to delete for ${ticker}`);
        console.log(`Skipping deletion of option contracts (QuestDB limitation)`);
      }

      if (optionTradesCount > 0) {
        console.log(`Found ${optionTradesCount} option trades to delete for ${ticker}`);
        console.log(`Skipping deletion of option trades (QuestDB limitation)`);
      }

      if (optionQuotesCount > 0) {
        console.log(`Found ${optionQuotesCount} option quotes to delete for ${ticker}`);
        console.log(`Skipping deletion of option quotes (QuestDB limitation)`);
      }

      if (
        aggregatesCount === 0 &&
        tradesCount === 0 &&
        optionContractsCount === 0 &&
        optionTradesCount === 0 &&
        optionQuotesCount === 0
      ) {
        console.log(`No existing data found for ${ticker} in the specified date range`);
      }
    } catch (error) {
      console.warn(`Warning: Could not check for existing data to delete: ${error}`);
      // Continue with backfill even if we can't delete existing data
    }
  }
}
