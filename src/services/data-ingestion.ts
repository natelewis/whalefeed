import { db } from '../db/connection';
import { PolygonClient } from './polygon-client';
import { PolygonWebSocketClient } from './websocket-client';
import { 
  StockTrade, 
  StockAggregate, 
  SyncState 
} from '../types/database';
import { 
  PolygonTrade, 
  PolygonQuote, 
  PolygonAggregate
} from '../types/polygon';
import { config } from '../config';

export class DataIngestionService {
  private polygonClient: PolygonClient;
  private wsClient: PolygonWebSocketClient;
  private isIngesting = false;
  private syncStates = new Map<string, SyncState>();

  constructor() {
    this.polygonClient = new PolygonClient();
    this.wsClient = new PolygonWebSocketClient();
    this.setupWebSocketHandlers();
  }

  private setupWebSocketHandlers(): void {
    this.wsClient.setEventHandlers({
      onTrade: (trade: unknown, symbol: string) => this.handleTrade(trade as PolygonTrade, symbol),
      onQuote: (quote: unknown, symbol: string) => this.handleQuote(quote as PolygonQuote, symbol),
      onAggregate: (aggregate: unknown, symbol: string) => this.handleAggregate(aggregate as PolygonAggregate, symbol),
      onError: (error: Error) => {
        console.error('WebSocket error:', error);
        this.handleIngestionError(error);
      },
      onConnect: () => {
        console.log('WebSocket connected, starting data ingestion');
        this.startStreaming();
      },
      onDisconnect: () => {
        console.log('WebSocket disconnected, stopping data ingestion');
        this.isIngesting = false;
      }
    });
  }

  async startIngestion(): Promise<void> {
    try {
      console.log('Starting data ingestion...');
      
      // Connect to database
      await db.connect();
      
      // Initialize sync states
      await this.initializeSyncStates();
      
      // Catch up on missing data before starting real-time
      await this.catchUpData();
      
      // Connect to WebSocket
      await this.wsClient.connect();
      
      this.isIngesting = true;
      console.log('Data ingestion started successfully');
      
    } catch (error) {
      console.error('Failed to start ingestion:', error);
      throw error;
    }
  }

  async stopIngestion(): Promise<void> {
    console.log('Stopping data ingestion...');
    this.isIngesting = false;
    this.wsClient.disconnect();
    await db.disconnect();
    console.log('Data ingestion stopped');
  }

  private async initializeSyncStates(): Promise<void> {
    try {
      // Get current sync states from database
      const result = await db.query(`
        SELECT ticker, last_trade_timestamp, last_aggregate_timestamp, last_sync, is_streaming
        FROM sync_state
        WHERE ticker IN (${config.tickers.map(() => '?').join(',')})
      `, config.tickers);

      const rows = result as { 
        ticker: string; 
        last_trade_timestamp: Date | null; 
        last_aggregate_timestamp: Date | null; 
        last_sync: Date; 
        is_streaming: boolean; 
      }[];

      for (const row of rows) {
        this.syncStates.set(row.ticker, {
          ticker: row.ticker,
        last_trade_timestamp: row.last_trade_timestamp ?? undefined,
        last_aggregate_timestamp: row.last_aggregate_timestamp ?? undefined,
          last_sync: row.last_sync,
          is_streaming: row.is_streaming
        });
      }

      // Initialize missing tickers
      for (const ticker of config.tickers) {
        if (!this.syncStates.has(ticker)) {
          this.syncStates.set(ticker, {
            ticker,
            last_sync: new Date(),
            is_streaming: false
          });
        }
      }

      console.log(`Initialized sync states for ${this.syncStates.size} tickers`);
    } catch (error) {
      console.error('Error initializing sync states:', error);
      throw error;
    }
  }

  private async catchUpData(): Promise<void> {
    console.log('Catching up on missing data...');
    
    for (const ticker of config.tickers) {
      try {
        await this.catchUpTickerData(ticker);
      } catch (error) {
        console.error(`Error catching up data for ${ticker}:`, error);
      }
    }
    
    console.log('Catch-up completed');
  }

  private async catchUpTickerData(ticker: string): Promise<void> {
    const syncState = this.syncStates.get(ticker);
    if (!syncState) return;

    const now = new Date();
    const lastSync = syncState.last_aggregate_timestamp || new Date(now.getTime() - 24 * 60 * 60 * 1000); // 24 hours ago if no previous sync

    // Adjust end time to respect data delay restriction
    const minDelayMinutes = config.polygon.minDataDelayMinutes;
    const endTime = minDelayMinutes > 0 ? new Date(now.getTime() - minDelayMinutes * 60 * 1000) : now;

    if (minDelayMinutes > 0 && endTime < now) {
      console.log(
        `Catch-up for ${ticker}: Adjusted end time from ${now.toISOString()} to ${endTime.toISOString()} due to ${minDelayMinutes}-minute data delay restriction`
      );
    }

    // Get missing aggregates
    const aggregates = await this.polygonClient.getHistoricalAggregates(ticker, lastSync, endTime, 'minute', 1);

    if (aggregates.length > 0) {
      await this.insertAggregates(ticker, aggregates);
      syncState.last_aggregate_timestamp = new Date(aggregates[aggregates.length - 1].t);
    }

    // Update sync state
    syncState.last_sync = now;
    syncState.is_streaming = true;
    await this.updateSyncState(syncState);
  }

  private startStreaming(): void {
    // Subscribe to trades and aggregates for all tickers
    this.wsClient.subscribeToTrades(config.tickers);
    this.wsClient.subscribeToAggregates(config.tickers);
    
    console.log(`Subscribed to real-time data for ${config.tickers.length} tickers`);
  }

  private async handleTrade(trade: PolygonTrade, symbol: string): Promise<void> {
    if (!this.isIngesting) return;

    try {
      const stockTrade: StockTrade = {
        symbol: symbol,
        timestamp: PolygonClient.convertTimestamp(trade.t, true),
        price: trade.p,
        size: trade.s,
        conditions: trade.c ? JSON.stringify(trade.c) : '[]',
        exchange: trade.x,
        tape: trade.z,
        trade_id: trade.i
      };

      await this.insertStockTrade(stockTrade);
    } catch (error) {
      console.error('Error handling trade:', error);
    }
  }

  private async handleQuote(quote: PolygonQuote, symbol: string): Promise<void> {
    // Quotes are not stored in our current schema, but we could add them if needed
    console.log('Quote received:', quote, 'for symbol:', symbol);
  }

  private async handleAggregate(aggregate: PolygonAggregate, symbol: string): Promise<void> {
    if (!this.isIngesting) return;

    try {
      const stockAggregate: StockAggregate = {
        symbol: symbol,
        timestamp: PolygonClient.convertTimestamp(aggregate.t),
        open: aggregate.o,
        high: aggregate.h,
        low: aggregate.l,
        close: aggregate.c,
        volume: aggregate.v,
        vwap: aggregate.vw,
        transaction_count: aggregate.n
      };

      await this.insertStockAggregate(stockAggregate);
    } catch (error) {
      console.error('Error handling aggregate:', error);
    }
  }

  private async insertStockTrade(trade: StockTrade): Promise<void> {
    try {
      await db.query(
        `
        INSERT INTO stock_trades (symbol, timestamp, price, size, conditions, exchange, tape, trade_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
        [
          trade.symbol,
          trade.timestamp,
          trade.price,
          trade.size,
          trade.conditions,
          trade.exchange,
          trade.tape,
          trade.trade_id,
        ]
      );
    } catch (error) {
      // Ignore unique constraint violations (duplicates)
      if (error instanceof Error && error.message.includes('duplicate')) {
        console.log(`Skipping duplicate stock trade: ${trade.symbol} at ${trade.timestamp}`);
        return;
      }
      throw error;
    }
  }

  private async insertStockAggregate(aggregate: StockAggregate): Promise<void> {
    try {
      await db.query(
        `
        INSERT INTO stock_aggregates (symbol, timestamp, open, high, low, close, volume, vwap, transaction_count)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `,
        [
          aggregate.symbol,
          aggregate.timestamp,
          aggregate.open,
          aggregate.high,
          aggregate.low,
          aggregate.close,
          aggregate.volume,
          aggregate.vwap,
          aggregate.transaction_count,
        ]
      );
    } catch (error) {
      // Ignore unique constraint violations (duplicates)
      if (error instanceof Error && error.message.includes('duplicate')) {
        console.log(`Skipping duplicate stock aggregate: ${aggregate.symbol} at ${aggregate.timestamp}`);
        return;
      }
      throw error;
    }
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
        transaction_count: aggregate.n
      };

      await this.insertStockAggregate(stockAggregate);
    }
  }

  private async updateSyncState(syncState: SyncState): Promise<void> {
    await db.query(`
      INSERT INTO sync_state (ticker, last_trade_timestamp, last_aggregate_timestamp, last_sync, is_streaming)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (ticker) DO UPDATE SET
        last_trade_timestamp = EXCLUDED.last_trade_timestamp,
        last_aggregate_timestamp = EXCLUDED.last_aggregate_timestamp,
        last_sync = EXCLUDED.last_sync,
        is_streaming = EXCLUDED.is_streaming
    `, [
      syncState.ticker,
      syncState.last_trade_timestamp,
      syncState.last_aggregate_timestamp,
      syncState.last_sync,
      syncState.is_streaming
    ]);
  }

  private handleIngestionError(error: Error): void {
    console.error('Ingestion error:', error);
    // Implement retry logic or error recovery here
  }
}
