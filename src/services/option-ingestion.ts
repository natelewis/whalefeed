import { db } from '../db/connection';
import { PolygonClient } from './polygon-client';
import { config } from '../config';
import { OptionContract, OptionTrade, OptionQuote } from '../types/database';
import { PolygonOptionContract, PolygonOptionTrade, PolygonOptionQuote } from '../types/polygon';

export class OptionIngestionService {
  private polygonClient: PolygonClient;

  constructor() {
    this.polygonClient = new PolygonClient();
  }

  async ingestOptionContracts(underlyingTicker: string): Promise<void> {
    try {
      console.log(`Ingesting option contracts for ${underlyingTicker}...`);

      const contracts = await this.polygonClient.getOptionContracts(underlyingTicker);

      for (const contract of contracts) {
        await this.insertOptionContract(contract);
      }

      console.log(`Ingested ${contracts.length} option contracts for ${underlyingTicker}`);
    } catch (error) {
      console.error(`Error ingesting option contracts for ${underlyingTicker}:`, error);
      throw error;
    }
  }

  async ingestOptionTrades(ticker: string, from: Date, to: Date): Promise<void> {
    try {
      console.log(`Ingesting option trades for ${ticker} from ${from.toISOString()} to ${to.toISOString()}...`);

      const trades = await this.polygonClient.getOptionTrades(ticker, from, to);

      for (const trade of trades) {
        await this.insertOptionTrade(trade as unknown as PolygonOptionTrade, ticker);
      }

      console.log(`Ingested ${trades.length} option trades for ${ticker}`);
    } catch (error) {
      console.error(`Error ingesting option trades for ${ticker}:`, error);
      throw error;
    }
  }

  async ingestOptionQuotes(ticker: string, from: Date, to: Date): Promise<void> {
    try {
      console.log(`Ingesting option quotes for ${ticker} from ${from.toISOString()} to ${to.toISOString()}...`);

      const quotes = await this.polygonClient.getOptionQuotes(ticker, from, to);

      for (const quote of quotes) {
        await this.insertOptionQuote(quote, ticker);
      }

      console.log(`Ingested ${quotes.length} option quotes for ${ticker}`);
    } catch (error) {
      console.error(`Error ingesting option quotes for ${ticker}:`, error);
      throw error;
    }
  }

  private async insertOptionContract(contract: PolygonOptionContract): Promise<void> {
    const optionContract: OptionContract = {
      ticker: contract.ticker,
      contract_type: contract.contract_type,
      exercise_style: contract.exercise_style,
      expiration_date: contract.expiration_date,
      shares_per_contract: contract.shares_per_contract,
      strike_price: contract.strike_price,
      underlying_ticker: contract.underlying_ticker,
      created_at: new Date(),
    };

    try {
      await db.query(
        `
        INSERT INTO option_contracts (ticker, contract_type, exercise_style, expiration_date, shares_per_contract, strike_price, underlying_ticker, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `,
        [
          optionContract.ticker,
          optionContract.contract_type,
          optionContract.exercise_style,
          optionContract.expiration_date,
          optionContract.shares_per_contract,
          optionContract.strike_price,
          optionContract.underlying_ticker,
          optionContract.created_at,
        ]
      );
    } catch (error) {
      // QuestDB doesn't support ON CONFLICT, so we ignore duplicate key errors
      // This is a workaround for the lack of UPSERT functionality
      if (error instanceof Error && error.message.includes('duplicate')) {
        // Ignore duplicate key errors
        return;
      }
      throw error;
    }
  }

  private async insertOptionTrade(trade: PolygonOptionTrade, ticker: string): Promise<void> {
    const now = new Date();
    const tradeDate = PolygonClient.convertTimestamp(trade.sip_timestamp, true);
    const isCurrentDay = tradeDate.toDateString() === now.toDateString();

    // Apply the "noon rule" for historical data, current time for today's data
    const receivedAt = isCurrentDay
      ? now
      : new Date(tradeDate.getFullYear(), tradeDate.getMonth(), tradeDate.getDate(), 12, 0, 0);

    const optionTrade: OptionTrade = {
      ticker: ticker,
      timestamp: tradeDate,
      price: trade.price,
      size: trade.size,
      conditions: trade.conditions ? JSON.stringify(trade.conditions) : '[]',
      exchange: trade.exchange || 0, // Default to 0 if exchange is undefined
      tape: trade.tape || 0, // Default to 0 if tape is undefined
      sequence_number: trade.sequence_number,
      received_at: receivedAt,
    };

    try {
      await db.query(
        `
        INSERT INTO option_trades (ticker, timestamp, price, size, conditions, exchange, tape, sequence_number, received_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `,
        [
          optionTrade.ticker,
          optionTrade.timestamp,
          optionTrade.price,
          optionTrade.size,
          optionTrade.conditions,
          optionTrade.exchange,
          optionTrade.tape,
          optionTrade.sequence_number,
          optionTrade.received_at,
        ]
      );
    } catch (error) {
      // Ignore unique constraint violations (duplicates)
      if (error instanceof Error && error.message.includes('duplicate')) {
        console.log(`Skipping duplicate option trade: ${optionTrade.ticker} at ${optionTrade.timestamp}`);
        return;
      }
      throw error;
    }
  }

  private async insertOptionQuote(quote: PolygonOptionQuote, ticker: string): Promise<void> {
    const now = new Date();
    const quoteDate = PolygonClient.convertTimestamp(quote.sip_timestamp, true);
    const isCurrentDay = quoteDate.toDateString() === now.toDateString();

    // Apply the "noon rule" for historical data, current time for today's data
    const receivedAt = isCurrentDay
      ? now
      : new Date(quoteDate.getFullYear(), quoteDate.getMonth(), quoteDate.getDate(), 12, 0, 0);

    const optionQuote: OptionQuote = {
      ticker: ticker,
      timestamp: quoteDate,
      bid_price: quote.bid_price ?? 0, // Default to 0 if undefined
      bid_size: quote.bid_size ?? 0, // Default to 0 if undefined
      ask_price: quote.ask_price ?? 0, // Default to 0 if undefined
      ask_size: quote.ask_size ?? 0, // Default to 0 if undefined
      bid_exchange: quote.bid_exchange || 0, // Default to 0 if undefined
      ask_exchange: quote.ask_exchange || 0, // Default to 0 if undefined
      sequence_number: quote.sequence_number || 0, // Default to 0 if undefined
      received_at: receivedAt,
    };

    try {
      await db.query(
        `
        INSERT INTO option_quotes (ticker, timestamp, bid_price, bid_size, ask_price, ask_size, bid_exchange, ask_exchange, sequence_number, received_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `,
        [
          optionQuote.ticker,
          optionQuote.timestamp,
          optionQuote.bid_price,
          optionQuote.bid_size,
          optionQuote.ask_price,
          optionQuote.ask_size,
          optionQuote.bid_exchange,
          optionQuote.ask_exchange,
          optionQuote.sequence_number,
          optionQuote.received_at,
        ]
      );
    } catch (error) {
      // Ignore unique constraint violations (duplicates)
      if (error instanceof Error && error.message.includes('duplicate')) {
        console.log(`Skipping duplicate option quote: ${optionQuote.ticker} at ${optionQuote.timestamp}`);
        return;
      }
      throw error;
    }
  }

  async getAllOptionTickers(): Promise<string[]> {
    try {
      const tickerList = config.tickers.map(t => `'${t}'`).join(',');
      const result = await db.query(`
        SELECT DISTINCT ticker FROM option_contracts
        WHERE underlying_ticker IN (${tickerList})
      `);

      // Handle QuestDB result format
      const rows = (result as any)?.dataset || [];
      return rows.map((row: string[]) => row[0]);
    } catch (error) {
      console.error('Error getting option tickers:', error);
      return [];
    }
  }

  async backfillOptionData(underlyingTicker: string, from: Date, to: Date): Promise<void> {
    try {
      // First, get all option contracts for this underlying
      await this.ingestOptionContracts(underlyingTicker);

      // Get all option tickers for this underlying
      const optionTickers = await this.getOptionTickersForUnderlying(underlyingTicker);

      // Backfill trades for each option ticker (if not skipped)
      if (!config.polygon.skipOptionTrades) {
        for (const ticker of optionTickers) {
          try {
            await this.ingestOptionTrades(ticker, from, to);
          } catch (error) {
            console.error(`Error backfilling option trades for ${ticker}:`, error);
          }
        }
      } else {
        console.log(`Skipping option trades ingestion (POLYGON_SKIP_OPTION_TRADES=true)`);
      }

      // Backfill quotes for each option ticker (if not skipped)
      if (!config.polygon.skipOptionQuotes) {
        for (const ticker of optionTickers) {
          try {
            await this.ingestOptionQuotes(ticker, from, to);
          } catch (error) {
            console.error(`Error backfilling option quotes for ${ticker}:`, error);
          }
        }
      } else {
        console.log(`Skipping option quotes ingestion (POLYGON_SKIP_OPTION_QUOTES=true)`);
      }
    } catch (error) {
      console.error(`Error backfilling option data for ${underlyingTicker}:`, error);
      throw error;
    }
  }

  private async getOptionTickersForUnderlying(underlyingTicker: string): Promise<string[]> {
    try {
      const result = await db.query(`
        SELECT DISTINCT ticker FROM option_contracts
        WHERE underlying_ticker = '${underlyingTicker}'
      `);

      // Handle QuestDB result format
      const rows = (result as any)?.dataset || [];
      return rows.map((row: string[]) => row[0]);
    } catch (error) {
      console.error('Error getting option tickers for underlying:', error);
      return [];
    }
  }
}
