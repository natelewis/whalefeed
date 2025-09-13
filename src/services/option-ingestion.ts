import { db } from '../db/connection';
import { PolygonClient } from './polygon-client';
import { config } from '../config';
import { OptionContract, OptionTrade } from '../types/database';
import { PolygonOptionContract, PolygonOptionTrade } from '../types/polygon';

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

  private async insertOptionContract(contract: PolygonOptionContract): Promise<void> {
    const optionContract: OptionContract = {
      ticker: contract.ticker,
      contract_type: contract.contract_type,
      exercise_style: contract.exercise_style,
      expiration_date: contract.expiration_date,
      shares_per_contract: contract.shares_per_contract,
      strike_price: contract.strike_price,
      underlying_ticker: contract.underlying_ticker,
      created_at: new Date()
    };

    await db.query(`
      INSERT INTO option_contracts (ticker, contract_type, exercise_style, expiration_date, shares_per_contract, strike_price, underlying_ticker, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (ticker) DO NOTHING
    `, [
      optionContract.ticker,
      optionContract.contract_type,
      optionContract.exercise_style,
      optionContract.expiration_date,
      optionContract.shares_per_contract,
      optionContract.strike_price,
      optionContract.underlying_ticker,
      optionContract.created_at
    ]);
  }

  private async insertOptionTrade(trade: PolygonOptionTrade, ticker: string): Promise<void> {
    const now = new Date();
    const tradeDate = PolygonClient.convertTimestamp(trade.sip_timestamp, true);
    const isCurrentDay = tradeDate.toDateString() === now.toDateString();
    
    // Apply the "noon rule" for historical data, current time for today's data
    const receivedAt = isCurrentDay ? now : new Date(tradeDate.getFullYear(), tradeDate.getMonth(), tradeDate.getDate(), 12, 0, 0);

    const optionTrade: OptionTrade = {
      ticker: ticker,
      timestamp: tradeDate,
      price: trade.price,
      size: trade.size,
      conditions: trade.conditions ? JSON.stringify(trade.conditions) : '[]',
      exchange: trade.exchange,
      tape: trade.tape,
      sequence_number: trade.sequence_number,
      received_at: receivedAt
    };

    await db.query(`
      INSERT INTO option_trades (ticker, timestamp, price, size, conditions, exchange, tape, sequence_number, received_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [
      optionTrade.ticker,
      optionTrade.timestamp,
      optionTrade.price,
      optionTrade.size,
      optionTrade.conditions,
      optionTrade.exchange,
      optionTrade.tape,
      optionTrade.sequence_number,
      optionTrade.received_at
    ]);
  }

  async getAllOptionTickers(): Promise<string[]> {
    try {
      const result = await db.query(`
        SELECT DISTINCT ticker FROM option_contracts
        WHERE underlying_ticker IN (${config.tickers.map(() => '?').join(',')})
      `, config.tickers);

      const rows = result as { ticker: string }[];
      return rows.map(row => row.ticker);
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
      
      // Backfill trades for each option ticker
      for (const ticker of optionTickers) {
        try {
          await this.ingestOptionTrades(ticker, from, to);
        } catch (error) {
          console.error(`Error backfilling option trades for ${ticker}:`, error);
        }
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
        WHERE underlying_ticker = $1
      `, [underlyingTicker]);

      const rows = result as { ticker: string }[];
      return rows.map(row => row.ticker);
    } catch (error) {
      console.error('Error getting option tickers for underlying:', error);
      return [];
    }
  }
}
