// Database table schemas and types
export interface StockTrade {
  symbol: string;
  timestamp: Date;
  price: number;
  size: number;
  conditions: string;
  exchange?: number | undefined;
  tape?: number | undefined;
  trade_id?: string | undefined;
}

export interface StockAggregate {
  symbol: string;
  timestamp: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  transaction_count: number;
}

export interface OptionContract {
  contract_type: 'call' | 'put';
  exercise_style: 'american' | 'european';
  expiration_date: string; // YYYY-MM-DD
  shares_per_contract: number;
  strike_price: number;
  ticker: string;
  underlying_ticker: string;
  created_at: Date;
}

export interface OptionTrade {
  ticker: string;
  timestamp: Date;
  price: number;
  size: number;
  conditions: string;
  exchange: number;
  tape: number;
  sequence_number: number;
  received_at: Date; // When we received this from websocket
}

export interface TickerConfig {
  symbol: string;
  enabled: boolean;
  last_sync?: Date;
  last_aggregate?: Date;
}

export interface SyncState {
  ticker: string;
  last_trade_timestamp?: Date | undefined;
  last_aggregate_timestamp?: Date | undefined;
  last_sync: Date;
  is_streaming: boolean;
}
