// Polygon.io API response types
export interface PolygonTrade {
  p: number; // price
  s: number; // size
  t: number; // timestamp (nanoseconds)
  c?: number[]; // conditions
  i?: string; // id
  x?: number; // exchange
  z?: number; // tape
}

export interface PolygonQuote {
  p: number; // bid price
  s: number; // bid size
  P: number; // ask price
  S: number; // ask size
  t: number; // timestamp (nanoseconds)
  c?: number[]; // conditions
  x?: number; // exchange
  z?: number; // tape
}

export interface PolygonAggregate {
  v: number; // volume
  vw: number; // volume weighted average price
  o: number; // open
  c: number; // close
  h: number; // high
  l: number; // low
  t: number; // timestamp (milliseconds)
  n: number; // number of transactions
}

export interface PolygonOptionContract {
  contract_type: 'call' | 'put';
  exercise_style: 'american' | 'european';
  expiration_date: string; // YYYY-MM-DD
  shares_per_contract: number;
  strike_price: number;
  ticker: string;
  underlying_ticker: string;
}

export interface PolygonOptionTrade {
  conditions: number[];
  exchange: number;
  price: number;
  sip_timestamp: number; // nanoseconds
  size: number;
  participant_timestamp: number; // nanoseconds
  sequence_number: number;
  tape: number;
}

export interface PolygonWebSocketMessage {
  ev: string; // event type
  status?: string;
  message?: string;
  results?: PolygonTrade[] | PolygonQuote[] | PolygonAggregate[];
}

export interface PolygonWebSocketSubscription {
  action: 'subscribe' | 'unsubscribe';
  params: string;
}

export interface PolygonHistoricalDataResponse {
  results: PolygonAggregate[];
  status: string;
  ticker: string;
  queryCount: number;
  resultsCount: number;
  adjusted: boolean;
  next_url?: string;
}
