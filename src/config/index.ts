import dotenv from 'dotenv';

dotenv.config();

export interface Config {
  polygon: {
    apiKey: string;
    baseUrl: string;
    wsUrl: string;
    minDataDelayMinutes: number;
    logRequests: boolean;
  };
  questdb: {
    host: string;
    port: number;
    user: string;
    password: string;
  };
  app: {
    logLevel: string;
    maxRetries: number;
    retryDelayMs: number;
    backfillMaxDays: number;
  };
  tickers: string[];
}

const requiredEnvVars = ['POLYGON_API_KEY'] as const;

function validateConfig(): void {
  const missing = requiredEnvVars.filter(key => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

export const config: Config = {
  polygon: {
    apiKey: process.env.POLYGON_API_KEY!,
    baseUrl: 'https://api.polygon.io',
    wsUrl: 'wss://socket.polygon.io/stocks',
    minDataDelayMinutes: parseInt(process.env.POLYGON_MIN_DATA_DELAY_MINUTES || '0'),
    logRequests: process.env.POLYGON_LOG_REQUESTS === 'true',
  },
  questdb: {
    host: process.env.QUESTDB_HOST || '127.0.0.1',
    port: parseInt(process.env.QUESTDB_PORT || '9000'),
    user: process.env.QUESTDB_USER || 'admin',
    password: process.env.QUESTDB_PASSWORD || 'quest',
  },
  app: {
    logLevel: process.env.LOG_LEVEL || 'info',
    maxRetries: parseInt(process.env.MAX_RETRIES || '3'),
    retryDelayMs: parseInt(process.env.RETRY_DELAY_MS || '1000'),
    backfillMaxDays: parseInt(process.env.BACKFILL_MAX_DAYS || '30'),
  },
  tickers: process.env.TICKERS
    ? process.env.TICKERS.split(',').map(t => t.trim())
    : [
        'AAPL',
        'MSFT',
        'GOOGL',
        'AMZN',
        'TSLA',
        'META',
        'NVDA',
        'NFLX',
        'AMD',
        'INTC',
        'SPY',
        'QQQ',
        'IWM',
        'VTI',
        'VOO',
        'ARKK',
        'TQQQ',
        'SQQQ',
        'UPRO',
        'SPXL',
      ],
};

// Validate configuration on import
validateConfig();
