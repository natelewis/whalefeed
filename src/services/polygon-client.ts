import axios, { AxiosInstance } from 'axios';
import { config } from '../config';
import {
  PolygonHistoricalDataResponse,
  PolygonAggregate,
  PolygonOptionContract,
  PolygonOptionContractsResponse,
  PolygonOptionQuote,
  PolygonOptionQuotesResponse,
  PolygonTrade,
  PolygonQuote,
} from '../types/polygon';
import { getPolygonRateLimiter } from '../utils/rate-limiter';
import { validateDataDelay, adjustDateRangeForDelay } from '../utils/date-validation';

export class PolygonClient {
  private api: AxiosInstance;
  private apiKey: string;
  private rateLimiter = getPolygonRateLimiter();

  constructor() {
    this.apiKey = config.polygon.apiKey;
    this.api = axios.create({
      baseURL: config.polygon.baseUrl,
      timeout: 30000,
      params: {
        apikey: this.apiKey,
      },
    });
  }

  private logRequest(method: string, endpoint: string, params?: Record<string, unknown>): void {
    if (config.polygon.logRequests) {
      let fullUrl = `${config.polygon.baseUrl}${endpoint}`;

      if (params) {
        const queryParams = new URLSearchParams();
        Object.entries(params).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            queryParams.append(key, String(value));
          }
        });
        queryParams.append('apikey', this.apiKey);

        const queryString = queryParams.toString();
        if (queryString) {
          fullUrl += `?${queryString}`;
        }
      } else {
        fullUrl += `?apikey=${this.apiKey}`;
      }

      console.log(`[POLYGON API] ${method} ${fullUrl}`);
    }
  }

  async getHistoricalAggregates(
    ticker: string,
    from: Date,
    to: Date,
    timespan: 'minute' | 'hour' | 'day' = 'minute',
    multiplier: number = 5
  ): Promise<PolygonAggregate[]> {
    return this.rateLimiter.execute(async () => {
      try {
        // Validate and adjust date range for data delay restriction
        if (!validateDataDelay(from, to)) {
          const adjustedRange = adjustDateRangeForDelay(from, to);
          if (adjustedRange.adjusted) {
            // Use adjusted dates
            from = adjustedRange.from;
            to = adjustedRange.to;
          } else {
            // If validation failed but no adjustment was made, skip the query
            console.log(`Skipping historical aggregates query for ${ticker} due to data delay restriction`);
            return [];
          }
        }

        const fromStr = from.toISOString().split('T')[0];
        const toStr = to.toISOString().split('T')[0];
        const endpoint = `/v2/aggs/ticker/${ticker}/range/${multiplier}/${timespan}/${fromStr}/${toStr}`;
        const requestParams = {
          adjusted: true,
          sort: 'asc',
        };

        this.logRequest('GET', endpoint, requestParams);

        const response = await this.api.get<PolygonHistoricalDataResponse>(endpoint, {
          params: requestParams,
        });

        if (response.data.status !== 'OK') {
          throw new Error(`Polygon API error: ${response.data.status}`);
        }

        return response.data.results || [];
      } catch (error) {
        console.error(`Error fetching historical data for ${ticker}:`, error);
        throw error;
      }
    });
  }

  async getOptionContracts(underlyingTicker: string, expirationDate?: string): Promise<PolygonOptionContract[]> {
    return this.rateLimiter.execute(async () => {
      try {
        const allContracts: PolygonOptionContract[] = [];
        let nextUrl: string | undefined;

        // Initial request
        const params: Record<string, string> = {
          underlying_ticker: underlyingTicker,
          limit: config.polygon.optionContractsLimit.toString(),
        };

        if (expirationDate) {
          params.expiration_date = expirationDate;
        }

        const endpoint = '/v3/reference/options/contracts';
        this.logRequest('GET', endpoint, params);

        let response = await this.api.get<PolygonOptionContractsResponse>(endpoint, { params });

        // Add results from first page
        if (response.data.results) {
          allContracts.push(...response.data.results);
        }

        // Follow pagination
        nextUrl = response.data.next_url;
        while (nextUrl) {
          this.logRequest('GET', nextUrl);
          response = await this.api.get<PolygonOptionContractsResponse>(nextUrl);

          if (response.data.results) {
            allContracts.push(...response.data.results);
          }

          nextUrl = response.data.next_url;
        }

        console.log(`Fetched ${allContracts.length} option contracts for ${underlyingTicker}`);
        return allContracts;
      } catch (error) {
        console.error(`Error fetching option contracts for ${underlyingTicker}:`, error);
        throw error;
      }
    });
  }

  async getOptionTrades(ticker: string, from: Date, to: Date): Promise<PolygonTrade[]> {
    return this.rateLimiter.execute(async () => {
      try {
        // Validate and adjust date range for data delay restriction
        if (!validateDataDelay(from, to)) {
          const adjustedRange = adjustDateRangeForDelay(from, to);
          if (adjustedRange.adjusted) {
            // Use adjusted dates
            from = adjustedRange.from;
            to = adjustedRange.to;
          } else {
            // If validation failed but no adjustment was made, skip the query
            console.log(`Skipping option trades query for ${ticker} due to data delay restriction`);
            return [];
          }
        }

        const fromStr = from.toISOString();
        const toStr = to.toISOString();
        const endpoint = `/v3/trades/${ticker}`;
        const requestParams = {
          'timestamp.gte': fromStr,
          'timestamp.lte': toStr,
          order: 'asc',
          limit: 50000,
        };

        this.logRequest('GET', endpoint, requestParams);

        const response = await this.api.get<{ results: PolygonTrade[] }>(endpoint, { params: requestParams });

        return response.data.results || [];
      } catch (error) {
        console.error(`Error fetching option trades for ${ticker}:`, error);
        throw error;
      }
    });
  }

  async getOptionQuotes(ticker: string, from: Date, to: Date): Promise<PolygonOptionQuote[]> {
    return this.rateLimiter.execute(async () => {
      try {
        // Validate and adjust date range for data delay restriction
        if (!validateDataDelay(from, to)) {
          const adjustedRange = adjustDateRangeForDelay(from, to);
          if (adjustedRange.adjusted) {
            // Use adjusted dates
            from = adjustedRange.from;
            to = adjustedRange.to;
          } else {
            // If validation failed but no adjustment was made, skip the query
            console.log(`Skipping option quotes query for ${ticker} due to data delay restriction`);
            return [];
          }
        }

        const allQuotes: PolygonOptionQuote[] = [];
        let nextUrl: string | undefined;

        // Initial request
        const fromStr = from.toISOString();
        const toStr = to.toISOString();
        const endpoint = `/v3/quotes/${ticker}`;
        const requestParams = {
          'timestamp.gte': fromStr,
          'timestamp.lte': toStr,
          order: 'asc',
          limit: 50000,
        };

        this.logRequest('GET', endpoint, requestParams);

        let response = await this.api.get<PolygonOptionQuotesResponse>(endpoint, { params: requestParams });

        // Debug: Log the first quote to see the actual structure
        if (response.data.results && response.data.results.length > 0) {
          console.log('Sample option quote response:', JSON.stringify(response.data.results[0], null, 2));
        }

        // Add results from first page
        if (response.data.results) {
          allQuotes.push(...response.data.results);
        }

        // Follow pagination
        nextUrl = response.data.next_url;
        while (nextUrl) {
          this.logRequest('GET', nextUrl);
          response = await this.api.get<PolygonOptionQuotesResponse>(nextUrl);

          if (response.data.results) {
            allQuotes.push(...response.data.results);
          }

          nextUrl = response.data.next_url;
        }

        console.log(`Fetched ${allQuotes.length} option quotes for ${ticker}`);
        return allQuotes;
      } catch (error) {
        console.error(`Error fetching option quotes for ${ticker}:`, error);
        throw error;
      }
    });
  }

  async getStockTrades(ticker: string, from: Date, to: Date): Promise<PolygonTrade[]> {
    return this.rateLimiter.execute(async () => {
      try {
        // Validate and adjust date range for data delay restriction
        if (!validateDataDelay(from, to)) {
          const adjustedRange = adjustDateRangeForDelay(from, to);
          if (adjustedRange.adjusted) {
            // Use adjusted dates
            from = adjustedRange.from;
            to = adjustedRange.to;
          } else {
            // If validation failed but no adjustment was made, skip the query
            console.log(`Skipping stock trades query for ${ticker} due to data delay restriction`);
            return [];
          }
        }

        const fromStr = from.toISOString();
        const toStr = to.toISOString();
        const endpoint = `/v3/trades/${ticker}`;
        const requestParams = {
          'timestamp.gte': fromStr,
          'timestamp.lte': toStr,
          order: 'asc',
          limit: 50000,
        };

        this.logRequest('GET', endpoint, requestParams);

        const response = await this.api.get<{ results: PolygonTrade[] }>(endpoint, { params: requestParams });

        return response.data.results || [];
      } catch (error) {
        console.error(`Error fetching stock trades for ${ticker}:`, error);
        throw error;
      }
    });
  }

  async getLatestTrade(ticker: string): Promise<PolygonTrade | null> {
    return this.rateLimiter.execute(async () => {
      try {
        const endpoint = `/v2/last/trade/${ticker}`;
        this.logRequest('GET', endpoint);

        const response = await this.api.get<{ results: PolygonTrade[] }>(endpoint);

        return response.data.results?.[0] || null;
      } catch (error) {
        console.error(`Error fetching latest trade for ${ticker}:`, error);
        return null;
      }
    });
  }

  async getLatestQuote(ticker: string): Promise<PolygonQuote | null> {
    return this.rateLimiter.execute(async () => {
      try {
        const endpoint = `/v2/last/quote/${ticker}`;
        this.logRequest('GET', endpoint);

        const response = await this.api.get<{ results: PolygonQuote[] }>(endpoint);

        return response.data.results?.[0] || null;
      } catch (error) {
        console.error(`Error fetching latest quote for ${ticker}:`, error);
        return null;
      }
    });
  }

  // Helper method to convert Polygon timestamp to Date
  static convertTimestamp(timestamp: number, isNanoseconds = false): Date {
    if (isNanoseconds) {
      return new Date(timestamp / 1000000);
    }
    return new Date(timestamp);
  }

  // Helper method to convert Date to Polygon timestamp
  static toPolygonTimestamp(date: Date, asNanoseconds = false): number {
    const ms = date.getTime();
    return asNanoseconds ? ms * 1000000 : ms;
  }
}
