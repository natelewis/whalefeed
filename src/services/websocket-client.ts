import WebSocket from 'ws';
import { config } from '../config';
import { PolygonWebSocketMessage, PolygonWebSocketSubscription } from '../types/polygon';

export interface WebSocketEventHandler {
  onTrade: (trade: unknown, symbol: string) => void;
  onQuote: (quote: unknown, symbol: string) => void;
  onOptionQuote: (quote: unknown, symbol: string) => void;
  onAggregate: (aggregate: unknown, symbol: string) => void;
  onError: (error: Error) => void;
  onConnect: () => void;
  onDisconnect: () => void;
}

export class PolygonWebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectDelay = 1000;
  private isConnecting = false;
  private subscriptions = new Set<string>();
  private subscriptionMap = new Map<string, string>(); // Maps subscription params to symbol
  private eventHandlers: Partial<WebSocketEventHandler> = {};

  constructor() {
    // Constructor for WebSocket client
  }

  setEventHandlers(handlers: Partial<WebSocketEventHandler>): void {
    this.eventHandlers = { ...this.eventHandlers, ...handlers };
  }

  async connect(): Promise<void> {
    if (this.isConnecting || (this.ws && this.ws.readyState === WebSocket.OPEN)) {
      return;
    }

    this.isConnecting = true;

    return new Promise((resolve, reject) => {
      const wsUrl = `${config.polygon.wsUrl}?apikey=${config.polygon.apiKey}`;

      this.ws = new WebSocket(wsUrl);

      this.ws.on('open', () => {
        console.log('WebSocket connected to Polygon.io');
        this.isConnecting = false;
        this.reconnectAttempts = 0;
        this.eventHandlers.onConnect?.();
        resolve();
      });

      this.ws.on('message', (data: WebSocket.Data) => {
        try {
          const message: PolygonWebSocketMessage = JSON.parse(data.toString());
          this.handleMessage(message);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
          this.eventHandlers.onError?.(error as Error);
        }
      });

      this.ws.on('close', (code: number, reason: string) => {
        console.log(`WebSocket closed: ${code} - ${reason}`);
        this.isConnecting = false;
        this.eventHandlers.onDisconnect?.();
        this.handleReconnect();
      });

      this.ws.on('error', (error: Error) => {
        console.error('WebSocket error:', error);
        this.isConnecting = false;
        this.eventHandlers.onError?.(error);
        reject(error);
      });
    });
  }

  private handleMessage(message: PolygonWebSocketMessage): void {
    switch (message.ev) {
      case 'status':
        if (message.status === 'connected') {
          console.log('WebSocket authenticated successfully');
          // Re-subscribe to all previous subscriptions
          this.resubscribeAll();
        } else if (message.status === 'error') {
          console.error('WebSocket error:', message.message);
          this.eventHandlers.onError?.(new Error(message.message || 'Unknown WebSocket error'));
        }
        break;

      case 'T': // Trade
        if (message.results && Array.isArray(message.results)) {
          message.results.forEach(trade => {
            const symbol = this.extractSymbolFromTrade(trade);
            this.eventHandlers.onTrade?.(trade, symbol);
          });
        }
        break;

      case 'Q': // Quote
        if (message.results && Array.isArray(message.results)) {
          message.results.forEach(quote => {
            const symbol = this.extractSymbolFromQuote(quote);
            // Check if this is an option quote by looking at the symbol pattern
            if (this.isOptionSymbol(symbol)) {
              this.eventHandlers.onOptionQuote?.(quote, symbol);
            } else {
              this.eventHandlers.onQuote?.(quote, symbol);
            }
          });
        }
        break;

      case 'A': // Aggregate
        if (message.results && Array.isArray(message.results)) {
          message.results.forEach(aggregate => {
            const symbol = this.extractSymbolFromAggregate(aggregate);
            this.eventHandlers.onAggregate?.(aggregate, symbol);
          });
        }
        break;

      default:
        console.log('Unknown message type:', message.ev);
    }
  }

  subscribeToTrades(symbols: string[]): void {
    const params = symbols.map(symbol => `T.${symbol}`).join(',');
    symbols.forEach(symbol => this.subscriptionMap.set(`T.${symbol}`, symbol));
    this.subscribe(params);
  }

  subscribeToQuotes(symbols: string[]): void {
    const params = symbols.map(symbol => `Q.${symbol}`).join(',');
    symbols.forEach(symbol => this.subscriptionMap.set(`Q.${symbol}`, symbol));
    this.subscribe(params);
  }

  subscribeToOptionQuotes(symbols: string[]): void {
    const params = symbols.map(symbol => `Q.${symbol}`).join(',');
    symbols.forEach(symbol => this.subscriptionMap.set(`Q.${symbol}`, symbol));
    this.subscribe(params);
  }

  subscribeToAggregates(symbols: string[]): void {
    const params = symbols.map(symbol => `A.${symbol}`).join(',');
    symbols.forEach(symbol => this.subscriptionMap.set(`A.${symbol}`, symbol));
    this.subscribe(params);
  }

  private subscribe(params: string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('WebSocket not connected, queuing subscription:', params);
      this.subscriptions.add(params);
      return;
    }

    const message: PolygonWebSocketSubscription = {
      action: 'subscribe',
      params,
    };

    this.ws.send(JSON.stringify(message));
    this.subscriptions.add(params);
    console.log('Subscribed to:', params);
  }

  private resubscribeAll(): void {
    for (const subscription of this.subscriptions) {
      const message: PolygonWebSocketSubscription = {
        action: 'subscribe',
        params: subscription,
      };
      this.ws?.send(JSON.stringify(message));
    }
  }

  private handleReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('Max reconnection attempts reached');
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);

    console.log(
      `Attempting to reconnect in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`
    );

    setTimeout(() => {
      this.connect().catch(error => {
        console.error('Reconnection failed:', error);
      });
    }, delay);
  }

  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.subscriptions.clear();
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  private extractSymbolFromTrade(_trade: unknown): string {
    // Polygon trade data doesn't include symbol, so we need to track it from subscriptions
    // For now, return a placeholder - in a real implementation, you'd need to track this
    return 'UNKNOWN';
  }

  private extractSymbolFromQuote(_quote: unknown): string {
    // Polygon quote data doesn't include symbol, so we need to track it from subscriptions
    return 'UNKNOWN';
  }

  private extractSymbolFromAggregate(_aggregate: unknown): string {
    // Polygon aggregate data doesn't include symbol, so we need to track it from subscriptions
    return 'UNKNOWN';
  }

  private isOptionSymbol(symbol: string): boolean {
    // Option symbols typically have a pattern like: AAPL240315C00150000
    // They contain letters, numbers, and often end with C or P followed by strike price
    return /^[A-Z]+\d{6}[CP]\d+$/.test(symbol);
  }
}
