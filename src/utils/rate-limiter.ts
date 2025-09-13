import pLimit from 'p-limit';

export class RateLimiter {
  private limit: ReturnType<typeof pLimit>;
  private delayMs: number;

  constructor(requestsPerSecond: number) {
    // Create a concurrency limiter that allows only 1 request at a time
    this.limit = pLimit(1);
    // Calculate delay between requests to achieve the desired RPS
    this.delayMs = 1000 / requestsPerSecond;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    return this.limit(async () => {
      const result = await fn();
      // Add delay to respect rate limits
      if (this.delayMs > 0) {
        await new Promise(resolve => setTimeout(resolve, this.delayMs));
      }
      return result;
    });
  }
}

// Get rate limiter based on environment variables
export function getPolygonRateLimiter(): RateLimiter {
  // Read from environment variables with sensible defaults
  const requestsPerSecond = parseFloat(process.env.POLYGON_REQUESTS_PER_SECOND || '1.5');
  const requestsPerMinute = parseFloat(process.env.POLYGON_REQUESTS_PER_MINUTE || '90');
  
  // Use the more restrictive limit
  const effectiveRPS = Math.min(requestsPerSecond, requestsPerMinute / 60);
  
  console.log(`Rate limiting Polygon.io requests to ${effectiveRPS.toFixed(2)} requests per second`);
  
  return new RateLimiter(effectiveRPS);
}
