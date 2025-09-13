import { config } from '../config';

/**
 * Validates that the provided date range respects the minimum data delay configuration.
 * For lower-tier Polygon.io accounts, data queries must be at least N minutes before current time.
 * 
 * @param from - Start date of the query
 * @param to - End date of the query
 * @returns true if the date range is valid, false otherwise
 */
export function validateDataDelay(from: Date, to: Date): boolean {
  const minDelayMinutes = config.polygon.minDataDelayMinutes;
  
  // If no delay is configured, allow all queries
  if (minDelayMinutes <= 0) {
    return true;
  }
  
  const now = new Date();
  const minAllowedTime = new Date(now.getTime() - minDelayMinutes * 60 * 1000);
  
  // Check if the 'to' date is within the restricted time window
  if (to > minAllowedTime) {
    console.warn(
      `Data query rejected: end date ${to.toISOString()} is within ${minDelayMinutes} minutes of current time. ` +
      `Minimum allowed end time: ${minAllowedTime.toISOString()}. ` +
      `Set POLYGON_MIN_DATA_DELAY_MINUTES=0 to disable this restriction.`
    );
    return false;
  }
  
  // Also check if the 'from' date is in the future (which would be invalid)
  if (from > now) {
    console.warn(
      `Data query rejected: start date ${from.toISOString()} is in the future. ` +
      `Current time: ${now.toISOString()}`
    );
    return false;
  }
  
  return true;
}

/**
 * Adjusts the 'to' date to respect the minimum data delay if needed.
 * If the original 'to' date is too recent, it will be adjusted to the maximum allowed time.
 * 
 * @param from - Start date of the query
 * @param to - End date of the query
 * @returns Adjusted date range that respects the minimum delay
 */
export function adjustDateRangeForDelay(from: Date, to: Date): { from: Date; to: Date; adjusted: boolean } {
  const minDelayMinutes = config.polygon.minDataDelayMinutes;
  
  // If no delay is configured, return original dates
  if (minDelayMinutes <= 0) {
    return { from, to, adjusted: false };
  }
  
  const now = new Date();
  const minAllowedTime = new Date(now.getTime() - minDelayMinutes * 60 * 1000);
  
  // If the 'to' date is within the restricted time window, adjust it
  if (to > minAllowedTime) {
    console.log(
      `Adjusting query end date from ${to.toISOString()} to ${minAllowedTime.toISOString()} ` +
      `to respect ${minDelayMinutes}-minute data delay restriction`
    );
    return { from, to: minAllowedTime, adjusted: true };
  }
  
  return { from, to, adjusted: false };
}
