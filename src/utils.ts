// Apex Discord Scraper - Utility Functions

import { statfsSync } from 'fs';
import { extname, join } from 'path';
import type { TimeSegment } from './types.js';

// Discord epoch for snowflake conversion
export const DISCORD_EPOCH = 1420070400000n;

/**
 * Resolve the JSONL export path for one channel.
 *
 * Exports are per channel, so a shared path would mean two concurrent writers
 * interleaving into one file. `{channel}` is substituted when present;
 * otherwise, for a multi-channel run the channel id is inserted before the
 * extension, or the path is treated as a directory when it has no extension.
 */
export function resolveJsonlPath(
  template: string,
  channelId: string,
  singleChannel: boolean,
): string {
  if (template.includes('{channel}')) {
    return template.split('{channel}').join(channelId);
  }
  if (singleChannel) return template;

  const ext = extname(template);
  if (ext) return `${template.slice(0, -ext.length)}-${channelId}${ext}`;
  return join(template, `${channelId}.jsonl`);
}

/**
 * Convert a Discord snowflake ID to a Unix timestamp in milliseconds
 */
export function snowflakeToTs(snowflake: string): number {
  return Number((BigInt(snowflake) >> 22n) + DISCORD_EPOCH);
}

/**
 * Convert a Unix timestamp (ms) to a Discord snowflake ID
 */
export function tsToSnowflake(timestamp: number): string {
  return ((BigInt(timestamp) - DISCORD_EPOCH) << 22n).toString();
}

/**
 * Get current Unix timestamp in milliseconds
 */
export function now(): number {
  return Date.now();
}

/**
 * Sleep for a specified duration
 */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Calculate the number of 5-segment time shards for parallel fetching
 * Same algorithm as the original Vencord plugin
 */
export function calculateTimeSegments(
  channelId: string,
  parallelism: number = 5
): TimeSegment[] {
  const channelTs = snowflakeToTs(channelId);
  const nowTs = now();
  const span = nowTs - channelTs;

  // For small channels (<7 days), use single segment
  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  if (span < sevenDaysMs) {
    return [{
      after: channelId,  // Use channel ID as the start (oldest possible)
      before: tsToSnowflake(nowTs + 60000),  // Add 1 minute buffer
      afterInclusive: true,
    }];
  }

  // Split into parallel segments
  const segDuration = Math.floor(span / parallelism);
  const segments: TimeSegment[] = [];

  for (let i = 0; i < parallelism; i++) {
    const start = channelTs + i * segDuration;
    const end = i === parallelism - 1
      ? nowTs + 60000  // Last segment extends to now + 1 min
      : channelTs + (i + 1) * segDuration;

    segments.push({
      after: tsToSnowflake(start),
      before: tsToSnowflake(end),
      afterInclusive: true,
    });
  }

  return segments;
}

/**
 * Exponential backoff with jitter
 */
export function getBackoffDelay(
  attempt: number,
  baseMs: number = 1000,
  maxDelayMs: number = 16000
): number {
  const exponentialDelay = baseMs * Math.pow(2, attempt);
  const jitter = Math.random() * 0.3 * exponentialDelay;  // 0-30% jitter
  return Math.min(exponentialDelay + jitter, maxDelayMs);
}

/**
 * Format bytes to human readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Format duration in milliseconds to human readable string
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

/**
 * Calculate rate (messages per second)
 */
export function calculateRate(count: number, elapsedMs: number): number {
  if (elapsedMs === 0) return 0;
  return Math.round((count / elapsedMs) * 1000 * 100) / 100;
}

/**
 * Truncate string with ellipsis
 */
export function truncate(str: string, maxLen: number): string {
  if (str.length <= maxLen) return str;
  return str.slice(0, maxLen - 3) + '...';
}

/**
 * Parse JSON, returning null instead of throwing.
 */
export function tryParseJson<T = unknown>(text: string | null | undefined): T | null {
  if (!text) return null;
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

/**
 * Best-effort free disk space check. Returns null when unsupported.
 */
export function getFreeDiskBytes(path: string): number | null {
  try {
    // Node >= 18.15 exposes statfsSync; guard in case it is unavailable.
    if (typeof statfsSync !== 'function') return null;
    const stats = statfsSync(path) as { bavail: number | bigint; bsize: number | bigint };
    return Number(stats.bavail) * Number(stats.bsize);
  } catch {
    return null;
  }
}

/**
 * Safe JSON parse with fallback
 */
export function safeJsonParse<T>(json: string | null, fallback: T): T {
  if (!json) return fallback;
  try {
    return JSON.parse(json) as T;
  } catch {
    return fallback;
  }
}

/**
 * Simple logger with levels
 */
export class Logger {
  constructor(private level: 'debug' | 'info' | 'warn' | 'error' = 'info') {}

  private shouldLog(level: 'debug' | 'info' | 'warn' | 'error'): boolean {
    const levels = ['debug', 'info', 'warn', 'error'];
    return levels.indexOf(level) >= levels.indexOf(this.level);
  }

  debug(msg: string, ...args: unknown[]): void {
    if (this.shouldLog('debug')) {
      console.debug(`[DEBUG] ${msg}`, ...args);
    }
  }

  info(msg: string, ...args: unknown[]): void {
    if (this.shouldLog('info')) {
      console.info(`[INFO] ${msg}`, ...args);
    }
  }

  warn(msg: string, ...args: unknown[]): void {
    if (this.shouldLog('warn')) {
      console.warn(`[WARN] ${msg}`, ...args);
    }
  }

  error(msg: string, ...args: unknown[]): void {
    if (this.shouldLog('error')) {
      console.error(`[ERROR] ${msg}`, ...args);
    }
  }
}
