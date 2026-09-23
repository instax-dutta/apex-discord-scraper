// Apex Discord Scraper - User Token API Client
//
// This is the ONLY place that retries network requests. The fetcher above it
// does not implement its own retry loop, which keeps failure handling in one
// place: every failure is classified (retryable vs fatal), every wait is
// bounded, and authentication failures fail fast instead of looping.

import { Logger, sleep, tryParseJson } from './utils.js';
import { RateLimitFailsafe } from './rateLimit.js';
import {
  ExtractorError,
  classifyStatus,
  createTimeoutSignal,
  isAbortError,
  isNetworkError,
} from './errors.js';
import type { DiscordMessage } from './types.js';

const DISCORD_API_BASE = 'https://discord.com/api/v9';
const PAGE_SIZE = 100; // Discord API max

export interface UserTokenClientOptions {
  /** Retries *after* the first attempt. Total attempts = retries + 1. */
  retries?: number;
  /** Per-request deadline (ms). */
  timeoutMs?: number;
  /** Shared rate limiter. One is created if omitted. */
  rateLimiter?: RateLimitFailsafe;
  /** Override the API base (useful for tests / proxies). */
  apiBase?: string;
}

export interface GetOptions {
  retries?: number;
  timeoutMs?: number;
  signal?: AbortSignal;
  /** Rate limit bucket to charge. Defaults to one derived from the endpoint. */
  bucketKey?: string;
}

export class UserTokenClient {
  private readonly token: string;
  private readonly log: Logger;
  private readonly userAgent: string;
  private readonly apiBase: string;
  readonly rateLimiter: RateLimitFailsafe;

  private readonly defaultRetries: number;
  private readonly defaultTimeoutMs: number;

  constructor(token: string, log?: Logger, options: UserTokenClientOptions = {}) {
    this.token = token;
    this.log = log || new Logger();
    this.apiBase = options.apiBase || DISCORD_API_BASE;
    this.defaultRetries = options.retries ?? 8;
    this.defaultTimeoutMs = options.timeoutMs ?? 30000;
    this.rateLimiter = options.rateLimiter ?? new RateLimitFailsafe({}, this.log);
    this.userAgent =
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Discord/1.0.9007 Chrome/120.0.6099.283 Electron/28.1.0 Safari/537.36';
  }

  /**
   * Perform a GET with bounded retries.
   *
   * Retried: 429, 408/425/5xx, network errors, timeouts.
   * Fatal:   401/403 (bad token), 404 (missing channel), other 4xx, caller abort.
   */
  async get<T>(endpoint: string, options: GetOptions = {}): Promise<T> {
    const retries = options.retries ?? this.defaultRetries;
    const timeoutMs = options.timeoutMs ?? this.defaultTimeoutMs;
    const bucketKey = options.bucketKey ?? this.bucketKeyFor(endpoint);
    const maxAttempts = retries + 1;

    let attempt = 0;
    let lastError: ExtractorError | undefined;

    while (attempt < maxAttempts) {
      if (options.signal?.aborted) {
        throw this.aborted(endpoint);
      }

      await this.rateLimiter.acquire(bucketKey);

      let retryDelayMs: number | undefined;

      try {
        const response = await this.requestOnce(endpoint, timeoutMs, options.signal);
        this.rateLimiter.trackBucket(bucketKey, response.headers);

        if (response.ok) {
          this.rateLimiter.recordSuccess();
          return await this.parseJson<T>(response, endpoint);
        }

        const { kind, retryable } = classifyStatus(response.status);
        const body = await this.safeReadBody(response);
        const error = new ExtractorError(
          `Discord API ${response.status} ${response.statusText} for ${endpoint}` +
            (body ? `: ${body.slice(0, 300)}` : ''),
          { kind, status: response.status, retryable },
        );

        if (kind === 'rate_limit') {
          this.rateLimiter.recordRateLimit();

          // Discord's headers are authoritative, but it also returns the delay
          // (and whether the limit is global) in the JSON body. Use whichever
          // is available so a missing header cannot cause a too-fast retry.
          const payload = tryParseJson<{ retry_after?: number; global?: boolean }>(body);
          const bodyRetryMs =
            typeof payload?.retry_after === 'number' ? payload.retry_after * 1000 : undefined;
          const scope = response.headers.get('x-ratelimit-scope');
          const isGlobal =
            payload?.global === true ||
            response.headers.get('x-ratelimit-global') === 'true' ||
            scope === 'global';

          if (isGlobal) {
            this.rateLimiter.markGlobalBlock(bodyRetryMs ?? 1000);
          }

          retryDelayMs =
            bodyRetryMs ?? this.rateLimiter.getRetryDelay(response.headers, attempt);
        }

        if (!retryable || attempt + 1 >= maxAttempts) {
          error.attempts = attempt + 1;
          throw error;
        }
        lastError = error;
      } catch (thrown) {
        const error = this.toExtractorError(thrown, endpoint);

        if (error.kind === 'aborted') {
          throw error;
        }
        if (error.kind === 'server' || error.kind === 'network' || error.kind === 'timeout') {
          this.rateLimiter.recordError();
        }

        if (!error.retryable || attempt + 1 >= maxAttempts) {
          error.attempts = error.attempts ?? attempt + 1;
          throw error;
        }
        lastError = error;
      } finally {
        this.rateLimiter.release();
      }

      const delay = retryDelayMs ?? this.rateLimiter.getRetryDelay(undefined, attempt);
      attempt++;
      this.log.warn(
        `Retry ${attempt}/${retries} for ${endpoint} in ${Math.round(delay)}ms ` +
          `(${lastError?.kind ?? 'unknown'})`,
      );
      await sleep(delay);
    }

    throw lastError ?? new ExtractorError(`Request to ${endpoint} failed`, {
      kind: 'unknown',
      retryable: false,
    });
  }

  /** A single attempt. Classifies transport-level failures. */
  private async requestOnce(
    endpoint: string,
    timeoutMs: number,
    parent?: AbortSignal,
  ): Promise<Response> {
    const { signal, clear } = createTimeoutSignal(timeoutMs, parent);

    try {
      return await fetch(`${this.apiBase}${endpoint}`, {
        method: 'GET',
        headers: {
          Authorization: this.token,
          'User-Agent': this.userAgent,
          'Content-Type': 'application/json',
          Accept: 'application/json',
          'X-Discord-Locale': 'en-US',
          'X-Discord-Timezone': 'UTC',
        },
        signal,
      });
    } catch (error) {
      if (isAbortError(error)) {
        if (parent?.aborted) {
          throw this.aborted(endpoint, error);
        }
        throw new ExtractorError(`Request timed out after ${timeoutMs}ms for ${endpoint}`, {
          kind: 'timeout',
          status: 408,
          retryable: true,
          cause: error,
        });
      }
      if (isNetworkError(error)) {
        const detail = (error as Error).message || 'socket failure';
        throw new ExtractorError(`Network error for ${endpoint}: ${detail}`, {
          kind: 'network',
          retryable: true,
          cause: error,
        });
      }
      const detail = error instanceof Error ? error.message : String(error);
      throw new ExtractorError(`Request failed for ${endpoint}: ${detail}`, {
        kind: 'unknown',
        retryable: false,
        cause: error,
      });
    } finally {
      clear();
    }
  }

  private toExtractorError(error: unknown, endpoint: string): ExtractorError {
    if (error instanceof ExtractorError) return error;
    const detail = error instanceof Error ? error.message : String(error);
    return new ExtractorError(`Unexpected error for ${endpoint}: ${detail}`, {
      kind: 'unknown',
      retryable: false,
      cause: error,
    });
  }

  private aborted(endpoint: string, cause?: unknown): ExtractorError {
    return new ExtractorError(`Request to ${endpoint} was aborted`, {
      kind: 'aborted',
      retryable: false,
      cause,
    });
  }

  private async parseJson<T>(response: Response, endpoint: string): Promise<T> {
    try {
      return (await response.json()) as T;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new ExtractorError(`Invalid JSON from ${endpoint}: ${detail}`, {
        kind: 'server',
        status: response.status,
        retryable: false,
        cause: error,
      });
    }
  }

  private async safeReadBody(response: Response): Promise<string> {
    try {
      return await response.text();
    } catch {
      return '';
    }
  }

  /** Group requests by channel/guild so the limiter can pace per bucket. */
  private bucketKeyFor(endpoint: string): string {
    const channelMessages = endpoint.match(/^\/channels\/(\d+)\/messages/);
    if (channelMessages) return `messages:${channelMessages[1]}`;

    const channel = endpoint.match(/^\/channels\/(\d+)/);
    if (channel) return `channel:${channel[1]}`;

    const guild = endpoint.match(/^\/guilds\/(\d+)/);
    if (guild) return `guild:${guild[1]}`;

    return endpoint.split('?')[0];
  }

  async fetchMessages(
    channelId: string,
    options: {
      before?: string;
      after?: string;
      limit?: number;
      signal?: AbortSignal;
      retries?: number;
    } = {},
  ): Promise<DiscordMessage[]> {
    const { limit = PAGE_SIZE } = options;

    const query = new URLSearchParams({ limit: String(limit) });
    if (options.before) query.set('before', options.before);
    if (options.after) query.set('after', options.after);

    return this.get<DiscordMessage[]>(
      `/channels/${channelId}/messages?${query.toString()}`,
      { signal: options.signal, retries: options.retries, bucketKey: `messages:${channelId}` },
    );
  }

  async getChannel(channelId: string, signal?: AbortSignal): Promise<unknown> {
    return this.get(`/channels/${channelId}`, { signal, retries: 2 });
  }

  async getGuild(guildId: string, signal?: AbortSignal): Promise<unknown> {
    return this.get(`/guilds/${guildId}`, { signal, retries: 2 });
  }

  async getCurrentUser(signal?: AbortSignal): Promise<{ id: string; username: string; email?: string }> {
    return this.get('/users/@me', { signal, retries: 2 });
  }

  /** `/users/@me` does not include guilds - this is the correct endpoint. */
  async getCurrentUserGuilds(signal?: AbortSignal): Promise<Array<{ id: string; name: string }>> {
    return this.get('/users/@me/guilds', { signal, retries: 3 });
  }

  async getGuildChannels(guildId: string, signal?: AbortSignal): Promise<unknown[]> {
    return this.get(`/guilds/${guildId}/channels`, { signal, retries: 3 });
  }

  async validateToken(signal?: AbortSignal): Promise<{ valid: boolean; user?: unknown; error?: string; errorKind?: string }> {
    try {
      const user = await this.getCurrentUser(signal);
      return { valid: true, user };
    } catch (error) {
      if (error instanceof ExtractorError) {
        return { valid: false, error: `${error.message} ${error.hint}`, errorKind: error.kind };
      }
      return { valid: false, error: error instanceof Error ? error.message : String(error) };
    }
  }
}
