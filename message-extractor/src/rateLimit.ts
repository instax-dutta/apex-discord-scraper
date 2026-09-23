// Apex Discord Scraper - Rate Limit Failsafe
//
// One place that decides *how fast* we are allowed to talk to Discord.
// It combines:
//   - a spacing gate (sustained requests/second with optional burst)
//   - per-bucket pacing from X-RateLimit-* response headers
//   - global rate limit awareness (X-RateLimit-Global)
//   - adaptive slowdown after 429s that decays back to the floor on success
//   - a circuit breaker so a sustained block pauses instead of hammering
//
// Every request in the process goes through `acquire()` / `release()` so
// parallelism can never outrun the limiter.

import { Logger, sleep } from './utils.js';

export interface RateLimitConfig {
  /** Sustained requests per second. User tokens are throttled aggressively. */
  requestsPerSecond: number;
  /** Extra requests allowed immediately at startup before pacing applies. */
  burstSize: number;
  /** Hard cap on in-flight requests, regardless of scheduling. */
  maxConcurrent: number;

  /** Floor between two consecutive requests (ms). */
  minDelayMs: number;
  /** Ceiling for the adaptive interval (ms). */
  maxDelayMs: number;
  /** Starting multiplier applied to the interval on a 429. */
  backoffMultiplier: number;
  /** Successful requests required before the adaptive interval decays. */
  recoveryStreak: number;
  /** Factor applied to the interval per recovery streak (0..1). */
  recoveryFactor: number;

  /** Cap for our own exponential backoff when Discord tells us nothing (ms). */
  maxBackoffMs: number;
  /**
   * Hard cap for a server-supplied Retry-After. Kept well above the backoff
   * cap so an abuse/global limit (which can be minutes) is actually respected
   * instead of turning into a retry storm.
   */
  maxRetryAfterMs: number;
  /** Fallback base for exponential backoff when headers are missing (ms). */
  baseBackoffMs: number;

  /** Consecutive failures before the circuit opens. */
  circuitBreakerThreshold: number;
  /** How long the circuit stays open before a half-open probe (ms). */
  circuitBreakerResetMs: number;

  enableAdaptivePacing: boolean;
}

const DEFAULT_CONFIG: RateLimitConfig = {
  requestsPerSecond: 4,
  burstSize: 4,
  maxConcurrent: 8,
  minDelayMs: 200,
  maxDelayMs: 30000,
  backoffMultiplier: 2,
  recoveryStreak: 5,
  recoveryFactor: 0.85,
  maxBackoffMs: 60000,
  maxRetryAfterMs: 15 * 60 * 1000,
  baseBackoffMs: 1000,
  circuitBreakerThreshold: 6,
  circuitBreakerResetMs: 20000,
  enableAdaptivePacing: true,
};

const HEADER_RATE_LIMIT_REMAINING = 'x-ratelimit-remaining';
const HEADER_RATE_LIMIT_RESET = 'x-ratelimit-reset';
const HEADER_RATE_LIMIT_BUCKET = 'x-ratelimit-bucket';
/**
 * Seconds from *now* until the bucket resets. Preferred over
 * `x-ratelimit-reset`, which Discord has served both as an absolute epoch and
 * as a relative delay, forcing callers to guess from the magnitude.
 */
const HEADER_RATE_LIMIT_RESET_AFTER = 'x-ratelimit-reset-after';
const HEADER_RATE_LIMIT_GLOBAL = 'x-ratelimit-global';
const HEADER_RETRY_AFTER = 'retry-after';

interface BucketState {
  remaining: number;
  resetAt: number;
  lastRequestAt: number;
}

/**
 * Resolve when a bucket resets, as an absolute epoch in ms.
 *
 * `x-ratelimit-reset-after` is preferred because it is unambiguously relative
 * to now. `x-ratelimit-reset` is ambiguous in the wild - Discord has sent both
 * absolute epoch seconds and a relative delay - so its magnitude is used to
 * decide, which is what the previous implementation did for both headers.
 * Returns null when neither header carries a usable value.
 */
function parseResetAt(headers: Headers): number | null {
  const resetAfter = headers.get(HEADER_RATE_LIMIT_RESET_AFTER);
  if (resetAfter !== null) {
    const seconds = Number.parseFloat(resetAfter);
    if (!Number.isNaN(seconds) && seconds >= 0) return Date.now() + seconds * 1000;
  }

  const reset = headers.get(HEADER_RATE_LIMIT_RESET);
  if (reset !== null) {
    const parsed = Number.parseFloat(reset);
    if (!Number.isNaN(parsed) && parsed >= 0) {
      return parsed > 1e6 ? parsed * 1000 : Date.now() + parsed * 1000;
    }
  }

  return null;
}

type CircuitState = 'closed' | 'half-open' | 'open';

export interface RateLimitState {
  circuitState: CircuitState;
  inFlight: number;
  currentIntervalMs: number;
  consecutiveFailures: number;
  buckets: number;
  globalBlockedUntil: number;
}

export class RateLimitFailsafe {
  private readonly config: RateLimitConfig;
  private readonly log: Logger;

  /** Earliest epoch (ms) at which the next request may start. */
  private nextAllowedAt = 0;
  /** Tokens available for the initial burst. */
  private burstTokens: number;
  private inFlight = 0;

  private buckets = new Map<string, BucketState>();
  private globalBlockedUntil = 0;

  private currentIntervalMs: number;
  private consecutiveFailures = 0;
  private successStreak = 0;

  private circuitState: CircuitState = 'closed';
  private circuitOpenedAt = 0;

  constructor(config: Partial<RateLimitConfig> = {}, log?: Logger) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.log = log || new Logger();
    this.currentIntervalMs = this.baseIntervalMs();
    this.burstTokens = this.config.burstSize;
    this.nextAllowedAt = Date.now();
  }

  private baseIntervalMs(): number {
    return Math.max(this.config.minDelayMs, 1000 / this.config.requestsPerSecond);
  }

  /**
   * Block until a request slot is available, reserving the slot so that
   * concurrent callers cannot all wake at once. Always pair with `release()`.
   */
  async acquire(bucketKey = 'global'): Promise<void> {
    await this.waitForCircuit();
    while (this.inFlight >= this.config.maxConcurrent) {
      await sleep(25);
      await this.waitForCircuit();
    }

    const now = Date.now();

    // Global spacing is reserved independently of any single bucket, so a
    // bucket resetting in 2s does not stall requests to other buckets.
    let startAt = Math.max(now, this.nextAllowedAt, this.globalBlockedUntil);

    // The first few requests may start without waiting; after that we reserve
    // the next slot so concurrent callers are spaced out instead of bursting.
    if (this.burstTokens > 0 && startAt <= now + 1) {
      this.burstTokens--;
    } else {
      this.nextAllowedAt = startAt + this.intervalMs();
    }

    // Per-bucket header pacing applies only to this bucket.
    const bucketAt = this.bucketReadyAt(bucketKey);
    if (bucketAt > startAt) startAt = bucketAt;

    const wait = startAt - now;
    if (wait > 0) {
      this.log.debug(`Rate gate: waiting ${Math.round(wait)}ms (${bucketKey})`);
      await sleep(wait);
    }

    this.inFlight++;
  }

  /** Mark a request as finished. Must be called for every `acquire()`. */
  release(): void {
    this.inFlight = Math.max(0, this.inFlight - 1);
  }

  private intervalMs(): number {
    return this.config.enableAdaptivePacing ? this.currentIntervalMs : this.baseIntervalMs();
  }

  /**
   * Track X-RateLimit-* headers from a response so later requests know when
   * this particular bucket resets.
   */
  trackBucket(bucketKey: string, headers: Headers): void {
    // Key by the caller's logical bucket (route + resource), not Discord's
    // opaque hash, so `bucketReadyAt()` looks up the same entry later.
    const key = bucketKey || headers.get(HEADER_RATE_LIMIT_BUCKET) || 'global';

    const state: BucketState = this.buckets.get(key) ?? {
      remaining: 1,
      resetAt: 0,
      lastRequestAt: 0,
    };

    const remaining = headers.get(HEADER_RATE_LIMIT_REMAINING);
    if (remaining !== null) {
      const parsed = Number.parseInt(remaining, 10);
      if (!Number.isNaN(parsed)) state.remaining = parsed;
    }

    const resetAt = parseResetAt(headers);
    if (resetAt !== null) state.resetAt = resetAt;

    state.lastRequestAt = Date.now();
    this.buckets.set(key, state);

    if (headers.get(HEADER_RATE_LIMIT_GLOBAL) === 'true') {
      const blockUntil = state.resetAt > Date.now() ? state.resetAt : Date.now() + 1000;
      this.globalBlockedUntil = Math.max(this.globalBlockedUntil, blockUntil);
      this.log.warn(`Discord global rate limit active until ${new Date(blockUntil).toISOString()}`);
    }
  }

  private bucketReadyAt(bucketKey: string): number {
    const state = this.buckets.get(bucketKey);
    if (!state) return 0;
    if (state.remaining <= 0 && state.resetAt > Date.now()) {
      return state.resetAt + Math.random() * 250;
    }
    return 0;
  }

  private async waitForCircuit(): Promise<void> {
    if (this.circuitState !== 'open') return;

    const remaining = this.circuitOpenedAt + this.config.circuitBreakerResetMs - Date.now();
    if (remaining > 0) {
      this.log.warn(`Circuit breaker open, pausing ${Math.round(remaining)}ms`);
      await sleep(remaining);
    }
    this.circuitState = 'half-open';
  }

  /**
   * Call after every 429. Grows the interval and, if it keeps happening,
   * opens the circuit so we stop hammering Discord.
   */
  recordRateLimit(): void {
    this.consecutiveFailures++;
    this.successStreak = 0;
    this.burstTokens = 0;

    if (this.config.enableAdaptivePacing) {
      this.currentIntervalMs = Math.min(
        this.config.maxDelayMs,
        Math.max(this.intervalMs() * this.config.backoffMultiplier, this.baseIntervalMs()),
      );
      this.log.warn(
        `Rate limited (${this.consecutiveFailures} in a row). ` +
        `Pacing at ${Math.round(this.currentIntervalMs)}ms between requests.`,
      );
    }

    this.maybeOpenCircuit('rate limits');
  }

  /**
   * Block every bucket until `delayMs` from now. Used when Discord signals a
   * global rate limit (header or JSON body), where per-bucket pacing is not
   * enough - the whole account is throttled.
   */
  markGlobalBlock(delayMs: number): void {
    const until = Date.now() + Math.max(1, Math.min(delayMs, this.config.maxRetryAfterMs));
    this.globalBlockedUntil = Math.max(this.globalBlockedUntil, until);
    this.log.warn(`Global rate limit: blocking all requests until ${new Date(until).toISOString()}`);
  }

  /** Call after a successful request to let pacing recover. */
  recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.successStreak++;

    if (this.circuitState === 'half-open') {
      this.log.info('Circuit breaker: closing after a successful request');
      this.circuitState = 'closed';
    }

    if (this.config.enableAdaptivePacing && this.successStreak >= this.config.recoveryStreak) {
      this.successStreak = 0;
      this.currentIntervalMs = Math.max(
        this.baseIntervalMs(),
        this.currentIntervalMs * this.config.recoveryFactor,
      );
    }
  }

  /** Call for non-429 failures (5xx/network) so the circuit still protects us. */
  recordError(): void {
    this.consecutiveFailures++;
    this.successStreak = 0;
    this.maybeOpenCircuit('consecutive errors');
  }

  private maybeOpenCircuit(reason: string): void {
    if (this.consecutiveFailures < this.config.circuitBreakerThreshold) return;
    if (this.circuitState === 'open') return;

    const wasHalfOpen = this.circuitState === 'half-open';
    this.circuitState = 'open';
    this.circuitOpenedAt = Date.now();
    this.log.error(
      `Circuit breaker OPEN after ${this.consecutiveFailures} ${reason}` +
      (wasHalfOpen ? ' (half-open probe failed)' : ''),
    );
  }

  /**
   * How long to wait before retrying, preferring Discord's own guidance.
   */
  getRetryDelay(headers: Headers | undefined, attempt: number): number {
    const retryAfter = headers?.get(HEADER_RETRY_AFTER);
    if (retryAfter) {
      const seconds = Number.parseFloat(retryAfter);
      if (!Number.isNaN(seconds) && seconds >= 0) {
        return Math.min(seconds * 1000, this.config.maxRetryAfterMs) + Math.random() * 250;
      }
    }

    if (headers) {
      const resetAt = parseResetAt(headers);
      if (resetAt !== null) {
        const delay = resetAt - Date.now();
        if (delay > 0) return Math.min(delay, this.config.maxRetryAfterMs) + Math.random() * 250;
      }
    }

    const exp = this.config.baseBackoffMs * Math.pow(2, attempt);
    const capped = Math.min(exp, this.config.maxBackoffMs);
    return capped + Math.random() * 0.3 * capped;
  }

  getState(): RateLimitState {
    return {
      circuitState: this.circuitState,
      inFlight: this.inFlight,
      currentIntervalMs: Math.round(this.currentIntervalMs),
      consecutiveFailures: this.consecutiveFailures,
      buckets: this.buckets.size,
      globalBlockedUntil: this.globalBlockedUntil,
    };
  }

  reset(): void {
    this.nextAllowedAt = Date.now();
    this.burstTokens = this.config.burstSize;
    this.inFlight = 0;
    this.buckets.clear();
    this.globalBlockedUntil = 0;
    this.currentIntervalMs = this.baseIntervalMs();
    this.consecutiveFailures = 0;
    this.successStreak = 0;
    this.circuitState = 'closed';
    this.circuitOpenedAt = 0;
    this.log.info('Rate limit state reset');
  }
}
