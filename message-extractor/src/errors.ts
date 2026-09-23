// Apex Discord Scraper - Error Model
// A single source of truth for classifying failures so retry logic never
// falls into an unconditional loop or gives up on a transient error.

export type ErrorKind =
  | 'rate_limit'   // 429 - retry with server-provided delay
  | 'auth'         // 401 / 403 - token invalid, expired, or no permission
  | 'not_found'    // 404 - channel/message gone
  | 'client'       // other 4xx - malformed request, do not retry
  | 'server'       // 5xx - transient on Discord's side
  | 'network'      // socket reset / DNS / fetch rejected
  | 'timeout'      // request exceeded our deadline
  | 'aborted'      // caller asked us to stop
  | 'unknown';

export interface ExtractorErrorOptions {
  kind: ErrorKind;
  status?: number;
  retryable?: boolean;
  attempts?: number;
  cause?: unknown;
}

/**
 * Every failure surfaced by the network layer is wrapped in this type so
 * callers can make a decision instead of string-matching messages.
 */
export class ExtractorError extends Error {
  readonly kind: ErrorKind;
  readonly status?: number;
  readonly retryable: boolean;
  attempts?: number;

  constructor(message: string, options: ExtractorErrorOptions) {
    super(message);
    this.name = 'ExtractorError';
    this.kind = options.kind;
    this.status = options.status;
    this.retryable = options.retryable ?? false;
    this.attempts = options.attempts;
    if (options.cause !== undefined) {
      (this as { cause?: unknown }).cause = options.cause;
    }
  }

  /** Short, actionable hint for the CLI / logs. */
  get hint(): string {
    switch (this.kind) {
      case 'auth':
        return 'Your token is invalid or expired. Re-copy the "authorization" header from discord.com.';
      case 'not_found':
        return 'The channel/message does not exist or is not visible to this account.';
      case 'rate_limit':
        return 'Discord is throttling this account. The extractor will slow down and retry.';
      case 'server':
        return 'Discord returned a server error. Retrying with backoff.';
      case 'network':
        return 'Network error. Check connectivity and retry.';
      case 'timeout':
        return 'Request timed out. The extractor will retry with a longer deadline.';
      default:
        return 'See the error below for details.';
    }
  }
}

/**
 * Map an HTTP status to a kind + whether a retry could reasonably succeed.
 */
export function classifyStatus(status: number): { kind: ErrorKind; retryable: boolean } {
  if (status === 429) return { kind: 'rate_limit', retryable: true };
  if (status === 401 || status === 403) return { kind: 'auth', retryable: false };
  if (status === 404) return { kind: 'not_found', retryable: false };
  // 408 Request Timeout, 425 Too Early, and all 5xx are worth another attempt.
  if (status === 408 || status === 425 || status >= 500) {
    return { kind: 'server', retryable: true };
  }
  if (status >= 400) return { kind: 'client', retryable: false };
  return { kind: 'unknown', retryable: false };
}

const NETWORK_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ECONNABORTED',
  'EPIPE',
  'ETIMEDOUT',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_SOCKET',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
]);

/** True when the error is a fetch/undici/Node socket failure worth retrying. */
export function isNetworkError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  const err = error as { name?: string; code?: string; message?: string; cause?: { code?: string } };
  if (err.code && NETWORK_CODES.has(err.code)) return true;
  if (err.cause?.code && NETWORK_CODES.has(err.cause.code)) return true;
  if (err.name === 'TypeError' && typeof err.message === 'string' && err.message.includes('fetch failed')) {
    return true;
  }
  if (err.name === 'FetchError') return true;
  return false;
}

const DISK_FULL_CODES = new Set(['ENOSPC', 'EDQUOT', 'EFBIG']);

/**
 * True when a write failed because the filesystem is out of space. Retrying
 * will not help; the run should stop cleanly and preserve its resume cursor.
 */
export function isDiskFullError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  const code = (error as { code?: string }).code;
  return typeof code === 'string' && DISK_FULL_CODES.has(code);
}

/** True when the request was cancelled (by us or by a timeout signal). */
export function isAbortError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  const err = error as { name?: string; code?: string };
  return err.name === 'AbortError' || err.code === 'ABORT_ERR';
}

/** Cancel a `setTimeout` scheduled by `createTimeoutSignal`. */
export interface TimeoutSignal {
  signal: AbortSignal;
  clear: () => void;
}

/**
 * Build an abort signal that fires after `timeoutMs`, linked to an optional
 * parent signal. Falls back to a manual timer when `AbortSignal.timeout`
 * is unavailable (older runtimes / non-Node hosts).
 */
export function createTimeoutSignal(timeoutMs: number, parent?: AbortSignal): TimeoutSignal {
  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function' && !parent) {
    return { signal: AbortSignal.timeout(timeoutMs), clear: () => {} };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error(`Request timed out after ${timeoutMs}ms`));
  }, timeoutMs);

  const onParentAbort = () => controller.abort(parent?.reason);
  if (parent) {
    if (parent.aborted) {
      controller.abort(parent.reason);
    } else {
      parent.addEventListener('abort', onParentAbort, { once: true });
    }
  }

  return {
    signal: controller.signal,
    clear: () => {
      clearTimeout(timer);
      parent?.removeEventListener('abort', onParentAbort);
    },
  };
}
