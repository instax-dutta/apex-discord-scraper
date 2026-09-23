// Apex Discord Scraper - Discord Gateway client
//
// Connects to Discord's Gateway over WebSocket and surfaces MESSAGE_CREATE
// dispatches. It handles the parts of the protocol that matter for a long
// running listener:
//   - HELLO -> heartbeat at the server-provided interval (first beat jittered)
//   - IDENTIFY for a fresh session, RESUME after a drop
//   - sequence tracking, heartbeat ACKs, zombie detection
//   - a handshake deadline, so a socket that connects and then says nothing
//     cannot leave a capture hanging silently
//   - bounded reconnect with exponential backoff + jitter
//   - fatal close codes (bad token / bad intents) fail fast instead of looping
//
// No dependency is required on Node 22+ (global WebSocket); on older Node it
// falls back to an `ws` install if one is present.

import { createRequire } from 'module';
import type { DiscordMessage } from './types.js';
import { Logger, sleep } from './utils.js';
import { ExtractorError } from './errors.js';

const GATEWAY_VERSION = 10;
const DEFAULT_GATEWAY_URL = 'wss://gateway.discord.gg';
/**
 * How long a socket may sit between "connected" and "session ready" before we
 * drop it and let the retry loop try again. Discord answers a handshake in
 * well under a second when it is healthy.
 */
const DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;

export const GatewayIntents = {
  GUILDS: 1 << 0,
  GUILD_MESSAGES: 1 << 9,
  DIRECT_MESSAGES: 1 << 12,
  MESSAGE_CONTENT: 1 << 15,
} as const;

export type GatewayState =
  | 'connecting'
  | 'identifying'
  | 'resuming'
  | 'ready'
  | 'reconnecting'
  | 'closed'
  | 'fatal';

export interface DiscordGatewayOptions {
  token: string;
  /** Bot tokens must send intents; user tokens must not. */
  isBot?: boolean;
  gatewayUrl?: string;
  log?: Logger;
  /** Consecutive connection failures before we give up. */
  maxConnectAttempts?: number;
  /** Deadline for a socket to reach READY/RESUMED before it is dropped. */
  handshakeTimeoutMs?: number;
}

interface WebSocketLike {
  addEventListener(type: string, listener: (event: any) => void): void;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  readonly readyState: number;
}

/** Fatal protocol errors that retrying will not fix. */
export class GatewayFatalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'GatewayFatalError';
  }
}

export function classifyGatewayClose(code: number): 'fatal' | 'resume' | 'identify' {
  switch (code) {
    case 4004: // authentication failed
    case 4010: // invalid shard
    case 4011: // sharding required
    case 4012: // invalid API version
    case 4013: // invalid intents
    case 4014: // disallowed intents
      return 'fatal';
    case 4007: // invalid seq
    case 4009: // session timed out
      return 'identify';
    default:
      return 'resume';
  }
}

/**
 * Exponential backoff with jitter, capped.
 *
 * The cap is applied last so it is a real bound: jittering *after* the cap
 * lets a delay overshoot `maxMs` by up to 20%, which makes the ceiling a
 * suggestion rather than a ceiling. `getBackoffDelay` in utils caps the same
 * way.
 */
export function nextGatewayDelay(attempt: number, baseMs = 1000, maxMs = 30000): number {
  const exponential = baseMs * Math.pow(2, Math.max(0, attempt));
  return Math.min(exponential * (0.8 + Math.random() * 0.4), maxMs);
}

function toText(data: unknown): string | null {
  if (typeof data === 'string') return data;
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString('utf8');
  }
  if (data == null) return null;
  return String(data);
}

function createSocket(url: string): WebSocketLike {
  const globalCtor = (globalThis as { WebSocket?: new (u: string) => WebSocketLike }).WebSocket;
  if (typeof globalCtor === 'function') {
    return new globalCtor(url);
  }

  try {
    const require = createRequire(import.meta.url);
    const WS = require('ws') as new (u: string) => WebSocketLike;
    return new WS(url);
  } catch {
    throw new GatewayFatalError(
      'Live capture needs a WebSocket implementation. Use Node 22+ (global WebSocket) or run `npm install ws`.',
    );
  }
}

export class DiscordGateway {
  private readonly token: string;
  private readonly isBot: boolean;
  private readonly gatewayUrl: string;
  private readonly log: Logger;
  private readonly maxConnectAttempts: number;
  private readonly handshakeTimeoutMs: number;

  private ws: WebSocketLike | null = null;
  private seq: number | null = null;
  private sessionId: string | null = null;
  private resumeUrl: string | null = null;
  private heartbeatIntervalMs = 45000;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private awaitingAck = false;
  private closedByUser = false;
  private attempts = 0;
  private state: GatewayState = 'closed';

  private messageHandlers: Array<(message: DiscordMessage) => void> = [];
  private stateHandlers: Array<(state: GatewayState, detail?: string) => void> = [];
  private fatalHandlers: Array<(error: Error) => void> = [];

  constructor(options: DiscordGatewayOptions) {
    this.token = options.token;
    this.isBot = options.isBot ?? false;
    this.gatewayUrl = options.gatewayUrl || DEFAULT_GATEWAY_URL;
    this.log = options.log || new Logger();
    this.maxConnectAttempts = options.maxConnectAttempts ?? 12;
    this.handshakeTimeoutMs =
      options.handshakeTimeoutMs && options.handshakeTimeoutMs > 0
        ? options.handshakeTimeoutMs
        : DEFAULT_HANDSHAKE_TIMEOUT_MS;
  }

  onMessage(handler: (message: DiscordMessage) => void): this {
    this.messageHandlers.push(handler);
    return this;
  }

  onState(handler: (state: GatewayState, detail?: string) => void): this {
    this.stateHandlers.push(handler);
    return this;
  }

  onFatal(handler: (error: Error) => void): this {
    this.fatalHandlers.push(handler);
    return this;
  }

  getState(): GatewayState {
    return this.state;
  }

  getSessionId(): string | null {
    return this.sessionId;
  }

  /** Connect and resolve once the session is READY or RESUMED. */
  async start(): Promise<void> {
    this.closedByUser = false;
    this.attempts = 0;

    while (!this.closedByUser) {
      const useResume = this.canResume();
      try {
        await this.open(useResume);
        return;
      } catch (error) {
        if (error instanceof GatewayFatalError) {
          this.setState('fatal', error.message);
          throw error;
        }
        this.attempts++;
        if (this.attempts > this.maxConnectAttempts) {
          const fatal = new ExtractorError(
            `Gateway could not connect after ${this.attempts} attempts: ${(error as Error).message}`,
            { kind: 'network', retryable: true, cause: error },
          );
          this.setState('fatal', fatal.message);
          throw fatal;
        }
        const delay = nextGatewayDelay(this.attempts - 1);
        this.setState('reconnecting', `retry in ${Math.round(delay)}ms`);
        this.log.warn(
          `Gateway connect failed, retrying in ${Math.round(delay)}ms ` +
          `(${this.attempts}/${this.maxConnectAttempts}): ${(error as Error).message}`,
        );
        await sleep(delay);
      }
    }
  }

  close(): void {
    this.closedByUser = true;
    this.clearHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      this.ws?.close(1000, 'client closing');
    } catch {
      // ignore
    }
    this.ws = null;
    this.setState('closed');
  }

  private canResume(): boolean {
    return !!(this.sessionId && this.seq !== null && this.resumeUrl);
  }

  private open(resume: boolean): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      /** Set when *we* gave up on this socket, so its close is not a reconnect. */
      let abandoned = false;
      const url =
        resume && this.resumeUrl
          ? `${this.resumeUrl}/?v=${GATEWAY_VERSION}&encoding=json`
          : `${this.gatewayUrl}/?v=${GATEWAY_VERSION}&encoding=json`;

      this.setState(resume ? 'resuming' : 'connecting');

      let ws: WebSocketLike;
      try {
        ws = createSocket(url);
      } catch (error) {
        reject(error);
        return;
      }
      this.ws = ws;

      const fail = (error: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(handshakeTimer);
        reject(error);
      };

      const succeed = () => {
        if (settled) return;
        settled = true;
        clearTimeout(handshakeTimer);
        this.attempts = 0;
        resolve();
      };

      /**
       * A socket that connects but never finishes the handshake - a blackholed
       * proxy, a half-open TCP session, a server that stops answering - leaves
       * this promise pending forever without a deadline. Nothing resolves,
       * nothing rejects, so the retry loop never runs and live capture looks
       * healthy while capturing nothing. Timing out turns that silence into an
       * ordinary connect failure the caller already knows how to retry.
       */
      const handshakeTimer = setTimeout(() => {
        abandoned = true;
        this.log.warn(
          `Gateway handshake did not complete within ${this.handshakeTimeoutMs}ms; dropping the socket`,
        );
        fail(new Error(`Gateway handshake timed out after ${this.handshakeTimeoutMs}ms`));
        try {
          ws.close(4000, 'handshake timeout');
        } catch {
          // Nothing else to do; the caller is already retrying.
        }
      }, this.handshakeTimeoutMs);

      ws.addEventListener('error', (event: any) => {
        this.log.warn(`Gateway socket error: ${event?.message ?? 'unknown'}`);
      });

      ws.addEventListener('message', (event: any) => {
        const text = toText(event?.data);
        if (!text) return;

        let payload: any;
        try {
          payload = JSON.parse(text);
        } catch {
          this.log.warn('Gateway sent unparseable payload; ignoring');
          return;
        }

        try {
          this.handlePayload(payload, succeed);
        } catch (error) {
          fail(error instanceof Error ? error : new Error(String(error)));
        }
      });

      ws.addEventListener('close', (event: any) => {
        this.clearHeartbeat();
        const code = typeof event?.code === 'number' ? event.code : 1006;
        const reason = event?.reason ? String(event.reason) : '';

        // This socket was already abandoned by the handshake deadline, and the
        // caller is retrying. Letting the close schedule its own reconnect
        // would race that retry.
        if (abandoned) return;

        if (!settled) {
          const action = classifyGatewayClose(code);
          fail(
            action === 'fatal'
              ? new GatewayFatalError(`Gateway closed with fatal code ${code}${reason ? `: ${reason}` : ''}`)
              : new Error(`Gateway closed before ready (${code}${reason ? ` ${reason}` : ''})`),
          );
          return;
        }

        this.handleClose(code, reason);
      });
    });
  }

  private handlePayload(payload: any, onReady: () => void): void {
    const op = payload?.op;

    switch (op) {
      case 10: {
        // HELLO
        this.heartbeatIntervalMs = Number(payload?.d?.heartbeat_interval) || 45000;
        this.startHeartbeat();
        if (this.canResume()) {
          this.log.info('Resuming Gateway session');
          this.setState('resuming');
          this.send({ op: 6, d: { token: this.token, session_id: this.sessionId, seq: this.seq } });
        } else {
          this.sendIdentify();
        }
        return;
      }

      case 11: {
        // Heartbeat ACK
        this.awaitingAck = false;
        return;
      }

      case 1: {
        // Server requested an immediate heartbeat.
        this.sendHeartbeat();
        return;
      }

      case 7: {
        this.log.warn('Gateway requested a reconnect');
        this.reconnectNow('op7 reconnect');
        return;
      }

      case 9: {
        const resumable = payload?.d === true;
        this.log.warn(`Gateway invalid session (resumable=${resumable})`);
        if (!resumable) {
          this.sessionId = null;
          this.seq = null;
          this.resumeUrl = null;
        }
        this.reconnectNow('op9 invalid session');
        return;
      }

      case 0: {
        if (typeof payload?.s === 'number') this.seq = payload.s;
        const type = payload?.t;

        if (type === 'READY') {
          this.sessionId = payload?.d?.session_id ?? null;
          this.resumeUrl = payload?.d?.resume_gateway_url ?? this.resumeUrl;
          const user = payload?.d?.user;
          this.log.info(`Gateway ready as ${user?.username ?? 'unknown'} (${user?.id ?? '?'})`);
          this.setState('ready');
          onReady();
        } else if (type === 'RESUMED') {
          this.log.info('Gateway session resumed');
          this.setState('ready');
          onReady();
        } else if (type === 'MESSAGE_CREATE') {
          const message = payload.d as DiscordMessage;
          for (const handler of this.messageHandlers) {
            try {
              handler(message);
            } catch (error) {
              this.log.warn(`Live message handler failed: ${error instanceof Error ? error.message : error}`);
            }
          }
        }
        return;
      }

      default:
        return;
    }
  }

  private sendIdentify(): void {
    this.setState('identifying');

    const properties = {
      os: process.platform === 'win32' ? 'Windows' : process.platform === 'darwin' ? 'Mac OS X' : 'Linux',
      browser: 'Chrome',
      device: '',
      system_locale: 'en-US',
      browser_user_agent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Discord/1.0.9007 Chrome/120.0.6099.283 Electron/28.1.0 Safari/537.36',
      browser_version: '120.0.6099.283',
      os_version: '10',
      release_channel: 'stable',
      client_build_number: 300000,
    };

    const d: Record<string, unknown> = {
      token: this.token,
      properties,
      presence: { status: 'online', since: 0, activities: [], afk: false },
      compress: false,
    };

    // Bot tokens are gated by intents; user tokens are not and must not send them.
    if (this.isBot) {
      d.intents =
        GatewayIntents.GUILDS |
        GatewayIntents.GUILD_MESSAGES |
        GatewayIntents.DIRECT_MESSAGES |
        GatewayIntents.MESSAGE_CONTENT;
    }

    this.send({ op: 2, d });
  }

  private startHeartbeat(): void {
    this.clearHeartbeat();
    this.awaitingAck = false;

    const beat = () => {
      if (this.closedByUser || !this.ws) return;

      if (this.awaitingAck) {
        this.log.warn('Gateway heartbeat was not acknowledged - reconnecting');
        this.reconnectNow('heartbeat timeout');
        return;
      }

      this.sendHeartbeat();
      this.heartbeatTimer = setTimeout(beat, this.heartbeatIntervalMs);
    };

    // Discord requires the first beat to be jittered.
    const jitter = Math.floor(Math.random() * this.heartbeatIntervalMs);
    this.heartbeatTimer = setTimeout(beat, jitter);
  }

  private sendHeartbeat(): void {
    this.awaitingAck = true;
    this.send({ op: 1, d: this.seq });
  }

  private clearHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.awaitingAck = false;
  }

  /** Tear down the socket so the close handler schedules a reconnect. */
  private reconnectNow(reason: string): void {
    this.clearHeartbeat();
    this.log.warn(`Reconnecting Gateway (${reason})`);
    try {
      this.ws?.close(4000, reason);
    } catch {
      // If we could not close cleanly, schedule directly.
      this.scheduleReconnect(reason);
    }
  }

  private handleClose(code: number, reason: string): void {
    if (this.closedByUser) {
      this.setState('closed');
      return;
    }

    const action = classifyGatewayClose(code);

    if (action === 'fatal') {
      const error = new GatewayFatalError(
        `Gateway closed with fatal code ${code}${reason ? `: ${reason}` : ''}`,
      );
      this.setState('fatal', error.message);
      this.emitFatal(error);
      return;
    }

    if (action === 'identify') {
      this.sessionId = null;
      this.seq = null;
      this.resumeUrl = null;
    }

    this.scheduleReconnect(`close ${code}${reason ? ` (${reason})` : ''}`);
  }

  private scheduleReconnect(reason: string): void {
    if (this.closedByUser || this.reconnectTimer) return;

    this.attempts++;
    if (this.attempts > this.maxConnectAttempts) {
      const error = new ExtractorError(
        `Gateway gave up after ${this.attempts} reconnect attempts (${reason})`,
        { kind: 'network', retryable: true },
      );
      this.setState('fatal', error.message);
      this.emitFatal(error);
      return;
    }

    const delay = nextGatewayDelay(this.attempts - 1);
    this.setState('reconnecting', `retry in ${Math.round(delay)}ms`);
    this.log.warn(
      `Gateway reconnecting in ${Math.round(delay)}ms ` +
      `(${this.attempts}/${this.maxConnectAttempts}): ${reason}`,
    );

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.closedByUser) return;

      this.open(this.canResume()).catch((error: Error) => {
        if (this.closedByUser) return;
        this.log.warn(`Gateway reconnect failed: ${error.message}`);
        this.scheduleReconnect(error.message);
      });
    }, delay);
  }

  private send(payload: unknown): void {
    if (!this.ws || this.ws.readyState !== 1) {
      this.log.warn('Gateway send skipped: socket is not open');
      return;
    }
    try {
      this.ws.send(JSON.stringify(payload));
    } catch (error) {
      this.log.warn(`Gateway send failed: ${error instanceof Error ? error.message : error}`);
    }
  }

  private setState(state: GatewayState, detail?: string): void {
    if (this.state === state && !detail) return;
    this.state = state;
    for (const handler of this.stateHandlers) {
      try {
        handler(state, detail);
      } catch {
        // observers must not break the gateway
      }
    }
  }

  private emitFatal(error: Error): void {
    for (const handler of this.fatalHandlers) {
      try {
        handler(error);
      } catch {
        // ignore
      }
    }
  }
}
