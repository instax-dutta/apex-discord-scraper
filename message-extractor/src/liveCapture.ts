// Apex Discord Scraper - Live Capture
//
// Consumes MESSAGE_CREATE events from a `LiveMessageSource` (the Gateway) and
// appends them to the *same* chunked archive the bulk extractor writes to.
//
// Live capture and bulk extraction share one archive:
//   - the same channel directory and `_archive.json` manifest
//   - part numbers continue where extraction stopped
//   - `newest_message_id` keeps advancing so a later incremental catch-up knows
//     where to start
//
// Messages are buffered per channel and flushed on an interval, on a batch
// size, or on shutdown. Flushes are serialized per channel, so the
// double-buffer + pending-write model cannot reorder writes.

import type { DiscordMessage, ExportRow } from './types.js';
import type { Storage } from './storage.js';
import type { JsonStorage } from './jsonStorage.js';
import { toExportRow } from './userTokenFetcher.js';
import { Logger } from './utils.js';

/** Anything that can push live messages: the Gateway, or a test double. */
export interface LiveMessageSource {
  /** Connect; resolves once the source is live. */
  start(): Promise<void>;
  /** Disconnect and release resources. */
  close(): void;
  /** Register a handler for new messages. */
  onMessage(handler: (message: DiscordMessage) => void): void;
}

export interface LiveChannel {
  channelId: string;
  channelName: string;
  guildId: string;
  guildName: string;
}

export interface LiveCaptureOptions {
  storage: Storage;
  jsonStorage: JsonStorage;
  source: LiveMessageSource;
  log?: Logger;
  /** How often buffered messages are flushed to disk (ms). */
  flushIntervalMs?: number;
  /** Buffer size that triggers a flush. */
  batchMessages?: number;
  /** Safety cap: force a flush if a buffer grows past this. */
  maxBufferPerChannel?: number;
  /** Skip messages authored by bots or webhooks. */
  excludeBots?: boolean;
  /** Called as messages arrive, for progress output. */
  onMessage?: (channelId: string, sessionCount: number) => void;
}

export interface LiveChannelStats {
  channelId: string;
  channelName: string;
  /** Messages captured in this live session. */
  sessionMessages: number;
  /** Total messages in the archive for this channel. */
  totalMessages: number;
  jsonParts: number;
}

export interface LiveCaptureSummary {
  sessionId: string | null;
  startedAt: string;
  stoppedAt: string;
  messages: number;
  channels: LiveChannelStats[];
}

const DEFAULT_FLUSH_INTERVAL_MS = 2000;
const DEFAULT_BATCH_MESSAGES = 200;
const DEFAULT_MAX_BUFFER = 5000;
/** Bounded duplicate guard; live streams should not deliver duplicates. */
const MAX_DEDUP_ENTRIES = 200_000;

/** True when `id` is strictly newer than `floor`. Unparseable ids are kept. */
function isNewerThan(id: string, floor: string): boolean {
  try {
    return BigInt(id) > BigInt(floor);
  } catch {
    return true;
  }
}

export class LiveCapture {
  private readonly storage: Storage;
  private readonly jsonStorage: JsonStorage;
  private readonly source: LiveMessageSource;
  private readonly log: Logger;
  private readonly flushIntervalMs: number;
  private readonly batchMessages: number;
  private readonly maxBufferPerChannel: number;
  private readonly excludeBots: boolean;
  private readonly onMessage?: (channelId: string, count: number) => void;

  private readonly targets = new Map<string, LiveChannel>();
  private readonly buffers = new Map<string, ExportRow[]>();
  private readonly sessionCounts = new Map<string, number>();
  private readonly baseTotals = new Map<string, number>();
  private readonly newestIds = new Map<string, string | null>();
  /**
   * Catch-up baseline captured at session start. Messages at or below it are
   * already in the archive, so they are dropped instead of appended a second
   * time (a restarted capture, or a replayed dispatch, would otherwise
   * duplicate history into the shared archive).
   */
  private readonly floorIds = new Map<string, string>();
  private readonly dedup = new Map<string, Set<string>>();
  private readonly flushChains = new Map<string, Promise<void>>();

  private timer: NodeJS.Timeout | null = null;
  private sessionId: string | null = null;
  private startedAt = '';
  private running = false;

  constructor(options: LiveCaptureOptions) {
    this.storage = options.storage;
    this.jsonStorage = options.jsonStorage;
    this.source = options.source;
    this.log = options.log || new Logger();
    this.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.batchMessages = options.batchMessages ?? DEFAULT_BATCH_MESSAGES;
    this.maxBufferPerChannel = options.maxBufferPerChannel ?? DEFAULT_MAX_BUFFER;
    this.excludeBots = options.excludeBots ?? false;
    this.onMessage = options.onMessage;
  }

  isRunning(): boolean {
    return this.running;
  }

  async start(channels: LiveChannel[]): Promise<void> {
    if (this.running) throw new Error('Live capture is already running');
    if (channels.length === 0) throw new Error('No channels provided for live capture');

    this.startedAt = new Date().toISOString();

    for (const channel of channels) {
      this.targets.set(channel.channelId, channel);
      this.buffers.set(channel.channelId, []);
      this.sessionCounts.set(channel.channelId, 0);
      this.dedup.set(channel.channelId, new Set());

      const progress = await this.storage.getOrCreateProgress(
        channel.channelId,
        channel.guildId,
        channel.channelName,
      );
      this.baseTotals.set(channel.channelId, progress.total_extracted || 0);
      this.newestIds.set(channel.channelId, progress.newest_message_id ?? null);
      this.floorIds.set(channel.channelId, progress.newest_message_id ?? '');
    }

    this.sessionId = `live-${Date.now()}`;

    for (const channel of this.targets.values()) {
      await this.storage.startLiveSession(this.sessionKey(channel.channelId), channel.channelId);
      await this.storage.updateProgress(channel.channelId, { status: 'live', error_message: null });
    }

    this.source.onMessage((message) => this.handleMessage(message));
    await this.source.start();

    this.running = true;
    this.timer = setInterval(() => {
      void this.flushAll();
    }, this.flushIntervalMs);
    this.timer.unref?.();

    this.log.info(
      `Live capture started (${this.targets.size} channel(s), flush every ${this.flushIntervalMs}ms)`,
    );
  }

  async stop(): Promise<LiveCaptureSummary> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    // Stop the source first so no new messages arrive during the final flush.
    this.source.close();
    await this.flushAll();
    await Promise.allSettled([...this.flushChains.values()]);

    const channels: LiveChannelStats[] = [];

    for (const [channelId, target] of this.targets) {
      const sessionMessages = this.sessionCounts.get(channelId) ?? 0;

      // Compacts the open part (every append is already durable) so a stopped
      // capture leaves the archive in the normal chunk format, and so the part
      // counts reported below are the final ones.
      this.jsonStorage.finalizeChannel(channelId);

      const archive = this.jsonStorage.loadArchive(channelId);
      const cumulative = (this.baseTotals.get(channelId) ?? 0) + sessionMessages;

      await this.storage.endLiveSession(this.sessionKey(channelId), 'stopped');
      await this.storage.updateProgress(channelId, {
        status: 'done',
        total_extracted: cumulative,
        last_message_id: this.newestIds.get(channelId) ?? null,
        newest_message_id: this.newestIds.get(channelId) ?? null,
      });

      channels.push({
        channelId,
        channelName: target.channelName,
        sessionMessages,
        totalMessages: archive?.totalMessages ?? 0,
        jsonParts: archive?.totalParts ?? 0,
      });
    }

    this.running = false;

    const summary: LiveCaptureSummary = {
      sessionId: this.sessionId,
      startedAt: this.startedAt,
      stoppedAt: new Date().toISOString(),
      messages: channels.reduce((sum, c) => sum + c.sessionMessages, 0),
      channels,
    };

    this.log.info(
      `Live capture stopped: ${summary.messages} message(s) across ${channels.length} channel(s)`,
    );

    return summary;
  }

  /**
   * Channels this capture is listening on. Empty once stopped: a stopped
   * capture no longer owns its archive, so it must not keep the catch-up
   * scheduler away from it.
   */
  getChannelIds(): string[] {
    return this.running ? [...this.targets.keys()] : [];
  }

  getStats(): LiveChannelStats[] {
    return [...this.targets.values()].map((target) => {
      const archive = this.jsonStorage.loadArchive(target.channelId);
      return {
        channelId: target.channelId,
        channelName: target.channelName,
        sessionMessages: this.sessionCounts.get(target.channelId) ?? 0,
        totalMessages: archive?.totalMessages ?? 0,
        jsonParts: archive?.totalParts ?? 0,
      };
    });
  }

  private sessionKey(channelId: string): string {
    return `${this.sessionId}:${channelId}`;
  }

  private handleMessage(message: DiscordMessage): void {
    const channelId = message.channel_id;
    if (!channelId) return;

    const target = this.targets.get(channelId);
    if (!target) return; // not one of our channels (or a thread we did not ask for)

    if (this.excludeBots && (message.author?.bot || message.webhook_id)) return;
    if (!message.id) return;

    // Anything at or below the baseline the session started from is already on
    // disk. Dropping it is what keeps a restart from duplicating history.
    const floor = this.floorIds.get(channelId);
    if (floor && !isNewerThan(message.id, floor)) return;

    const seen = this.dedup.get(channelId)!;
    if (seen.has(message.id)) return;
    if (seen.size >= MAX_DEDUP_ENTRIES) seen.clear();
    seen.add(message.id);

    const buffer = this.buffers.get(channelId)!;
    buffer.push(toExportRow(message));

    const count = (this.sessionCounts.get(channelId) ?? 0) + 1;
    this.sessionCounts.set(channelId, count);
    this.onMessage?.(channelId, count);

    const threshold = Math.min(this.batchMessages, this.maxBufferPerChannel);
    if (buffer.length >= threshold) {
      void this.flushChannel(channelId);
    }
  }

  private async flushAll(): Promise<void> {
    await Promise.all([...this.targets.keys()].map((id) => this.flushChannel(id)));
  }

  /** Serialize flushes per channel so writes (and part numbers) stay ordered. */
  private flushChannel(channelId: string): Promise<void> {
    const previous = this.flushChains.get(channelId) ?? Promise.resolve();
    const next = previous
      .then(() => this.doFlush(channelId))
      .catch((error: any) => {
        this.log.error(`Live flush failed for ${channelId}: ${error?.message ?? error}`);
      });
    this.flushChains.set(channelId, next);
    return next;
  }

  private async doFlush(channelId: string): Promise<void> {
    const buffer = this.buffers.get(channelId);
    const target = this.targets.get(channelId);
    if (!buffer || !target || buffer.length === 0) return;

    const toFlush = buffer.splice(0, buffer.length);

    // Append into the shared archive (continues part numbering). Messages the
    // archive already holds are dropped; they are not counted as captured.
    const { skipped } = this.jsonStorage.appendMessages(channelId, target.channelName, toFlush);
    if (skipped > 0) {
      const remaining = Math.max(0, (this.sessionCounts.get(channelId) ?? 0) - skipped);
      this.sessionCounts.set(channelId, remaining);
    }

    let newest = this.newestIds.get(channelId) ?? null;
    for (const row of toFlush) {
      if (newest === null || BigInt(row.id) > BigInt(newest)) newest = row.id;
    }
    this.newestIds.set(channelId, newest);

    const cumulative = (this.baseTotals.get(channelId) ?? 0) + (this.sessionCounts.get(channelId) ?? 0);

    await this.storage.updateProgress(channelId, {
      status: 'live',
      total_extracted: cumulative,
      last_message_id: newest,
      newest_message_id: newest,
    });

    await this.storage.updateLiveSession(
      this.sessionKey(channelId),
      this.sessionCounts.get(channelId) ?? 0,
      newest,
    );
  }
}
