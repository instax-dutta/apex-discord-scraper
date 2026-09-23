// Apex Discord Scraper - Type Definitions

// Discord API Types (what the API returns)
export interface DiscordAttachment {
  id: string;
  filename: string;
  url: string;
  proxy_url: string;
  content_type: string | null;
  width: number | null;
  height: number | null;
  size: number;
}

export interface DiscordMessage {
  id: string;
  content: string;
  timestamp: string;
  /** Present on Gateway dispatches; useful for filtering live messages. */
  channel_id?: string;
  guild_id?: string | null;
  /** Present on webhook messages (which are effectively bots). */
  webhook_id?: string;
  type?: number;
  author: {
    id: string;
    username: string;
    discriminator: string;
    bot?: boolean;
  };
  message_reference?: {
    message_id: string;
    channel_id?: string;
    guild_id?: string;
  };
  referenced_message?: {
    author: {
      id: string;
      username: string;
      discriminator: string;
    };
    content: string;
  } | null;
  attachments?: DiscordAttachment[];
  embeds?: Array<{
    image?: { url: string; proxy_url?: string; width?: number; height?: number };
    thumbnail?: { url: string; proxy_url?: string; width?: number; height?: number };
  }>;
  thread?: { id: string; name: string };
}

// Export row type (AI-ready format, matches original plugin)
export interface ExportRow {
  id: string;
  author: string;              // "username#discriminator (id)"
  content: string;
  timestamp: string;
  replyTo: string | null;       // message_id being replied to
  replyToAuthor: string | null;
  attachments: number;
  attachmentUrls: string[];
  imageUrls: string[];
  attachmentsDetailed: AttachmentExport[];
  threadId: string | null;
}

export interface AttachmentExport {
  url: string;
  proxyUrl: string;
  filename: string;
  contentType: string | null;
  width: number | null;
  height: number | null;
}

// Database row type (what we store)
export interface MessageRow {
  id: string;
  channel_id: string;
  server_id: string;
  author_id: string;
  author_username: string | null;
  author_discriminator: string | null;
  content: string;
  timestamp: string;
  reply_to_id: string | null;
  reply_to_author_username: string | null;
  attachment_count: number;
  attachment_urls: string | null;     // JSON array
  image_urls: string | null;          // JSON array
  attachments_detail: string | null;   // JSON array
  embed_image_urls: string | null;    // JSON array
  thread_id: string | null;
  extracted_at: string;
}

// Channel progress tracking
export interface ChannelProgress {
  channel_id: string;
  server_id: string;
  channel_name: string | null;
  last_message_id: string | null;
  last_extracted_ts: string | null;
  total_extracted: number;
  status: 'pending' | 'extracting' | 'done' | 'error' | 'live';
  error_message: string | null;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
  /** JSON-encoded ChannelResumeState for per-shard resume. */
  resume_state: string | null;
  /**
   * Highest message id ever written for this channel. This is the baseline an
   * incremental catch-up fetches from, so it must only be advanced for data
   * that is durably on disk.
   */
  newest_message_id: string | null;
}

/** A live capture session (Gateway listener writing into the archive). */
export interface LiveSession {
  session_id: string;
  channel_id: string;
  started_at: string;
  last_message_id: string | null;
  message_count: number;
  status: 'active' | 'stopped' | 'error';
}

// Channel configuration
export interface ChannelConfig {
  channel_id: string;
  server_id: string;
  server_name?: string;
  channel_name?: string;
  enabled: boolean;
}

// Time segment for parallel fetching
export interface TimeSegment {
  after: string;   // snowflake ID (inclusive)
  before: string;  // snowflake ID (exclusive)
}

// Fetch progress callback
export type ProgressCallback = (stats: {
  channelId: string;
  total: number;
  segment: number;
  segments: number;
  rate: number;  // messages per second
}) => void;

// Abort signal support. `signal` is optional so in-flight requests can be
// cancelled for real, not only between pages.
export interface AbortState {
  aborted: boolean;
  signal?: AbortSignal;
  /** Cancels the underlying request, if a controller is available. */
  abort?: () => void;
}

// ==================== Resume State ====================

/**
 * Per-shard resume cursor. Time-sharded fetches each page backwards within
 * their own window, so a single global cursor cannot describe where work
 * stopped. One of these per segment does.
 */
export interface SegmentResumeState {
  index: number;
  /**
   * Exact shard window this cursor belongs to. Persisted because shard
   * boundaries are derived from the current time, so recomputing them on a
   * later run would shift the windows and could leave gaps on resume.
   */
  after: string;
  before: string;
  /** Next `before` value to use for this shard, or null when finished. */
  cursor: string | null;
  done: boolean;
}

/**
 * How this layout's shard boundaries were chosen, plus the estimated message
 * load per shard. Persisted with the resume state so `status` can explain the
 * current plan without re-probing.
 */
export interface SegmentBalance {
  strategy: 'density' | 'equal-time';
  /** (max - min) / mean of the estimated loads. Null when unknown. */
  imbalance: number | null;
  /** Estimated message count per shard, aligned with `segments`. */
  loads: number[] | null;
  /** Messages per shard either side of the estimate, when known. */
  minLoad?: number | null;
  maxLoad?: number | null;
  /** Probe samples the estimate was built from (0 for equal-time). */
  probes?: number;
  /** When the estimate was taken. */
  measuredAt?: string;
}

export interface ChannelResumeState {
  version: number;
  parallelism: number;
  segments: SegmentResumeState[];
  updatedAt: string;
  /** Optional so states written before balancing existed still parse. */
  balance?: SegmentBalance | null;
}

/** Result of a single shard fetch. */
export interface SegmentResult {
  index: number;
  fetched: number;
  done: boolean;
  error?: Error;
}

export interface SegmentErrorInfo {
  index: number;
  message: string;
  kind: string;
}

/** Result of fetching a whole channel, including partial-failure detail. */
export interface FetchResult {
  fetched: number;
  aborted: boolean;
  resumeState: ChannelResumeState;
  segmentErrors: SegmentErrorInfo[];
  usedFallback: boolean;
}

// Configuration
export interface ScraperConfig {
  botToken: string;
  userToken?: string;
  dbPath: string;
  parallelism: number;
  chunkSize: number;
  pageDelayMs: number;
  maxRetries: number;
  backoffBaseMs: number;
  liveMaxBuffer: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  /** Sustained requests/second allowed by the rate limiter. */
  requestsPerSecond?: number;
  /** Per-request deadline in milliseconds. */
  timeoutMs?: number;
  /** Upper bound for adaptive pacing / retry backoff (ms). */
  maxBackoffMs?: number;
  /** Pretty-print chunk JSON (larger + slower; off by default at scale). */
  prettyJson?: boolean;
  /**
   * How many of the most recently written archive parts feed the duplicate
   * guard when `exactArchiveDedup` is off. `0` disables the guard.
   */
  archiveDedupParts?: number;
  /**
   * Index the whole archive for duplicate ids instead of only its newest parts,
   * so a message is recognized as archived no matter how old it is. Costs about
   * 56 bytes of memory per archived id, capped by `archiveDedupMaxIds`.
   */
  exactArchiveDedup?: boolean;
  /**
   * Ceiling on the duplicate guard, in ids. Past it the guard keeps the newest
   * ids - the ones a re-fetch can reach - and reports that it is no longer
   * exact rather than growing without bound.
   */
  archiveDedupMaxIds?: number;
  /**
   * Place shard boundaries by observed message density instead of equal time
   * slices. Costs a few probe requests; off means equal-time windows.
   */
  balanceShards?: boolean;
  /**
   * Explicit number of density probes to take before a fresh extraction.
   * Overrides the shard-count-derived default.
   */
  densityProbes?: number;
  /**
   * When true, a plain extract of a channel already marked `done` performs an
   * incremental catch-up instead of resuming (which would be a no-op).
   */
  autoIncremental?: boolean;
  /** Cadence for the recurring catch-up scheduler, in milliseconds. */
  watchIntervalMs?: number;
  /** How often the live capture flushes buffered messages to disk (ms). */
  liveFlushIntervalMs?: number;
  /** Messages buffered per channel before a live flush is forced. */
  liveBatchMessages?: number;
  /** True when the configured token is a bot token (affects Gateway IDENTIFY). */
  isBot?: boolean;
}

// JSON Archive types (for message storage)
export interface JsonChunk {
  channelId: string;
  channelName: string;
  part: number;
  startId: string;
  endId: string;
  messageCount: number;
  extractedAt: string;
  messages: ExportRow[];
}

export interface JsonArchive {
  channelId: string;
  channelName: string;
  totalMessages: number;
  totalParts: number;
  extractedAt: string;
  completedAt: string | null;
  parts: JsonPartMeta[];
}

export interface JsonPartMeta {
  part: number;
  filename: string;
  messageCount: number;
  startId: string;
  endId: string;
  /** Set while the part is still being appended to (JSON Lines on disk). */
  open?: boolean;
  /** When the part was last written; absent on archives built before this. */
  updatedAt?: string;
}
