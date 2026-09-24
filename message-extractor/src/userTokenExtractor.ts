// Apex Discord Scraper - User Token Extractor
//
// Orchestrates per-channel extraction. Two properties matter at 100M scale:
//
//   1. Memory is bounded. Messages flow through a capped buffer that is
//      drained by flushes; a `afterPage` backpressure hook stops shards from
//      running ahead of the disk.
//   2. Resume can never lose data. The per-shard cursors that get persisted
//      are a *durable snapshot* taken at flush time, so a crash between
//      flushes re-fetches a page (a duplicate) instead of skipping it.

import type {
  ExportRow,
  ProgressCallback,
  AbortState,
  ScraperConfig,
  ChannelProgress,
  ChannelResumeState,
  TimeSegment,
} from './types.js';
import { Storage } from './storage.js';
import { JsonStorage } from './jsonStorage.js';
import type { DedupCostEstimate } from './jsonStorage.js';
import { UserTokenClient } from './userTokenClient.js';
import { UserTokenFetcher } from './userTokenFetcher.js';
import { RateLimitFailsafe } from './rateLimit.js';
import {
  Logger,
  formatDuration,
  formatBytes,
  sleep,
  getFreeDiskBytes,
  tsToSnowflake,
} from './utils.js';
import { isDiskFullError } from './errors.js';
import {
  buildBalance,
  cloneResumeState,
  createResumeState,
  decideIncrementalWindow,
  describeResumeState,
  hasReusableWindows,
  isResumeComplete,
  parseResumeState,
  resolveSegments,
  serializeResumeState,
} from './resumable.js';
import { balanceSegmentsByDensity, type BalancedSegments } from './segments.js';
import { DiscordGateway } from './gateway.js';
import { LiveCapture, type LiveMessageSource, type LiveCaptureOptions } from './liveCapture.js';
import { acquireWriterLock, type WriterLockHandle } from './writerLock.js';
import { dirname } from 'path';
import { createWriteStream, type WriteStream } from 'fs';
import { existsSync, mkdirSync } from 'fs';

export interface ExtractResult {
  channelId: string;
  channelName: string;
  guildId: string;
  guildName: string;
  success: boolean;
  messagesExtracted: number;
  duplicatesSkipped: number;
  jsonParts: number;
  duration: number;
  error?: string;
  errorKind?: string;
  /** True when a retry of the whole channel could plausibly succeed. */
  retryable?: boolean;
  failedShards?: number;
  usedFallback?: boolean;
  /** Bytes of disk free-space check, when available. */
  freeDiskBytes?: number | null;
  /** Lines written to the JSONL export, when one was requested. */
  jsonlRows?: number;
  /** Set when the JSONL export failed; the archive is unaffected. */
  jsonlError?: string;
}

export interface ChannelInfo {
  channelId: string;
  channelName: string;
  guildId: string;
  guildName: string;
}

/**
 * Durability cadence. A flush writes everything buffered and only then lets
 * the persisted resume cursor move past it, so a crash between flushes
 * re-fetches work (no data loss). Longer intervals mean bigger chunk files and
 * less manifest I/O; 60s bounds the worst-case rework at 60s of messages.
 */
const FLUSH_INTERVAL_MS = 60_000;
/** Keep message buffers and chunk sizes in the same ballpark (see below). */
const MAX_FLUSH_MESSAGES = 50_000;
/**
 * Cap on the in-run duplicate guard. Shards are disjoint and resume is
 * durable, so this is only a safety net; bounding it keeps memory flat across
 * a 100M-message run instead of accumulating one string per message.
 */
const MAX_DEDUP_ENTRIES = 200_000;
/**
 * Rows per JSONL write. Each write is awaited, so this bounds the extra memory
 * a JSONL export adds on top of the normal flush buffer (~a few MB), rather
 * than letting the stream buffer a whole channel internally.
 */
const JSONL_BATCH_ROWS = 2_000;
/** Abort before starting if free space is below this. */
const MIN_FREE_DISK_BYTES = 256 * 1024 * 1024;
/** Warn if free space is below this. */
const WARN_FREE_DISK_BYTES = 2 * 1024 * 1024 * 1024;
const CHANNEL_RETRY_DELAY_MS = 2000;

export class UserTokenExtractor {
  private storage!: Storage;
  private jsonStorage?: JsonStorage;
  private client: UserTokenClient;
  private fetcher: UserTokenFetcher;
  private log: Logger;
  private config: ScraperConfig;
  private activeExports: Map<string, AbortState> = new Map();
  /** Live captures this process started, so nothing else writes over them. */
  private readonly liveCaptures = new Set<LiveCapture>();
  private stopRequested = false;
  readonly rateLimiter: RateLimitFailsafe;
  /** Released in close() and retained so a failed unlink can be retried. */
  private writerLock: WriterLockHandle | null = null;

  constructor(config: ScraperConfig, log?: Logger) {
    this.config = config;
    this.log = log || new Logger(config.logLevel);
    this.storage = new Storage(config.dbPath, this.log);

    const parallelism = config.parallelism || 6;
    const requestsPerSecond = config.requestsPerSecond ?? 4;
    this.rateLimiter = new RateLimitFailsafe(
      {
        requestsPerSecond,
        // Derive the floor from the requested rate so raising
        // REQUESTS_PER_SECOND is not silently capped by the default floor.
        minDelayMs: Math.min(200, Math.floor(1000 / requestsPerSecond)),
        maxConcurrent: Math.max(4, parallelism * 2),
        baseBackoffMs: config.backoffBaseMs,
        maxDelayMs: config.maxBackoffMs ?? 30000,
        maxBackoffMs: config.maxBackoffMs ?? 60000,
      },
      this.log,
    );

    this.client = new UserTokenClient(config.userToken!, this.log, {
      retries: config.maxRetries,
      timeoutMs: config.timeoutMs ?? 30000,
      rateLimiter: this.rateLimiter,
    });
    this.fetcher = new UserTokenFetcher(this.client, this.log);
  }

  private getJsonStorage(): JsonStorage {
    if (this.jsonStorage) return this.jsonStorage;

    const jsonPath = this.config.dbPath.replace(/\.db$/, '_json');
    this.jsonStorage = new JsonStorage(jsonPath, this.log, {
      chunkSize: this.config.chunkSize,
      pretty: this.config.prettyJson ?? false,
      dedupParts: this.config.archiveDedupParts,
      exactDedup: this.config.exactArchiveDedup,
      maxDedupIds: this.config.archiveDedupMaxIds,
    });
    return this.jsonStorage;
  }

  async init(): Promise<void> {
    // Before any storage: a second process must not load its own snapshot of
    // the metadata file and then race this one to overwrite it.
    this.writerLock = acquireWriterLock(this.config.dbPath, this.log);
    try {
      const jsonPath = this.config.dbPath.replace(/\.db$/, '_json');
      this.jsonStorage = new JsonStorage(jsonPath, this.log, {
        chunkSize: this.config.chunkSize,
        pretty: this.config.prettyJson ?? false,
        dedupParts: this.config.archiveDedupParts,
        exactDedup: this.config.exactArchiveDedup,
        maxDedupIds: this.config.archiveDedupMaxIds,
      });
      await this.storage.init();
    } catch (error) {
      if (this.writerLock.release()) this.writerLock = null;
      throw error;
    }
  }

  async validateToken(): Promise<{ valid: boolean; user?: any; error?: string; errorKind?: string }> {
    return this.client.validateToken();
  }

  async listChannels(): Promise<Map<string, ChannelInfo[]>> {
    const channelsByGuild = await this.fetcher.listAccessibleChannels();
    const result = new Map<string, ChannelInfo[]>();

    for (const [guildId, channels] of channelsByGuild) {
      result.set(
        guildId,
        channels.map((ch) => ({
          channelId: ch.channelId,
          channelName: ch.channelName,
          guildId: ch.guildId,
          guildName: ch.guildName,
        })),
      );
    }

    return result;
  }

  private dataDir(): string {
    return dirname(this.config.dbPath.replace(/\.db$/, '_json'));
  }

  async extractChannel(
    channelInfo: ChannelInfo,
    options: {
      onProgress?: ProgressCallback;
      resume?: boolean;
      exportJsonl?: string | null;
      /** Fetch only messages newer than the last extraction. */
      incremental?: boolean;
      /** Explicit lower bound for an incremental fetch (overrides stored state). */
      since?: string | null;
      /** Never auto-promote to incremental; force plain resume/full behaviour. */
      full?: boolean;
    } = {},
  ): Promise<ExtractResult> {
    const {
      onProgress,
      resume = true,
      exportJsonl,
      incremental = false,
      since = null,
      full = false,
    } = options;
    const { channelId, channelName, guildId, guildName } = channelInfo;

    const existing = this.activeExports.get(channelId);
    if (existing && !existing.aborted) {
      return this.failure(channelInfo, 'Already extracting this channel', 'client', false);
    }

    // Preflight: refuse to start (rather than fail midway) when there is
    // essentially no room left, and warn early on low space.
    const freeDiskBytes = getFreeDiskBytes(this.dataDir());
    if (freeDiskBytes !== null && freeDiskBytes < MIN_FREE_DISK_BYTES) {
      return this.failure(
        channelInfo,
        `Not enough free disk space: ${formatBytes(freeDiskBytes)} available, ` +
          `need at least ${formatBytes(MIN_FREE_DISK_BYTES)}`,
        'disk_full',
        false,
      );
    }
    if (freeDiskBytes !== null && freeDiskBytes < WARN_FREE_DISK_BYTES) {
      this.log.warn(`Low disk space: ${formatBytes(freeDiskBytes)} free`);
    }

    const progress = await this.storage.getOrCreateProgress(channelId, guildId, channelName);
    const existingArchive = this.getJsonStorage().loadArchive(channelId) ?? undefined;
    const alreadyDone = progress.status === 'done' && !!existingArchive?.completedAt;

    // Auto-incremental: resuming an already-finished channel is a no-op, so a
    // plain re-extract is far more useful as a cheap catch-up for messages
    // posted since the last run. Only do this when a baseline exists and the
    // caller did not override (via `--full`/`AUTO_INCREMENTAL=false`) or ask
    // for something explicit.
    const autoIncremental =
      !full &&
      !incremental &&
      (this.config.autoIncremental ?? true) &&
      alreadyDone &&
      !!progress.newest_message_id;
    const wantIncremental = incremental || autoIncremental;

    if (alreadyDone && !resume && !wantIncremental) {
      this.log.info(
        `Channel ${channelId} already extracted (${progress.total_extracted} messages), skipping`,
      );
      return {
        channelId, channelName, guildId, guildName,
        success: true, messagesExtracted: 0, duplicatesSkipped: 0,
        jsonParts: existingArchive.totalParts, duration: 0,
      };
    }

    if (autoIncremental) {
      this.log.info(
        `Channel ${channelName} is already extracted; catching up on messages newer than ` +
          `${progress.newest_message_id}`,
      );
    }

    const controller = new AbortController();
    const abortState: AbortState = {
      aborted: false,
      signal: controller.signal,
      abort: () => controller.abort(),
    };
    this.activeExports.set(channelId, abortState);

    await this.storage.updateProgress(channelId, { status: 'extracting', error_message: null });

    const startTime = Date.now();
    const configuredParallelism = Math.max(1, Math.min(this.config.parallelism || 6, 12));
    const pageDelayMs = this.config.pageDelayMs ?? 100;
    const retries = this.config.maxRetries ?? 8;

    // Flush when the buffer can fill roughly one chunk, so sustained runs
    // produce full-size parts instead of one tiny part per flush.
    const chunkSize = Math.max(1000, Math.min(this.config.chunkSize || 50000, 200_000));
    const flushMessageThreshold = Math.min(chunkSize, MAX_FLUSH_MESSAGES);
    const backpressureThreshold = flushMessageThreshold * 2;

    const savedState = resume ? parseResumeState(progress.resume_state) : null;

    // ---- Window selection -------------------------------------------------
    // 1. Incremental catch-up: one cursor covering only messages newer than
    //    the baseline, so a routine update is cheap.
    // 2. Resume: reuse the persisted windows exactly (boundaries do not drift).
    // 3. Fresh run: optionally place boundaries by observed message density.
    let segments: TimeSegment[];
    let liveResumeState: ChannelResumeState;

    const baseline = wantIncremental ? (since ?? progress.newest_message_id ?? null) : null;
    const incrementalDecision = wantIncremental
      ? decideIncrementalWindow(baseline, savedState, alreadyDone)
      : null;

    if (incrementalDecision?.useIncrementalWindow && baseline) {
      if (incrementalDecision.reuseSavedWindow) {
        segments = [{ after: savedState!.segments[0].after, before: savedState!.segments[0].before }];
        liveResumeState = savedState!;
      } else {
        segments = [{ after: baseline, before: tsToSnowflake(Date.now() + 60_000) }];
        liveResumeState = createResumeState(segments, 1);
      }

      this.log.info(`Incremental catch-up for ${channelId} since message ${baseline}`);
    } else {
      if (incrementalDecision?.reason === 'no-baseline') {
        this.log.warn(
          `Incremental catch-up requested for ${channelName} but no previous extraction ` +
            `baseline was found; performing a full extraction instead.`,
        );
      } else if (incrementalDecision?.reason === 'pending-shards') {
        this.log.warn(
          `${channelName} has shards left pending from an interrupted run; ` +
            `resuming the full extraction instead of a catch-up so the unfinished ` +
            `windows are not skipped.`,
        );
      } else if (incrementalDecision?.reason === 'unknown-state') {
        this.log.warn(
          `The resume state for ${channelName} was missing or unreadable; ` +
            `performing a full extraction instead of a catch-up because the prior ` +
            `shard layout is unknown.`,
        );
      }

      let plan: BalancedSegments | null = null;
      if (
        !hasReusableWindows(savedState, configuredParallelism) &&
        (this.config.balanceShards ?? true) &&
        configuredParallelism > 1
      ) {
        plan = await this.computeBalancedSegments(
          channelId,
          configuredParallelism,
          abortState,
        );
      }

      const resolved = resolveSegments(
        channelId,
        configuredParallelism,
        savedState,
        plan?.segments ?? null,
        plan ? buildBalance(plan) : null,
      );
      segments = resolved.segments;
      liveResumeState = resolved.state;
    }

    const parallelism = segments.length;

    // `live` is mutated by the fetcher as pages arrive. `durable` is only ever
    // advanced after the messages it describes have been written to disk.
    let durableResumeState: ChannelResumeState = cloneResumeState(liveResumeState);
    let durableLastMessageId: string | null = progress.last_message_id;
    let durableNewestId: string | null = progress.newest_message_id ?? null;
    const baseExtracted = progress.total_extracted || 0;

    let sessionExtracted = 0;
    let duplicatesSkipped = 0;
    const dedup = new Set<string>();
    let buffer: ExportRow[] = [];
    let jsonlStream: WriteStream | null = null;
    // Grouped in an object: these are mutated from async callbacks, and a bare
    // `let` gets narrowed to its initializer by control-flow analysis.
    const jsonl = { rows: 0, error: null as Error | null };
    let flushChain: Promise<void> = Promise.resolve();
    let flushError: Error | null = null;

    /**
     * Append rows to the JSONL export in bounded sub-batches, awaiting each
     * write. Awaiting is what applies backpressure: a slow destination (or a
     * full disk) throttles the flush loop instead of letting Node's internal
     * write buffer grow without bound.
     *
     * The export is auxiliary - the JSON archive is the source of truth - so a
     * write failure is logged once and stops the export rather than failing
     * the extraction.
     */
    const appendJsonl = async (rows: ExportRow[]): Promise<void> => {
      if (!jsonlStream || jsonl.error || rows.length === 0) return;

      const stream = jsonlStream;
      for (let i = 0; i < rows.length; i += JSONL_BATCH_ROWS) {
        const payload =
          rows.slice(i, i + JSONL_BATCH_ROWS).map((row) => JSON.stringify(row)).join('\n') + '\n';

        const written = await new Promise<boolean>((resolve) => {
          try {
            stream.write(payload, (err) => {
              if (err && !jsonl.error) {
                jsonl.error = err;
                this.log.error(`JSONL export write failed: ${err.message}`);
              }
              resolve(!err);
            });
          } catch (error: any) {
            if (!jsonl.error) {
              jsonl.error = error instanceof Error ? error : new Error(String(error));
              this.log.error(`JSONL export write failed: ${jsonl.error.message}`);
            }
            resolve(false);
          }
        });

        if (!written) return;
      }

      jsonl.rows += rows.length;
    };

    /** Flush and close the export, waiting for the OS to accept every line. */
    const closeJsonl = async (): Promise<void> => {
      const stream = jsonlStream;
      jsonlStream = null;
      if (!stream) return;

      if (!stream.destroyed) {
        await new Promise<void>((resolve) => {
          let settled = false;
          const done = () => {
            if (!settled) {
              settled = true;
              resolve();
            }
          };
          stream.once('finish', done);
          stream.once('close', done);
          stream.end();
        });
      }
    };

    const flushBuffer = async (): Promise<void> => {
      if (buffer.length > 0) {
        // Snapshot synchronously, before draining: this captures exactly the
        // pages whose messages are in `buffer`. Anything fetched during the
        // async write stays in the next snapshot, so durability never runs
        // ahead of the data.
        const snapshot = cloneResumeState(liveResumeState);
        const toFlush = buffer;
        buffer = [];

        // Appends into the same archive the live capture writes to, and
        // continues part numbering across resumes/restarts. Messages the
        // archive already has (the crash window: written but not acknowledged
        // when the process died) are dropped rather than stored twice, so the
        // run's counters must come from the storage layer, not the batch size.
        const { written, skipped } = this.getJsonStorage().appendMessages(channelId, channelName, toFlush);

        // Written here (not as messages arrive) so the export is ordered, is
        // only ever ahead of nothing the archive already has, and shares the
        // flush path's backpressure.
        await appendJsonl(toFlush);

        sessionExtracted += written;
        duplicatesSkipped += skipped;
        durableLastMessageId = toFlush[toFlush.length - 1]?.id ?? durableLastMessageId;

        // `newest_message_id` is the incremental catch-up baseline, so it must
        // be the *maximum* id written - pages arrive in descending order, so
        // the last row of a batch is the oldest, not the newest.
        for (const row of toFlush) {
          if (durableNewestId === null || BigInt(row.id) > BigInt(durableNewestId)) {
            durableNewestId = row.id;
          }
        }

        durableResumeState = snapshot;
      } else {
        // Nothing buffered means the live cursors describe fully-written data.
        durableResumeState = cloneResumeState(liveResumeState);
      }

      await this.storage.updateProgress(channelId, {
        last_message_id: durableLastMessageId,
        newest_message_id: durableNewestId,
        total_extracted: baseExtracted + sessionExtracted,
        resume_state: serializeResumeState(durableResumeState),
      });
    };

    // Serialize flushes so concurrent writers cannot reuse a part number.
    const scheduleFlush = (): Promise<void> => {
      flushChain = flushChain
        .then(flushBuffer)
        .catch((e: any) => {
          flushError = e instanceof Error ? e : new Error(String(e));
          this.log.error(`Flush failed: ${flushError.message}`);
          if (isDiskFullError(flushError)) {
            // Stop the whole run rather than burning through the remaining
            // buffer in memory; resume state is already durable.
            this.abortAll();
          }
        });
      return flushChain;
    };

    const handleMessage = (row: ExportRow) => {
      if (dedup.has(row.id)) {
        duplicatesSkipped++;
        return;
      }
      if (dedup.size >= MAX_DEDUP_ENTRIES) {
        dedup.clear();
      }
      dedup.add(row.id);

      buffer.push(row);

      if (buffer.length >= flushMessageThreshold) {
        void scheduleFlush();
      }
    };

    // Backpressure: called by the fetcher after each page. Shards wait here
    // whenever the buffer has grown past the threshold, which keeps memory
    // flat even if disk flushes are temporarily slower than ingestion.
    const afterPage = async (): Promise<void> => {
      if (buffer.length >= backpressureThreshold) {
        await scheduleFlush();
      }
    };

    let fetchResult;
    try {
      if (exportJsonl) {
        const exportDir = dirname(exportJsonl);
        if (exportDir && exportDir !== '.' && !existsSync(exportDir)) {
          mkdirSync(exportDir, { recursive: true });
        }
        // Append: a catch-up adds only what is new, which is exactly what you
        // want for a running export. An unhandled 'error' event would take the
        // whole process down, so it is captured and the export degrades.
        jsonlStream = createWriteStream(exportJsonl, { flags: 'a' });
        jsonlStream.on('error', (error) => {
          if (!jsonl.error) {
            jsonl.error = error;
            this.log.error(`JSONL export failed: ${error.message}`);
          }
        });
      }

      this.log.info(`Resuming ${channelName}: ${describeResumeState(liveResumeState)}`);

      fetchResult = await this.fetcher.fetchChannel(channelId, abortState, (stats) => {
        onProgress?.(stats);
      }, {
        parallelism,
        segments,
        pageDelayMs,
        retries,
        resumeState: liveResumeState,
        onMessage: handleMessage,
        onFlush: scheduleFlush,
        flushIntervalMs: FLUSH_INTERVAL_MS,
        afterPage,
      });

      // Final flush drains everything; if it succeeded, live and durable match.
      await scheduleFlush();
      if (flushError) throw flushError;

      // Wait for the export to reach the OS before reporting success.
      await closeJsonl();

      const failedShards = fetchResult.segmentErrors.length;
      const complete = !fetchResult.aborted && failedShards === 0 && isResumeComplete(durableResumeState);

      if (complete) {
        this.getJsonStorage().completeArchive(channelId);
      }

      const cumulative = baseExtracted + sessionExtracted;

      // Release the in-progress part (and its duplicate guard) and persist the
      // newest id so a later incremental catch-up (or live session) knows
      // where to start. The resume state was persisted by the final flush
      // above, so nothing this run wrote can be re-fetched after this point.
      this.getJsonStorage().finalizeChannel(channelId);
      const archive = this.getJsonStorage().loadArchive(channelId);
      const duration = Date.now() - startTime;

      await this.storage.updateProgress(channelId, {
        status: complete ? 'done' : 'error',
        total_extracted: cumulative,
        last_message_id: durableLastMessageId,
        newest_message_id: durableNewestId ?? durableLastMessageId,
        resume_state: serializeResumeState(durableResumeState),
        error_message: complete
          ? null
          : fetchResult.aborted
            ? 'aborted by user'
            : `${failedShards} shard(s) did not complete`,
      });

      const jsonlNote = exportJsonl
        ? `, ${jsonl.rows} JSONL line(s)${jsonl.error ? ' (export failed)' : ''}`
        : '';

      this.log.info(
        `Extracted ${sessionExtracted} new messages from ${channelName} in ${formatDuration(duration)} ` +
        `(${archive?.totalParts || 0} JSON parts, ${describeResumeState(durableResumeState)}${jsonlNote}${this.describeDedup(channelId)})`,
      );

      this.activeExports.delete(channelId);

      return {
        channelId, channelName, guildId, guildName,
        success: complete,
        messagesExtracted: sessionExtracted,
        duplicatesSkipped,
        jsonParts: archive?.totalParts || 0,
        duration,
        error: complete ? undefined : fetchResult.aborted ? 'Aborted' : `${failedShards} shard(s) failed`,
        errorKind: complete ? undefined : fetchResult.aborted ? 'aborted' : 'shard_failure',
        retryable: !complete && !fetchResult.aborted,
        failedShards,
        usedFallback: fetchResult.usedFallback,
        freeDiskBytes,
        jsonlRows: exportJsonl ? jsonl.rows : undefined,
        jsonlError: jsonl.error ? jsonl.error.message : undefined,
      };
    } catch (error: any) {
      // Persist whatever progress is durable so the next run can resume.
      try {
        await scheduleFlush();
      } catch { /* already logged */ }

      const duration = Date.now() - startTime;
      const diskFull = isDiskFullError(error);
      const kind = diskFull ? 'disk_full' : (error?.kind as string | undefined);

      await this.storage.updateProgress(channelId, {
        status: 'error',
        total_extracted: baseExtracted + sessionExtracted,
        resume_state: serializeResumeState(durableResumeState),
        error_message: error?.message ?? String(error),
      });

      await closeJsonl();
      this.activeExports.delete(channelId);

      this.log.error(`Extraction failed for ${channelId}: ${error?.message ?? error}`);
      if (diskFull) {
        this.log.error('Disk is full. Free space, then rerun to resume from the last durable page.');
      }

      return {
        channelId, channelName, guildId, guildName,
        success: false,
        messagesExtracted: sessionExtracted,
        duplicatesSkipped,
        jsonParts: this.getJsonStorage().loadArchive(channelId)?.totalParts || 0,
        duration,
        error: error?.message ?? String(error),
        errorKind: kind ?? 'unknown',
        retryable: !diskFull && kind !== 'auth' && kind !== 'not_found' && kind !== 'aborted',
        freeDiskBytes,
        jsonlRows: exportJsonl ? jsonl.rows : undefined,
        jsonlError: jsonl.error ? jsonl.error.message : undefined,
      };
    }
  }

  /**
   * Place shard boundaries by observed message density so parallel shards
   * finish at roughly the same time instead of waiting on one busy window.
   *
   * Probing is best-effort and never throws: on failure it returns null and the
   * caller computes plain equal-time windows. The estimate that produced the
   * plan is returned too, so it can be persisted and shown in `status`.
   */
  private async computeBalancedSegments(
    channelId: string,
    parallelism: number,
    abortState: AbortState,
  ): Promise<BalancedSegments | null> {
    try {
      const probes = await this.fetcher.probeDensity(channelId, parallelism, abortState, {
        count: this.config.densityProbes,
      });
      if (probes.length === 0) {
        this.log.info(`No density samples for ${channelId}; using equal-time shards`);
        return null;
      }

      const balanced = balanceSegmentsByDensity(channelId, parallelism, probes, Date.now());

      if (balanced.strategy === 'density') {
        const imbalance = balanced.imbalance;
        this.log.info(
          `Balanced ${channelId} into ${balanced.segments.length} shards from ${balanced.probes} probes` +
            (imbalance !== null ? ` (imbalance ${(imbalance * 100).toFixed(0)}%)` : ''),
        );
      } else {
        this.log.info(`Density samples for ${channelId} were degenerate; using equal-time shards`);
      }

      return balanced;
    } catch (error: any) {
      if (abortState.aborted) return null;
      this.log.warn(
        `Density balancing failed for ${channelId} (${error?.message ?? error}); using equal-time shards`,
      );
      return null;
    }
  }

  private failure(
    channelInfo: ChannelInfo,
    message: string,
    errorKind: string,
    retryable: boolean,
  ): ExtractResult {
    return {
      channelId: channelInfo.channelId,
      channelName: channelInfo.channelName,
      guildId: channelInfo.guildId,
      guildName: channelInfo.guildName,
      success: false,
      messagesExtracted: 0,
      duplicatesSkipped: 0,
      jsonParts: 0,
      duration: 0,
      error: message,
      errorKind,
      retryable,
    };
  }

  async extractAll(
    channels: ChannelInfo[],
    options: {
      concurrency?: number;
      onChannelProgress?: ProgressCallback;
      onChannelComplete?: (result: ExtractResult) => void;
      /**
       * JSONL export target. A plain string is used for every channel (fine
       * for a single-channel run); pass a function to give concurrent channels
       * their own file, which is what the CLI does.
       */
      exportJsonl?: string | null | ((channel: ChannelInfo) => string | null);
      /** Extra whole-channel attempts for transient failures. */
      channelRetries?: number;
      /** Fetch only messages newer than the last extraction. */
      incremental?: boolean;
      /** Explicit lower bound for an incremental fetch. */
      since?: string | null;
      /** Disable auto-incremental promotion for already-finished channels. */
      full?: boolean;
    } = {},
  ): Promise<ExtractResult[]> {
    const {
      concurrency = 1,
      onChannelProgress,
      onChannelComplete,
      exportJsonl,
      channelRetries = 1,
      incremental = false,
      since = null,
      full = false,
    } = options;

    if (channels.length === 0) {
      this.log.warn('No channels provided');
      return [];
    }

    const limit = Math.max(1, Math.min(concurrency, channels.length));
    this.log.info(`Starting extraction for ${channels.length} channel(s) with concurrency ${limit}`);

    const queue = [...channels];
    const results: ExtractResult[] = [];

    const exportFor = (channel: ChannelInfo): string | null =>
      typeof exportJsonl === 'function' ? exportJsonl(channel) : (exportJsonl ?? null);

    const runOne = async (channel: ChannelInfo): Promise<ExtractResult> => {
      let result = await this.extractChannel(channel, {
        onProgress: onChannelProgress,
        resume: true,
        exportJsonl: exportFor(channel),
        incremental,
        since,
        full,
      });

      // One graceful retry for transient failures. Auth/not-found/abort/disk
      // are not retried - retrying them would just be a debugging loop.
      if (!result.success && result.retryable && channelRetries > 0 && !this.stopRequested) {
        this.log.warn(
          `Retrying channel ${result.channelName} once after ${result.errorKind} failure`,
        );
        await sleep(CHANNEL_RETRY_DELAY_MS);
        result = await this.extractChannel(channel, {
          onProgress: onChannelProgress,
          resume: true,
          exportJsonl: exportFor(channel),
          incremental,
          since,
          full,
        });
      }

      if (onChannelComplete) onChannelComplete(result);
      return result;
    };

    const workers = Array.from({ length: limit }, async () => {
      while (!this.stopRequested) {
        const channel = queue.shift();
        if (!channel) return;
        const result = await runOne(channel);
        results.push(result);
      }
    });

    await Promise.all(workers);

    const successful = results.filter((r) => r.success).length;
    const failed = results.length - successful;
    const totalExtracted = results.reduce((sum, r) => sum + r.messagesExtracted, 0);

    this.log.info(
      `Extraction complete: ${successful} succeeded, ${failed} failed, ${totalExtracted} messages extracted`,
    );

    return results;
  }

  /**
   * Build the live message source (Discord Gateway) for this config. Bot
   * tokens send intents; user tokens must not.
   */
  createLiveSource(): DiscordGateway {
    const isBot =
      this.config.isBot ?? (!!this.config.botToken && this.config.botToken === this.config.userToken);
    return new DiscordGateway({
      token: this.config.userToken!,
      isBot,
      log: this.log,
    });
  }

  /**
   * Build a live capture that appends into the same archive as bulk
   * extraction. Requires `init()` to have run (storage must be ready).
   */
  createLiveCapture(
    source: LiveMessageSource,
    options: Omit<LiveCaptureOptions, 'storage' | 'jsonStorage' | 'source'> = {},
  ): LiveCapture {
    const capture = new LiveCapture({
      ...options,
      storage: this.storage,
      jsonStorage: this.getJsonStorage(),
      source,
      log: this.log,
      batchMessages: options.batchMessages ?? this.config.liveBatchMessages,
      maxBufferPerChannel: options.maxBufferPerChannel ?? this.config.liveMaxBuffer,
    });
    this.liveCaptures.add(capture);
    return capture;
  }

  /**
   * Channels this process is actively writing to right now: an in-flight
   * extraction or an open live capture. The catch-up scheduler uses this to
   * leave them alone, so two writers never fight over one archive (and a
   * catch-up never re-fetches what live capture already appended).
   */
  activeChannelIds(): string[] {
    const ids = new Set<string>(this.activeExports.keys());
    for (const capture of this.liveCaptures) {
      for (const channelId of capture.getChannelIds()) ids.add(channelId);
    }
    return [...ids];
  }

  loadChannelMessages(channelId: string): ExportRow[] {
    return this.getJsonStorage().loadAllMessages(channelId);
  }

  exportChannelToJson(channelId: string, outputPath: string): number {
    return this.getJsonStorage().exportToSingleJson(channelId, outputPath);
  }

  exportChannelToJsonl(channelId: string, outputPath: string): number {
    return this.getJsonStorage().exportToJsonl(channelId, outputPath);
  }

  /**
   * What indexing a channel's archive costs, read from the manifest alone - so
   * `status` can report it without reading or building anything.
   */
  estimateDedupCost(channelId: string): DedupCostEstimate | null {
    return this.getJsonStorage().estimateDedupCost(channelId);
  }

  /** How the archive guard is configured. */
  getDedupConfig(): { exact: boolean; parts: number; maxIds: number } {
    return this.getJsonStorage().dedupConfig;
  }

  /**
   * One-line description of the duplicate index for a finished run, so a
   * truncated index is visible in the run's own output and not only in
   * `status`.
   */
  private describeDedup(channelId: string): string {
    const stats = this.getJsonStorage().getDedupStats(channelId);
    if (!stats) return '';

    const ids = stats.ids.toLocaleString('en-US');
    const dropped = stats.skipped > 0 ? `, ${stats.skipped.toLocaleString('en-US')} duplicate(s) dropped` : '';
    const coverage = stats.truncated
      ? `from ${stats.parts}/${stats.partsAvailable} part(s), no longer exact`
      : 'covering the archive';

    return `, dedup: ${ids} id(s) ${coverage}${dropped}`;
  }

  getChannelStats(channelId: string): {
    messages: number;
    jsonParts: number;
    sizeBytes: number;
    isComplete: boolean;
  } | null {
    const archive = this.getJsonStorage().loadArchive(channelId);
    const stats = this.getJsonStorage().getStorageStats(channelId);
    if (!archive) return null;
    return {
      messages: archive.totalMessages,
      jsonParts: archive.totalParts,
      sizeBytes: stats?.totalSizeBytes || 0,
      isComplete: archive.completedAt !== null,
    };
  }

  abortChannel(channelId: string): boolean {
    const state = this.activeExports.get(channelId);
    if (state && !state.aborted) {
      state.aborted = true;
      state.abort?.();
      this.log.info(`Aborted extraction for channel ${channelId}`);
      return true;
    }
    return false;
  }

  abortAll(): void {
    this.stopRequested = true;
    for (const [channelId, state] of this.activeExports.entries()) {
      if (!state.aborted) {
        state.aborted = true;
        state.abort?.();
        this.log.info(`Aborted extraction for channel ${channelId}`);
      }
    }
  }

  /** Every channel the metadata DB knows about (used by the scheduler). */
  async knownChannels(): Promise<ChannelProgress[]> {
    return this.storage.getAllProgress();
  }

  async getStatus(): Promise<{ channelId: string; status: string; total: number }[]> {
    const progress = await this.storage.getAllProgress();
    return progress.map((p) => ({
      channelId: p.channel_id,
      status: p.status,
      total: p.total_extracted,
    }));
  }

  async getProgressDetail(channelId: string) {
    return this.storage.getProgress(channelId);
  }

  async resetChannel(channelId: string): Promise<void> {
    await this.storage.updateProgress(channelId, {
      status: 'pending',
      last_message_id: null,
      last_extracted_ts: null,
      total_extracted: 0,
      error_message: null,
      resume_state: null,
      // The catch-up baseline has to go too. Leaving it behind would make the
      // next `extract`/`watch` treat this as a routine catch-up from a message
      // that no longer has an archive under it - silently skipping everything
      // the reset just deleted instead of re-extracting it.
      newest_message_id: null,
    });
    this.getJsonStorage().deleteAllChunks(channelId);
    this.log.info(`Reset all data for channel ${channelId}`);
  }

  /**
   * Stop admitting work and release the data directory without draining all owned work.
   * A flush already in flight when this is called can complete after the lock is released.
   */
  close(): void {
    try {
      this.abortAll();
      for (const capture of this.liveCaptures) capture.stopAdmitting();
      this.liveCaptures.clear();
      this.storage.close();
    } finally {
      // A cleanup failure must not leave the data directory locked until manual removal.
      if (this.writerLock?.release()) this.writerLock = null;
    }
  }
}
