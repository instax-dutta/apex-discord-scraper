// Apex Discord Scraper - User Token Fetcher
//
// Paginates a channel using time shards. Unlike the old implementation, this
// version:
//   - does NOT retry requests itself - `UserTokenClient` owns retries
//   - isolates shard failures instead of letting one kill the whole channel
//   - retries failed shards sequentially, then falls back to a single sweep
//   - persists a per-shard cursor so resume is exact
//   - has a no-progress guard so a stuck pagination cursor cannot loop forever

import type {
  DiscordMessage,
  ExportRow,
  TimeSegment,
  ProgressCallback,
  AbortState,
  FetchResult,
  SegmentResult,
  SegmentErrorInfo,
  ChannelResumeState,
} from './types.js';
import { UserTokenClient } from './userTokenClient.js';
import {
  calculateTimeSegments,
  sleep,
  calculateRate,
  snowflakeToTs,
  tsToSnowflake,
  Logger,
} from './utils.js';
import { resolveProbeCount, type DensityProbe } from './segments.js';
import { ExtractorError, isAbortError } from './errors.js';
import {
  createResumeState,
  getSegmentState,
  markSegmentDone,
  setSegmentCursor,
  isResumeComplete,
} from './resumable.js';

const PAGE_SIZE = 100; // Discord API max
/** Only probe channels with at least this much history; below it probing costs more than it saves. */
const PROBE_MIN_SPAN_MS = 24 * 60 * 60 * 1000;

/** True for a raw AbortError or an ExtractorError already classified as abort. */
function isAborted(error: unknown): boolean {
  return isAbortError(error) || (error instanceof ExtractorError && error.kind === 'aborted');
}

/**
 * The span a last-resort sequential sweep has to cover: from the earliest
 * pending shard's lower bound up to the highest cursor the pending shards
 * reached.
 *
 * Bounding the sweep matters. An unbounded sweep would start at the newest
 * message and walk the *whole* channel, which turns a failed incremental
 * catch-up into a full re-download (and a full duplicate of an archive that is
 * already on disk). Callers only use this when no shard was complete before
 * the run, so the span never contains finished work either.
 */
function sweepWindow(
  segments: TimeSegment[],
  state: ChannelResumeState,
  indexes: number[],
): { after: string; before: string } {
  let after = BigInt(segments[indexes[0]].after);
  let before = BigInt(getSegmentState(state, indexes[0]).cursor ?? segments[indexes[0]].before);

  for (const index of indexes) {
    const lower = BigInt(segments[index].after);
    if (lower < after) after = lower;

    // A shard that already fetched part of its window continues from its
    // cursor, so the sweep must not re-fetch above it.
    const upper = BigInt(getSegmentState(state, index).cursor ?? segments[index].before);
    if (upper > before) before = upper;
  }

  return { after: after.toString(), before: before.toString() };
}

/**
 * Convert a raw Discord message (REST or Gateway payload) into the export row
 * shape. Standalone so live capture can reuse it without a fetcher instance.
 */
export function toExportRow(m: DiscordMessage): ExportRow {
  const attachmentUrls: string[] = [];
  const imageUrls: string[] = [];
  const attachmentsDetailed: ExportRow['attachmentsDetailed'] = [];

  if (m.attachments) {
    for (const att of m.attachments) {
      attachmentUrls.push(att.url);
      if (att.content_type?.startsWith('image/')) {
        imageUrls.push(att.url);
      }
      attachmentsDetailed.push({
        url: att.url,
        proxyUrl: att.proxy_url,
        filename: att.filename,
        contentType: att.content_type,
        width: att.width,
        height: att.height,
      });
    }
  }

  if (m.embeds) {
    for (const embed of m.embeds) {
      if (embed.image) imageUrls.push(embed.image.url);
      if (embed.thumbnail) imageUrls.push(embed.thumbnail.url);
    }
  }

  const authorId = m.author?.id || 'unknown';
  const authorUsername = m.author?.username || 'unknown';
  const authorDiscriminator = m.author?.discriminator || '0';

  let replyToAuthor: string | null = null;
  if (m.referenced_message?.author) {
    replyToAuthor = m.referenced_message.author.username || null;
  }

  return {
    id: m.id,
    author: `${authorUsername}#${authorDiscriminator} (${authorId})`,
    content: m.content || '',
    timestamp: m.timestamp,
    replyTo: m.message_reference?.message_id || null,
    replyToAuthor,
    attachments: m.attachments?.length || 0,
    attachmentUrls,
    imageUrls,
    attachmentsDetailed,
    threadId: m.thread?.id || null,
  };
}

export interface FetchChannelOptions {
  /** Number of time shards. 1 = single sequential sweep. */
  parallelism?: number;
  /** Pause between pages within a shard (ms). */
  pageDelayMs?: number;
  /** Retries passed to the client, per request. */
  retries?: number;
  /** Exact shard windows to use. Derived from resume state when resuming. */
  segments?: TimeSegment[];
  /** Prior per-shard state to resume from. */
  resumeState?: ChannelResumeState | null;
  /** Called for every converted message. */
  onMessage?: (row: ExportRow) => void;
  /** Periodic flush hook (used to bound memory + persist progress). */
  onFlush?: () => Promise<void>;
  flushIntervalMs?: number;
  /**
   * Called after every page. Used for backpressure: the extractor flushes here
   * when its buffer is large, so a multi-day run cannot grow without bound.
   */
  afterPage?: () => Promise<void>;
}

export class UserTokenFetcher {
  private readonly log: Logger;

  constructor(private readonly client: UserTokenClient, log?: Logger) {
    this.log = log || new Logger();
  }

  toExportRow(m: DiscordMessage): ExportRow {
    return toExportRow(m);
  }

  /**
   * Fetch one time shard, following `before` back to the shard's lower bound.
   * Client-level retries handle transient failures; anything that survives
   * them is returned as a segment error so the caller can recover.
   */
  private async fetchSegment(
    channelId: string,
    index: number,
    segment: TimeSegment,
    state: ChannelResumeState,
    signal: AbortState,
    onMessage: (row: ExportRow) => void,
    options: { pageDelayMs: number; retries: number; afterPage?: () => Promise<void> },
  ): Promise<SegmentResult> {
    const seg = getSegmentState(state, index);
    if (seg.done) return { index, fetched: 0, done: true };

    const afterBound = BigInt(segment.after);
    const beforeBound = BigInt(segment.before);

    let before = seg.cursor ?? segment.before;
    let lastOldest: string | null = null;
    let fetched = 0;
    let pages = 0;

    try {
      while (!signal.aborted) {
        const messages = await this.client.fetchMessages(channelId, {
          before,
          limit: PAGE_SIZE,
          signal: signal.signal,
          retries: options.retries,
        });

        if (!Array.isArray(messages)) {
          throw new ExtractorError('Malformed response: expected an array of messages', {
            kind: 'server',
            retryable: true,
          });
        }

        if (messages.length === 0) {
          markSegmentDone(state, index);
          return { index, fetched, done: true };
        }

        for (const msg of messages) {
          const id = BigInt(msg.id);
          // Strict on both ends: adjacent shards share a boundary value, and an
          // incremental segment's lower bound is a real message id we already
          // have, so it must not be re-fetched.
          if (id > afterBound && id < beforeBound) {
            onMessage(toExportRow(msg));
            fetched++;
          }
        }

        const oldest = messages[messages.length - 1]?.id;
        if (!oldest) {
          throw new ExtractorError('Malformed response: message without an id', {
            kind: 'server',
            retryable: true,
          });
        }

        // No-progress guard: if the cursor does not move we would spin forever.
        if (pages > 0 && lastOldest !== null && BigInt(oldest) >= BigInt(lastOldest)) {
          throw new ExtractorError('Pagination cursor stalled (Discord returned the same page)', {
            kind: 'unknown',
            retryable: false,
          });
        }
        lastOldest = oldest;
        setSegmentCursor(state, index, oldest);

        if (BigInt(oldest) <= afterBound || messages.length < PAGE_SIZE) {
          markSegmentDone(state, index);
          return { index, fetched, done: true };
        }

        before = oldest;
        pages++;
        if (options.pageDelayMs > 0) await sleep(options.pageDelayMs);
        if (options.afterPage) await options.afterPage();
      }

      return { index, fetched, done: false };
    } catch (error) {
      if (isAborted(error)) {
        return { index, fetched, done: false };
      }
      const err = error instanceof Error ? error : new Error(String(error));
      setSegmentCursor(state, index, lastOldest ?? before);
      this.log.warn(`Shard ${index} failed: ${err.message}`);
      return { index, fetched, done: false, error: err };
    }
  }

  /**
   * Last-resort fallback: walk a bounded span with a single cursor instead of
   * sharding it. Used only when every shard failed and nothing was fetched, so
   * `window` is the union of the pending shard windows and the sweep can never
   * touch history that is already on disk.
   */
  private async fetchSequential(
    channelId: string,
    signal: AbortState,
    onMessage: (row: ExportRow) => void,
    options: { pageDelayMs: number; retries: number; afterPage?: () => Promise<void> },
    window: { after: string; before: string },
  ): Promise<{ fetched: number; completed: boolean; error?: Error }> {
    const afterBound = BigInt(window.after);
    let before: string | undefined = window.before;
    let fetched = 0;

    try {
      while (!signal.aborted) {
        const messages = await this.client.fetchMessages(channelId, {
          before,
          limit: PAGE_SIZE,
          signal: signal.signal,
          retries: options.retries,
        });

        if (!Array.isArray(messages) || messages.length === 0) {
          return { fetched, completed: true };
        }

        for (const msg of messages) {
          // Same exclusive lower bound the shard fetcher applies, so a sweep
          // cannot re-append a message the archive already has.
          if (BigInt(msg.id) <= afterBound) continue;
          onMessage(toExportRow(msg));
          fetched++;
        }

        if (messages.length < PAGE_SIZE) {
          return { fetched, completed: true };
        }

        const oldest = messages[messages.length - 1].id;
        if (BigInt(oldest) <= afterBound) {
          return { fetched, completed: true };
        }

        before = oldest;
        if (options.pageDelayMs > 0) await sleep(options.pageDelayMs);
        if (options.afterPage) await options.afterPage();
      }
      return { fetched, completed: false };
    } catch (error) {
      if (isAborted(error)) return { fetched, completed: false };
      return {
        fetched,
        completed: false,
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
  }

  /**
   * Sample the channel's message rate at evenly spaced timestamps so shard
   * boundaries can be placed by density rather than by time. Costs one request
   * per probe and never throws: a failed probe simply reduces the sample set.
   */
  async probeDensity(
    channelId: string,
    parallelism: number,
    signal: AbortState,
    options: { count?: number | null } = {},
  ): Promise<DensityProbe[]> {
    const channelTs = snowflakeToTs(channelId);
    const nowTs = Date.now();
    const span = nowTs - channelTs;

    if (span < PROBE_MIN_SPAN_MS) return [];

    const count = resolveProbeCount(parallelism, options.count);
    const probes: DensityProbe[] = [];

    for (let k = 1; k <= count; k++) {
      if (signal.aborted) break;

      const timestampMs = channelTs + Math.floor((span * k) / (count + 1));

      try {
        const messages = await this.client.fetchMessages(channelId, {
          before: tsToSnowflake(timestampMs),
          limit: PAGE_SIZE,
          signal: signal.signal,
          retries: 1, // probes are best-effort; do not burn the retry budget
        });

        if (!Array.isArray(messages) || messages.length === 0) {
          probes.push({ timestampMs, ratePerMs: 0, sampleCount: 0 });
          continue;
        }

        // The page holds every message between the oldest one and the probe
        // timestamp, so that span gives a local rate even for sparse regions.
        let oldestTs = Number.POSITIVE_INFINITY;
        for (const m of messages) {
          const ts = Date.parse(m.timestamp);
          if (Number.isFinite(ts) && ts < oldestTs) oldestTs = ts;
        }
        const windowMs = Number.isFinite(oldestTs) && timestampMs > oldestTs ? timestampMs - oldestTs : 1000;

        probes.push({
          timestampMs,
          ratePerMs: messages.length / Math.max(1, windowMs),
          sampleCount: messages.length,
        });
      } catch (error) {
        if (isAborted(error)) break;
        this.log.debug(
          `Density probe at ${timestampMs} failed: ${error instanceof Error ? error.message : error}`,
        );
      }
    }

    return probes;
  }

  async fetchChannel(
    channelId: string,
    signal: AbortState,
    onProgress: ProgressCallback,
    options: FetchChannelOptions = {},
  ): Promise<FetchResult> {
    const {
      parallelism: requestedParallelism = 6,
      pageDelayMs = 100,
      retries = 8,
      resumeState: savedState = null,
      onMessage: externalOnMessage,
      onFlush,
      flushIntervalMs = 5000,
      afterPage,
    } = options;

    const parallelism = Math.max(1, Math.min(Math.floor(requestedParallelism), 12));
    // Prefer the extractor-provided windows (which match the resume state)
    // over recomputing them, so resume never shifts shard boundaries.
    const segments =
      options.segments && options.segments.length > 0
        ? options.segments
        : calculateTimeSegments(channelId, parallelism);
    const state =
      savedState && savedState.segments.length === segments.length
        ? savedState
        : createResumeState(segments, parallelism);

    if (isResumeComplete(state)) {
      this.log.info(`Channel ${channelId} already fully extracted per resume state`);
      return { fetched: 0, aborted: signal.aborted, resumeState: state, segmentErrors: [], usedFallback: false };
    }

    let fetched = 0;
    let lastUpdate = Date.now();
    let lastCount = 0;
    let lastFlush = Date.now();
    let flushInFlight = false;
    let usedFallback = false;
    const errorsByIndex = new Map<number, SegmentErrorInfo>();

    const handleMessage = (row: ExportRow) => {
      fetched++;
      externalOnMessage?.(row);

      const now = Date.now();
      if (now - lastUpdate >= 1000) {
        onProgress({
          channelId,
          total: fetched,
          segment: 0,
          segments: segments.length,
          rate: calculateRate(fetched - lastCount, now - lastUpdate),
        });
        lastUpdate = now;
        lastCount = fetched;
      }

      if (onFlush && !flushInFlight && now - lastFlush >= flushIntervalMs) {
        flushInFlight = true;
        onFlush()
          .catch((e) => this.log.warn(`Periodic flush failed: ${e?.message ?? e}`))
          .finally(() => {
            lastFlush = Date.now();
            flushInFlight = false;
          });
      }
    };

    const pending = segments
      .map((segment, index) => ({ segment, index }))
      .filter(({ index }) => !getSegmentState(state, index).done);

    this.log.info(
      `Channel ${channelId}: ${pending.length}/${segments.length} shards to fetch ` +
      `(parallelism ${parallelism}, ${pageDelayMs}ms page delay)`,
    );

    // ---- Phase 1: shards in parallel. allSettled so one failure cannot
    // discard the shards that succeeded.
    const settled = await Promise.allSettled(
      pending.map(({ segment, index }) =>
        this.fetchSegment(channelId, index, segment, state, signal, handleMessage, {
          pageDelayMs,
          retries,
          afterPage,
        }),
      ),
    );

    let failedIndexes: number[] = [];
    settled.forEach((outcome, i) => {
      const index = pending[i].index;
      if (outcome.status === 'fulfilled') {
        if (outcome.value.error) {
          failedIndexes.push(index);
          errorsByIndex.set(index, this.toSegmentError(index, outcome.value.error));
        } else if (outcome.value.done) {
          errorsByIndex.delete(index);
        }
      } else {
        failedIndexes.push(index);
        errorsByIndex.set(index, this.toSegmentError(index, outcome.reason));
      }
    });

    // ---- Phase 2: retry failed shards one at a time. Serial retries reduce
    // pressure, which is usually why they failed in the first place.
    if (failedIndexes.length > 0 && !signal.aborted) {
      usedFallback = true;
      this.log.warn(
        `Retrying ${failedIndexes.length} failed shard(s) sequentially: [${failedIndexes.join(', ')}]`,
      );

      for (const index of failedIndexes) {
        if (signal.aborted) break;
        const result = await this.fetchSegment(
          channelId,
          index,
          segments[index],
          state,
          signal,
          handleMessage,
          { pageDelayMs, retries, afterPage },
        );
        if (result.error) {
          errorsByIndex.set(index, this.toSegmentError(index, result.error));
        } else if (result.done) {
          errorsByIndex.delete(index);
        }
      }
      failedIndexes = [...errorsByIndex.keys()];
    }

    // ---- Phase 3: if every shard failed and nothing came through, abandon
    // sharding entirely and sweep the pending windows linearly.
    //
    // Two guards keep this from damaging an archive that already exists:
    //   - the sweep is bounded to the pending shards' own windows (see
    //     `sweepWindow`), so a failed incremental catch-up re-fetches the
    //     catch-up window rather than the whole channel;
    //   - it is skipped when any shard was already complete, because then the
    //     pending windows are disjoint and one linear pass would traverse - and
    //     duplicate - the finished shards in between.
    const hadCompletedShards = state.segments.some((s) => s.done);
    if (
      !signal.aborted &&
      fetched === 0 &&
      pending.length > 0 &&
      errorsByIndex.size >= pending.length &&
      !isResumeComplete(state) &&
      !hadCompletedShards
    ) {
      const window = sweepWindow(segments, state, pending.map((p) => p.index));
      this.log.warn(
        `All shards failed - falling back to a single sequential sweep of ` +
          `(${window.after}, ${window.before})`,
      );
      usedFallback = true;
      const sweep = await this.fetchSequential(
        channelId,
        signal,
        handleMessage,
        { pageDelayMs, retries, afterPage },
        window,
      );

      if (sweep.error) {
        errorsByIndex.set(-1, this.toSegmentError(-1, sweep.error));
      } else if (sweep.completed) {
        for (const { index } of pending) markSegmentDone(state, index);
        errorsByIndex.clear();
      }
    }

    if (onFlush) {
      try {
        await onFlush();
      } catch (e: any) {
        this.log.warn(`Final flush failed: ${e?.message ?? e}`);
      }
    }

    return {
      fetched,
      aborted: signal.aborted,
      resumeState: state,
      segmentErrors: [...errorsByIndex.values()],
      usedFallback,
    };
  }

  private toSegmentError(index: number, error: unknown): SegmentErrorInfo {
    const err = error instanceof Error ? error : new Error(String(error));
    return {
      index,
      message: err.message,
      kind: error instanceof ExtractorError ? error.kind : 'unknown',
    };
  }

  async listAccessibleChannels(): Promise<Map<string, { guildId: string; guildName: string; channelId: string; channelName: string; type: number }[]>> {
    const user = await this.client.getCurrentUser();
    this.log.info(`Logged in as ${user.username}`);

    // `/users/@me` does not include guilds; fetch them explicitly.
    const guilds = await this.client.getCurrentUserGuilds();
    const channelsByGuild = new Map<string, { guildId: string; guildName: string; channelId: string; channelName: string; type: number }[]>();
    let failedGuilds = 0;

    for (const guild of guilds) {
      try {
        const guildChannels = (await this.client.getGuildChannels(guild.id)) as any[];
        const textChannels = guildChannels
          .filter((ch: any) => ch.type === 0 || ch.type === 5 || ch.type === 15)
          .map((ch: any) => ({
            guildId: guild.id,
            guildName: guild.name,
            channelId: ch.id,
            channelName: ch.name,
            type: ch.type,
          }));

        if (textChannels.length > 0) {
          channelsByGuild.set(guild.id, textChannels);
          this.log.info(`Guild ${guild.name}: ${textChannels.length} text channels`);
        }
      } catch (e: any) {
        failedGuilds++;
        this.log.warn(`Could not fetch channels for guild ${guild.name}: ${e?.message ?? e}`);
      }
    }

    if (failedGuilds > 0) {
      this.log.warn(`${failedGuilds}/${guilds.length} guild(s) could not be listed`);
    }

    return channelsByGuild;
  }
}
