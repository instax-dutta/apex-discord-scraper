// Apex Discord Scraper - Recurring catch-up scheduler
//
// Runs an incremental catch-up for every already-extracted channel on a
// cadence, so an archive stays current without a manual re-run. It is
// deliberately conservative about what it will touch unattended:
//
//   - channels never extracted are SKIPPED (a full extraction is an expensive,
//     long-running action that should be an explicit decision), unless
//     `includeMissingBaseline` is set
//   - channels currently being captured live are skipped, so the scheduler
//     never fights the Gateway listener over the same archive
//   - channels mid-extraction are skipped
//
// A cycle is sequential-but-concurrent: channels are handed to `extractAll`
// with a bounded worker pool, and one channel failing never stops the rest.

import type { ChannelInfo } from './userTokenExtractor.js';
import type { UserTokenExtractor } from './userTokenExtractor.js';
import type { ChannelProgress } from './types.js';
import { Logger } from './utils.js';

/** Default cadence between cycles. */
export const DEFAULT_INTERVAL_MS = 15 * 60 * 1000;
/** Upper bound on random jitter added to each interval. */
const MAX_JITTER_MS = 60_000;

export interface CatchUpPlanItem {
  channelId: string;
  channelName: string;
  guildId: string;
  /** Message id the catch-up fetches from (exclusive). */
  baseline: string;
}

export interface SkippedChannel {
  channelId: string;
  reason: 'no-baseline' | 'live' | 'in-progress';
}

export interface CatchUpPlan {
  due: CatchUpPlanItem[];
  skipped: SkippedChannel[];
}

export interface CatchUpCycleResult {
  startedAt: string;
  finishedAt: string;
  /** Channels the cycle actually attempted. */
  attempted: number;
  skipped: SkippedChannel[];
  results: import('./userTokenExtractor.js').ExtractResult[];
  /** Messages captured this cycle. */
  messages: number;
  failures: number;
}

export interface CatchUpSchedulerOptions {
  /** Milliseconds between cycles (default 15 minutes). */
  intervalMs?: number;
  /** Maximum random extra delay per cycle, to avoid a thundering herd. */
  jitterMs?: number;
  /** Channels extracted in parallel within a cycle (default 1). */
  concurrency?: number;
  /** Also full-extract channels that have never been extracted. */
  includeMissingBaseline?: boolean;
  onCycleStart?: (plan: CatchUpPlan) => void;
  onCycleComplete?: (result: CatchUpCycleResult) => void;
  log?: Logger;
}

/**
 * Decide which channels are worth an unattended catch-up.
 *
 * Pure and synchronous on purpose: the eligibility rules are the part worth
 * testing without a database or network.
 */
export function planCatchUp(
  channels: ChannelProgress[],
  options: { includeMissingBaseline?: boolean; activeChannelIds?: Iterable<string> } = {},
): CatchUpPlan {
  const active = new Set(options.activeChannelIds ?? []);
  const due: CatchUpPlanItem[] = [];
  const skipped: SkippedChannel[] = [];

  for (const channel of channels) {
    const channelId = channel.channel_id;

    if (active.has(channelId)) {
      skipped.push({ channelId, reason: 'live' });
      continue;
    }

    if (channel.status === 'extracting') {
      skipped.push({ channelId, reason: 'in-progress' });
      continue;
    }

    if (!channel.newest_message_id) {
      // Nothing to catch up *from*: this would be a full extraction.
      if (options.includeMissingBaseline) {
        due.push({
          channelId,
          channelName: channel.channel_name || channelId,
          guildId: channel.server_id || '',
          baseline: '',
        });
      } else {
        skipped.push({ channelId, reason: 'no-baseline' });
      }
      continue;
    }

    due.push({
      channelId,
      channelName: channel.channel_name || channelId,
      guildId: channel.server_id || '',
      baseline: channel.newest_message_id,
    });
  }

  return { due, skipped };
}

export class CatchUpScheduler {
  private readonly extractor: UserTokenExtractor;
  private readonly log: Logger;
  private readonly intervalMs: number;
  private readonly jitterMs: number;
  private readonly concurrency: number;
  private readonly includeMissingBaseline: boolean;
  private readonly onCycleStart?: (plan: CatchUpPlan) => void;
  private readonly onCycleComplete?: (result: CatchUpCycleResult) => void;

  private stopped = false;
  private cycling = false;
  private wake: (() => void) | null = null;

  constructor(extractor: UserTokenExtractor, options: CatchUpSchedulerOptions = {}) {
    this.extractor = extractor;
    this.log = options.log ?? new Logger();
    this.intervalMs = Math.max(1000, options.intervalMs ?? DEFAULT_INTERVAL_MS);
    this.jitterMs = Math.max(0, options.jitterMs ?? Math.min(MAX_JITTER_MS, this.intervalMs / 10));
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.includeMissingBaseline = options.includeMissingBaseline ?? false;
    this.onCycleStart = options.onCycleStart;
    this.onCycleComplete = options.onCycleComplete;
  }

  isRunning(): boolean {
    return !this.stopped;
  }

  isCycling(): boolean {
    return this.cycling;
  }

  /** Build the plan for the next cycle from the current progress table. */
  async plan(): Promise<CatchUpPlan> {
    const channels = await this.extractor.knownChannels();
    return planCatchUp(channels, {
      includeMissingBaseline: this.includeMissingBaseline,
      // Channels this process is already writing (an in-flight extraction or an
      // open live capture) must be left alone - the listener owns that archive.
      activeChannelIds: this.extractor.activeChannelIds(),
    });
  }

  async runCycle(): Promise<CatchUpCycleResult> {
    const startedAt = new Date().toISOString();
    const plan = await this.plan();
    this.onCycleStart?.(plan);

    if (plan.due.length === 0) {
      this.log.info(
        `Catch-up cycle: nothing due (${plan.skipped.length} channel(s) skipped)`,
      );
      const empty: CatchUpCycleResult = {
        startedAt,
        finishedAt: new Date().toISOString(),
        attempted: 0,
        skipped: plan.skipped,
        results: [],
        messages: 0,
        failures: 0,
      };
      this.onCycleComplete?.(empty);
      return empty;
    }

    this.log.info(
      `Catch-up cycle: ${plan.due.length} channel(s) due, ${plan.skipped.length} skipped`,
    );

    const targets: ChannelInfo[] = plan.due.map((item) => ({
      channelId: item.channelId,
      channelName: item.channelName,
      guildId: item.guildId,
      guildName: item.guildId || 'Unknown',
    }));

    this.cycling = true;
    let results: import('./userTokenExtractor.js').ExtractResult[] = [];
    try {
      results = await this.extractor.extractAll(targets, {
        concurrency: this.concurrency,
        // Explicit: only fetch messages newer than each channel's baseline.
        // Planning already excluded channels without one.
        incremental: true,
      });
    } finally {
      this.cycling = false;
    }

    const messages = results.reduce((sum, r) => sum + r.messagesExtracted, 0);
    const failures = results.filter((r) => !r.success).length;

    const result: CatchUpCycleResult = {
      startedAt,
      finishedAt: new Date().toISOString(),
      attempted: results.length,
      skipped: plan.skipped,
      results,
      messages,
      failures,
    };

    this.log.info(
      `Catch-up cycle finished: ${messages} new message(s), ${failures} failure(s)`,
    );
    this.onCycleComplete?.(result);
    return result;
  }

  /** Run cycles until `stop()`. Never rejects: a failed cycle is logged. */
  async start(): Promise<void> {
    this.stopped = false;

    while (!this.stopped) {
      try {
        await this.runCycle();
      } catch (error: any) {
        this.log.error(`Catch-up cycle failed: ${error?.message ?? error}`);
      }

      if (this.stopped) break;
      await this.sleepUntilNextCycle();
    }
  }

  stop(): void {
    this.stopped = true;
    // Cancel the current wait, then abort anything in flight.
    this.wake?.();
    this.extractor.abortAll();
  }

  private sleepUntilNextCycle(): Promise<void> {
    const delay = this.intervalMs + Math.floor(Math.random() * this.jitterMs);
    return new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, delay);
      this.wake = () => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }
}
