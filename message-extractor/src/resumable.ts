// Apex Discord Scraper - Resume State
//
// Time-sharded extraction cannot be resumed from a single "last message id":
// each shard walks backwards through its own window independently. This module
// persists one cursor per shard AND the exact window that cursor belongs to.
//
// Persisting the windows matters. Shard boundaries are derived from the current
// time, so recomputing them a day later shifts every boundary; resuming an old
// cursor against new windows could skip a slice of history. Reusing the stored
// windows makes resume exact.

import type {
  ChannelResumeState,
  SegmentBalance,
  SegmentResumeState,
  TimeSegment,
} from './types.js';
import { calculateTimeSegments } from './utils.js';

export const RESUME_STATE_VERSION = 2;

/** Keep only the fields we understand, so a corrupt blob cannot leak through. */
function sanitizeBalance(raw: any): SegmentBalance | null {
  if (!raw || typeof raw !== 'object') return null;
  const strategy = raw.strategy === 'density' ? 'density' : 'equal-time';
  const loads = Array.isArray(raw.loads)
    ? raw.loads.filter((x: unknown) => typeof x === 'number' && Number.isFinite(x))
    : null;
  const finite = (x: unknown): number | null =>
    typeof x === 'number' && Number.isFinite(x) ? x : null;

  return {
    strategy,
    imbalance: finite(raw.imbalance),
    loads: loads && loads.length > 0 ? loads : null,
    minLoad: finite(raw.minLoad),
    maxLoad: finite(raw.maxLoad),
    probes: finite(raw.probes) ?? undefined,
    measuredAt: typeof raw.measuredAt === 'string' ? raw.measuredAt : undefined,
  };
}

export function createResumeState(
  segments: TimeSegment[],
  parallelism: number,
  balance?: SegmentBalance | null,
): ChannelResumeState {
  return {
    version: RESUME_STATE_VERSION,
    parallelism,
    updatedAt: new Date().toISOString(),
    segments: segments.map((segment, index) => ({
      index,
      after: segment.after,
      before: segment.before,
      cursor: null,
      done: false,
    })),
    balance: balance ?? null,
  };
}

/** Attach/replace the shard-load estimate for a layout. */
export function setBalance(
  state: ChannelResumeState,
  balance: SegmentBalance | null,
): void {
  state.balance = balance;
  state.updatedAt = new Date().toISOString();
}

/** Build a persistable estimate from a `BalancedSegments` result. */
export function buildBalance(result: {
  strategy: 'density' | 'equal-time';
  imbalance: number | null;
  loads: number[] | null;
  probes: number;
}): SegmentBalance {
  const loads = result.loads;
  return {
    strategy: result.strategy,
    imbalance: result.imbalance,
    loads: loads && loads.length > 0 ? loads : null,
    minLoad: loads && loads.length > 0 ? Math.min(...loads) : null,
    maxLoad: loads && loads.length > 0 ? Math.max(...loads) : null,
    probes: result.probes,
    measuredAt: new Date().toISOString(),
  };
}

/**
 * Parse a persisted resume state. Returns null when the blob is missing,
 * corrupt, or written by an incompatible version - callers then start fresh.
 */
export function parseResumeState(json: string | null | undefined): ChannelResumeState | null {
  if (!json) return null;
  try {
    const parsed = JSON.parse(json) as Partial<ChannelResumeState>;
    if (
      parsed?.version !== RESUME_STATE_VERSION ||
      !Array.isArray(parsed.segments) ||
      typeof parsed.parallelism !== 'number'
    ) {
      return null;
    }
    const segments: SegmentResumeState[] = parsed.segments.map((seg, index) => ({
      index: typeof seg?.index === 'number' ? seg.index : index,
      after: typeof seg?.after === 'string' ? seg.after : '',
      before: typeof seg?.before === 'string' ? seg.before : '',
      cursor: typeof seg?.cursor === 'string' ? seg.cursor : null,
      done: seg?.done === true,
    }));
    return {
      version: RESUME_STATE_VERSION,
      parallelism: parsed.parallelism,
      segments,
      updatedAt: parsed.updatedAt ?? new Date().toISOString(),
      balance: sanitizeBalance((parsed as any).balance),
    };
  } catch {
    return null;
  }
}

export function serializeResumeState(state: ChannelResumeState): string {
  state.updatedAt = new Date().toISOString();
  return JSON.stringify(state);
}

/**
 * Deep copy. Used to mark state as durable: the live state advances as pages
 * are fetched, but only a flushed snapshot may be persisted, otherwise a crash
 * would skip messages that were never written to disk.
 */
export function cloneResumeState(state: ChannelResumeState): ChannelResumeState {
  return {
    version: state.version,
    parallelism: state.parallelism,
    updatedAt: state.updatedAt,
    segments: state.segments.map((s) => ({
      index: s.index,
      after: s.after,
      before: s.before,
      cursor: s.cursor,
      done: s.done,
    })),
    balance: state.balance
      ? { ...state.balance, loads: state.balance.loads ? [...state.balance.loads] : null }
      : null,
  };
}

/** True when every segment carries the window it belongs to. */
function hasWindows(state: ChannelResumeState): boolean {
  return (
    state.segments.length > 0 &&
    state.segments.every((s) => s.after.length > 0 && s.before.length > 0)
  );
}

/** True when a saved state can be resumed as-is for this shard layout. */
export function hasReusableWindows(
  state: ChannelResumeState | null,
  parallelism: number,
): boolean {
  return !!state && state.parallelism === parallelism && hasWindows(state);
}

/**
 * Decide which shard windows to fetch and which state to resume from.
 *
 * Reuses the persisted windows when the shard count is unchanged, so resume is
 * exact. Otherwise (first run, config change, corrupt/old state) it computes
 * fresh windows and starts that layout from scratch.
 */
export function resolveSegments(
  channelId: string,
  parallelism: number,
  saved: ChannelResumeState | null,
  /** Freshly computed windows (e.g. density-balanced) for a non-resumed run. */
  freshSegments?: TimeSegment[] | null,
  /** Estimate that produced `freshSegments`, persisted for `status`. */
  freshBalance?: SegmentBalance | null,
): { segments: TimeSegment[]; state: ChannelResumeState } {
  if (hasReusableWindows(saved, parallelism)) {
    return {
      segments: saved!.segments
        .slice()
        .sort((a, b) => a.index - b.index)
        .map((s) => ({ after: s.after, before: s.before })),
      state: saved!,
    };
  }

  const segments =
    freshSegments && freshSegments.length > 0
      ? freshSegments
      : calculateTimeSegments(channelId, parallelism);
  return { segments, state: createResumeState(segments, parallelism, freshBalance ?? null) };
}

export type IncrementalWindowReason = 'ok' | 'no-baseline' | 'pending-shards';

export interface IncrementalWindowDecision {
  /** True when this run may use a single catch-up window instead of the saved layout. */
  useIncrementalWindow: boolean;
  /** True when the saved state is itself that catch-up window and should be resumed as-is. */
  reuseSavedWindow: boolean;
  reason: IncrementalWindowReason;
}

/**
 * Decide whether a catch-up window may replace the saved shard layout.
 *
 * A one-segment window only covers messages newer than the baseline. Applying it
 * to a layout that still has pending shards silently discards those shards'
 * windows, so the channel would finish as `done` with a permanent hole in its
 * history. Two states are safe: every shard is done (the normal catch-up after a
 * completed run), or the saved state is a single segment whose lower bound is the
 * baseline - that is a previous catch-up resuming after a failure, not a lost
 * multi-shard layout.
 */
export function decideIncrementalWindow(
  baseline: string | null,
  savedState: ChannelResumeState | null,
): IncrementalWindowDecision {
  if (!baseline) {
    return { useIncrementalWindow: false, reuseSavedWindow: false, reason: 'no-baseline' };
  }

  if (
    savedState &&
    savedState.parallelism === 1 &&
    savedState.segments.length === 1 &&
    savedState.segments[0].after === baseline &&
    savedState.segments[0].before.length > 0 &&
    !savedState.segments[0].done
  ) {
    return { useIncrementalWindow: true, reuseSavedWindow: true, reason: 'ok' };
  }

  if (!savedState || isResumeComplete(savedState)) {
    return { useIncrementalWindow: true, reuseSavedWindow: false, reason: 'ok' };
  }

  return { useIncrementalWindow: false, reuseSavedWindow: false, reason: 'pending-shards' };
}

export function getSegmentState(
  state: ChannelResumeState,
  index: number,
): SegmentResumeState {
  let seg = state.segments.find((s) => s.index === index);
  if (!seg) {
    seg = { index, after: '', before: '', cursor: null, done: false };
    state.segments.push(seg);
    state.segments.sort((a, b) => a.index - b.index);
  }
  return seg;
}

export function setSegmentCursor(
  state: ChannelResumeState,
  index: number,
  cursor: string | null,
): void {
  const seg = getSegmentState(state, index);
  seg.cursor = cursor;
  state.updatedAt = new Date().toISOString();
}

export function markSegmentDone(state: ChannelResumeState, index: number): void {
  const seg = getSegmentState(state, index);
  seg.done = true;
  seg.cursor = null;
  state.updatedAt = new Date().toISOString();
}

export function isResumeComplete(state: ChannelResumeState): boolean {
  return state.segments.length > 0 && state.segments.every((s) => s.done);
}

export function pendingSegmentCount(state: ChannelResumeState): number {
  return state.segments.filter((s) => !s.done).length;
}

/** Human-readable summary for logs / status output. */
export function describeResumeState(state: ChannelResumeState): string {
  const done = state.segments.filter((s) => s.done).length;
  const partial = state.segments.filter((s) => !s.done && s.cursor).length;
  const pending = state.segments.filter((s) => !s.done && !s.cursor).length;
  return `${done}/${state.segments.length} shards done, ${partial} partial, ${pending} pending`;
}
