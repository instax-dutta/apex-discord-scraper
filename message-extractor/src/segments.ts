// Apex Discord Scraper - Shard segmentation
//
// Equal-time shards are unbalanced: Discord traffic is bursty, so one shard can
// hold many times the messages of another and the parallel phase ends up
// waiting on it. This module places boundaries so each shard holds a similar
// *estimated* number of messages, using a handful of cheap probe pages.
//
// The estimate is a piecewise-constant message rate sampled across the
// channel's lifetime. It is not exact, but it turns "one shard has 60% of the
// history" into "shards finish at roughly the same time". When probes are
// unavailable or degenerate we fall back to the non-uniform-aware equal-time
// split, so this can only help.

import type { SegmentBalance, TimeSegment } from './types.js';
import { snowflakeToTs, tsToSnowflake, calculateTimeSegments } from './utils.js';

export interface DensityProbe {
  /** Timestamp (ms) the probe was taken at. */
  timestampMs: number;
  /** Estimated messages per millisecond around this timestamp. */
  ratePerMs: number;
  /** Number of messages returned by the probe page. */
  sampleCount: number;
}

export interface BalancedSegments {
  segments: TimeSegment[];
  strategy: 'density' | 'equal-time';
  /**
   * (max - min) / mean of the estimated per-shard message load. Lower is more
   * balanced. Null when the estimate is unavailable.
   */
  imbalance: number | null;
  /** Estimated message count per shard, aligned with `segments`. */
  loads: number[] | null;
  /** Samples the estimate was built from (0 when it fell back). */
  probes: number;
}

/** Samples per shard in the derived default probe count. */
export const PROBES_PER_SHARD = 3;
/** Never take fewer than this many samples when balancing is enabled. */
export const MIN_PROBE_COUNT = 4;
/** Hard cap on probe requests per channel. */
export const MAX_PROBES = 36;

/**
 * How many density probes to take. `override` (from `DENSITY_PROBES`) wins;
 * otherwise derive from the shard count. More samples give a finer density
 * curve, but each one costs a request, so the default keeps it under
 * `MAX_PROBES` and proportional to the parallelism actually in use.
 */
export function resolveProbeCount(parallelism: number, override?: number | null): number {
  if (override && Number.isFinite(override) && override > 0) {
    return Math.max(1, Math.min(Math.round(override), MAX_PROBES));
  }
  const derived = Math.max(MIN_PROBE_COUNT, parallelism * PROBES_PER_SHARD);
  return Math.min(derived, MAX_PROBES);
}

interface ProbePoint {
  t: number;
  rate: number;
}

const MIN_PROBES = 2;

/** Integral of the piecewise-constant density over [a, b]. */
export function massBetween(a: number, b: number, points: ProbePoint[]): number {
  let mass = 0;
  for (let i = 0; i < points.length - 1; i++) {
    const lo = Math.max(a, points[i].t);
    const hi = Math.min(b, points[i + 1].t);
    if (hi > lo) mass += points[i].rate * (hi - lo);
  }
  return mass;
}

function equalFallback(channelId: string, parallelism: number): BalancedSegments {
  return {
    segments: calculateTimeSegments(channelId, parallelism),
    strategy: 'equal-time',
    imbalance: null,
    loads: null,
    probes: 0,
  };
}

export function balanceSegmentsByDensity(
  channelId: string,
  parallelism: number,
  probes: DensityProbe[],
  nowTs: number,
): BalancedSegments {
  const start = snowflakeToTs(channelId);
  const end = nowTs + 60_000;
  const p = Math.max(1, Math.min(parallelism, 12));

  if (p < 2) {
    return {
      segments: [{ after: channelId, before: tsToSnowflake(end), afterInclusive: true }],
      strategy: 'equal-time',
      imbalance: null,
      loads: null,
      probes: 0,
    };
  }

  const usable = (probes ?? [])
    .filter(
      (x) =>
        Number.isFinite(x.timestampMs) &&
        x.timestampMs > start &&
        x.timestampMs < end &&
        Number.isFinite(x.ratePerMs) &&
        x.ratePerMs >= 0,
    )
    .sort((a, b) => a.timestampMs - b.timestampMs);

  if (usable.length < MIN_PROBES) return equalFallback(channelId, p);

  // Piecewise-constant density anchored at the channel start and now.
  const points: ProbePoint[] = [{ t: start, rate: usable[0].ratePerMs }];
  for (const probe of usable) {
    if (probe.timestampMs > points[points.length - 1].t) {
      points.push({ t: probe.timestampMs, rate: probe.ratePerMs });
    }
  }
  if (end > points[points.length - 1].t) {
    points.push({ t: end, rate: usable[usable.length - 1].ratePerMs });
  }
  if (points.length < 3) return equalFallback(channelId, p);

  const totalMass = massBetween(start, end, points);
  if (!(totalMass > 0)) return equalFallback(channelId, p);

  // Walk the density curve and drop a boundary each time we cross an equal
  // share of the estimated total mass.
  const target = totalMass / p;
  const fractional: number[] = [start];
  let accumulated = 0;
  let want = 1;

  for (let i = 0; i < points.length - 1 && want < p; i++) {
    const dt = points[i + 1].t - points[i].t;
    const segmentMass = points[i].rate * dt;
    if (segmentMass <= 0) continue;

    while (want < p && accumulated + segmentMass >= target * want) {
      const needed = Math.max(0, target * want - accumulated);
      const fraction = Math.min(1, needed / segmentMass);
      const boundary = points[i].t + fraction * dt;
      if (boundary > fractional[fractional.length - 1] && boundary < end) {
        fractional.push(boundary);
      }
      want++;
    }
    accumulated += segmentMass;
  }

  fractional.push(end);
  if (fractional.length !== p + 1) return equalFallback(channelId, p);

  // Snap to integer milliseconds. Adjacent shards share the exact same
  // boundary value, so each window is half-open - `id >= after && id < before` -
  // and a message on the boundary belongs to exactly one of them.
  const bounds = fractional.map((t, i) => {
    if (i === 0) return start;
    if (i === fractional.length - 1) return end;
    return Math.round(t);
  });

  for (let i = 1; i < bounds.length; i++) {
    if (!(bounds[i] > bounds[i - 1])) return equalFallback(channelId, p);
  }

  const segments: TimeSegment[] = [];
  for (let i = 0; i < p; i++) {
    segments.push({
      after: i === 0 ? channelId : tsToSnowflake(bounds[i]),
      before: tsToSnowflake(bounds[i + 1]),
      afterInclusive: true,
    });
  }

  const loads = bounds.slice(0, -1).map((a, i) => massBetween(a, bounds[i + 1], points));
  const mean = loads.reduce((sum, x) => sum + x, 0) / loads.length;
  const imbalance = mean > 0 ? (Math.max(...loads) - Math.min(...loads)) / mean : null;

  return { segments, strategy: 'density', imbalance, loads, probes: usable.length };
}

/**
 * Human-readable summary of a persisted balance estimate, for `status`.
 *
 * @example "density, 6 shards, ~9.9K-11.2K msgs/shard (imbalance 12%, 21 probes)"
 */
export function describeBalance(balance: SegmentBalance | null | undefined): string | null {
  if (!balance) return null;

  const shards = balance.loads?.length ?? 0;
  const label = balance.strategy === 'density' ? 'density' : 'equal-time';

  if (balance.strategy === 'density' && shards > 0 && balance.minLoad != null && balance.maxLoad != null) {
    const parts = [
      `${label}`,
      `${shards} shards`,
      `~${formatCount(balance.minLoad)}-${formatCount(balance.maxLoad)} msgs/shard`,
    ];
    if (balance.imbalance != null) parts.push(`imbalance ${(balance.imbalance * 100).toFixed(0)}%`);
    if (balance.probes) parts.push(`${balance.probes} probes`);
    return parts.join(', ');
  }

  return `${label} (no density estimate)`;
}

/** Compact integer formatting: 1234 -> 1.2K, 1500000 -> 1.5M. */
export function formatCount(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return String(Math.round(value));
}
