// Shard boundary tests: a message whose id equals a shared boundary belongs to
// exactly one shard.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { UserTokenFetcher } from '../src/userTokenFetcher.js';
import { UserTokenClient } from '../src/userTokenClient.js';
import { balanceSegmentsByDensity } from '../src/segments.js';
import { Logger, calculateTimeSegments, snowflakeToTs, tsToSnowflake } from '../src/utils.js';
import {
  cloneResumeState,
  createResumeState,
  parseResumeState,
  RESUME_STATE_VERSION,
  resolveSegments,
  serializeResumeState,
} from '../src/resumable.js';
import type { AbortState, TimeSegment } from '../src/types.js';

const quiet = new Logger('error');
const DAY_MS = 24 * 60 * 60 * 1000;

test('a message sitting exactly on a shared shard boundary is captured once', async () => {
  const channelId = tsToSnowflake(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const segments: TimeSegment[] = calculateTimeSegments(channelId, 2);
  assert.equal(segments.length, 2, 'this test needs a two-shard layout');

  // The boundary is the lowest valid snowflake for its millisecond, so a real
  // message can land exactly on it.
  const boundary = segments[1].after;
  assert.equal(segments[0].before, boundary, 'the shards must share this value');

  const low = (BigInt(boundary) - 5n).toString();
  const atBoundary = boundary;
  const high = (BigInt(boundary) + 5n).toString();
  const descending = [high, atBoundary, low];

  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages(_channelId: string, options: any) {
        const bound = options.before ? BigInt(options.before) : null;
        return descending
          .filter((id) => bound === null || BigInt(id) < bound)
          .slice(0, 100)
          .map((id) => ({
            id,
            content: `msg-${id}`,
            timestamp: new Date().toISOString(),
            author: { id: 'u1', username: 'tester', discriminator: '0' },
          }));
      },
    } as unknown as UserTokenClient,
    quiet,
  );

  const emitted: string[] = [];
  const signal: AbortState = { aborted: false };
  const result = await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 2,
    segments,
    pageDelayMs: 0,
    resumeState: createResumeState(segments, 2),
    onMessage: (row) => emitted.push(row.id),
  });

  assert.equal(result.segmentErrors.length, 0, `shard errors: ${JSON.stringify(result.segmentErrors)}`);
  assert.ok(
    emitted.includes(atBoundary),
    'the message on the shared boundary was dropped by both shards',
  );
  assert.equal(
    emitted.filter((id) => id === atBoundary).length,
    1,
    'the boundary message was captured twice',
  );
  assert.deepEqual(
    [...emitted].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : 1)),
    [low, atBoundary, high].sort((a, b) => (BigInt(a) < BigInt(b) ? -1 : 1)),
    'the two shards must cover the whole range exactly once',
  );
});

test('an incremental lower bound stays exclusive so the baseline is not re-appended', async () => {
  const channelId = tsToSnowflake(Date.now() - 1000);
  const baseline = (BigInt(channelId) + 100n).toString();
  // No `afterInclusive` flag: a message-id baseline is already archived.
  const segments: TimeSegment[] = [{ after: baseline, before: (BigInt(channelId) + 200n).toString() }];

  const descending = [
    (BigInt(channelId) + 150n).toString(),
    baseline,
    (BigInt(channelId) + 50n).toString(),
  ];

  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages(_channelId: string, options: any) {
        const bound = options.before ? BigInt(options.before) : null;
        return descending
          .filter((id) => bound === null || BigInt(id) < bound)
          .slice(0, 100)
          .map((id) => ({
            id,
            content: `msg-${id}`,
            timestamp: new Date().toISOString(),
            author: { id: 'u1', username: 'tester', discriminator: '0' },
          }));
      },
    } as unknown as UserTokenClient,
    quiet,
  );

  const emitted: string[] = [];
  const signal: AbortState = { aborted: false };
  await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 1,
    segments,
    pageDelayMs: 0,
    resumeState: createResumeState(segments, 1),
    onMessage: (row) => emitted.push(row.id),
  });

  assert.deepEqual(emitted, [(BigInt(channelId) + 150n).toString()], 'the baseline message was re-emitted');
});

test('density-balanced segments mark every lower bound inclusive', () => {
  const now = Date.now();
  const channelId = tsToSnowflake(now - 10 * DAY_MS);
  const start = snowflakeToTs(channelId);
  const probes = Array.from({ length: 9 }, (_, i) => {
    const day = i + 1;
    return {
      timestampMs: start + day * DAY_MS,
      ratePerMs: day >= 6 ? 1e-4 : 1e-6,
      sampleCount: 10,
    };
  });

  const balanced = balanceSegmentsByDensity(channelId, 4, probes, now);

  assert.equal(balanced.strategy, 'density');
  assert.equal(balanced.segments.length, 4);
  assert.deepEqual(
    balanced.segments.map((segment) => segment.afterInclusive),
    [true, true, true, true],
  );
});

test('inclusive flags survive create, clone, serialization, parse, and resolution', () => {
  const segments: TimeSegment[] = [
    { after: '1', before: '2', afterInclusive: true },
    { after: '2', before: '3', afterInclusive: false },
  ];

  const created = createResumeState(segments, 2);
  assert.deepEqual(created.segments.map((segment) => segment.afterInclusive), [true, false]);

  const cloned = cloneResumeState(created);
  assert.deepEqual(cloned.segments.map((segment) => segment.afterInclusive), [true, false]);

  const parsed = parseResumeState(serializeResumeState(cloned));
  assert.ok(parsed);
  assert.equal(parsed.version, RESUME_STATE_VERSION);
  assert.deepEqual(parsed.segments.map((segment) => segment.afterInclusive), [true, false]);

  const resolved = resolveSegments('channel', 2, parsed);
  assert.deepEqual(
    resolved.segments.map((segment) => segment.afterInclusive),
    [true, false],
  );
});

test('an inclusive sequential fallback keeps the lower-bound message', async () => {
  const channelId = tsToSnowflake(Date.now() - 1000);
  const lowerBound = (BigInt(channelId) + 100n).toString();
  const upperBound = (BigInt(channelId) + 200n).toString();
  const segments: TimeSegment[] = [
    { after: lowerBound, before: upperBound, afterInclusive: true },
  ];
  const descending = [upperBound, lowerBound];
  let calls = 0;

  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages(_channelId: string, options: any) {
        calls++;
        if (calls <= 2) throw new Error('shard unavailable');
        const bound = options.before ? BigInt(options.before) : null;
        return descending
          .filter((id) => bound === null || BigInt(id) < bound)
          .slice(0, 100)
          .map((id) => ({
            id,
            content: `msg-${id}`,
            timestamp: new Date().toISOString(),
            author: { id: 'u1', username: 'tester', discriminator: '0' },
          }));
      },
    } as unknown as UserTokenClient,
    quiet,
  );

  const emitted: string[] = [];
  const signal: AbortState = { aborted: false };
  const result = await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 1,
    segments,
    pageDelayMs: 0,
    resumeState: createResumeState(segments, 1),
    onMessage: (row) => emitted.push(row.id),
  });

  assert.equal(result.segmentErrors.length, 0, `shard errors: ${JSON.stringify(result.segmentErrors)}`);
  assert.equal(result.usedFallback, true);
  assert.deepEqual(emitted, [lowerBound]);
});
