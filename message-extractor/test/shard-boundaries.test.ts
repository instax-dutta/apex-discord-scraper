// Shard boundary tests: a message whose id equals a shared boundary belongs to
// exactly one shard.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { UserTokenFetcher } from '../src/userTokenFetcher.js';
import { UserTokenClient } from '../src/userTokenClient.js';
import { Logger, calculateTimeSegments, tsToSnowflake } from '../src/utils.js';
import { createResumeState } from '../src/resumable.js';
import type { AbortState, TimeSegment } from '../src/types.js';

const quiet = new Logger('error');

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
