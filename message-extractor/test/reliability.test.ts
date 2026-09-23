// Reliability tests for the message extractor.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { RateLimitFailsafe } from '../src/rateLimit.js';
import { ExtractorError, classifyStatus, isAbortError } from '../src/errors.js';
import { calculateTimeSegments, tsToSnowflake, getBackoffDelay, Logger } from '../src/utils.js';
import {
  createResumeState,
  describeResumeState,
  getSegmentState,
  isResumeComplete,
  markSegmentDone,
  parseResumeState,
  pendingSegmentCount,
  resolveSegments,
  serializeResumeState,
  setSegmentCursor,
} from '../src/resumable.js';
import { UserTokenClient } from '../src/userTokenClient.js';
import { UserTokenFetcher } from '../src/userTokenFetcher.js';
import type { AbortState, ProgressCallback } from '../src/types.js';

const quiet = new Logger('error');

function recentChannelId(): string {
  return tsToSnowflake(Date.now());
}

function oldChannelId(years = 3): string {
  return tsToSnowflake(Date.now() - years * 365 * 24 * 60 * 60 * 1000);
}

/** A realistic message id: snowflakes are always newer than their channel. */
function messageIdIn(channelId: string, offset = 1): string {
  return (BigInt(channelId) + BigInt(offset)).toString();
}

function testLimiter(overrides = {}) {
  return new RateLimitFailsafe(
    {
      minDelayMs: 0,
      baseBackoffMs: 1,
      maxBackoffMs: 5,
      requestsPerSecond: 1000,
      burstSize: 4,
      circuitBreakerThreshold: 100,
      ...overrides,
    },
    quiet,
  );
}

async function withMockFetch<T>(impl: typeof fetch, fn: () => Promise<T>): Promise<T> {
  const original = globalThis.fetch;
  globalThis.fetch = impl;
  try {
    return await fn();
  } finally {
    globalThis.fetch = original;
  }
}

// ==================== Error classification ====================

test('classifyStatus marks transient statuses retryable and auth fatal', () => {
  assert.deepEqual(classifyStatus(429), { kind: 'rate_limit', retryable: true });
  assert.deepEqual(classifyStatus(500), { kind: 'server', retryable: true });
  assert.deepEqual(classifyStatus(503), { kind: 'server', retryable: true });
  assert.deepEqual(classifyStatus(401), { kind: 'auth', retryable: false });
  assert.deepEqual(classifyStatus(403), { kind: 'auth', retryable: false });
  assert.deepEqual(classifyStatus(404), { kind: 'not_found', retryable: false });
  assert.deepEqual(classifyStatus(400), { kind: 'client', retryable: false });
});

test('ExtractorError exposes actionable hints', () => {
  const err = new ExtractorError('bad token', { kind: 'auth', status: 401 });
  assert.equal(err.retryable, false);
  assert.match(err.hint, /token/i);
});

test('isAbortError only matches real aborts', () => {
  assert.equal(isAbortError({ name: 'AbortError' }), true);
  assert.equal(isAbortError({ code: 'ABORT_ERR' }), true);
  assert.equal(isAbortError(new Error('nope')), false);
});

test('backoff delay is bounded and monotonic in expectation', () => {
  for (let attempt = 0; attempt < 8; attempt++) {
    const delay = getBackoffDelay(attempt, 100, 16000);
    assert.ok(delay >= 0 && delay <= 16000, `delay ${delay} out of range`);
  }
});

// ==================== Time sharding ====================

test('small channels use a single segment', () => {
  const segments = calculateTimeSegments(recentChannelId(), 6);
  assert.equal(segments.length, 1);
});

test('large channels split into the requested number of shards', () => {
  const segments = calculateTimeSegments(oldChannelId(), 5);
  assert.equal(segments.length, 5);
  for (const seg of segments) {
    assert.ok(BigInt(seg.after) < BigInt(seg.before));
  }
});

// ==================== Resume state ====================

test('resume state round-trips through JSON', () => {
  const segments = calculateTimeSegments(oldChannelId(), 3);
  const state = createResumeState(segments, 3);
  setSegmentCursor(state, 1, '12345');
  markSegmentDone(state, 0);

  const parsed = parseResumeState(serializeResumeState(state));
  assert.ok(parsed);
  assert.equal(parsed.parallelism, 3);
  assert.equal(getSegmentState(parsed, 0).done, true);
  assert.equal(getSegmentState(parsed, 1).cursor, '12345');
  assert.equal(pendingSegmentCount(parsed), 2);
  assert.equal(isResumeComplete(parsed), false);
});

test('resume state rejects corrupt or unknown-version blobs', () => {
  assert.equal(parseResumeState(null), null);
  assert.equal(parseResumeState('not json'), null);
  assert.equal(parseResumeState(JSON.stringify({ version: 99, segments: [] })), null);
});

test('resume reuses persisted shard windows exactly', () => {
  const channelId = oldChannelId();
  const segments = calculateTimeSegments(channelId, 3);
  const state = createResumeState(segments, 3);
  markSegmentDone(state, 0);

  const resumed = resolveSegments(channelId, 3, state);
  assert.equal(resumed.segments.length, 3);
  for (let i = 0; i < segments.length; i++) {
    assert.equal(resumed.segments[i].after, segments[i].after);
    assert.equal(resumed.segments[i].before, segments[i].before);
  }
  assert.equal(getSegmentState(resumed.state, 0).done, true);
});

test('resume starts fresh when the shard count changes', () => {
  const channelId = oldChannelId();
  const state = createResumeState(calculateTimeSegments(channelId, 3), 3);
  markSegmentDone(state, 0);

  const fresh = resolveSegments(channelId, 4, state);
  assert.equal(fresh.segments.length, 4);
  assert.equal(fresh.state.segments.every((s) => !s.done), true);
  assert.equal(fresh.state.segments.length, 4);
});

test('resume discards a state whose windows were never recorded', () => {
  const channelId = oldChannelId();
  const legacy = {
    version: 2,
    parallelism: 2,
    updatedAt: new Date().toISOString(),
    segments: [
      { index: 0, after: '', before: '', cursor: '5', done: false },
      { index: 1, after: '', before: '', cursor: null, done: false },
    ],
  };

  const fresh = resolveSegments(channelId, 2, legacy as any);
  assert.equal(fresh.state.segments.every((s) => !s.done), true);
  assert.equal(getSegmentState(fresh.state, 0).cursor, null);
});

test('describeResumeState summarises progress', () => {
  const segments = calculateTimeSegments(oldChannelId(), 4);
  const state = createResumeState(segments, 4);
  markSegmentDone(state, 0);
  setSegmentCursor(state, 1, '9');
  assert.match(describeResumeState(state), /1\/4 shards done, 1 partial, 2 pending/);
});

// ==================== Rate limiter ====================

test('rate limiter resets the failure counter on success (no permanent circuit)', () => {
  const limiter = testLimiter({ circuitBreakerThreshold: 3 });

  limiter.recordRateLimit();
  assert.equal(limiter.getState().consecutiveFailures, 1);
  limiter.recordSuccess();
  assert.equal(limiter.getState().consecutiveFailures, 0);

  limiter.recordRateLimit();
  limiter.recordRateLimit();
  assert.equal(limiter.getState().circuitState, 'closed');
});

test('rate limiter opens the circuit after repeated failures and recovers', async () => {
  const limiter = testLimiter({ circuitBreakerThreshold: 2, circuitBreakerResetMs: 5 });

  limiter.recordRateLimit();
  limiter.recordRateLimit();
  assert.equal(limiter.getState().circuitState, 'open');

  await limiter.acquire('bucket');
  limiter.release();
  limiter.recordSuccess();
  assert.equal(limiter.getState().circuitState, 'closed');
});

test('acquire/release tracks in-flight requests', async () => {
  const limiter = testLimiter();
  await limiter.acquire('b');
  assert.equal(limiter.getState().inFlight, 1);
  limiter.release();
  assert.equal(limiter.getState().inFlight, 0);
});

test('getRetryDelay honours Retry-After', () => {
  const limiter = testLimiter({ maxBackoffMs: 60000 });
  const headers = new Headers({ 'retry-after': '2' });
  const delay = limiter.getRetryDelay(headers, 0);
  assert.ok(delay >= 2000 && delay < 2600, `unexpected delay ${delay}`);
});

test('getRetryDelay understands the relative x-ratelimit-reset-after header', () => {
  const limiter = testLimiter({ maxRetryAfterMs: 900_000, minDelayMs: 0 });
  const delay = limiter.getRetryDelay(new Headers({ 'x-ratelimit-reset-after': '2' }), 0);
  assert.ok(delay >= 2000 && delay < 2600, `unexpected delay ${delay}`);
});

test('an exhausted bucket trusts reset-after over a distant reset epoch', async () => {
  const limiter = testLimiter({ minDelayMs: 0, requestsPerSecond: 1000, burstSize: 4, baseBackoffMs: 1 });

  limiter.trackBucket(
    'messages:9',
    new Headers({
      'x-ratelimit-remaining': '0',
      // An epoch a minute away; the relative header (150ms) is authoritative.
      'x-ratelimit-reset': String((Date.now() + 60_000) / 1000),
      'x-ratelimit-reset-after': '0.15',
    }),
  );

  const start = Date.now();
  await limiter.acquire('messages:9');
  limiter.release();
  const waited = Date.now() - start;
  assert.ok(waited >= 100 && waited < 5000, `expected the relative reset, waited ${waited}ms`);
});

// ==================== Client retry behavior ====================

test('client retries a 429 then succeeds', async () => {
  let calls = 0;
  const client = new UserTokenClient('token', quiet, {
    retries: 3,
    timeoutMs: 1000,
    rateLimiter: testLimiter(),
    apiBase: 'https://example.test',
  });

  await withMockFetch(
    (async () => {
      calls++;
      if (calls === 1) {
        return new Response('rate limited', {
          status: 429,
          headers: { 'retry-after': '0', 'x-ratelimit-bucket': 'b' },
        });
      }
      return Response.json([{ id: '1' }]);
    }) as typeof fetch,
    async () => {
      const messages = await client.fetchMessages('123');
      assert.equal(calls, 2);
      assert.equal(messages.length, 1);
    },
  );
});

test('client does not retry an auth failure', async () => {
  let calls = 0;
  const client = new UserTokenClient('token', quiet, {
    retries: 5,
    timeoutMs: 1000,
    rateLimiter: testLimiter(),
    apiBase: 'https://example.test',
  });

  await withMockFetch(
    (async () => {
      calls++;
      return new Response('unauthorized', { status: 401 });
    }) as typeof fetch,
    async () => {
      await assert.rejects(
        () => client.fetchMessages('123'),
        (err: unknown) => err instanceof ExtractorError && err.kind === 'auth',
      );
      assert.equal(calls, 1);
    },
  );
});

test('client exhausts retries on persistent 5xx', async () => {
  let calls = 0;
  const client = new UserTokenClient('token', quiet, {
    retries: 3,
    timeoutMs: 1000,
    rateLimiter: testLimiter(),
    apiBase: 'https://example.test',
  });

  await withMockFetch(
    (async () => {
      calls++;
      return new Response('boom', { status: 500 });
    }) as typeof fetch,
    async () => {
      await assert.rejects(
        () => client.fetchMessages('123'),
        (err: unknown) => err instanceof ExtractorError && err.kind === 'server',
      );
      // 1 initial attempt + 3 retries, never more.
      assert.equal(calls, 4);
    },
  );
});

test('client stops immediately when the caller aborts', async () => {
  const controller = new AbortController();
  const client = new UserTokenClient('token', quiet, {
    retries: 5,
    timeoutMs: 1000,
    rateLimiter: testLimiter(),
    apiBase: 'https://example.test',
  });

  let calls = 0;
  await withMockFetch(
    (async () => {
      calls++;
      return new Response('nope', { status: 500 });
    }) as typeof fetch,
    async () => {
      controller.abort();
      await assert.rejects(
        () => client.fetchMessages('123', { signal: controller.signal }),
        (err: unknown) => err instanceof ExtractorError && err.kind === 'aborted',
      );
      assert.equal(calls, 0);
    },
  );
});

// ==================== Fetcher fallbacks ====================

interface FakePage {
  id: string;
  content: string;
  timestamp: string;
  author: { id: string; username: string; discriminator: string };
}

function onePageClient(page: FakePage[]) {
  let calls = 0;
  return {
    calls: () => calls,
    client: {
      async fetchMessages() {
        calls++;
        return calls === 1 ? page : [];
      },
    } as unknown as UserTokenClient,
  };
}

test('fetcher completes a single-shard channel and reports done', async () => {
  const channelId = recentChannelId();
  const page: FakePage[] = [
    {
      id: messageIdIn(channelId),
      content: 'hello',
      timestamp: new Date().toISOString(),
      author: { id: 'u1', username: 'user', discriminator: '0' },
    },
  ];
  const fake = onePageClient(page);
  const fetcher = new UserTokenFetcher(fake.client, quiet);
  const signal: AbortState = { aborted: false };
  const progress: ProgressCallback = () => {};

  const result = await fetcher.fetchChannel(channelId, signal, progress, { parallelism: 1 });

  assert.equal(result.fetched, 1);
  assert.equal(result.aborted, false);
  assert.equal(result.segmentErrors.length, 0);
  assert.equal(isResumeComplete(result.resumeState), true);
});

test('fetcher returns aborted without fetching when signal is already aborted', async () => {
  const fetcher = new UserTokenFetcher({ async fetchMessages() { throw new Error('should not be called'); } } as unknown as UserTokenClient, quiet);
  const signal: AbortState = { aborted: true };
  const result = await fetcher.fetchChannel(recentChannelId(), signal, () => {}, { parallelism: 1 });
  assert.equal(result.aborted, true);
  assert.equal(result.fetched, 0);
});

test('fetcher degrades to sequential sweep and reports shard failures', async () => {
  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages() {
        throw new ExtractorError('unavailable', { kind: 'server', retryable: false });
      },
    } as unknown as UserTokenClient,
    quiet,
  );

  const signal: AbortState = { aborted: false };
  const result = await fetcher.fetchChannel(oldChannelId(), signal, () => {}, {
    parallelism: 2,
    pageDelayMs: 0,
  });

  assert.equal(result.fetched, 0);
  assert.equal(result.usedFallback, true);
  assert.equal(result.aborted, false);
  assert.ok(result.segmentErrors.length >= 2, 'expected per-shard errors to be reported');
  assert.equal(isResumeComplete(result.resumeState), false);
});

test('fetcher retries a failed shard after a transient failure', async () => {
  const channelId = recentChannelId();
  const page = [{
    id: messageIdIn(channelId),
    content: 'recovered',
    timestamp: new Date().toISOString(),
    author: { id: 'u', username: 'u', discriminator: '0' },
  }];

  let calls = 0;
  const fetchMessages = async () => {
    calls++;
    if (calls === 1) throw new ExtractorError('temporary', { kind: 'server', retryable: false });
    return calls === 2 ? page : [];
  };

  const fetcher = new UserTokenFetcher({ fetchMessages } as unknown as UserTokenClient, quiet);
  const signal: AbortState = { aborted: false };

  const result = await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 1,
    pageDelayMs: 0,
  });

  assert.equal(result.fetched, 1);
  assert.equal(result.usedFallback, true);
  assert.equal(result.segmentErrors.length, 0);
  assert.equal(isResumeComplete(result.resumeState), true);
});

// ==================== Sequential-sweep fallback bounds ====================

test('a failed catch-up sweeps only its own window, never the whole channel', async () => {
  // An incremental catch-up is a single shard covering (baseline, now]. If that
  // shard fails, the fallback must sweep that window - not the channel - or it
  // re-appends every message the archive already holds.
  const channelId = recentChannelId();
  const baseline = messageIdIn(channelId, 100);
  const upper = messageIdIn(channelId, 500);
  const segments = [{ after: baseline, before: upper }];

  // Everything from "channel start" up to `upper`, descending: the sweep sees
  // ids both inside and below the window and must keep only the former.
  const descending = Array.from({ length: 500 }, (_, i) => messageIdIn(channelId, 499 - i));
  const requests: (string | undefined)[] = [];
  const emitted: string[] = [];
  let calls = 0;

  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages(_channelId: string, options: any) {
        calls++;
        requests.push(options.before);
        // The shard attempt and its serial retry both fail; the sweep succeeds.
        if (calls <= 2) {
          throw new ExtractorError('shard unavailable', { kind: 'server', retryable: false });
        }
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

  const signal: AbortState = { aborted: false };
  const result = await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 1,
    segments,
    pageDelayMs: 0,
    resumeState: createResumeState(segments, 1),
    onMessage: (row) => emitted.push(row.id),
  });

  // 399 = every id strictly between the baseline (100) and the upper bound
  // (500). The last page it serves reaches down to the baseline message itself,
  // which must be filtered out rather than re-appended.
  assert.equal(result.fetched, 399);
  assert.equal(emitted.length, 399);
  assert.ok(
    emitted.every((id) => BigInt(id) > BigInt(baseline)),
    'the sweep emitted a message that is already in the archive',
  );
  assert.ok(
    emitted.every((id) => BigInt(id) < BigInt(upper)),
    'the sweep emitted a message above its window',
  );
  // The shard, its serial retry, then 4 sweep pages (399 messages at 100/page).
  assert.equal(calls, 6);
  // Starting from the layout's upper bound - not from the newest message in the
  // channel - is what keeps the sweep inside the catch-up window.
  assert.equal(requests[2], upper, 'the sweep must start from the layout upper bound');
  assert.ok(
    requests.slice(2).every((before) => BigInt(before!) <= BigInt(upper)),
    'the sweep walked past its window',
  );
});

test('a resume with completed shards never falls back to a linear sweep', async () => {
  // Shard 0 is already done; shard 1 keeps failing. Sweeping between them would
  // traverse - and duplicate - shard 0, so the fallback has to stay off.
  const channelId = oldChannelId();
  const segments = calculateTimeSegments(channelId, 2);
  const state = createResumeState(segments, 2);
  markSegmentDone(state, 0);

  let calls = 0;
  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages() {
        calls++;
        throw new ExtractorError('unavailable', { kind: 'server', retryable: false });
      },
    } as unknown as UserTokenClient,
    quiet,
  );

  const signal: AbortState = { aborted: false };
  const result = await fetcher.fetchChannel(channelId, signal, () => {}, {
    parallelism: 2,
    segments,
    pageDelayMs: 0,
    resumeState: state,
  });

  assert.equal(result.fetched, 0);
  // Shard 1, then its serial retry. A sweep would be a third call.
  assert.equal(calls, 2);
  assert.equal(getSegmentState(result.resumeState, 0).done, true, 'completed shard must stay complete');
  assert.equal(result.segmentErrors.length, 1);
});
