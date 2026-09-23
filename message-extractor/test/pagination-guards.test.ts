// Pagination guard tests for the sequential fallback.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { UserTokenFetcher } from '../src/userTokenFetcher.js';
import { UserTokenClient } from '../src/userTokenClient.js';
import { ExtractorError } from '../src/errors.js';
import { Logger, tsToSnowflake } from '../src/utils.js';
import { createResumeState } from '../src/resumable.js';
import type { AbortState } from '../src/types.js';

const quiet = new Logger('error');

test('the sequential sweep refuses to spin when Discord repeats a page', async () => {
  const channelId = tsToSnowflake(Date.now() - 1000);
  const baseline = (BigInt(channelId) + 500n).toString();
  const upper = (BigInt(channelId) + 900n).toString();
  const segments = [{ after: baseline, before: upper }];
  // A full page, always the same one: `before` never moves. Its oldest id sits
  // above the baseline, so the sweep keeps paginating instead of deciding it
  // reached the lower bound on the first page. Discord returns pages newest
  // first, so the last element is the oldest. Every id derives from the one
  // channelId, because tsToSnowflake truncates to the millisecond and two
  // Date.now() calls a tick apart would produce unrelated ids.
  const stuck = Array.from({ length: 100 }, (_, i) => ({
    id: (BigInt(channelId) + BigInt(600 + (99 - i))).toString(),
    content: `msg-${i}`,
    timestamp: new Date().toISOString(),
    author: { id: 'u1', username: 'tester', discriminator: '0' },
  }));

  let calls = 0;
  const fetcher = new UserTokenFetcher(
    {
      async fetchMessages() {
        calls++;
        // Shard attempt and serial retry both fail, so the sweep is reached.
        if (calls <= 2) {
          throw new ExtractorError('shard unavailable', { kind: 'server', retryable: false });
        }
        // Without the guard this keeps returning forever; the cap turns that
        // into a failure the assertions below can read.
        if (calls > 10) {
          throw new ExtractorError('mock page cap reached', { kind: 'server', retryable: false });
        }
        return stuck;
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
  });

  assert.equal(result.usedFallback, true);
  const stall = result.segmentErrors.find((e) => /stalled/i.test(e.message));
  assert.ok(
    stall,
    `expected a stalled-cursor error, got: ${result.segmentErrors.map((e) => e.message).join(' | ') || 'none'}`,
  );
  // The sweep must bail on the second identical page, not burn the mock's budget.
  assert.ok(calls <= 5, `expected the sweep to stop early, made ${calls} calls`);
});
