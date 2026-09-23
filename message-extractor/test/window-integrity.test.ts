// Window-selection tests: an incremental catch-up must never discard pending shards.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { UserTokenExtractor } from '../src/userTokenExtractor.js';
import { JsonStorage } from '../src/jsonStorage.js';
import { Storage } from '../src/storage.js';
import { Logger, calculateTimeSegments, snowflakeToTs, tsToSnowflake } from '../src/utils.js';
import {
  createResumeState,
  decideIncrementalWindow,
  markSegmentDone,
  parseResumeState,
  serializeResumeState,
} from '../src/resumable.js';
import type { ExportRow, ScraperConfig } from '../src/types.js';

const quiet = new Logger('error');
const DAY_MS = 24 * 60 * 60 * 1000;

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-window-'));
}

function makeConfig(dir: string, overrides: Partial<ScraperConfig> = {}): ScraperConfig {
  return {
    botToken: '',
    userToken: 'test-token',
    dbPath: join(dir, 'test.db'),
    parallelism: 3,
    chunkSize: 50,
    pageDelayMs: 0,
    maxRetries: 1,
    backoffBaseMs: 1,
    liveMaxBuffer: 1000,
    logLevel: 'error',
    requestsPerSecond: 1000,
    timeoutMs: 2000,
    maxBackoffMs: 5,
    prettyJson: false,
    balanceShards: false,
    ...overrides,
  };
}

function exportRow(id: string): ExportRow {
  return {
    id,
    author: 'tester#0 (u1)',
    content: `msg-${id}`,
    timestamp: new Date().toISOString(),
    replyTo: null,
    replyToAuthor: null,
    attachments: 0,
    attachmentUrls: [],
    imageUrls: [],
    attachmentsDetailed: [],
    threadId: null,
  };
}

/** Serve descending message pages based on the `before` cursor, like Discord. */
function installPagedFetch(idsDescending: string[]) {
  const original = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = idsDescending
      .filter((id) => bound === null || BigInt(id) < bound)
      .slice(0, 100);
    return Response.json(
      page.map((id) => ({
        id,
        content: `msg-${id}`,
        timestamp: new Date().toISOString(),
        author: { id: 'u1', username: 'tester', discriminator: '0' },
      })),
    );
  }) as typeof fetch;
  return { restore: () => { globalThis.fetch = original; } };
}

test('decideIncrementalWindow allows a catch-up only when nothing is left pending', () => {
  const channelId = tsToSnowflake(Date.now() - 30 * DAY_MS);
  const segments = calculateTimeSegments(channelId, 3);

  // No baseline at all: there is nothing to catch up from.
  assert.deepEqual(decideIncrementalWindow(null, null), {
    useIncrementalWindow: false,
    reuseSavedWindow: false,
    reason: 'no-baseline',
  });

  // A completed run: every shard is done, so a catch-up window is safe.
  const complete = createResumeState(segments, 3);
  for (const s of complete.segments) markSegmentDone(complete, s.index);
  assert.deepEqual(decideIncrementalWindow('999', complete), {
    useIncrementalWindow: true,
    reuseSavedWindow: false,
    reason: 'ok',
  });

  // A previous *incremental* run that died mid-window: the saved state IS the
  // catch-up window and must be resumed, not replaced.
  const failedCatchUp = createResumeState([{ after: '500', before: '900' }], 1);
  assert.deepEqual(decideIncrementalWindow('500', failedCatchUp), {
    useIncrementalWindow: true,
    reuseSavedWindow: true,
    reason: 'ok',
  });

  // The data-loss case: a multi-shard run with a hole in the middle. Taking a
  // one-segment catch-up here would discard shard 1's window for good.
  const partial = createResumeState(segments, 3);
  markSegmentDone(partial, 0);
  markSegmentDone(partial, 2);
  assert.deepEqual(decideIncrementalWindow('999', partial), {
    useIncrementalWindow: false,
    reuseSavedWindow: false,
    reason: 'pending-shards',
  });
});

test('an incremental catch-up on a channel with pending shards does not orphan them', async () => {
  const dir = makeTmpDir();
  const jsonPath = join(dir, 'test_json');
  const channelId = tsToSnowflake(Date.now() - 30 * DAY_MS);
  const info = { channelId, channelName: 'partial', guildId: 'g1', guildName: 'g' };

  const segments = calculateTimeSegments(channelId, 3);
  // One message at the midpoint of each shard's window.
  const idAt = (index: number): string => {
    const seg = segments[index];
    const lo = snowflakeToTs(seg.after);
    const hi = snowflakeToTs(seg.before);
    return (BigInt(tsToSnowflake(lo + Math.floor((hi - lo) / 2))) + BigInt(index + 1)).toString();
  };
  const low = [idAt(0)];
  const mid = [idAt(1)];
  const high = [idAt(2)];
  const all = [...low, ...mid, ...high].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  // Shards 0 and 2 completed; shard 1 failed before fetching anything. The
  // baseline points at the newest archived message, in shard 2.
  const saved = createResumeState(segments, 3);
  markSegmentDone(saved, 0);
  markSegmentDone(saved, 2);

  const seeded = new JsonStorage(jsonPath, quiet, { chunkSize: 50 });
  seeded.appendMessages(channelId, 'partial', [...low, ...high].map(exportRow));
  seeded.finalizeChannel(channelId);

  const mock = installPagedFetch(all);
  const extractor = new UserTokenExtractor(makeConfig(dir), quiet);

  try {
    await extractor.init();
    const storage = (extractor as unknown as { storage: Storage }).storage;
    await storage.getOrCreateProgress(channelId, 'g1', 'partial');
    await storage.updateProgress(channelId, {
      status: 'error',
      total_extracted: low.length + high.length,
      newest_message_id: high[0],
      resume_state: serializeResumeState(saved),
    });

    // This is exactly what the `watch` scheduler does to a failed channel.
    const result = await extractor.extractChannel(info, { resume: true, incremental: true });
    const finalState = parseResumeState((await storage.getProgress(channelId))?.resume_state ?? null);

    assert.equal(result.success, true, `run failed: ${result.error}`);
    assert.equal(
      finalState?.segments.length,
      3,
      'the run replaced the 3-shard layout with a catch-up window',
    );
    assert.equal(result.messagesExtracted, mid.length);

    const stored = new JsonStorage(jsonPath, quiet, { chunkSize: 50 })
      .loadAllMessages(channelId)
      .map((m) => m.id)
      .sort();
    assert.deepEqual(
      stored,
      [...low, ...mid, ...high].sort(),
      'the pending middle shard was orphaned by the catch-up',
    );
  } finally {
    extractor.close();
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});
