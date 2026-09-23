// Feature tests: density-balanced shards, incremental catch-up, live capture.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { UserTokenExtractor } from '../src/userTokenExtractor.js';
import { JsonStorage } from '../src/jsonStorage.js';
import { Storage } from '../src/storage.js';
import { LiveCapture, type LiveMessageSource } from '../src/liveCapture.js';
import { balanceSegmentsByDensity, massBetween } from '../src/segments.js';
import { Logger, snowflakeToTs, tsToSnowflake } from '../src/utils.js';
import {
  buildBalance,
  cloneResumeState,
  createResumeState,
  parseResumeState,
  serializeResumeState,
} from '../src/resumable.js';
import {
  describeBalance,
  formatCount,
  PROBES_PER_SHARD,
  resolveProbeCount,
} from '../src/segments.js';
import { CatchUpScheduler, planCatchUp } from '../src/scheduler.js';
import type { ChannelProgress, DiscordMessage, ScraperConfig } from '../src/types.js';

const quiet = new Logger('error');
const DAY_MS = 24 * 60 * 60 * 1000;

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-feat-'));
}

function makeConfig(dir: string, overrides: Partial<ScraperConfig> = {}): ScraperConfig {
  return {
    botToken: '',
    userToken: 'test-token',
    dbPath: join(dir, 'test.db'),
    parallelism: 1,
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

function gatewayMessage(id: string, channelId: string): DiscordMessage {
  return {
    id,
    channel_id: channelId,
    content: `msg-${id}`,
    timestamp: new Date().toISOString(),
    author: { id: 'u1', username: 'tester', discriminator: '0' },
  };
}

/** Serve descending message pages based on the `before` cursor. */
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

// ==================== Density-balanced shards ====================

test('density-balanced shards stay contiguous and even out message load', () => {
  const now = Date.now();
  const channelId = tsToSnowflake(now - 10 * DAY_MS);
  const start = snowflakeToTs(channelId);

  // Sparse for the first half of the channel's life, then 100x denser.
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

  // Full coverage with no gaps and no overlaps.
  assert.equal(balanced.segments[0].after, channelId);
  assert.equal(balanced.segments[3].before, tsToSnowflake(now + 60_000));
  for (let i = 1; i < balanced.segments.length; i++) {
    assert.equal(balanced.segments[i - 1].before, balanced.segments[i].after, `shard ${i} is not contiguous`);
  }
  for (const s of balanced.segments) {
    assert.ok(BigInt(s.after) < BigInt(s.before), 'shard window must be non-empty');
  }

  // Reconstruct the same piecewise density the balancer integrates over.
  const points: { t: number; rate: number }[] = [{ t: start, rate: 1e-6 }];
  probes.forEach((p) => points.push({ t: p.timestampMs, rate: p.ratePerMs }));
  points.push({ t: now + 60_000, rate: 1e-4 });

  const loads = balanced.segments.map((s) =>
    massBetween(snowflakeToTs(s.after), snowflakeToTs(s.before), points),
  );
  const mean = loads.reduce((sum, x) => sum + x, 0) / loads.length;
  const spread = (Math.max(...loads) - Math.min(...loads)) / mean;

  assert.ok(spread < 0.3, `shards are not balanced (spread ${spread.toFixed(2)})`);
  assert.ok(balanced.imbalance !== null && balanced.imbalance < 0.3);

  // The dense tail must absorb most of the boundaries, not split evenly.
  const boundaryTs = balanced.segments.slice(0, -1).map((s) => snowflakeToTs(s.before));
  const lateBoundaries = boundaryTs.filter((t) => t > start + 5 * DAY_MS).length;
  assert.ok(lateBoundaries >= 3, `expected boundaries to favour the dense region, got ${lateBoundaries}`);
});

test('density balancing falls back to equal-time shards without enough probes', () => {
  const now = Date.now();
  const channelId = tsToSnowflake(now - 10 * DAY_MS);
  const start = snowflakeToTs(channelId);

  const balanced = balanceSegmentsByDensity(
    channelId,
    3,
    [{ timestampMs: start + DAY_MS, ratePerMs: 0.001, sampleCount: 1 }],
    now,
  );

  assert.equal(balanced.strategy, 'equal-time');
  assert.equal(balanced.segments.length, 3);
  assert.equal(balanced.imbalance, null);
});

// ==================== Incremental catch-up ====================

test('incremental catch-up fetches only messages newer than the last run', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);

  const older = Array.from({ length: 120 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString());
  const newer = Array.from({ length: 30 }, (_, i) => (BigInt(channelId) + BigInt(1000 + i)).toString());

  // Only the messages that exist at each point in time are served; `newer`
  // "arrives" between the two runs.
  let visible = [...older].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = visible
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
  const mock = { restore: () => { globalThis.fetch = originalFetch; } };

  const info = { channelId, channelName: 'incremental', guildId: 'g1', guildName: 'g' };

  try {
    const full = new UserTokenExtractor(makeConfig(dir), quiet);
    await full.init();
    const first = await full.extractChannel(info, { resume: true });
    full.close();
    assert.equal(first.success, true, `full run failed: ${first.error}`);
    assert.equal(first.messagesExtracted, 120);

    const jar = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 });
    const baseline = jar.loadAllMessages(channelId).map((m) => m.id).sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1))[0];
    assert.equal(baseline, older[119], 'newest_message_id should be the max id of the first run');

    // 30 new messages arrive, then a routine catch-up runs.
    visible = [...older, ...newer].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

    const inc = new UserTokenExtractor(makeConfig(dir), quiet);
    await inc.init();
    const second = await inc.extractChannel(info, { resume: true, incremental: true });
    inc.close();

    assert.equal(second.success, true, `incremental run failed: ${second.error}`);
    assert.equal(second.messagesExtracted, 30, 'incremental run re-fetched old messages');

    const allRows = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 }).loadAllMessages(channelId);
    assert.equal(allRows.length, 150, 'incremental append duplicated or dropped messages');
    assert.equal(new Set(allRows.map((m) => m.id)).size, 150);
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('incremental catch-up with no baseline falls back to a full extraction', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const ids = Array.from({ length: 60 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString())
    .sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const mock = installPagedFetch(ids);
  const info = { channelId, channelName: 'fresh', guildId: 'g1', guildName: 'g' };

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
    await extractor.init();
    const result = await extractor.extractChannel(info, { resume: true, incremental: true });
    extractor.close();

    assert.equal(result.success, true, `run failed: ${result.error}`);
    assert.equal(result.messagesExtracted, 60);
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Crash window ====================

test('a crash window does not double-store messages on the next run', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const jsonPath = join(dir, 'test_json');

  const stored = Array.from({ length: 50 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString());
  // Written before the crash, but the baseline never got to record them.
  const orphaned = Array.from({ length: 10 }, (_, i) => (BigInt(channelId) + BigInt(101 + i)).toString());
  const desc = (ids: string[]) => [...ids].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  let visible = desc(stored);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = visible.filter((id) => bound === null || BigInt(id) < bound).slice(0, 100);
    return Response.json(
      page.map((id) => ({
        id,
        content: `msg-${id}`,
        timestamp: new Date().toISOString(),
        author: { id: 'u1', username: 'tester', discriminator: '0' },
      })),
    );
  }) as typeof fetch;

  const info = { channelId, channelName: 'crash', guildId: 'g1', guildName: 'g' };

  try {
    const first = new UserTokenExtractor(makeConfig(dir), quiet);
    await first.init();
    const run1 = await first.extractChannel(info, { resume: true });
    first.close();
    assert.equal(run1.success, true, `run 1 failed: ${run1.error}`);
    assert.equal(run1.messagesExtracted, 50);

    // Simulate the crash: ten more messages reached the archive, but the
    // progress row still points the baseline at message 50.
    const crashed = new JsonStorage(jsonPath, quiet, { chunkSize: 50 });
    const wrote = crashed.appendMessages(
      channelId,
      'crash',
      orphaned.map((id) => ({
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
      })),
    );
    assert.equal(wrote.written, 10);

    // The next run re-fetches them - and must recognise them as archived.
    visible = desc([...stored, ...orphaned]);
    const second = new UserTokenExtractor(makeConfig(dir), quiet);
    await second.init();
    const run2 = await second.extractChannel(info, { resume: true });
    second.close();

    assert.equal(run2.success, true, `run 2 failed: ${run2.error}`);
    assert.equal(run2.messagesExtracted, 0, 're-fetched messages were written again');
    assert.equal(run2.duplicatesSkipped, 10, 'the duplicate guard did not report its skips');

    const rows = new JsonStorage(jsonPath, quiet, { chunkSize: 50 }).loadAllMessages(channelId);
    assert.equal(rows.length, 60);
    assert.equal(new Set(rows.map((r) => r.id)).size, 60, 'the archive holds duplicates');
  } finally {
    globalThis.fetch = originalFetch;
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Auto incremental on plain extract ====================

test('a plain extract of a done channel catches up instead of doing nothing', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);

  const base = Array.from({ length: 40 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString());
  const arrived = Array.from({ length: 10 }, (_, i) => (BigInt(channelId) + BigInt(500 + i)).toString());

  let visible = [...base].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = visible.filter((id) => bound === null || BigInt(id) < bound).slice(0, 100);
    return Response.json(
      page.map((id) => ({
        id,
        content: `msg-${id}`,
        timestamp: new Date().toISOString(),
        author: { id: 'u1', username: 'tester', discriminator: '0' },
      })),
    );
  }) as typeof fetch;

  const info = { channelId, channelName: 'auto', guildId: 'g1', guildName: 'g' };

  try {
    const first = new UserTokenExtractor(makeConfig(dir), quiet);
    await first.init();
    const run1 = await first.extractChannel(info, { resume: true });
    first.close();
    assert.equal(run1.success, true, `initial run failed: ${run1.error}`);
    assert.equal(run1.messagesExtracted, 40);

    visible = [...base, ...arrived].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

    // No --incremental flag: the channel is done, so this becomes a catch-up.
    const second = new UserTokenExtractor(makeConfig(dir), quiet);
    await second.init();
    const run2 = await second.extractChannel(info, { resume: true });
    second.close();

    assert.equal(run2.success, true, `catch-up failed: ${run2.error}`);
    assert.equal(run2.messagesExtracted, 10, 'plain extract did not catch up new messages');

    const rows = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 }).loadAllMessages(channelId);
    assert.equal(rows.length, 50);
    assert.equal(new Set(rows.map((m) => m.id)).size, 50);

    // Running it again finds nothing new.
    const third = new UserTokenExtractor(makeConfig(dir), quiet);
    await third.init();
    const run3 = await third.extractChannel(info, { resume: true });
    third.close();
    assert.equal(run3.success, true);
    assert.equal(run3.messagesExtracted, 0);
  } finally {
    globalThis.fetch = originalFetch;
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Catch-up scheduler ====================

function progressRow(overrides: Partial<ChannelProgress>): ChannelProgress {
  return {
    channel_id: 'c1',
    server_id: 'g1',
    channel_name: 'chan',
    last_message_id: null,
    last_extracted_ts: null,
    total_extracted: 0,
    status: 'done',
    error_message: null,
    started_at: null,
    completed_at: null,
    updated_at: new Date().toISOString(),
    resume_state: null,
    newest_message_id: null,
    ...overrides,
  };
}

test('planCatchUp selects only channels that have a baseline', () => {
  const plan = planCatchUp(
    [
      progressRow({ channel_id: 'a', newest_message_id: '1000' }),
      progressRow({ channel_id: 'b', newest_message_id: null }),
      progressRow({ channel_id: 'c', newest_message_id: '2000', status: 'live' }),
      progressRow({ channel_id: 'd', newest_message_id: '3000', status: 'extracting' }),
    ],
    { activeChannelIds: ['c'] },
  );

  assert.deepEqual(plan.due.map((d) => d.channelId), ['a']);
  assert.deepEqual(
    Object.fromEntries(plan.skipped.map((s) => [s.channelId, s.reason])),
    { b: 'no-baseline', c: 'live', d: 'in-progress' },
  );
});

test('planCatchUp can opt into first-time channels', () => {
  const plan = planCatchUp([progressRow({ channel_id: 'b', newest_message_id: null })], {
    includeMissingBaseline: true,
  });
  assert.equal(plan.due.length, 1);
  assert.equal(plan.due[0].baseline, '');
  assert.equal(plan.skipped.length, 0);
});

test('scheduler cycle catches up known channels and skips the rest', async () => {
  const dir = makeTmpDir();
  const ready = tsToSnowflake(Date.now() - 1000);
  const empty = tsToSnowflake(Date.now() - 2000);

  const base = Array.from({ length: 20 }, (_, i) => (BigInt(ready) + BigInt(i + 1)).toString());
  const arrived = Array.from({ length: 4 }, (_, i) => (BigInt(ready) + BigInt(900 + i)).toString());
  let visible = [...base].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const channelId = url.pathname.split('/channels/')[1]?.split('/')[0];
    if (channelId !== ready) return Response.json([]); // the never-populated channel
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = visible.filter((id) => bound === null || BigInt(id) < bound).slice(0, 100);
    return Response.json(
      page.map((id) => ({
        id,
        content: `msg-${id}`,
        timestamp: new Date().toISOString(),
        author: { id: 'u1', username: 'tester', discriminator: '0' },
      })),
    );
  }) as typeof fetch;

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
    await extractor.init();

    // Seed: one channel with history, one that ends up with no messages.
    await extractor.extractChannel(
      { channelId: ready, channelName: 'ready', guildId: 'g1', guildName: 'g' },
      { resume: true },
    );
    await extractor.extractChannel(
      { channelId: empty, channelName: 'empty', guildId: 'g1', guildName: 'g' },
      { resume: true },
    );

    visible = [...base, ...arrived].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

    const scheduler = new CatchUpScheduler(extractor, { log: quiet, intervalMs: 1000 });
    const result = await scheduler.runCycle();

    assert.equal(result.attempted, 1, 'only the channel with a baseline should be attempted');
    assert.equal(result.messages, 4);
    assert.equal(result.failures, 0);
    assert.deepEqual(
      result.skipped.map((s) => s.reason),
      ['no-baseline'],
    );

    const rows = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 }).loadAllMessages(ready);
    assert.equal(rows.length, 24);
    assert.equal(new Set(rows.map((m) => m.id)).size, 24);

    extractor.close();
  } finally {
    globalThis.fetch = originalFetch;
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Reset ====================

test('reset clears the catch-up baseline so the next run re-extracts', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const ids = Array.from({ length: 40 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString())
    .sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const mock = installPagedFetch(ids);
  const info = { channelId, channelName: 'reset', guildId: 'g1', guildName: 'g' };

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
    await extractor.init();

    const first = await extractor.extractChannel(info, { resume: true });
    assert.equal(first.success, true, `first run failed: ${first.error}`);
    assert.equal(first.messagesExtracted, 40);

    const beforeReset = await extractor.getProgressDetail(channelId);
    assert.ok(beforeReset?.newest_message_id, 'expected a baseline after the first run');

    await extractor.resetChannel(channelId);

    const afterReset = await extractor.getProgressDetail(channelId);
    assert.equal(afterReset?.newest_message_id, null, 'reset left a stale baseline behind');
    assert.equal(afterReset?.total_extracted, 0);
    assert.equal(afterReset?.resume_state, null);

    // With no baseline the plain extract must be a real re-extraction, not a
    // one-request catch-up from an id whose data was just deleted.
    const second = await extractor.extractChannel(info, { resume: true });
    extractor.close();

    assert.equal(second.success, true, `second run failed: ${second.error}`);
    assert.equal(second.messagesExtracted, 40, 'reset did not force a full re-extraction');

    const rows = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 }).loadAllMessages(channelId);
    assert.equal(rows.length, 40);
    assert.equal(new Set(rows.map((m) => m.id)).size, 40);
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Active writers ====================

test('the extractor reports the channels it is actively writing', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
  const source = new FakeSource();

  try {
    await extractor.init();
    assert.deepEqual(extractor.activeChannelIds(), [], 'nothing should be active yet');

    const capture = extractor.createLiveCapture(source, {
      flushIntervalMs: 60_000,
      batchMessages: 1000,
    });
    await capture.start([{ channelId, channelName: 'active', guildId: 'g1', guildName: 'g' }]);

    assert.deepEqual(extractor.activeChannelIds(), [channelId]);

    // The scheduler must therefore leave it alone: it has no baseline yet, but
    // `live` is the reason that gets reported.
    const scheduler = new CatchUpScheduler(extractor, { log: quiet });
    const plan = await scheduler.plan();
    assert.equal(plan.due.length, 0);
    assert.deepEqual(plan.skipped, [{ channelId, reason: 'live' }]);

    await capture.stop();
    assert.deepEqual(extractor.activeChannelIds(), [], 'a stopped capture must stop blocking');
  } finally {
    extractor.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the scheduler takes active channels from the extractor', async () => {
  // Guards the wiring, not the rule: `planCatchUp` is covered separately.
  const fake = {
    knownChannels: async () => [
      progressRow({ channel_id: 'a', newest_message_id: '10' }),
      progressRow({ channel_id: 'b', newest_message_id: '20' }),
    ],
    activeChannelIds: () => ['b'],
    extractAll: async () => [],
  } as unknown as UserTokenExtractor;

  const plan = await new CatchUpScheduler(fake, { log: quiet }).plan();
  assert.deepEqual(plan.due.map((d) => d.channelId), ['a']);
  assert.deepEqual(plan.skipped, [{ channelId: 'b', reason: 'live' }]);
});

// ==================== Probe tuning ====================

test('probe count scales with shard count and honours an override', () => {
  assert.equal(resolveProbeCount(1), 4); // floor
  assert.equal(resolveProbeCount(6), 6 * PROBES_PER_SHARD);
  assert.equal(resolveProbeCount(6, 5), 5); // explicit override wins
  assert.equal(resolveProbeCount(6, 10_000), 36); // capped
  assert.equal(resolveProbeCount(6, 0), 6 * PROBES_PER_SHARD); // 0 means "derive"
  assert.ok(resolveProbeCount(12) <= 36, 'never exceeds the probe cap');
});

// ==================== Balance summary ====================

test('a balance estimate survives the resume-state round trip', () => {
  const segments = [
    { after: '1', before: '2' },
    { after: '2', before: '3' },
  ];
  const state = createResumeState(
    segments,
    2,
    buildBalance({ strategy: 'density', imbalance: 0.1, loads: [100, 110], probes: 8 }),
  );

  const round = parseResumeState(serializeResumeState(state));
  assert.ok(round, 'resume state failed to round trip');
  assert.deepEqual(round!.balance!.loads, [100, 110]);
  assert.equal(round!.balance!.minLoad, 100);
  assert.equal(round!.balance!.maxLoad, 110);
  assert.equal(round!.balance!.probes, 8);

  assert.equal(
    describeBalance(round!.balance),
    'density, 2 shards, ~100-110 msgs/shard, imbalance 10%, 8 probes',
  );

  // Cloning must not alias the loads array.
  const clone = cloneResumeState(round!);
  clone.balance!.loads![0] = 999;
  assert.equal(round!.balance!.loads![0], 100);
});

test('a fresh extraction plumbs the probe override through to the balancer', async () => {
  const dir = makeTmpDir();
  const now = Date.now();
  const spanMs = 30 * DAY_MS;
  const channelId = tsToSnowflake(now - spanMs);

  // 200 messages spread evenly across the channel's life, so every probe has data.
  const ids: string[] = [];
  const byId = new Map<string, string>();
  for (let i = 0; i < 200; i++) {
    const ts = now - spanMs + Math.floor((spanMs * (i + 1)) / 200);
    const id = tsToSnowflake(ts);
    ids.push(id);
    byId.set(id, new Date(ts).toISOString());
  }
  const descending = [...ids].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = descending.filter((id) => bound === null || BigInt(id) < bound).slice(0, 100);
    return Response.json(
      page.map((id) => ({
        id,
        content: `msg-${id}`,
        timestamp: byId.get(id),
        author: { id: 'u1', username: 'tester', discriminator: '0' },
      })),
    );
  }) as typeof fetch;

  try {
    const extractor = new UserTokenExtractor(
      makeConfig(dir, { parallelism: 4, balanceShards: true, densityProbes: 6 }),
      quiet,
    );
    await extractor.init();

    const result = await extractor.extractChannel(
      { channelId, channelName: 'balanced', guildId: 'g1', guildName: 'g' },
      { resume: true },
    );
    assert.equal(result.success, true, `extraction failed: ${result.error}`);
    assert.equal(result.messagesExtracted, 200);

    const detail = await extractor.getProgressDetail(channelId);
    const state = parseResumeState(detail?.resume_state ?? null);
    extractor.close();

    assert.ok(state?.balance, 'no balance was persisted for a balanced run');
    // 6 proves DENSITY_PROBES reached probeDensity; loads proves the plan stuck.
    assert.equal(state!.balance!.probes, 6);
    assert.equal(state!.balance!.strategy, 'density');
    assert.equal(state!.balance!.loads!.length, 4);
    assert.ok(state!.balance!.minLoad! <= state!.balance!.maxLoad!);
  } finally {
    globalThis.fetch = originalFetch;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('equal-time layouts report no density estimate', () => {
  assert.equal(describeBalance(null), null);
  assert.equal(
    describeBalance({ strategy: 'equal-time', imbalance: null, loads: null }),
    'equal-time (no density estimate)',
  );
});

test('counts are formatted compactly for status output', () => {
  assert.equal(formatCount(999), '999');
  assert.equal(formatCount(1500), '1.5K');
  assert.equal(formatCount(2_400_000), '2.4M');
});

// ==================== Live capture ====================

class FakeSource implements LiveMessageSource {
  private handlers: ((message: DiscordMessage) => void)[] = [];
  started = false;

  onMessage(handler: (message: DiscordMessage) => void): void {
    this.handlers.push(handler);
  }

  async start(): Promise<void> {
    this.started = true;
  }

  close(): void {
    this.started = false;
  }

  emit(message: DiscordMessage): void {
    for (const handler of this.handlers) handler(message);
  }
}

test('live capture appends into the same archive without duplicates', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const jsonPath = join(dir, 'test_json');

  const storage = new Storage(join(dir, 'test.db'), quiet);
  await storage.init();
  const jsonStorage = new JsonStorage(jsonPath, quiet, { chunkSize: 50 });

  try {
    // Pre-seed an archive the way a bulk extraction would leave it.
    const seeded = Array.from({ length: 120 }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString());
    jsonStorage.appendMessages(
      channelId,
      'live-test',
      seeded.map((id) => ({
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
      })),
    );
    jsonStorage.finalizeChannel(channelId);
    assert.equal(jsonStorage.loadArchive(channelId)!.totalParts, 3);

    const source = new FakeSource();
    const capture = new LiveCapture({
      storage,
      jsonStorage,
      source,
      log: quiet,
      flushIntervalMs: 60_000, // rely on stop() to flush instead of the timer
      batchMessages: 1000,
      maxBufferPerChannel: 1000,
    });

    await capture.start([{ channelId, channelName: 'live-test', guildId: 'g1', guildName: 'g' }]);
    assert.equal(source.started, true);

    const live = Array.from({ length: 30 }, (_, i) => (BigInt(channelId) + BigInt(1000 + i)).toString());
    for (const id of live) source.emit(gatewayMessage(id, channelId));
    // A duplicate delivery must not be stored twice.
    source.emit(gatewayMessage(live[0], channelId));

    const summary = await capture.stop();

    assert.equal(summary.messages, 30, 'duplicate live delivery was counted twice');
    assert.equal(summary.channels[0].sessionMessages, 30);

    const archive = jsonStorage.loadArchive(channelId)!;
    assert.equal(archive.totalMessages, 150);
    // Part numbering continues instead of restarting at 1.
    assert.equal(archive.totalParts, 4);

    const rows = jsonStorage.loadAllMessages(channelId);
    assert.equal(rows.length, 150, 'live append lost or duplicated messages');
    assert.equal(new Set(rows.map((m) => m.id)).size, 150);

    const parts = new Set(archive.parts.map((p) => p.part));
    assert.equal(parts.size, archive.parts.length, 'duplicate part numbers across bulk + live');

    // Files on disk match the manifest (no stray temp files, no extra parts).
    const files = readdirSync(join(jsonPath, channelId)).filter(
      (f) => f.endsWith('.json') && f !== '_archive.json',
    );
    assert.equal(files.length, archive.parts.length);
    assert.equal(files.some((f) => f.includes('.tmp-')), false);
  } finally {
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('live capture never re-archives messages at or below the stored baseline', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const baseline = (BigInt(channelId) + 100n).toString();

  const storage = new Storage(join(dir, 'test.db'), quiet);
  await storage.init();
  const jsonStorage = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 });
  const source = new FakeSource();

  try {
    // Pretend a bulk extraction already archived everything up to `baseline`.
    await storage.updateProgress(channelId, {
      status: 'done',
      total_extracted: 100,
      newest_message_id: baseline,
    });

    const capture = new LiveCapture({
      storage,
      jsonStorage,
      source,
      log: quiet,
      flushIntervalMs: 60_000,
      batchMessages: 1000,
    });
    await capture.start([{ channelId, channelName: 'floor', guildId: 'g1', guildName: 'g' }]);

    // A replay of archived history (both below and exactly at the baseline),
    // then one genuinely new message.
    source.emit(gatewayMessage((BigInt(channelId) + 50n).toString(), channelId));
    source.emit(gatewayMessage(baseline, channelId));
    source.emit(gatewayMessage((BigInt(channelId) + 101n).toString(), channelId));

    const summary = await capture.stop();

    assert.equal(summary.messages, 1, 'replayed history was appended again');
    const archive = jsonStorage.loadArchive(channelId)!;
    assert.equal(archive.totalMessages, 1);
    assert.deepEqual(
      jsonStorage.loadAllMessages(channelId).map((m) => m.id),
      [(BigInt(channelId) + 101n).toString()],
    );
  } finally {
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('live capture marks the channel live and advances the newest id', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);

  const storage = new Storage(join(dir, 'test.db'), quiet);
  await storage.init();
  const jsonStorage = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 });
  const source = new FakeSource();

  try {
    const capture = new LiveCapture({
      storage,
      jsonStorage,
      source,
      log: quiet,
      flushIntervalMs: 60_000,
      batchMessages: 1000,
    });

    await capture.start([{ channelId, channelName: 'live-status', guildId: 'g1', guildName: 'g' }]);

    const before = await storage.getProgress(channelId);
    assert.equal(before?.status, 'live');

    source.emit(gatewayMessage((BigInt(channelId) + 5n).toString(), channelId));
    source.emit(gatewayMessage((BigInt(channelId) + 9n).toString(), channelId));

    const summary = await capture.stop();
    assert.equal(summary.messages, 2);

    const after = await storage.getProgress(channelId);
    assert.equal(after?.status, 'done');
    assert.equal(after?.newest_message_id, (BigInt(channelId) + 9n).toString());
    assert.equal(after?.total_extracted, 2);
  } finally {
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
