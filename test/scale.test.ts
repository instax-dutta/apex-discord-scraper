// Scale and resilience tests.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { UserTokenExtractor } from '../src/userTokenExtractor.js';
import { JsonStorage, sortBySnowflake } from '../src/jsonStorage.js';
import { UserTokenClient } from '../src/userTokenClient.js';
import { RateLimitFailsafe } from '../src/rateLimit.js';
import { isDiskFullError } from '../src/errors.js';
import { Logger, tsToSnowflake } from '../src/utils.js';
import type { ExportRow, ScraperConfig } from '../src/types.js';

const quiet = new Logger('error');

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-scale-'));
}

function makeConfig(dir: string, overrides: Partial<ScraperConfig> = {}): ScraperConfig {
  return {
    botToken: '',
    userToken: 'test-token',
    dbPath: join(dir, 'test.db'),
    parallelism: 1,
    chunkSize: 100,
    pageDelayMs: 0,
    maxRetries: 1,
    backoffBaseMs: 1,
    liveMaxBuffer: 10,
    logLevel: 'error',
    requestsPerSecond: 1000,
    timeoutMs: 2000,
    maxBackoffMs: 5,
    prettyJson: false,
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

function discordMessage(id: string) {
  return {
    id,
    content: `msg-${id}`,
    timestamp: new Date().toISOString(),
    author: { id: 'u1', username: 'tester', discriminator: '0' },
  };
}

/** Serve descending message pages based on the `before` cursor. */
function installPagedFetch(idsDescending: string[]) {
  const original = globalThis.fetch;
  let calls = 0;

  globalThis.fetch = (async (input: any) => {
    calls++;
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = idsDescending
      .filter((id) => bound === null || BigInt(id) < bound)
      .slice(0, 100);
    return Response.json(page.map(discordMessage));
  }) as typeof fetch;

  return { calls: () => calls, restore: () => { globalThis.fetch = original; } };
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

// ==================== End-to-end ====================

test('extracts a channel end-to-end, then catches up as a no-op', async () => {
  const dir = makeTmpDir();
  const N = 250;
  const channelId = tsToSnowflake(Date.now() - 1000);
  const ids = Array.from({ length: N }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString())
    .sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const mock = installPagedFetch(ids);
  const extractor = new UserTokenExtractor(makeConfig(dir, { chunkSize: 100 }), quiet);

  try {
    await extractor.init();
    const info = { channelId, channelName: 'scale-test', guildId: 'g1', guildName: 'guild' };

    const first = await extractor.extractChannel(info, { resume: true });
    assert.equal(first.success, true, `first run failed: ${first.error}`);
    assert.equal(first.messagesExtracted, N);
    assert.equal(first.jsonParts, 3);

    const stats = extractor.getChannelStats(channelId);
    assert.ok(stats);
    assert.equal(stats!.messages, N);
    assert.equal(stats!.jsonParts, 3);
    assert.equal(stats!.isComplete, true);
    assert.ok(stats!.sizeBytes > 0);

    const chunkDir = join(dir, 'test_json', channelId);
    const files = readdirSync(chunkDir);
    assert.equal(files.some((f) => f.includes('.tmp-')), false, 'temp files were left behind');

    const manifest = JSON.parse(readFileSync(join(chunkDir, '_archive.json'), 'utf-8'));
    assert.equal(manifest.totalMessages, N);
    assert.equal(manifest.totalParts, 3);

    // A completed channel is auto-promoted to an incremental catch-up: it
    // costs one window probe and finds nothing new.
    const callsBefore = mock.calls();
    const second = await extractor.extractChannel(info, { resume: true });
    assert.equal(second.success, true);
    assert.equal(second.messagesExtracted, 0);
    assert.equal(mock.calls(), callsBefore + 1, 'catch-up should need exactly one window');

    // ...and `full: true` opts out, restoring the pure no-op resume.
    const callsAfterCatchUp = mock.calls();
    const third = await extractor.extractChannel(info, { resume: true, full: true });
    assert.equal(third.success, true);
    assert.equal(third.messagesExtracted, 0);
    assert.equal(mock.calls(), callsAfterCatchUp, 'full resume re-fetched a completed channel');

    // Neither run duplicated anything.
    const all = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 100 })
      .loadAllMessages(channelId);
    assert.equal(all.length, N);
    assert.equal(new Set(all.map((m) => m.id)).size, N);
  } finally {
    extractor.close();
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('resume after an abort loses nothing and duplicates nothing', async () => {
  const dir = makeTmpDir();
  const N = 250;
  const channelId = tsToSnowflake(Date.now() - 1000);
  const ids = Array.from({ length: N }, (_, i) => (BigInt(channelId) + BigInt(i + 1)).toString())
    .sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const original = globalThis.fetch;
  let calls = 0;
  let first: UserTokenExtractor | null = null;

  globalThis.fetch = (async (input: any) => {
    calls++;
    const url = new URL(typeof input === 'string' ? input : input.url);
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = ids.filter((id) => bound === null || BigInt(id) < bound).slice(0, 100);

    // Abort mid-run, after the second page has been served.
    if (calls === 2 && first) {
      queueMicrotask(() => first!.abortChannel(channelId));
    }
    return Response.json(page.map(discordMessage));
  }) as typeof fetch;

  const info = { channelId, channelName: 'resume-test', guildId: 'g1', guildName: 'guild' };

  try {
    first = new UserTokenExtractor(makeConfig(dir, { chunkSize: 100 }), quiet);
    await first.init();
    const run1 = await first.extractChannel(info, { resume: true });
    first.close();
    assert.equal(run1.success, false, 'run 1 should have been aborted');

    // Fresh process/instance, same data dir.
    const second = new UserTokenExtractor(makeConfig(dir, { chunkSize: 100 }), quiet);
    await second.init();
    const run2 = await second.extractChannel(info, { resume: true });
    second.close();

    assert.equal(run2.success, true, `run 2 failed: ${run2.error}`);
    assert.equal(run1.messagesExtracted + run2.messagesExtracted, N);

    const all = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 100 }).loadAllMessages(channelId);
    assert.equal(all.length, N, 'duplicate messages were written across resume');
    assert.equal(new Set(all.map((m) => m.id)).size, N);
  } finally {
    globalThis.fetch = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a second Storage instance initializes independently', async () => {
  const dir = makeTmpDir();
  try {
    const { Storage } = await import('../src/storage.js');
    const a = new Storage(join(dir, 'a.db'), quiet);
    const b = new Storage(join(dir, 'b.db'), quiet);
    await a.init();
    await b.init();
    await a.updateProgress('chan-a', { status: 'done', total_extracted: 1, resume_state: null });
    await b.updateProgress('chan-b', { status: 'done', total_extracted: 2, resume_state: null });
    const aRows = await a.getAllProgress();
    const bRows = await b.getAllProgress();
    assert.equal(aRows.length, 1);
    assert.equal(bRows.length, 1);
    assert.equal(aRows[0].channel_id, 'chan-a');
    assert.equal(bRows[0].channel_id, 'chan-b');
    a.close();
    b.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a corrupt metadata file is quarantined instead of blocking every command', async () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'broken.db');

  try {
    writeFileSync(dbPath, Buffer.from('this is definitely not a sqlite database'));

    // Must not throw: without this, a truncated write makes the tool unusable
    // until someone deletes the file by hand.
    const { Storage } = await import('../src/storage.js');
    const storage = new Storage(dbPath, quiet);
    await storage.init();

    await storage.updateProgress('chan', { status: 'done', total_extracted: 7 });
    const rows = await storage.getAllProgress();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total_extracted, 7);
    storage.close();

    const quarantined = readdirSync(dir).filter((f) => f.startsWith('broken.db.corrupt-'));
    assert.equal(quarantined.length, 1, 'the unreadable file should be kept for inspection');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the legacy per-message dedup table is dropped and never recreated', async () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'meta.db');

  try {
    const { Storage } = await import('../src/storage.js');

    // A database written by an older version can still carry the table, and an
    // old install could have millions of dead rows in it.
    const stale = new Storage(dbPath, quiet);
    await stale.init();
    const staleHandle = (stale as any).db;
    staleHandle.run('CREATE TABLE dedup_cache (channel_id TEXT, message_id TEXT, extracted_at TEXT)');
    staleHandle.run("INSERT INTO dedup_cache VALUES ('c', 'm', 'now')");
    stale.close();

    const fresh = new Storage(dbPath, quiet);
    await fresh.init();
    const names = tableNames(fresh);
    fresh.close();

    assert.equal(names.includes('dedup_cache'), false, 'the dead table should be dropped on init');
    assert.ok(names.includes('channel_progress'), 'real tables must survive');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tableNames(storage: unknown): string[] {
  // The `query` command reaches for the raw handle in the same way: sql.js has
  // no public "list tables" API.
  const db = (storage as { db: any }).db;
  const stmt = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'");
  const names: string[] = [];
  while (stmt.step()) names.push((stmt.getAsObject() as { name: string }).name);
  stmt.free();
  return names;
}

// ==================== JSON storage at scale ====================

test('snowflake sorting is numeric and never reorders the caller\'s array', () => {
  const rows = [exportRow('9'), exportRow('10'), exportRow('100'), exportRow('2')];
  const sorted = sortBySnowflake(rows);

  // A string sort would put '10' before '2' and '9'.
  assert.deepEqual(sorted.map((r) => r.id), ['2', '9', '10', '100']);
  assert.deepEqual(rows.map((r) => r.id), ['9', '10', '100', '2'], 'input was mutated');
  assert.equal(sorted.length, rows.length);

  // A malformed id must not make an append throw mid-run.
  const mixed = sortBySnowflake([exportRow('20'), exportRow('not-an-id'), exportRow('3')]);
  assert.equal(mixed.length, 3);
});

test('manifest totals stay exact across appends and part rotations', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const storage = new JsonStorage(base, quiet, { chunkSize: 10 });
    const ids = (from: number, count: number) =>
      Array.from({ length: count }, (_, i) => exportRow(String(from + i)));

    // The open part grows on every append and is replaced by a chunk when it
    // fills, so the running totals have to survive replacement as well as
    // insertion.
    storage.appendMessages('c1', 'chan', ids(1, 7));
    let archive = storage.loadArchive('c1')!;
    assert.equal(archive.totalMessages, 7);
    assert.equal(archive.totalParts, 1);

    storage.appendMessages('c1', 'chan', ids(8, 3)); // fills part 1 exactly
    archive = storage.loadArchive('c1')!;
    assert.equal(archive.totalMessages, 10);
    assert.equal(archive.totalParts, 1);
    assert.equal(archive.parts[0].messageCount, 10);

    storage.appendMessages('c1', 'chan', ids(11, 5)); // opens part 2
    archive = storage.loadArchive('c1')!;
    assert.equal(archive.totalMessages, 15);
    assert.equal(archive.totalParts, 2);

    storage.appendMessages('c1', 'chan', ids(16, 4)); // grows part 2
    archive = storage.loadArchive('c1')!;
    assert.equal(archive.totalMessages, 19);
    assert.equal(archive.totalParts, 2);
    assert.equal(archive.parts.reduce((sum, p) => sum + p.messageCount, 0), 19);

    const rows = storage.loadAllMessages('c1');
    assert.equal(rows.length, 19);
    assert.equal(new Set(rows.map((r) => r.id)).size, 19);

    // No temp files survive an atomic chunk write.
    assert.equal(readdirSync(join(base, 'c1')).some((f) => f.includes('.tmp-')), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('json storage chunks, streams export, and rebuilds a lost manifest', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const storage = new JsonStorage(base, quiet, { chunkSize: 10 });
    const messages = Array.from({ length: 25 }, (_, i) => exportRow(String(1000 + i)));
    const { partsWritten, archive } = storage.appendMessages('chan1', 'chan', messages);

    // 25 messages at chunkSize 10 => two full parts plus the in-progress tail.
    assert.equal(partsWritten, 2);
    assert.equal(archive.totalMessages, 25);
    assert.equal(archive.totalParts, 3);
    assert.equal(readdirSync(join(base, 'chan1')).some((f) => f.includes('.tmp-')), false);

    const stats = storage.getStorageStats('chan1');
    assert.ok(stats);
    assert.equal(stats!.totalMessages, 25);
    assert.ok(stats!.totalSizeBytes > 0);

    const outPath = join(dir, 'export.json');
    const count = storage.exportToSingleJson('chan1', outPath);
    assert.equal(count, 25);
    assert.equal(JSON.parse(readFileSync(outPath, 'utf-8')).length, 25);

    // Simulate a lost manifest: a fresh instance must recover from chunk files.
    rmSync(join(base, 'chan1', '_archive.json'));
    const fresh = new JsonStorage(base, quiet, { chunkSize: 10 });
    const rebuilt = fresh.loadArchive('chan1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.equal(rebuilt!.totalMessages, 25);
    assert.equal(rebuilt!.totalParts, 3);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the open part is appended to, never rewritten', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const storage = new JsonStorage(base, quiet, { chunkSize: 1000 });
    storage.appendMessages('c1', 'chan', Array.from({ length: 5 }, (_, i) => exportRow(String(1000 + i))));

    const partPath = join(base, 'c1', 'chan-000001.json');
    const before = readFileSync(partPath, 'utf-8');

    // A chunk stored as a single JSON value would have to be rewritten in full
    // here; an append must leave every byte already on disk exactly as it was.
    storage.appendMessages('c1', 'chan', Array.from({ length: 5 }, (_, i) => exportRow(String(2000 + i))));

    const after = readFileSync(partPath, 'utf-8');
    assert.ok(after.startsWith(before), 'the open part was rewritten instead of appended to');

    // And it is JSON Lines: a header line, then one message per line.
    const lines = after.split('\n').filter((line) => line.length > 0);
    assert.equal(lines.length, 11, 'expected a header plus ten messages');
    assert.equal(JSON.parse(lines[0]).open, true);
    assert.equal(JSON.parse(lines[0]).part, 1);
    assert.equal(JSON.parse(lines[1]).id, '1000');
    assert.equal(JSON.parse(lines[10]).id, '2004');

    // Every flush is durable on its own: a fresh process reads the rows back
    // from the open part without it having been compacted first.
    const fresh = new JsonStorage(base, quiet, { chunkSize: 1000 });
    assert.equal(fresh.loadAllMessages('c1').length, 10);
    assert.equal(fresh.getStorageStats('c1')!.totalMessages, 10);
    assert.equal(fresh.exportToSingleJson('c1', join(dir, 'out.json')), 10);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a torn line from a crash mid-append is dropped, not archived', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const storage = new JsonStorage(base, quiet, { chunkSize: 100 });
    storage.appendMessages('c1', 'chan', Array.from({ length: 4 }, (_, i) => exportRow(String(1000 + i))));

    const partPath = join(base, 'c1', 'chan-000001.json');
    const intact = readFileSync(partPath, 'utf-8');

    // The process dies in the middle of the next append. The manifest was
    // never updated for it, and the resume cursor never advanced past it.
    writeFileSync(partPath, `${intact}{"id":"1004","author":"tester#0 (u`);

    const second = new JsonStorage(base, quiet, { chunkSize: 100 });
    const result = second.appendMessages('c1', 'chan', [exportRow('1004')]);
    assert.equal(result.written, 1);

    const archive = second.loadArchive('c1')!;
    assert.equal(archive.totalMessages, 5, 'the torn row was counted in the manifest');
    assert.equal(archive.totalParts, 2);
    assert.deepEqual(
      second.loadAllMessages('c1').map((r) => r.id),
      ['1000', '1001', '1002', '1003', '1004'],
    );

    // The stale part was sealed into a normal chunk on the way through: one
    // JSON value, no `open` flag, and exactly the rows that were complete.
    const sealed = JSON.parse(readFileSync(partPath, 'utf-8'));
    assert.equal(sealed.open, undefined);
    assert.equal(sealed.messageCount, 4);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('finalizing a channel leaves every part in the normal chunk format', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const storage = new JsonStorage(base, quiet, { chunkSize: 10 });
    storage.appendMessages('c1', 'chan', Array.from({ length: 14 }, (_, i) => exportRow(String(i + 1))));

    const tailPath = join(base, 'c1', 'chan-000002.json');
    assert.throws(() => JSON.parse(readFileSync(tailPath, 'utf-8')), 'the tail should be appended, not a JSON value');

    const archive = storage.finalizeChannel('c1')!;
    assert.equal(archive.totalMessages, 14);
    assert.equal(archive.totalParts, 2);
    assert.equal(archive.parts[1].open, undefined);

    const chunk = JSON.parse(readFileSync(tailPath, 'utf-8'));
    assert.equal(chunk.messageCount, 4);
    assert.deepEqual(chunk.messages.map((m: ExportRow) => m.id), ['11', '12', '13', '14']);

    // Finalizing twice must not lose or duplicate the tail.
    assert.equal(storage.finalizeChannel('c1')!.totalMessages, 14);
    assert.equal(storage.loadAllMessages('c1').length, 14);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Archive duplicate guard ====================

test('a crash window does not store the same message twice', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    // Run 1: 120 messages written and acknowledged (cursor persisted).
    const first = new JsonStorage(base, quiet, { chunkSize: 50 });
    const acked = Array.from({ length: 120 }, (_, i) => exportRow(String(1000 + i)));
    const run1 = first.appendMessages('c1', 'chan', acked);
    assert.equal(run1.written, 120);
    assert.equal(run1.skipped, 0);
    first.finalizeChannel('c1');

    // The crash window: the process wrote one more page and died before the
    // cursor describing it was persisted, so the next run re-fetches that page.
    // Thirty of those ids are already in the archive; thirty are new.
    const reFetched = [
      ...acked.slice(-30),
      ...Array.from({ length: 30 }, (_, i) => exportRow(String(1500 + i))),
    ];

    const second = new JsonStorage(base, quiet, { chunkSize: 50 });
    const run2 = second.appendMessages('c1', 'chan', reFetched);
    assert.equal(run2.skipped, 30, 'the re-fetched page should have been dropped');
    assert.equal(run2.written, 30, 'only genuinely new messages should be written');
    second.finalizeChannel('c1');

    const rows = new JsonStorage(base, quiet, { chunkSize: 50 }).loadAllMessages('c1');
    assert.equal(rows.length, 150);
    assert.equal(new Set(rows.map((r) => r.id)).size, 150, 'the archive holds duplicates');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the duplicate index is exact for the whole channel, not only its newest parts', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const ids = (from: number, count: number) =>
      Array.from({ length: count }, (_, i) => exportRow(String(from + i)));

    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', ids(1, 30)); // three parts
    first.finalizeChannel('c1');

    // Id 1 lives in part 1 of 3. An index seeded from the newest parts cannot
    // see it; one covering the channel must.
    const second = new JsonStorage(base, quiet, { chunkSize: 10 });
    const stale = second.appendMessages('c1', 'chan', ids(1, 1));
    assert.equal(stale.skipped, 1, 'a duplicate from the oldest part must be dropped');
    assert.equal(stale.written, 0);

    const stats = second.getDedupStats('c1')!;
    assert.equal(stats.exact, true);
    assert.equal(stats.truncated, false);
    assert.equal(stats.ids, 30);
    assert.equal(stats.skipped, 1);
    assert.equal(stats.parts, stats.partsAvailable, 'every part should have been read');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('windowed mode covers only the parts it was seeded from', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const ids = (from: number, count: number) =>
      Array.from({ length: count }, (_, i) => exportRow(String(from + i)));

    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', ids(1, 30)); // three parts
    first.finalizeChannel('c1');

    // The cheap mode, kept for archives too large to index. One part of three is
    // read, so an old id is written again - the documented approximation.
    const storage = new JsonStorage(base, quiet, { chunkSize: 10, dedupParts: 1, exactDedup: false });
    const stale = storage.appendMessages('c1', 'chan', ids(1, 1));
    assert.equal(stale.skipped, 0, 'a one-part window must not see the oldest part');

    const stats = storage.getDedupStats('c1')!;
    assert.equal(stats.ids, 10, 'the newest part holds ten ids');
    assert.equal(stats.exact, false, 'a partial window is not an exact answer');
    assert.equal(stats.parts, 1);
    assert.equal(stats.partsAvailable, 3);

    // `dedupParts: 0` still turns the guard off entirely.
    const off = new JsonStorage(base, quiet, { chunkSize: 10, dedupParts: 0 });
    const disabled = off.appendMessages('c2', 'chan', ids(1, 30));
    assert.equal(disabled.written, 30);
    const again = off.appendMessages('c2', 'chan', ids(1, 30));
    assert.equal(again.skipped, 0, 'dedupParts: 0 must disable the guard entirely');
    assert.equal(again.written, 30);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('an index that hits its ceiling keeps the newest ids and reports the shortfall', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const ids = (from: number, count: number) =>
      Array.from({ length: count }, (_, i) => exportRow(String(from + i)));

    const first = new JsonStorage(base, quiet, { chunkSize: 5 });
    first.appendMessages('c1', 'chan', ids(1, 30)); // six parts of five
    first.finalizeChannel('c1');

    // A ceiling of seven ids: the newest seven win, the rest are no longer
    // covered, and the report must say so rather than claim exactness.
    const capped = new JsonStorage(base, quiet, { chunkSize: 5, maxDedupIds: 7 });
    assert.equal(capped.appendMessages('c1', 'chan', ids(24, 1)).skipped, 1, '24 is the newest ids');

    const stats = capped.getDedupStats('c1')!;
    assert.equal(stats.ids, 7);
    assert.equal(stats.truncated, true);
    assert.equal(stats.exact, false);
    assert.ok(stats.parts < stats.partsAvailable, 'it should have stopped before reading every part');

    // That is the cost of the ceiling: an evicted id is written to the archive a
    // second time. A duplicate, never a loss.
    assert.equal(capped.appendMessages('c1', 'chan', ids(20, 1)).skipped, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the dedup cost report describes the index without building it', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', Array.from({ length: 30 }, (_, i) => exportRow(String(i + 1))));
    first.finalizeChannel('c1');

    const cost = first.estimateDedupCost('c1')!;
    assert.equal(cost.archived, 30);
    assert.equal(cost.parts, 3);
    assert.equal(cost.ids, 30);
    assert.equal(cost.exact, true);
    assert.equal(cost.affordable, true);
    assert.ok(cost.bytes > 0);

    // A process that never appended has no index - that is the lazy seed - but
    // it can still project the cost from the manifest alone.
    assert.equal(new JsonStorage(base, quiet, { chunkSize: 10 }).getDedupStats('c1'), null);

    const capped = new JsonStorage(base, quiet, { chunkSize: 10, maxDedupIds: 10 }).estimateDedupCost('c1')!;
    assert.equal(capped.affordable, false, 'a ceiling under the archive size must be flagged');
    assert.equal(capped.ids, 30, 'the projection is the whole channel, not the ceiling');

    const windowed = new JsonStorage(base, quiet, { chunkSize: 10, dedupParts: 1, exactDedup: false })
      .estimateDedupCost('c1')!;
    assert.equal(windowed.exact, false);
    assert.equal(windowed.ids, 10, 'windowed mode indexes at most one part');
    assert.ok(windowed.bytes < cost.bytes);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Failure classification ====================

test('disk-full errors are classified as fatal', () => {
  assert.equal(isDiskFullError({ code: 'ENOSPC' }), true);
  assert.equal(isDiskFullError({ code: 'EDQUOT' }), true);
  assert.equal(isDiskFullError(new Error('boom')), false);
  assert.equal(isDiskFullError(null), false);
});

test('rate limiter respects a long Retry-After beyond the backoff cap', () => {
  const limiter = new RateLimitFailsafe(
    { maxBackoffMs: 1000, maxRetryAfterMs: 900_000, minDelayMs: 0 },
    quiet,
  );
  const delay = limiter.getRetryDelay(new Headers({ 'retry-after': '3600' }), 0);
  assert.ok(delay >= 900_000 && delay <= 900_400, `unexpected delay ${delay}`);
});

test('markGlobalBlock pauses every bucket', () => {
  const limiter = new RateLimitFailsafe({ minDelayMs: 0 }, quiet);
  limiter.markGlobalBlock(5000);
  assert.ok(limiter.getState().globalBlockedUntil > Date.now());
});

test('an exhausted bucket pauses only that bucket', async () => {
  const limiter = new RateLimitFailsafe(
    { minDelayMs: 0, requestsPerSecond: 1000, burstSize: 4, baseBackoffMs: 1 },
    quiet,
  );

  const resetAtSeconds = (Date.now() + 150) / 1000;
  limiter.trackBucket(
    'messages:1',
    new Headers({
      'x-ratelimit-remaining': '0',
      'x-ratelimit-reset': String(resetAtSeconds),
      'x-ratelimit-bucket': 'serverHash',
    }),
  );

  const start = Date.now();
  await limiter.acquire('messages:1');
  limiter.release();
  assert.ok(Date.now() - start >= 100, 'expected the exhausted bucket to wait');

  const otherStart = Date.now();
  await limiter.acquire('messages:2');
  limiter.release();
  assert.ok(Date.now() - otherStart < 100, 'an unrelated bucket should not be delayed');
});

test('client honours a global 429 signalled only in the JSON body', async () => {
  const seen: { rateLimited?: boolean; globalBlockMs?: number } = {};

  const fakeLimiter = {
    acquire: async () => {},
    release: () => {},
    trackBucket: () => {},
    recordSuccess: () => {},
    recordError: () => {},
    recordRateLimit: () => { seen.rateLimited = true; },
    markGlobalBlock: (ms: number) => { seen.globalBlockMs = ms; },
    getRetryDelay: () => 0,
    getState: () => ({}),
    reset: () => {},
  } as unknown as RateLimitFailsafe;

  const client = new UserTokenClient('token', quiet, {
    retries: 2,
    timeoutMs: 1000,
    rateLimiter: fakeLimiter,
    apiBase: 'https://example.test',
  });

  let calls = 0;
  await withMockFetch(
    (async () => {
      calls++;
      if (calls === 1) {
        return new Response(
          JSON.stringify({ message: 'You are being rate limited.', retry_after: 0.1, global: true }),
          { status: 429 },
        );
      }
      return Response.json([{ id: '1' }]);
    }) as typeof fetch,
    async () => {
      const messages = await client.fetchMessages('999');
      assert.equal(messages.length, 1);
    },
  );

  assert.equal(calls, 2);
  assert.equal(seen.rateLimited, true);
  assert.equal(seen.globalBlockMs, 100);
});

// ==================== Throughput config ====================

test('raising REQUESTS_PER_SECOND is not capped by the limiter floor', () => {
  const dir = makeTmpDir();
  try {
    const extractor = new UserTokenExtractor(
      makeConfig(dir, { requestsPerSecond: 100 }),
      quiet,
    );
    const interval = extractor.rateLimiter.getState().currentIntervalMs;
    assert.ok(interval <= 20, `expected ~10ms pacing, got ${interval}ms`);
    extractor.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
