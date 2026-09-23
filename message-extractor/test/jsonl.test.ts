// JSONL export tests.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { UserTokenExtractor } from '../src/userTokenExtractor.js';
import { JsonStorage } from '../src/jsonStorage.js';
import { Logger, resolveJsonlPath, tsToSnowflake } from '../src/utils.js';
import type { ScraperConfig } from '../src/types.js';

const quiet = new Logger('error');

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-jsonl-'));
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

function makeIds(channelId: string, count: number, offset = 1): string[] {
  return Array.from({ length: count }, (_, i) => (BigInt(channelId) + BigInt(i + offset)).toString());
}

/** Serve descending pages, keyed by the channel in the URL. */
function installChannelAwareFetch(pages: Map<string, string[]>) {
  const original = globalThis.fetch;

  globalThis.fetch = (async (input: any) => {
    const url = new URL(typeof input === 'string' ? input : input.url);
    const channelId = url.pathname.split('/channels/')[1]?.split('/')[0] ?? '';
    const all = pages.get(channelId) ?? [];
    const before = url.searchParams.get('before');
    const bound = before ? BigInt(before) : null;
    const page = all
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

function readJsonl(path: string): any[] {
  const raw = readFileSync(path, 'utf-8');
  assert.ok(raw.endsWith('\n'), 'JSONL output should end with a newline');
  return raw
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line, i) => {
      try {
        return JSON.parse(line);
      } catch (e: any) {
        throw new Error(`line ${i + 1} is not valid JSON: ${e.message}`);
      }
    });
}

// ==================== Path resolution ====================

test('resolveJsonlPath substitutes {channel} and isolates multi-channel runs', () => {
  // Placeholder wins, in every position.
  assert.equal(resolveJsonlPath('out-{channel}.jsonl', '123', false), 'out-123.jsonl');
  assert.equal(resolveJsonlPath('{channel}/out.jsonl', '123', true), '123/out.jsonl');
  assert.equal(
    resolveJsonlPath('a-{channel}-{channel}.jsonl', '9', false),
    'a-9-9.jsonl',
  );

  // A single channel uses the path verbatim.
  assert.equal(resolveJsonlPath('./data/out.jsonl', '123', true), './data/out.jsonl');

  // Several channels: the id goes before the extension...
  assert.equal(resolveJsonlPath('./data/out.jsonl', '123', false), './data/out-123.jsonl');
  assert.equal(resolveJsonlPath('out.JSON', '123', false), 'out-123.JSON');

  // ...or the path is treated as a directory when there is no extension.
  assert.equal(resolveJsonlPath('./data/exports', '123', false), join('./data/exports', '123.jsonl'));
});

// ==================== End-to-end export ====================

test('exports every extracted message as JSONL', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const N = 120;
  const ids = makeIds(channelId, N);

  const mock = installChannelAwareFetch(
    new Map([[channelId, [...ids].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1))]]),
  );
  const exportPath = join(dir, 'export', 'chan.jsonl');

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
    await extractor.init();

    const result = await extractor.extractChannel(
      { channelId, channelName: 'jsonl', guildId: 'g1', guildName: 'g' },
      { resume: true, exportJsonl: exportPath },
    );
    extractor.close();

    assert.equal(result.success, true, `extraction failed: ${result.error}`);
    assert.equal(result.messagesExtracted, N);
    assert.equal(result.jsonlRows, N);
    assert.equal(result.jsonlError, undefined);

    // The parent directory is created on demand.
    assert.ok(existsSync(exportPath), 'JSONL file was not created');

    const rows = readJsonl(exportPath);
    assert.equal(rows.length, N);
    assert.equal(new Set(rows.map((r) => r.id)).size, N, 'JSONL contains duplicate ids');

    // Exact same set as the archive.
    const archiveIds = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 })
      .loadAllMessages(channelId)
      .map((m) => m.id);
    assert.deepEqual(
      [...rows.map((r) => r.id)].sort(),
      [...archiveIds].sort(),
      'JSONL and archive disagree',
    );

    // Rows carry the export shape, not the raw Discord payload.
    const sample = rows[0];
    assert.ok(typeof sample.author === 'string');
    assert.ok(Array.isArray(sample.attachmentUrls));
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Append semantics ====================

test('a catch-up appends only the new messages to the JSONL export', async () => {
  const dir = makeTmpDir();
  const channelId = tsToSnowflake(Date.now() - 1000);
  const base = makeIds(channelId, 40);
  const arrived = makeIds(channelId, 10, 500);

  const pages = new Map<string, string[]>();
  pages.set(channelId, [...base].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1)));

  const mock = installChannelAwareFetch(pages);
  const exportPath = join(dir, 'chan.jsonl');
  const info = { channelId, channelName: 'append', guildId: 'g1', guildName: 'g' };

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir), quiet);
    await extractor.init();

    const first = await extractor.extractChannel(info, { resume: true, exportJsonl: exportPath });
    assert.equal(first.jsonlRows, 40);

    const afterFirstRun = readFileSync(exportPath, 'utf-8');
    assert.equal(readJsonl(exportPath).length, 40);

    // Ten new messages arrive, then a catch-up runs against the same file.
    pages.set(channelId, [...base, ...arrived].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1)));

    const second = await extractor.extractChannel(info, {
      resume: true,
      incremental: true,
      exportJsonl: exportPath,
    });
    extractor.close();

    assert.equal(second.success, true, `catch-up failed: ${second.error}`);
    assert.equal(second.messagesExtracted, 10);
    assert.equal(second.jsonlRows, 10, 'only new messages should be written this run');

    const raw = readFileSync(exportPath, 'utf-8');
    assert.ok(raw.startsWith(afterFirstRun), 'the export was truncated instead of appended');

    const rows = readJsonl(exportPath);
    assert.equal(rows.length, 50);
    assert.equal(new Set(rows.map((r) => r.id)).size, 50, 'catch-up duplicated lines');
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});

// ==================== Multi-channel isolation ====================

test('two concurrent channels write to their own JSONL files', async () => {
  const dir = makeTmpDir();
  const channelA = tsToSnowflake(Date.now() - 1000);
  const channelB = tsToSnowflake(Date.now() - 2000);

  const idsA = makeIds(channelA, 60);
  const idsB = makeIds(channelB, 30);
  const desc = (ids: string[]) => [...ids].sort((a, b) => (BigInt(a) < BigInt(b) ? 1 : -1));

  const mock = installChannelAwareFetch(new Map([
    [channelA, desc(idsA)],
    [channelB, desc(idsB)],
  ]));

  try {
    const extractor = new UserTokenExtractor(makeConfig(dir, { parallelism: 1 }), quiet);
    await extractor.init();

    const results = await extractor.extractAll(
      [
        { channelId: channelA, channelName: 'a', guildId: 'g1', guildName: 'g' },
        { channelId: channelB, channelName: 'b', guildId: 'g1', guildName: 'g' },
      ],
      {
        concurrency: 2,
        exportJsonl: (channel) => resolveJsonlPath(join(dir, 'out.jsonl'), channel.channelId, false),
      },
    );
    extractor.close();

    assert.equal(results.length, 2);
    assert.ok(results.every((r) => r.success), 'both channels should succeed');
    assert.ok(results.every((r) => r.jsonlError === undefined));

    const pathA = join(dir, `out-${channelA}.jsonl`);
    const pathB = join(dir, `out-${channelB}.jsonl`);
    assert.ok(existsSync(pathA) && existsSync(pathB), 'expected one file per channel');

    const rowsA = readJsonl(pathA);
    const rowsB = readJsonl(pathB);
    assert.equal(rowsA.length, 60);
    assert.equal(rowsB.length, 30);

    // No cross-contamination between the files.
    assert.ok(rowsA.every((r) => idsA.includes(r.id)));
    assert.ok(rowsB.every((r) => idsB.includes(r.id)));
  } finally {
    mock.restore();
    rmSync(dir, { recursive: true, force: true });
  }
});
