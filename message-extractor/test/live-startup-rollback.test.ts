// Live capture startup rollback tests.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { JsonStorage } from '../src/jsonStorage.js';
import { Storage } from '../src/storage.js';
import { LiveCapture, type LiveMessageSource } from '../src/liveCapture.js';
import { Logger, tsToSnowflake } from '../src/utils.js';
import type { DiscordMessage } from '../src/types.js';

const quiet = new Logger('error');

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-livestart-'));
}

class UnreachableSource implements LiveMessageSource {
  private handlers: ((message: DiscordMessage) => void)[] = [];

  onMessage(handler: (message: DiscordMessage) => void): void {
    this.handlers.push(handler);
  }

  async start(): Promise<void> {
    throw new Error('gateway handshake failed');
  }

  close(): void {}
}

test('a failed live startup restores the previous channel status and closes its session', async () => {
  const dir = makeTmpDir();
  const known = tsToSnowflake(Date.now() - 5000);
  const fresh = tsToSnowflake(Date.now() - 4000);

  const storage = new Storage(join(dir, 'test.db'), quiet);
  await storage.init();
  const jsonStorage = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 });

  try {
    // One channel was fully extracted before; the other has never been seen.
    await storage.updateProgress(known, {
      status: 'done',
      total_extracted: 10,
      newest_message_id: (BigInt(known) + 10n).toString(),
    });

    const capture = new LiveCapture({
      storage,
      jsonStorage,
      source: new UnreachableSource(),
      log: quiet,
      flushIntervalMs: 60_000,
      batchMessages: 1000,
    });

    await assert.rejects(
      () =>
        capture.start([
          { channelId: known, channelName: 'known', guildId: 'g1', guildName: 'g' },
          { channelId: fresh, channelName: 'fresh', guildId: 'g1', guildName: 'g' },
        ]),
      /gateway handshake failed/,
      'the connect error must propagate',
    );

    assert.equal(capture.isRunning(), false, 'a failed start must not report itself running');

    // The previously completed channel is back to `done`, not stuck in `live`.
    const knownRow = await storage.getProgress(known);
    assert.equal(knownRow?.status, 'done', 'a failed start left the channel in live');
    assert.equal(knownRow?.total_extracted, 10, 'the rollback must not disturb progress');
    assert.match(knownRow?.error_message ?? '', /gateway handshake failed/);

    // The never-extracted channel is back to `pending`.
    const freshRow = await storage.getProgress(fresh);
    assert.equal(freshRow?.status, 'pending', 'a never-extracted channel must not claim live');

    // Both session rows are closed rather than left active.
    const sessions = await storage.getLiveSessions();
    assert.equal(sessions.length, 2);
    assert.ok(
      sessions.every((s) => s.status === 'error'),
      `expected every session to be error, got ${sessions.map((s) => s.status).join(', ')}`,
    );

    // Nothing was written to the archive by a capture that never ran.
    assert.equal(jsonStorage.loadArchive(known), null);
    assert.equal(jsonStorage.loadArchive(fresh), null);
  } finally {
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
