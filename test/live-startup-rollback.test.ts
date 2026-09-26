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

class SwitchableSource implements LiveMessageSource {
  private handlers: ((message: DiscordMessage) => void)[] = [];
  private reachable = false;

  setReachable(): void {
    this.reachable = true;
  }

  onMessage(handler: (message: DiscordMessage) => void): void {
    this.handlers.push(handler);
  }

  async start(): Promise<void> {
    if (!this.reachable) throw new Error('gateway handshake failed');
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

test('a successful retry owns and stops only its new channel', async () => {
  const dir = makeTmpDir();
  const first = tsToSnowflake(Date.now() - 6000);
  const second = tsToSnowflake(Date.now() - 5000);
  const retry = tsToSnowflake(Date.now() - 4000);

  const storage = new Storage(join(dir, 'test.db'), quiet);
  await storage.init();
  const jsonStorage = new JsonStorage(join(dir, 'test_json'), quiet, { chunkSize: 50 });
  const source = new SwitchableSource();
  const capture = new LiveCapture({
    storage,
    jsonStorage,
    source,
    log: quiet,
    flushIntervalMs: 60_000,
    batchMessages: 1000,
  });
  let running = false;

  try {
    await storage.updateProgress(first, {
      status: 'error',
      total_extracted: 10,
      newest_message_id: (BigInt(first) + 10n).toString(),
    });
    await storage.updateProgress(second, {
      status: 'error',
      total_extracted: 20,
      newest_message_id: (BigInt(second) + 10n).toString(),
    });

    await assert.rejects(
      () =>
        capture.start([
          { channelId: first, channelName: 'first', guildId: 'g1', guildName: 'g' },
          { channelId: second, channelName: 'second', guildId: 'g1', guildName: 'g' },
        ]),
      /gateway handshake failed/,
    );
    const failedSessionIds = new Set((await storage.getLiveSessions()).map((s) => s.session_id));

    source.setReachable();
    await capture.start([
      { channelId: retry, channelName: 'retry', guildId: 'g1', guildName: 'g' },
    ]);
    running = true;

    const activeChannelIds = capture.getChannelIds();
    const retryRow = await storage.getProgress(retry);
    const activeSessions = (await storage.getLiveSessions()).filter((s) => s.status === 'active');

    await capture.stop();
    running = false;

    const firstRow = await storage.getProgress(first);
    const secondRow = await storage.getProgress(second);
    const sessionsAfterStop = await storage.getLiveSessions();
    const failedChannelSessions = sessionsAfterStop.filter(
      (s) => s.channel_id === first || s.channel_id === second,
    );
    const retrySessions = sessionsAfterStop.filter((s) => !failedSessionIds.has(s.session_id));

    assert.equal(firstRow?.status, 'error', 'stop rewrote a channel from the failed attempt');
    assert.equal(secondRow?.status, 'error', 'stop rewrote another channel from the failed attempt');
    assert.equal(firstRow?.total_extracted, 10, 'stop changed progress from the failed attempt');
    assert.equal(secondRow?.total_extracted, 20, 'stop changed progress from the failed attempt');
    assert.equal(failedChannelSessions.length, 2, 'the retry opened another session for a failed channel');
    assert.ok(
      failedChannelSessions.every((s) => s.status === 'error'),
      'the retry replaced failed session history for channels it does not own',
    );
    assert.deepEqual(
      retrySessions.map((s) => s.channel_id),
      [retry],
      'the retry opened session rows for channels it does not own',
    );
    assert.deepEqual(
      activeChannelIds,
      [retry],
      'the retry still owns channels from the failed attempt',
    );
    assert.equal(retryRow?.status, 'live', 'the retry channel must be live while capture is running');
    assert.deepEqual(
      activeSessions.map((s) => s.channel_id),
      [retry],
      'only the retry channel may own an active session',
    );
  } finally {
    if (running) await capture.stop();
    storage.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
