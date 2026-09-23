// Cross-process writer lock tests.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, rmSync, unlinkSync, symlinkSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir, hostname } from 'node:os';
import { join } from 'node:path';

import { acquireWriterLock, makeWriterLockCleanupError, WriterLockCleanupError, WriterLockError, type WriterLockInfo } from '../src/writerLock.js';
import { UserTokenExtractor } from '../src/userTokenExtractor.js';
import type { ScraperConfig } from '../src/types.js';
import { Logger } from '../src/utils.js';

const quiet = new Logger('error');

class ThrowingInfoLogger extends Logger {
  info(): void {
    throw new Error('info logger failure');
  }
}

class ThrowingWarnLogger extends Logger {
  warn(): void {
    throw new Error('warn logger failure');
  }
}

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-lock-'));
}

/** A pid that has definitely exited, so liveness checks report it as dead. */
function deadPid(): number {
  const child = spawnSync(process.execPath, ['-e', 'process.exit(0)']);
  assert.ok(typeof child.pid === 'number' && child.pid > 0, 'could not spawn a disposable process');
  return child.pid!;
}

function writeLock(dbPath: string, info: Partial<WriterLockInfo>): string {
  const lockPath = `${dbPath}.lock`;
  writeFileSync(
    lockPath,
    `${JSON.stringify({
      pid: process.pid,
      hostname: hostname(),
      acquiredAt: new Date().toISOString(),
      command: 'test',
      token: 'test-token',
      ...info,
    })}\n`,
    'utf-8',
  );
  return lockPath;
}

test('a second writer is refused while the lock is held, and allowed after release', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');

  try {
    const first = acquireWriterLock(dbPath, quiet);
    assert.equal(first.path, `${dbPath}.lock`);
    assert.ok(existsSync(first.path), 'the lock file should exist while held');

    assert.throws(
      () => acquireWriterLock(dbPath, quiet),
      (error: unknown) => {
        assert.ok(error instanceof WriterLockError, 'expected a WriterLockError');
        assert.equal(error.lockPath, `${dbPath}.lock`);
        assert.equal(error.holder?.pid, process.pid, 'the error should name the holder');
        assert.match(error.message, /Only one process may write a data directory/);
        return true;
      },
    );

    first.release();
    assert.equal(existsSync(first.path), false, 'release must remove the lock file');

    const second = acquireWriterLock(dbPath, quiet);
    second.release();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('release is idempotent and a released lock can be taken again', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');

  try {
    const handle = acquireWriterLock(dbPath, quiet);
    handle.release();
    handle.release();

    const again = acquireWriterLock(dbPath, quiet);
    again.release();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a dead same-host lock is not automatically reclaimed', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const dead = deadPid();
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  delete process.env.APEX_SCRAPER_FORCE_UNLOCK;

  try {
    writeLock(dbPath, { pid: dead, hostname: hostname(), token: 'stale-lock' });
    assert.throws(
      () => acquireWriterLock(dbPath, quiet),
      (error: unknown) => {
        assert.ok(error instanceof WriterLockError, 'expected a WriterLockError');
        assert.equal(error.holder?.pid, dead);
        assert.match(error.message, /will not be taken automatically/);
        assert.match(error.message, /APEX_SCRAPER_FORCE_UNLOCK=1/);
        return true;
      },
    );
  } finally {
    if (previous === undefined) delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
    else process.env.APEX_SCRAPER_FORCE_UNLOCK = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lock from another host needs the explicit override', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
  const otherHost = hostname() === 'some-other-host' ? 'some-other-host-2' : 'some-other-host';

  try {
    // Liveness cannot be checked across hosts, so the lock is respected.
    writeLock(dbPath, { pid: 4242, hostname: otherHost });
    assert.throws(() => acquireWriterLock(dbPath, quiet), WriterLockError);

    // The override is the only way past it.
    process.env.APEX_SCRAPER_FORCE_UNLOCK = '1';
    const handle = acquireWriterLock(dbPath, quiet);
    handle.release();
  } finally {
    if (previous === undefined) delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
    else process.env.APEX_SCRAPER_FORCE_UNLOCK = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('lock ownership is bound to its token across reacquisition', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');

  try {
    const first = acquireWriterLock(dbPath, quiet);
    const firstInfo = JSON.parse(readFileSync(first.path, 'utf-8')) as WriterLockInfo;
    unlinkSync(first.path);

    const second = acquireWriterLock(dbPath, quiet);
    const secondInfo = JSON.parse(readFileSync(second.path, 'utf-8')) as WriterLockInfo;

    first.release();
    assert.equal(existsSync(second.path), true, 'an old handle must not release a successor lock');
    assert.notEqual(secondInfo.token, firstInfo.token, 'a new acquisition needs a new ownership token');
    second.release();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the holder file contains the ownership token', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');

  try {
    const handle = acquireWriterLock(dbPath, quiet);
    const info = JSON.parse(readFileSync(handle.path, 'utf-8')) as WriterLockInfo;
    assert.match(info.token, /^[0-9a-f]{32}$/);
    handle.release();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a directory at the lock path surfaces as a filesystem error', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  mkdirSync(`${dbPath}.lock`);

  try {
    assert.throws(
      () => acquireWriterLock(dbPath, quiet),
      (error: unknown) => {
        assert.ok(!(error instanceof WriterLockError));
        assert.match((error as Error).message, /not a regular file|directory/i);
        return true;
      },
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a symlink at the lock path is rejected with or without the override', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const target = join(dir, 'target');
  writeFileSync(target, 'not a lock', 'utf-8');
  symlinkSync(target, `${dbPath}.lock`);
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  delete process.env.APEX_SCRAPER_FORCE_UNLOCK;

  const assertFilesystemError = (error: unknown): boolean => {
    assert.ok(!(error instanceof WriterLockError));
    assert.match((error as Error).message, /not a regular file|symbolic link|symlink/i);
    return true;
  };

  try {
    assert.throws(() => acquireWriterLock(dbPath, quiet), assertFilesystemError);
    process.env.APEX_SCRAPER_FORCE_UNLOCK = '1';
    assert.throws(() => acquireWriterLock(dbPath, quiet), assertFilesystemError);
  } finally {
    if (previous === undefined) delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
    else process.env.APEX_SCRAPER_FORCE_UNLOCK = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the override does not bypass a live same-host lock', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  process.env.APEX_SCRAPER_FORCE_UNLOCK = '1';

  try {
    const handle = acquireWriterLock(dbPath, quiet);
    assert.throws(() => acquireWriterLock(dbPath, quiet), WriterLockError);
    handle.release();
  } finally {
    if (previous === undefined) delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
    else process.env.APEX_SCRAPER_FORCE_UNLOCK = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the override reclaims a dead same-host lock', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  process.env.APEX_SCRAPER_FORCE_UNLOCK = '1';
  const dead = deadPid();

  try {
    writeLock(dbPath, { pid: dead, hostname: hostname(), token: 'stale-lock' });
    const handle = acquireWriterLock(dbPath, quiet);
    const info = JSON.parse(readFileSync(handle.path, 'utf-8')) as WriterLockInfo;
    assert.notEqual(info.token, 'stale-lock');
    handle.release();
    assert.equal(existsSync(handle.path), false);
  } finally {
    if (previous === undefined) delete process.env.APEX_SCRAPER_FORCE_UNLOCK;
    else process.env.APEX_SCRAPER_FORCE_UNLOCK = previous;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a throwing info logger cannot strand a lock', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const log = new ThrowingInfoLogger('error');

  try {
    const handle = acquireWriterLock(dbPath, log);
    assert.ok(existsSync(handle.path));
    handle.release();
    assert.equal(existsSync(handle.path), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a throwing warn logger cannot escape release', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const log = new ThrowingWarnLogger('error');

  try {
    const handle = acquireWriterLock(dbPath, log);
    rmSync(handle.path, { force: true });
    mkdirSync(handle.path);
    assert.doesNotThrow(() => handle.release());
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('cleanup error construction preserves both causes and names the lock path', () => {
  const lockPath = '/tmp/apex-scraper/archive.db.lock';
  const writeCause = new Error('write or close failed');
  const cleanupCause = new Error('lock cleanup failed');

  const error = makeWriterLockCleanupError(lockPath, writeCause, cleanupCause);

  assert.ok(error instanceof WriterLockCleanupError);
  assert.equal(error.lockPath, lockPath);
  assert.ok(error.message.includes(lockPath));
  assert.equal((error as Error & { cause?: unknown }).cause, writeCause);
  assert.equal(error.cleanupCause, cleanupCause);
});

function makeConfig(dir: string): ScraperConfig {
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
  };
}

test('a second extractor cannot initialise the same data directory', async () => {
  const dir = makeTmpDir();
  const first = new UserTokenExtractor(makeConfig(dir), quiet);

  try {
    await first.init();
    assert.ok(existsSync(join(dir, 'test.db.lock')), 'init should take the writer lock');

    const second = new UserTokenExtractor(makeConfig(dir), quiet);
    await assert.rejects(
      () => second.init(),
      (error: unknown) => error instanceof WriterLockError,
      'a second process must be refused',
    );
    // Closing an extractor that never initialised must not throw.
    second.close();

    first.close();
    assert.equal(existsSync(join(dir, 'test.db.lock')), false, 'close should release the lock');

    // The directory is usable again once the owner lets go.
    const third = new UserTokenExtractor(makeConfig(dir), quiet);
    await third.init();
    third.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
