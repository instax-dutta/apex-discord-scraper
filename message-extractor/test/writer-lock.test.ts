// Cross-process writer lock tests.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { tmpdir, hostname } from 'node:os';
import { join } from 'node:path';

import { acquireWriterLock, WriterLockError, type WriterLockInfo } from '../src/writerLock.js';
import { Logger } from '../src/utils.js';

const quiet = new Logger('error');

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

test('a lock left behind by a dead process on this host is reclaimed', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');

  try {
    writeLock(dbPath, { pid: deadPid(), hostname: hostname() });

    const handle = acquireWriterLock(dbPath, quiet);
    const info = JSON.parse(readFileSync(handle.path, 'utf-8')) as WriterLockInfo;
    assert.equal(info.pid, process.pid, 'the reclaimed lock should name this process');
    handle.release();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lock from another host needs the explicit override', () => {
  const dir = makeTmpDir();
  const dbPath = join(dir, 'test.db');
  const previous = process.env.APEX_SCRAPER_FORCE_UNLOCK;
  delete process.env.APEX_SCRAPER_FORCE_UNLOCK;

  try {
    // Liveness cannot be checked across hosts, so the lock is respected.
    writeLock(dbPath, { pid: 4242, hostname: 'some-other-host' });
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
