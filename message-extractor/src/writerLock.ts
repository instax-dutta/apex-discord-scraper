import { closeSync, existsSync, openSync, readFileSync, unlinkSync, writeSync } from 'fs';
import { hostname } from 'os';
import { Logger } from './utils.js';

/** What the lock file records about the process that owns the data directory. */
export interface WriterLockInfo {
  pid: number;
  hostname: string;
  acquiredAt: string;
  command: string;
}

export class WriterLockError extends Error {
  constructor(
    message: string,
    readonly lockPath: string,
    readonly holder: WriterLockInfo | null,
  ) {
    super(message);
    this.name = 'WriterLockError';
  }
}

export interface WriterLockHandle {
  readonly path: string;
  /** Removes the lock file. Idempotent, and never throws. */
  release(): void;
}

/** Set to `1` to reclaim a lock recorded on a different host, where liveness is unknowable. */
const FORCE_UNLOCK_ENV = 'APEX_SCRAPER_FORCE_UNLOCK';

function isProcessAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    // EPERM means the process exists but belongs to another user.
    return error?.code === 'EPERM';
  }
}

function readHolder(lockPath: string): WriterLockInfo | null {
  try {
    const parsed = JSON.parse(readFileSync(lockPath, 'utf-8')) as Partial<WriterLockInfo>;
    if (typeof parsed?.pid !== 'number') return null;
    return {
      pid: parsed.pid,
      hostname: typeof parsed.hostname === 'string' ? parsed.hostname : 'unknown',
      acquiredAt: typeof parsed.acquiredAt === 'string' ? parsed.acquiredAt : 'unknown',
      command: typeof parsed.command === 'string' ? parsed.command : 'unknown',
    };
  } catch {
    return null;
  }
}

function canReclaim(holder: WriterLockInfo): boolean {
  if (holder.hostname !== hostname()) {
    return process.env[FORCE_UNLOCK_ENV] === '1';
  }
  return !isProcessAlive(holder.pid);
}

function heldMessage(lockPath: string, holder: WriterLockInfo | null): string {
  const who = holder
    ? ` Another process owns this data directory: pid ${holder.pid} on ${holder.hostname}, ` +
      `holding it since ${holder.acquiredAt}.`
    : ' The lock file could not be read, so its owner is unknown.';
  return (
    `Another apex-scraper process holds the archive writer lock at ${lockPath}.${who}\n` +
    'Only one process may write a data directory at a time: concurrent runs overwrite each ' +
    "other's progress, baselines, and part files. Wait for it to finish, or delete the lock " +
    `file once you are certain it is gone. Set ${FORCE_UNLOCK_ENV}=1 only when the lock was ` +
    'created on a different host, where liveness cannot be checked.'
  );
}

/**
 * Take the single-writer lock for a data directory.
 *
 * The SQLite metadata file is loaded once per process and rewritten whole, and the
 * JSON archive keeps its part numbering in memory. Two processes writing the same
 * directory therefore clobber each other's state with no error. The lock file is
 * created with O_EXCL, which is atomic, and is only reclaimed when its owner is
 * provably gone.
 */
export function acquireWriterLock(dbPath: string, log?: Logger): WriterLockHandle {
  const lockPath = `${dbPath}.lock`;
  const info: WriterLockInfo = {
    pid: process.pid,
    hostname: hostname(),
    acquiredAt: new Date().toISOString(),
    command: process.argv.slice(1).join(' ') || 'unknown',
  };

  for (let attempt = 0; attempt < 3; attempt++) {
    let fd: number | null = null;
    try {
      fd = openSync(lockPath, 'wx');
      writeSync(fd, `${JSON.stringify(info)}\n`, null, 'utf-8');
      closeSync(fd);
      fd = null;
      log?.info(`Acquired the archive writer lock at ${lockPath}`);

      let released = false;
      return {
        path: lockPath,
        release() {
          if (released) return;
          released = true;
          try {
            unlinkSync(lockPath);
          } catch (error: any) {
            if (error?.code !== 'ENOENT') {
              log?.warn(
                `Could not remove the writer lock at ${lockPath}: ${error?.message ?? error}`,
              );
            }
          }
        },
      };
    } catch (error: any) {
      if (fd !== null) {
        try { closeSync(fd); } catch { /* best effort */ }
      }
      if (error?.code !== 'EEXIST') throw error;

      const holder = readHolder(lockPath);
      if (holder && canReclaim(holder)) {
        log?.warn(
          `Reclaiming the writer lock at ${lockPath} (recorded pid ${holder.pid} on ` +
            `${holder.hostname}, no longer running)`,
        );
        try {
          unlinkSync(lockPath);
        } catch {
          // Another process may have reclaimed it first; the retry re-reads it.
        }
        continue;
      }

      throw new WriterLockError(heldMessage(lockPath, holder), lockPath, holder);
    }
  }

  const holder = readHolder(lockPath);
  throw new WriterLockError(heldMessage(lockPath, holder), lockPath, holder);
}
