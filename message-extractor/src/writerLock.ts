import { randomBytes } from 'node:crypto';
import { closeSync, openSync, readFileSync, statSync, unlinkSync, writeFileSync } from 'fs';
import { hostname } from 'os';
import { Logger } from './utils.js';

/** What the lock file records about the process that owns the data directory. */
export interface WriterLockInfo {
  pid: number;
  hostname: string;
  acquiredAt: string;
  command: string;
  token: string;
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

const MAX_ACQUIRE_ATTEMPTS = 3;
const FORCE_UNLOCK_ENV = 'APEX_SCRAPER_FORCE_UNLOCK';

type PathState =
  | { status: 'missing' }
  | { status: 'present'; holder: WriterLockInfo | null };

type ReclaimReason = 'dead-process' | 'foreign-host-override';

interface GuardHandle {
  release(): void;
}

function makeWriterLockInfo(): WriterLockInfo {
  return {
    pid: process.pid,
    hostname: hostname(),
    acquiredAt: new Date().toISOString(),
    command: process.argv.slice(1).join(' ') || 'unknown',
    token: randomBytes(16).toString('hex'),
  };
}

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
    if (typeof parsed?.pid !== 'number' || typeof parsed.token !== 'string') return null;
    return {
      pid: parsed.pid,
      hostname: typeof parsed.hostname === 'string' ? parsed.hostname : 'unknown',
      acquiredAt: typeof parsed.acquiredAt === 'string' ? parsed.acquiredAt : 'unknown',
      command: typeof parsed.command === 'string' ? parsed.command : 'unknown',
      token: parsed.token,
    };
  } catch {
    return null;
  }
}

function nonRegularFileError(path: string, isDirectory: boolean): Error {
  const error = new Error(
    `Writer lock path ${path} is not a regular file${isDirectory ? ' because it is a directory' : ''}.`,
  ) as NodeJS.ErrnoException;
  error.code = isDirectory ? 'EISDIR' : 'EINVAL';
  return error;
}

function readPathState(path: string): PathState {
  let stats;
  try {
    stats = statSync(path);
  } catch (error: any) {
    if (error?.code === 'ENOENT') return { status: 'missing' };
    throw error;
  }
  if (!stats.isFile()) throw nonRegularFileError(path, stats.isDirectory());
  return { status: 'present', holder: readHolder(path) };
}

/**
 * A same-host owner is reclaimable only after liveness proves it is gone.
 * A foreign-host owner can be reclaimed only with the explicit operator override.
 */
function reclaimReason(holder: WriterLockInfo): ReclaimReason | null {
  if (holder.hostname !== hostname()) {
    return process.env[FORCE_UNLOCK_ENV] === '1' ? 'foreign-host-override' : null;
  }
  return isProcessAlive(holder.pid) ? null : 'dead-process';
}

function reclaimWarning(lockPath: string, holder: WriterLockInfo, reason: ReclaimReason): string {
  if (reason === 'foreign-host-override') {
    return (
      `Reclaiming the writer lock at ${lockPath} by operator override (recorded pid ${holder.pid} ` +
      `on ${holder.hostname}; cross-host liveness cannot be checked)`
    );
  }
  return (
    `Reclaiming the writer lock at ${lockPath} (recorded pid ${holder.pid} on ` +
    `${holder.hostname} is no longer running)`
  );
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

function warnSafely(log: Logger | undefined, message: string): void {
  try {
    log?.warn(message);
  } catch {
    // Logging must not make release throw.
  }
}

function createExclusiveFile(path: string, info: WriterLockInfo): void {
  const fd = openSync(path, 'wx');
  try {
    writeFileSync(fd, `${JSON.stringify(info)}\n`, { encoding: 'utf8' });
    closeSync(fd);
  } catch (error) {
    try {
      closeSync(fd);
    } catch {
      // Best effort while preserving the original failure.
    }
    try {
      unlinkSync(path);
    } catch {
      // Best effort cleanup of a file that could not be fully written.
    }
    throw error;
  }
}

function makeGuardHandle(path: string, token: string, log?: Logger): GuardHandle {
  let released = false;
  return {
    release() {
      if (released) return;
      released = true;
      try {
        const state = readPathState(path);
        if (state.status !== 'present' || state.holder?.token !== token) return;
        unlinkSync(path);
      } catch (error: any) {
        if (error?.code !== 'ENOENT') {
          warnSafely(log, `Could not remove the writer lock guard at ${path}: ${error?.message ?? error}`);
        }
      }
    },
  };
}

function acquireGuard(path: string, log?: Logger): GuardHandle {
  const info = makeWriterLockInfo();

  for (let attempt = 0; attempt < MAX_ACQUIRE_ATTEMPTS; attempt++) {
    try {
      createExclusiveFile(path, info);
      try {
        log?.info(`Acquired the archive writer lock guard at ${path}`);
      } catch {
        // Logging must not prevent a successfully held guard from being used.
      }
      return makeGuardHandle(path, info.token, log);
    } catch (error: any) {
      if (error?.code !== 'EEXIST') throw error;

      const state = readPathState(path);
      if (state.status === 'missing') continue;
      if (state.holder && state.holder.hostname === hostname() && !isProcessAlive(state.holder.pid)) {
        warnSafely(
          log,
          `Reclaiming the writer lock guard at ${path} (recorded pid ${state.holder.pid} on ` +
            `${state.holder.hostname} is no longer running)`,
        );
        try {
          unlinkSync(path);
        } catch (unlinkError: any) {
          if (unlinkError?.code !== 'ENOENT') throw unlinkError;
        }
        continue;
      }
    }
  }

  const state = readPathState(path);
  const holder = state.status === 'present' ? state.holder : null;
  throw new WriterLockError(heldMessage(path, holder), path, holder);
}

function makeWriterLockHandle(lockPath: string, token: string, log?: Logger): WriterLockHandle {
  let released = false;
  return {
    path: lockPath,
    release() {
      if (released) return;
      released = true;
      let guard: GuardHandle | null = null;
      try {
        guard = acquireGuard(`${lockPath}.guard`, log);
        const state = readPathState(lockPath);
        if (state.status !== 'present' || state.holder?.token !== token) return;
        unlinkSync(lockPath);
      } catch (error: any) {
        if (error?.code !== 'ENOENT') {
          warnSafely(log, `Could not remove the writer lock at ${lockPath}: ${error?.message ?? error}`);
        }
      } finally {
        try {
          guard?.release();
        } catch {
          // Guard cleanup is best effort and must not make release throw.
        }
      }
    },
  };
}

function acquireWithGuard(lockPath: string, info: WriterLockInfo, log?: Logger): WriterLockHandle {
  for (let attempt = 0; attempt < MAX_ACQUIRE_ATTEMPTS; attempt++) {
    const state = readPathState(lockPath);
    if (state.status === 'missing') {
      try {
        createExclusiveFile(lockPath, info);
        log?.info(`Acquired the archive writer lock at ${lockPath}`);
        return makeWriterLockHandle(lockPath, info.token, log);
      } catch (error: any) {
        if (error?.code === 'EEXIST') continue;
        throw error;
      }
    }

    const holder = state.holder;
    const reason = holder ? reclaimReason(holder) : null;
    if (holder && reason) {
      warnSafely(log, reclaimWarning(lockPath, holder, reason));
      try {
        unlinkSync(lockPath);
      } catch (error: any) {
        if (error?.code !== 'ENOENT') throw error;
      }
      try {
        createExclusiveFile(lockPath, info);
        log?.info(`Acquired the archive writer lock at ${lockPath}`);
        return makeWriterLockHandle(lockPath, info.token, log);
      } catch (error: any) {
        if (error?.code === 'EEXIST') continue;
        throw error;
      }
    }

    throw new WriterLockError(heldMessage(lockPath, holder), lockPath, holder);
  }

  const state = readPathState(lockPath);
  const holder = state.status === 'present' ? state.holder : null;
  throw new WriterLockError(heldMessage(lockPath, holder), lockPath, holder);
}

/**
 * Take the single-writer lock for a data directory.
 *
 * The SQLite metadata file is loaded once per process and rewritten whole, and the
 * JSON archive keeps its part numbering in memory. Two processes writing the same
 * directory therefore clobber each other's state with no error. The lock file is
 * created with O_EXCL, which is atomic, and is only reclaimed when its owner is
 * provably gone. Contended acquisition and token-bound release use a short-lived
 * guard file to serialize the revalidation, replacement, and removal operations.
 */
export function acquireWriterLock(dbPath: string, log?: Logger): WriterLockHandle {
  const lockPath = `${dbPath}.lock`;
  const info = makeWriterLockInfo();

  try {
    createExclusiveFile(lockPath, info);
    log?.info(`Acquired the archive writer lock at ${lockPath}`);
    return makeWriterLockHandle(lockPath, info.token, log);
  } catch (error: any) {
    if (error?.code !== 'EEXIST') throw error;
    readPathState(lockPath);
  }

  const guard = acquireGuard(`${lockPath}.guard`, log);
  try {
    return acquireWithGuard(lockPath, info, log);
  } finally {
    guard.release();
  }
}
