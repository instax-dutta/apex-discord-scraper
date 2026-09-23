import { randomBytes } from 'node:crypto';
import { closeSync, lstatSync, openSync, readFileSync, unlinkSync, writeFileSync } from 'fs';
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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

export class WriterLockCleanupError extends Error {
  readonly lockPath: string;
  readonly cleanupCause: unknown;

  constructor(lockPath: string, cause: unknown, cleanupCause: unknown) {
    super(
      `Could not finish creating the writer lock at ${lockPath}. ` +
      `Initial failure: ${errorMessage(cause)}. ` +
      `Cleanup also failed: ${errorMessage(cleanupCause)}. ` +
      `The lock file may remain; remove ${lockPath} manually before retrying.`,
    );
    this.name = 'WriterLockCleanupError';
    this.lockPath = lockPath;
    (this as Error & { cause?: unknown }).cause = cause;
    this.cleanupCause = cleanupCause;
  }
}

export function makeWriterLockCleanupError(
  lockPath: string,
  cause: unknown,
  cleanupCause: unknown,
): WriterLockCleanupError {
  return new WriterLockCleanupError(lockPath, cause, cleanupCause);
}

export interface WriterLockHandle {
  readonly path: string;
  /** Removes the lock file. Idempotent, never throws, and remains retryable after failure. */
  release(): void;
}

const FORCE_UNLOCK_ENV = 'APEX_SCRAPER_FORCE_UNLOCK';

type PathState =
  | { status: 'missing' }
  | { status: 'present'; holder: WriterLockInfo | null };

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

function nonRegularFileError(path: string, kind: 'directory' | 'symbolic link' | 'other'): Error {
  const detail = kind === 'directory'
    ? ' because it is a directory'
    : kind === 'symbolic link'
      ? ' because it is a symbolic link'
      : '';
  const error = new Error(`Writer lock path ${path} is not a regular file${detail}.`) as NodeJS.ErrnoException;
  error.code = kind === 'directory' ? 'EISDIR' : 'EINVAL';
  return error;
}

function readPathState(path: string): PathState {
  let stats;
  try {
    stats = lstatSync(path);
  } catch (error: any) {
    if (error?.code === 'ENOENT') return { status: 'missing' };
    throw error;
  }
  if (!stats.isFile()) {
    const kind = stats.isSymbolicLink() ? 'symbolic link' : stats.isDirectory() ? 'directory' : 'other';
    throw nonRegularFileError(path, kind);
  }
  return { status: 'present', holder: readHolder(path) };
}

/**
 * The environment override is the only sanctioned way to remove an existing lock.
 * A same-host holder is eligible only when its recorded pid is currently unprobeable;
 * this does not establish ownership and can be affected by pid reuse. A foreign-host
 * holder cannot be probed from this machine.
 */
function canForceUnlock(holder: WriterLockInfo | null): boolean {
  if (!holder || process.env[FORCE_UNLOCK_ENV] !== '1') return false;
  return holder.hostname !== hostname() || !isProcessAlive(holder.pid);
}

function infoSafely(log: Logger | undefined, message: string): void {
  try {
    log?.info(message);
  } catch {
    // Logging must not strand a successfully created lock.
  }
}

function warnSafely(log: Logger | undefined, message: string): void {
  try {
    log?.warn(message);
  } catch {
    // Logging must not make release throw.
  }
}

function writeAndCloseLock(fd: number, lockPath: string, info: WriterLockInfo): void {
  let closeAttempted = false;
  try {
    writeFileSync(fd, `${JSON.stringify(info)}\n`, { encoding: 'utf8' });
    closeAttempted = true;
    closeSync(fd);
  } catch (error) {
    if (!closeAttempted) {
      closeAttempted = true;
      try {
        closeSync(fd);
      } catch {
        // Preserve the original write or close failure.
      }
    }
    try {
      unlinkSync(lockPath);
    } catch (cleanupCause: any) {
      if (cleanupCause?.code !== 'ENOENT') {
        throw makeWriterLockCleanupError(lockPath, error, cleanupCause);
      }
    }
    throw error;
  }
}

function makeWriterLockHandle(lockPath: string, token: string, log?: Logger): WriterLockHandle {
  let released = false;
  return {
    path: lockPath,
    release() {
      if (released) return;
      try {
        const state = readPathState(lockPath);
        if (state.status !== 'present' || state.holder?.token !== token) {
          released = true;
          return;
        }
        unlinkSync(lockPath);
        released = true;
      } catch (error: any) {
        if (error?.code === 'ENOENT') released = true;
        else {
          warnSafely(log, `Could not remove the writer lock at ${lockPath}: ${error?.message ?? error}`);
        }
      }
    },
  };
}

function heldMessage(lockPath: string, holder: WriterLockInfo | null): string {
  const owner = holder
    ? ` The recorded owner is pid ${holder.pid} on ${holder.hostname}, holding it since ${holder.acquiredAt}.`
    : ' The lock file could not be read, so its owner is unknown.';
  const guidance = holder
    ? `Wait for the recorded owner to finish. If you have checked that it is no longer active, delete the lock file or set ` +
      `${FORCE_UNLOCK_ENV}=1 and retry; the override accepts the risk of racing another writer.`
    : 'Identify the owner and wait for it to finish. If you have checked that it is gone, delete the lock file; ' +
      'the force override cannot establish ownership from an unreadable file.';
  return (
    `Only one process may write a data directory at a time. The archive writer lock at ${lockPath} is held and will not be taken automatically.${owner}\n` +
    guidance
  );
}

function completeLock(fd: number, lockPath: string, info: WriterLockInfo, log?: Logger): WriterLockHandle {
  writeAndCloseLock(fd, lockPath, info);
  infoSafely(log, `Acquired the archive writer lock at ${lockPath}`);
  return makeWriterLockHandle(lockPath, info.token, log);
}

function acquireExistingLock(lockPath: string, info: WriterLockInfo, log?: Logger): WriterLockHandle {
  const state = readPathState(lockPath);
  const holder = state.status === 'present' ? state.holder : null;
  if (!canForceUnlock(holder)) {
    throw new WriterLockError(heldMessage(lockPath, holder), lockPath, holder);
  }

  try {
    unlinkSync(lockPath);
  } catch (error: any) {
    if (error?.code !== 'ENOENT') throw error;
  }

  let fd: number;
  try {
    fd = openSync(lockPath, 'wx');
  } catch (error: any) {
    if (error?.code !== 'EEXIST') throw error;
    const currentState = readPathState(lockPath);
    const currentHolder = currentState.status === 'present' ? currentState.holder : null;
    throw new WriterLockError(heldMessage(lockPath, currentHolder), lockPath, currentHolder);
  }
  return completeLock(fd, lockPath, info, log);
}

/**
 * Take the single-writer lock for a data directory.
 *
 * The SQLite metadata file is loaded once per process and rewritten whole, and the
 * JSON archive keeps its part numbering in memory. Two processes writing the same
 * directory therefore clobber each other's state with no error. The lock file is
 * created with O_EXCL, which is atomic. If it already exists, the lock is never
 * taken automatically; the recorded holder metadata is reported to the operator, and
 * no liveness probe is run unless the override is requested. The environment variable
 * is an explicit operator override that accepts the risk of a race and does not
 * establish ownership.
 */
export function acquireWriterLock(dbPath: string, log?: Logger): WriterLockHandle {
  const lockPath = `${dbPath}.lock`;
  readPathState(lockPath);
  const info = makeWriterLockInfo();

  let fd: number;
  try {
    fd = openSync(lockPath, 'wx');
  } catch (error: any) {
    if (error?.code !== 'EEXIST') throw error;
    return acquireExistingLock(lockPath, info, log);
  }
  return completeLock(fd, lockPath, info, log);
}
