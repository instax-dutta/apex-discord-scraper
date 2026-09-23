// Apex Discord Scraper - SQLite Metadata Cache
// SQLite is used ONLY for per-channel progress/tracking and channel config.
// Messages - and therefore any per-message duplicate guard - live in the JSON
// files (see jsonStorage.ts), so this database stays a few KB.

import initSqlJs, { type Database as SqlJsDatabase } from 'sql.js';
import { readFileSync, writeFileSync, existsSync, mkdirSync, renameSync, unlinkSync } from 'fs';
import { dirname } from 'path';
import type { ChannelProgress, ChannelConfig, LiveSession } from './types.js';
import { Logger } from './utils.js';

type SqlJsModule = Awaited<ReturnType<typeof initSqlJs>>;

/** The WASM module is expensive to load; share it across Storage instances. */
let sqlJsPromise: Promise<SqlJsModule> | null = null;
function loadSqlJs(): Promise<SqlJsModule> {
  if (!sqlJsPromise) sqlJsPromise = initSqlJs();
  return sqlJsPromise;
}

export class Storage {
  private db: SqlJsDatabase | null = null;
  private dbPath: string;
  private log: Logger;
  private saveTimer: NodeJS.Timeout | null = null;
  private initialized = false;
  /** Per-instance: a module-level promise would leak between instances. */
  private initPromise: Promise<void> | null = null;

  constructor(dbPath: string, log: Logger = new Logger()) {
    this.dbPath = dbPath;
    this.log = log;
  }

  private getDb(): SqlJsDatabase {
    if (!this.db) {
      throw new Error('Database not initialized. Call init() first.');
    }
    return this.db;
  }

  private async ensureInit(): Promise<void> {
    if (this.initialized && this.db) return;
    if (!this.initPromise) {
      this.initPromise = this.doInit();
    }
    await this.initPromise;
    this.initialized = true;
  }

  async init(): Promise<void> {
    await this.ensureInit();
  }

  private async doInit(): Promise<void> {
    const SQL = await loadSqlJs();

    const dir = dirname(this.dbPath);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    const existed = existsSync(this.dbPath);
    if (existed) {
      const buffer = readFileSync(this.dbPath);
      this.db = this.openOrQuarantine(SQL, buffer);
    } else {
      this.db = new SQL.Database();
    }

    this.log.info(`Metadata cache ${existed ? 'loaded' : 'created'} at ${this.dbPath}`);
    this.initSchema();
  }

  /**
   * Open a metadata file, quarantining it instead of throwing when it is not a
   * readable database.
   *
   * The file holds every channel's resume cursor and catch-up baseline, so a
   * truncated write (crash, full disk, a killed process) used to be fatal: the
   * constructor threw and *no* command worked until the file was deleted by
   * hand. Losing that state is recoverable - a channel just re-extracts - so
   * failing over is strictly better than refusing to start. The old file is
   * kept beside the new one so it can still be inspected.
   */
  private openOrQuarantine(SQL: SqlJsModule, buffer: Buffer): SqlJsDatabase {
    let candidate: SqlJsDatabase | null = null;
    try {
      candidate = new SQL.Database(buffer);
      // sql.js opens lazily, so prove the file really is a database before
      // trusting it. Without this, the failure would surface later as a
      // confusing error from an unrelated query.
      candidate.exec('SELECT count(*) FROM sqlite_master');
      return candidate;
    } catch (error: any) {
      try { candidate?.close(); } catch { /* nothing to release */ }

      const quarantine = `${this.dbPath}.corrupt-${Date.now()}`;
      try {
        renameSync(this.dbPath, quarantine);
        this.log.error(
          `Metadata cache at ${this.dbPath} is unreadable (${error?.message ?? error}); ` +
            `starting a new one and keeping the old file at ${quarantine}`,
        );
      } catch (moveError: any) {
        this.log.error(
          `Metadata cache at ${this.dbPath} is unreadable (${error?.message ?? error}) and ` +
            `could not be moved aside (${moveError?.message ?? moveError}); starting fresh`,
        );
      }

      return new SQL.Database();
    }
  }

  private initSchema(): void {
    if (!this.db) return;

    // Drop the legacy per-message dedup table. It was never read by the current
    // code (duplicate protection lives in the JSON archive and the extractor's
    // in-run guard), and a database written by an old version can carry
    // millions of dead rows that every `export()` would re-serialize on save.
    this.db.run('DROP TABLE IF EXISTS dedup_cache');

    // Channel progress tracking
    this.db.run(`
      CREATE TABLE IF NOT EXISTS channel_progress (
        channel_id TEXT PRIMARY KEY,
        server_id TEXT NOT NULL,
        channel_name TEXT,
        last_message_id TEXT,
        last_extracted_ts TEXT,
        total_extracted INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        error_message TEXT,
        started_at TEXT,
        completed_at TEXT,
        updated_at TEXT,
        resume_state TEXT,
        newest_message_id TEXT
      )
    `);

    // Migrations for databases created by older versions.
    try {
      this.db.run('ALTER TABLE channel_progress ADD COLUMN resume_state TEXT');
    } catch {
      // Column already present - nothing to do.
    }
    try {
      this.db.run('ALTER TABLE channel_progress ADD COLUMN newest_message_id TEXT');
    } catch {
      // Column already present - nothing to do.
    }

    // Channel configuration
    this.db.run(`
      CREATE TABLE IF NOT EXISTS channels (
        channel_id TEXT PRIMARY KEY,
        server_id TEXT NOT NULL,
        server_name TEXT,
        channel_name TEXT,
        enabled INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT
      )
    `);

    // Live sessions
    this.db.run(`
      CREATE TABLE IF NOT EXISTS live_sessions (
        session_id TEXT PRIMARY KEY,
        channel_id TEXT NOT NULL,
        started_at TEXT NOT NULL,
        last_message_id TEXT,
        message_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active'
      )
    `);

    this.save();
    this.log.info('Metadata cache schema initialized');
  }

  private save(): void {
    if (!this.db) return;
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
    }
    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      this.saveSync();
    }, 1000);
  }

  /**
   * Persist the whole database.
   *
   * Written to a temp file and renamed into place, like the JSON chunks: a
   * crash, a kill, or a full disk mid-write then leaves the previous (valid)
   * metadata file intact instead of a truncated one. The file stays tiny - no
   * message rows live here - so the extra rename is free.
   */
  private saveSync(): void {
    if (!this.db) return;
    const tmpPath = `${this.dbPath}.tmp-${process.pid}`;
    try {
      const buffer = Buffer.from(this.db.export());
      writeFileSync(tmpPath, buffer);
      renameSync(tmpPath, this.dbPath);
      this.log.debug('Metadata cache saved to disk');
    } catch (e: any) {
      this.log.error(`Failed to save metadata cache: ${e.message}`);
      try {
        if (existsSync(tmpPath)) unlinkSync(tmpPath);
      } catch { /* best effort */ }
    }
  }


  // ==================== Progress Operations ====================

  async getOrCreateProgress(channelId: string, serverId: string, channelName?: string): Promise<ChannelProgress> {
    await this.ensureInit();
    if (!this.db) throw new Error('Database not initialized');

    const stmt = this.db.prepare('SELECT * FROM channel_progress WHERE channel_id = ?');
    stmt.bind([channelId]);

    let existing: ChannelProgress | null = null;
    if (stmt.step()) {
      existing = stmt.getAsObject() as unknown as ChannelProgress;
    }
    stmt.free();

    if (existing) return existing;

    const now = new Date().toISOString();
    this.db.run(
      `INSERT INTO channel_progress (channel_id, server_id, channel_name, status, updated_at)
       VALUES (?, ?, ?, 'pending', ?)`,
      [channelId, serverId, channelName || null, now]
    );

    this.save();

    return {
      channel_id: channelId,
      server_id: serverId,
      channel_name: channelName || null,
      last_message_id: null,
      last_extracted_ts: null,
      total_extracted: 0,
      status: 'pending',
      error_message: null,
      started_at: null,
      completed_at: null,
      updated_at: now,
      resume_state: null,
      newest_message_id: null,
    };
  }

  async updateProgress(
    channelId: string,
    updates: Partial<Pick<ChannelProgress, 'last_message_id' | 'last_extracted_ts' | 'total_extracted' | 'status' | 'error_message' | 'resume_state' | 'newest_message_id'>>
  ): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    const progress = await this.getOrCreateProgress(channelId, '');
    const now = new Date().toISOString();

    const fields: string[] = ['updated_at = ?'];
    const values: (string | number | null)[] = [now];

    if (updates.last_message_id !== undefined) {
      fields.push('last_message_id = ?');
      values.push(updates.last_message_id);
    }
    if (updates.last_extracted_ts !== undefined) {
      fields.push('last_extracted_ts = ?');
      values.push(updates.last_extracted_ts);
    }
    if (updates.total_extracted !== undefined) {
      fields.push('total_extracted = ?');
      values.push(updates.total_extracted);
    }
    if (updates.status !== undefined) {
      fields.push('status = ?');
      values.push(updates.status);
      if (updates.status === 'extracting' && !progress.started_at) {
        fields.push('started_at = ?');
        values.push(now);
      }
      if (updates.status === 'done' || updates.status === 'error') {
        fields.push('completed_at = ?');
        values.push(now);
      }
    }
    if (updates.error_message !== undefined) {
      fields.push('error_message = ?');
      values.push(updates.error_message);
    }
    if (updates.resume_state !== undefined) {
      fields.push('resume_state = ?');
      values.push(updates.resume_state);
    }
    if (updates.newest_message_id !== undefined) {
      fields.push('newest_message_id = ?');
      values.push(updates.newest_message_id);
    }

    values.push(channelId);

    this.db.run(`UPDATE channel_progress SET ${fields.join(', ')} WHERE channel_id = ?`, values);
    this.save();
  }

  async getAllProgress(): Promise<ChannelProgress[]> {
    await this.ensureInit();
    if (!this.db) return [];

    const stmt = this.db.prepare('SELECT * FROM channel_progress ORDER BY updated_at DESC');
    const results: ChannelProgress[] = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject() as unknown as ChannelProgress);
    }
    stmt.free();
    return results;
  }

  async getProgress(channelId: string): Promise<ChannelProgress | null> {
    await this.ensureInit();
    if (!this.db) return null;

    const stmt = this.db.prepare('SELECT * FROM channel_progress WHERE channel_id = ?');
    stmt.bind([channelId]);

    let result: ChannelProgress | null = null;
    if (stmt.step()) {
      result = stmt.getAsObject() as unknown as ChannelProgress;
    }
    stmt.free();

    return result;
  }

  // ==================== Channel Config Operations ====================

  async upsertChannel(config: ChannelConfig): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    const now = new Date().toISOString();
    this.db.run(
      `INSERT OR REPLACE INTO channels (channel_id, server_id, server_name, channel_name, enabled, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        config.channel_id,
        config.server_id,
        config.server_name || null,
        config.channel_name || null,
        config.enabled ? 1 : 0,
        now,
        now,
      ]
    );
    this.save();
  }

  async getEnabledChannels(): Promise<ChannelConfig[]> {
    await this.ensureInit();
    if (!this.db) return [];

    const stmt = this.db.prepare('SELECT * FROM channels WHERE enabled = 1');
    const results: ChannelConfig[] = [];
    while (stmt.step()) {
      const row = stmt.getAsObject() as any;
      results.push({
        channel_id: row.channel_id,
        server_id: row.server_id,
        server_name: row.server_name || undefined,
        channel_name: row.channel_name || undefined,
        enabled: row.enabled === 1,
      });
    }
    stmt.free();
    return results;
  }

  async removeChannel(channelId: string): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    this.db.run('DELETE FROM channels WHERE channel_id = ?', [channelId]);
    this.save();
  }

  // ==================== Live Session Operations ====================

  async startLiveSession(sessionId: string, channelId: string): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    const now = new Date().toISOString();
    this.db.run(
      `INSERT OR REPLACE INTO live_sessions
         (session_id, channel_id, started_at, last_message_id, message_count, status)
       VALUES (?, ?, ?, NULL, 0, 'active')`,
      [sessionId, channelId, now],
    );
    this.save();
  }

  async updateLiveSession(
    sessionId: string,
    messageCount: number,
    lastMessageId: string | null,
  ): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    this.db.run(
      'UPDATE live_sessions SET message_count = ?, last_message_id = ? WHERE session_id = ?',
      [messageCount, lastMessageId, sessionId],
    );
    this.save();
  }

  async endLiveSession(
    sessionId: string,
    status: LiveSession['status'] = 'stopped',
  ): Promise<void> {
    await this.ensureInit();
    if (!this.db) return;

    this.db.run('UPDATE live_sessions SET status = ? WHERE session_id = ?', [status, sessionId]);
    this.save();
  }

  async getLiveSessions(limit = 20): Promise<LiveSession[]> {
    await this.ensureInit();
    if (!this.db) return [];

    const stmt = this.db.prepare('SELECT * FROM live_sessions ORDER BY started_at DESC LIMIT ?');
    stmt.bind([limit]);

    const rows: LiveSession[] = [];
    while (stmt.step()) {
      rows.push(stmt.getAsObject() as unknown as LiveSession);
    }
    stmt.free();
    return rows;
  }

  // ==================== Cleanup ====================

  close(): void {
    // Cancel any pending debounced write so it cannot fire after close.
    if (this.saveTimer) {
      clearTimeout(this.saveTimer);
      this.saveTimer = null;
    }
    this.saveSync();
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    this.initialized = false;
  }
}
