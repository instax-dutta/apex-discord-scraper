// Apex Discord Scraper - JSON Storage Layer
//
// Messages are stored in chunked JSON files. At 100M messages (thousands of
// chunks) the naive approach breaks in three ways, all fixed here:
//   1. rewriting + re-reading the manifest on every flush is O(n^2) I/O
//      -> the archive is cached in memory and persisted atomically
//   2. a crash mid-write leaves a truncated, silently-dropped chunk
//      -> chunks and manifest are written to a temp file then renamed
//   3. `status` used to read every chunk to total its size
//      -> sizes come from statSync, and export streams one chunk at a time
//   4. rewriting the in-progress part on every flush is O(messages in part)
//      -> the open part is appended to as JSON Lines, and only rewritten once,
//         when it fills or when the run ends (see `openPart`)
//
// If the manifest itself is lost, it is rebuilt by scanning chunk files.

import {
  writeFileSync,
  appendFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  openSync,
  writeSync,
  closeSync,
} from 'fs';
import { dirname, join } from 'path';
import type { ExportRow, JsonArchive, JsonChunk, JsonPartMeta } from './types.js';
import { Logger, formatBytes, tryParseJson } from './utils.js';

/**
 * The part that is currently open for appending.
 *
 * It carries a count and the id range it covers, not the messages themselves:
 * the rows live in the open part file, so an append is one `appendFileSync` of
 * the new rows instead of a rewrite of everything accumulated so far, and the
 * extractor's own flush buffer stays the only copy of un-flushed messages in
 * memory.
 */
interface TailState {
  part: number;
  count: number;
  startId: string;
  endId: string;
}

const MANIFEST_NAME = '_archive.json';
const DEFAULT_CHUNK_SIZE = 50000;

export interface JsonStorageOptions {
  /** Messages per chunk file. Smaller chunks = more files, less memory each. */
  chunkSize?: number;
  /** Pretty-print chunk JSON. Off by default; it roughly doubles disk + CPU. */
  pretty?: boolean;
  /**
   * How many of the most recently written parts to load into the duplicate
   * guard for a channel when `exactDedup` is off. `0` disables the guard
   * entirely.
   */
  dedupParts?: number;
  /**
   * Index the whole channel instead of its newest parts, so the guard can tell
   * an archived message from a new one no matter how old it is. Costs one entry
   * per archived id: see `maxDedupIds` and `getDedupStats`.
   */
  exactDedup?: boolean;
  /**
   * Ceiling on the guard, in ids, whatever the mode. Past it the guard keeps the
   * most recent ids - the ones a re-fetch can actually reach - and reports that
   * it is no longer exact.
   */
  maxDedupIds?: number;
}

/**
 * Default size of the duplicate guard, in parts, when the guard is not exact.
 *
 * Three covers a single flush that rotates the tail twice, which is the worst
 * case the callers can produce: a buffer is bounded near 2x the flush
 * threshold and a threshold is never larger than `chunkSize` (see
 * `MAX_FLUSH_MESSAGES` in the extractor), so one append can span at most three
 * part files - and the messages a crash exposes are exactly the ones from the
 * flush that was never acknowledged.
 */
export const DEFAULT_DEDUP_PARTS = 3;

/**
 * Default ceiling on the duplicate guard, in ids.
 *
 * An id costs roughly this many bytes as a member of a `Set<string>`: the
 * string itself plus a hash-table slot. Two million ids is therefore on the
 * order of 100MB, which is the point where an exact index stops being something
 * a capture process should hold without being asked. Past this ceiling the
 * guard keeps the newest ids and says so.
 */
export const BYTES_PER_DEDUP_ID = 56;
export const DEFAULT_MAX_DEDUP_IDS = 2_000_000;

/** What the duplicate guard for one channel actually covers. */
export interface DedupStats {
  /** Ids currently held. */
  ids: number;
  /** Parts read to seed the guard. */
  parts: number;
  /** Parts the archive had when the guard was seeded. */
  partsAvailable: number;
  /** True while the guard covers every archived id. */
  exact: boolean;
  /** True once ids were dropped, so `exact` is no longer true. */
  truncated: boolean;
  /** Wall-clock cost of building the guard. */
  seedMs: number;
  /** Approximate memory the guard holds, from `BYTES_PER_DEDUP_ID`. */
  bytes: number;
  /** Messages this guard has kept out of the archive. */
  skipped: number;
}

/** What indexing a channel would cost, without building the index. */
export interface DedupCostEstimate {
  /** Ids an index would hold under the configured mode. */
  ids: number;
  /** Ids the archive holds in total. */
  archived: number;
  parts: number;
  /** Approximate memory that index would hold. */
  bytes: number;
  /** True when the configured mode indexes the whole channel. */
  exact: boolean;
  /** True when `ids` fits under the ceiling, so it can be held at all. */
  affordable: boolean;
  /** The ceiling itself. */
  maxIds: number;
}

export class JsonStorage {
  private readonly basePath: string;
  private readonly log: Logger;
  private readonly chunkSize: number;
  private readonly pretty: boolean;

  /** Cache is authoritative for this process; avoids re-reading the manifest. */
  private readonly archives = new Map<string, JsonArchive>();
  /** The open (not yet chunk-sized) part per channel, for appendMessages. */
  private readonly tails = new Map<string, TailState>();

  /**
   * Bounded per-channel guard of message ids that are already in the archive,
   * used to keep a crash window from writing the same message twice.
   *
   * The crash window is real and inherent: a run appends data first and only
   * then persists the cursor/baseline it describes, because the opposite order
   * could skip messages. A crash in between therefore leaves up to one flush of
   * *already written* messages with no persisted record that they were written,
   * and the next run legitimately re-fetches them.
   *
   * By default the guard indexes the whole channel (`exactDedup`): every id in
   * the archive, so an id can be recognized no matter which part holds it. That
   * is the only way to make the answer exact, and it is why the guard is capped
   * rather than unbounded - an id per archived message is exactly the cost the
   * chunked design otherwise avoids. `maxDedupIds` is that cap. Past it the
   * guard keeps the newest ids (the ones a re-fetch can reach) and reports
   * `exact: false`, which `status` prints.
   *
   * Either way, dropping an id is only ever safe in one direction: every id in
   * the guard was read back from a part file on disk, or was appended
   * successfully, so a match means the message really is archived. A guard that
   * is short can miss a duplicate (a duplicate, never a loss), and it is seeded
   * lazily so a run that appends nothing pays nothing for it.
   */
  private readonly dedupGuards = new Map<string, Set<string>>();
  /** What each channel's guard covers, for reporting. See `getDedupStats`. */
  private readonly dedupStats = new Map<string, DedupStats>();
  private readonly dedupParts: number;
  private readonly exactDedup: boolean;
  private readonly maxDedupIds: number;

  constructor(basePath: string, log?: Logger, options: JsonStorageOptions = {}) {
    this.basePath = basePath;
    this.log = log || new Logger();
    this.chunkSize = Math.max(1, options.chunkSize ?? DEFAULT_CHUNK_SIZE);
    this.pretty = options.pretty ?? false;
    this.dedupParts = Math.max(0, options.dedupParts ?? DEFAULT_DEDUP_PARTS);
    this.exactDedup = options.exactDedup ?? true;
    this.maxDedupIds = Math.max(1, options.maxDedupIds ?? DEFAULT_MAX_DEDUP_IDS);

    if (!existsSync(basePath)) {
      mkdirSync(basePath, { recursive: true });
    }
  }

  // ==================== Paths ====================

  private getChannelDir(channelId: string): string {
    return join(this.basePath, channelId);
  }

  private ensureChannelDir(channelId: string): string {
    const dir = this.getChannelDir(channelId);
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    return dir;
  }

  private getChunkFilename(channelName: string, part: number): string {
    const safeName = (channelName || 'unknown').replace(/[^a-zA-Z0-9-_]/g, '_');
    return `${safeName}-${part.toString().padStart(6, '0')}.json`;
  }

  private getManifestPath(channelId: string): string {
    return join(this.getChannelDir(channelId), MANIFEST_NAME);
  }

  /** Write via a temp file + rename so readers never see a partial file. */
  private writeFileAtomic(path: string, data: string): void {
    const dir = dirname(path);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    const tmp = `${path}.tmp-${process.pid}`;
    writeFileSync(tmp, data, 'utf-8');
    renameSync(tmp, path);
  }

  private serialize(value: unknown): string {
    return this.pretty ? JSON.stringify(value, null, 2) : JSON.stringify(value);
  }

  // ==================== Archive manifest ====================

  private emptyArchive(channelId: string, channelName: string): JsonArchive {
    return {
      channelId,
      channelName,
      totalMessages: 0,
      totalParts: 0,
      extractedAt: new Date().toISOString(),
      completedAt: null,
      parts: [],
    };
  }

  loadArchive(channelId: string): JsonArchive | null {
    const cached = this.archives.get(channelId);
    if (cached) return cached;

    const manifestPath = this.getManifestPath(channelId);
    if (existsSync(manifestPath)) {
      try {
        const archive = JSON.parse(readFileSync(manifestPath, 'utf-8')) as JsonArchive;
        archive.parts = archive.parts ?? [];
        this.archives.set(channelId, archive);
        return archive;
      } catch (e) {
        this.log.warn(
          `Corrupt archive manifest for ${channelId} (${e instanceof Error ? e.message : e}); rebuilding from chunks`,
        );
      }
    }

    // Manifest missing or unreadable: recover from the chunk files themselves.
    const rebuilt = this.rebuildArchive(channelId);
    if (rebuilt) return rebuilt;

    return null;
  }

  /**
   * Rebuild a manifest by scanning chunk files. Only runs when the manifest is
   * missing/corrupt, so the cost (reading each chunk once) is acceptable.
   */
  rebuildArchive(channelId: string): JsonArchive | null {
    const dir = this.getChannelDir(channelId);
    if (!existsSync(dir)) return null;

    const chunkFiles = readdirSync(dir)
      .filter((f) => f.endsWith('.json') && f !== MANIFEST_NAME && !f.includes('.tmp-'))
      .sort();

    if (chunkFiles.length === 0) return null;

    this.log.warn(`Rebuilding archive manifest for ${channelId} from ${chunkFiles.length} chunk file(s)`);

    const parts: JsonPartMeta[] = [];
    let totalMessages = 0;
    let channelName = channelId;
    let extractedAt = new Date().toISOString();
    let completedAt: string | null = null;

    for (const filename of chunkFiles) {
      // Through `loadChunk`, so a part a crash left open (JSON Lines) is
      // recovered alongside the completed ones.
      const chunk = this.loadChunk(channelId, filename);
      if (!chunk || typeof chunk.part !== 'number' || !Array.isArray(chunk.messages)) {
        this.log.warn(`Skipping unreadable chunk ${filename}`);
        continue;
      }

      channelName = chunk.channelName || channelName;
      extractedAt = chunk.extractedAt || extractedAt;
      parts.push({
        part: chunk.part,
        filename,
        messageCount: chunk.messageCount,
        startId: chunk.startId,
        endId: chunk.endId,
        updatedAt: chunk.extractedAt,
      });
      totalMessages += chunk.messageCount;
    }

    parts.sort((a, b) => a.part - b.part);

    const archive: JsonArchive = {
      channelId,
      channelName,
      totalMessages,
      // The highest part number, not how many parts exist. `getTail` allocates
      // `totalParts + 1`, so a count would hand the next append a part number
      // that a surviving file already uses and overwrite it.
      totalParts: parts.reduce((max, p) => Math.max(max, p.part), 0),
      extractedAt,
      completedAt,
      parts,
    };

    this.persistArchive(archive);
    return archive;
  }

  private persistArchive(archive: JsonArchive): void {
    this.ensureChannelDir(archive.channelId);
    this.writeFileAtomic(this.getManifestPath(archive.channelId), this.serialize(archive));
    this.archives.set(archive.channelId, archive);
  }

  saveArchive(channelId: string, archive: JsonArchive): void {
    this.persistArchive(archive);
  }

  completeArchive(channelId: string): void {
    const archive = this.loadArchive(channelId);
    if (!archive) return;
    archive.completedAt = new Date().toISOString();
    this.persistArchive(archive);
  }

  // ==================== Writing ====================

  /** Write one chunk atomically and return its metadata. */
  private writeChunk(
    channelId: string,
    channelName: string,
    part: number,
    messages: ExportRow[],
  ): JsonPartMeta {
    const dir = this.ensureChannelDir(channelId);
    const filename = this.getChunkFilename(channelName, part);
    const filepath = join(dir, filename);

    const chunk: JsonChunk = {
      channelId,
      channelName,
      part,
      startId: messages[0]?.id || '0',
      endId: messages[messages.length - 1]?.id || '0',
      messageCount: messages.length,
      extractedAt: new Date().toISOString(),
      messages,
    };

    this.writeFileAtomic(filepath, this.serialize(chunk));

    // The manifest entry is built here, next to the file it describes, so the
    // name/count/range can never drift from what was actually written.
    return {
      part,
      filename,
      messageCount: chunk.messageCount,
      startId: chunk.startId,
      endId: chunk.endId,
      updatedAt: new Date().toISOString(),
    };
  }

  /**
   * Create or update the manifest entry for one part.
   *
   * Counters are adjusted by the delta rather than recomputed by reducing over
   * `archive.parts`. The tail part is rewritten on every append, so a running
   * total keeps an append O(1) instead of O(parts) - which matters once a
   * channel has thousands of parts and a live capture is appending every
   * couple of seconds.
   */
  private upsertPartMeta(archive: JsonArchive, meta: JsonPartMeta): void {
    // The tail part is almost always the last entry, so try it first.
    const lastIndex = archive.parts.length - 1;
    const last = archive.parts[lastIndex];
    const index = last?.part === meta.part
      ? lastIndex
      : archive.parts.findIndex((p) => p.part === meta.part);

    if (index >= 0) {
      archive.totalMessages += meta.messageCount - archive.parts[index].messageCount;
      archive.parts[index] = meta;
    } else {
      archive.parts.push(meta);
      archive.parts.sort((a, b) => a.part - b.part);
      archive.totalMessages += meta.messageCount;
    }

    archive.totalParts = Math.max(archive.totalParts, meta.part);
  }

  /**
   * The open part for a channel.
   *
   * A part left open by a previous process (crash, reboot, kill) is readable
   * but nothing will ever fill it, so it is sealed into a normal chunk on the
   * first append of the new process. That is also what repairs a torn trailing
   * line: sealing re-reads the rows from disk and drops anything incomplete.
   */
  private getTail(channelId: string, archive: JsonArchive): TailState {
    const cached = this.tails.get(channelId);
    if (cached) return cached;

    const stale = archive.parts.find((part) => part.open === true);
    if (stale) {
      this.log.debug(`Sealing part ${stale.part} of ${channelId} left open by an earlier run`);
      this.sealOpenPart(channelId, archive, stale);
    }

    const tail: TailState = {
      part: archive.totalParts + 1,
      count: 0,
      startId: '',
      endId: '',
    };
    this.tails.set(channelId, tail);
    return tail;
  }

  private getPartPath(channelId: string, channelName: string, part: number): string {
    return join(this.ensureChannelDir(channelId), this.getChunkFilename(channelName, part));
  }

  /**
   * Open a part: a header line naming it, followed by one message per line.
   *
   * JSON Lines is what makes an append cheap. A chunk is a single JSON value,
   * so adding to it means rewriting the whole file - fine once per part, but
   * ruinous per flush: a 50K-message part rewritten every couple of seconds
   * writes back ~125x the bytes of the messages themselves, and the cost grows
   * with the part. Appending 5KB of new rows is O(new messages) instead.
   *
   * The price is that the file is not one JSON value until it is compacted, so
   * readers go through `loadChunk` (which understands both shapes) and a clean
   * run compacts the part before it exits.
   *
   * The header is written atomically: a half-written first line would make the
   * rest of the part unreadable. Rows are always compact JSON, whatever
   * `PRETTY_JSON` says, because a pretty-printed row would span several lines
   * and stop being a row.
   */
  private openPart(channelId: string, archive: JsonArchive, tail: TailState): void {
    const header = {
      channelId,
      channelName: archive.channelName,
      part: tail.part,
      open: true,
      startedAt: new Date().toISOString(),
    };

    this.writeFileAtomic(
      this.getPartPath(channelId, archive.channelName, tail.part),
      `${JSON.stringify(header)}\n`,
    );
  }

  /**
   * Append rows to the open part - one write for the whole batch.
   *
   * This is the durability point of an append: once it returns, the messages
   * are in the file (the manifest that describes them is written immediately
   * after, but is only an index - the rows are the data).
   */
  private appendPartRows(
    channelId: string,
    archive: JsonArchive,
    tail: TailState,
    rows: ExportRow[],
  ): void {
    const payload = `${rows.map((row) => JSON.stringify(row)).join('\n')}\n`;
    appendFileSync(this.getPartPath(channelId, archive.channelName, tail.part), payload, 'utf-8');
  }

  /** Manifest entry for a part that is still being appended to. */
  private openPartMeta(archive: JsonArchive, tail: TailState): JsonPartMeta {
    return {
      part: tail.part,
      filename: this.getChunkFilename(archive.channelName, tail.part),
      messageCount: tail.count,
      startId: tail.startId,
      endId: tail.endId,
      open: true,
      updatedAt: new Date().toISOString(),
    };
  }

  /**
   * Rewrite an open part as a normal chunk file.
   *
   * Rows are read back from disk rather than taken from memory, which is what
   * makes a torn trailing line (a crash mid-append) vanish here instead of
   * being carried into the manifest.
   *
   * Returns null when the part holds nothing - a crash between the header and
   * the first batch - in which case the file is removed rather than left behind
   * as an empty part.
   */
  private compactPart(channelId: string, archive: JsonArchive, part: number): JsonPartMeta | null {
    const chunk = this.loadChunk(channelId, this.getChunkFilename(archive.channelName, part));
    if (!chunk || chunk.messages.length === 0) {
      this.removePart(channelId, archive, part);
      return null;
    }

    return this.writeChunk(channelId, archive.channelName, part, chunk.messages);
  }

  /** Seal a part that an earlier run left open. */
  private sealOpenPart(channelId: string, archive: JsonArchive, entry: JsonPartMeta): void {
    const sealed = this.compactPart(channelId, archive, entry.part);
    if (sealed) this.upsertPartMeta(archive, sealed);
  }

  /** Drop an aborted, empty part: both its file and its manifest entry. */
  private removePart(channelId: string, archive: JsonArchive, part: number): void {
    try {
      unlinkSync(this.getPartPath(channelId, archive.channelName, part));
    } catch {
      // Already gone; the manifest entry is what has to go.
    }

    const index = archive.parts.findIndex((p) => p.part === part);
    if (index < 0) return;

    archive.totalMessages = Math.max(0, archive.totalMessages - archive.parts[index].messageCount);
    archive.parts.splice(index, 1);
    archive.totalParts = archive.parts.reduce((max, p) => Math.max(max, p.part), 0);
  }  /** Upper bound on a channel's duplicate guard, in ids. */
  private get guardLimit(): number {
    const budget = Math.max(1, this.maxDedupIds);
    return this.exactDedup ? budget : Math.min(budget, this.dedupParts * this.chunkSize);
  }

  /**
   * The duplicate guard for a channel, seeded once per process.
   *
   * In exact mode this reads every part, so the guard holds every archived id
   * and membership is a definitive answer. Otherwise it reads the newest
   * `dedupParts` parts. Returns null when the guard is disabled.
   */
  private getGuard(channelId: string, archive: JsonArchive): Set<string> | null {
    if (this.dedupParts <= 0) return null;

    const cached = this.dedupGuards.get(channelId);
    if (cached) return cached;

    const parts = this.exactDedup ? archive.parts : archive.parts.slice(-this.dedupParts);
    const budget = this.maxDedupIds;
    const startedAt = Date.now();

    // Newest part first. If the archive is too large to index in full, the ids
    // worth keeping are the most recent ones - those are the ones a re-fetch
    // can reach - so scanning newest-first lets the budget stop the work early
    // instead of reading the whole channel and then discarding most of it.
    const collected: string[] = [];
    let partsRead = 0;
    let exhausted = false;

    for (let i = parts.length - 1; i >= 0; i--) {
      const chunk = this.loadChunk(channelId, parts[i].filename);
      partsRead++;

      const rows = chunk?.messages ?? [];
      for (let j = rows.length - 1; j >= 0; j--) {
        if (collected.length >= budget) {
          exhausted = true;
          break;
        }
        collected.push(rows[j].id);
      }
      if (exhausted) break;
    }

    // Oldest first, so the FIFO trim in `guardAdd` drops the oldest ids.
    collected.reverse();
    const guard = new Set(collected);

    this.dedupGuards.set(channelId, guard);
    this.dedupStats.set(channelId, {
      ids: guard.size,
      parts: partsRead,
      partsAvailable: archive.parts.length,
      // "Exact" means one thing only: every archived id is indexed, so a miss is
      // proof the message is not archived. Reading a subset of the parts - the
      // windowed mode - is not exact even when nothing was dropped.
      exact: !exhausted && partsRead >= archive.parts.length,
      truncated: exhausted,
      seedMs: Date.now() - startedAt,
      bytes: guard.size * BYTES_PER_DEDUP_ID,
      skipped: 0,
    });

    // Reported at info: the size of the index is a cost the caller chose, and a
    // truncated one changes what "no duplicates" means for this channel. An
    // empty archive has nothing to seed from - the guard is still built (it is
    // what keeps this run's own writes distinct), but saying "0 of 0 parts" for
    // every new channel is noise.
    if (partsRead > 0) {
      this.log.info(
        `Duplicate index for ${channelId}: ${guard.size} id(s) from ${partsRead}/${archive.parts.length} ` +
          `part(s) in ${Date.now() - startedAt}ms (~${formatBytes(guard.size * BYTES_PER_DEDUP_ID)})`,
      );
    }
    if (exhausted) {
      this.log.warn(
        `Duplicate index for ${channelId} stopped at ARCHIVE_DEDUP_MAX_IDS (${budget}) after ` +
          `${partsRead} of ${archive.parts.length} part(s): ids older than part ${parts.slice(parts.length - partsRead)[0]?.part ?? '?'} ` +
          'are no longer covered, so a duplicate re-fetched from them can be written again. ' +
          'Raise ARCHIVE_DEDUP_MAX_IDS to index the whole channel.',
      );
    }

    return guard;
  }

  /** How the archive guard is configured, for reporting. */
  get dedupConfig(): { exact: boolean; parts: number; maxIds: number } {
    return { exact: this.exactDedup, parts: this.dedupParts, maxIds: this.maxDedupIds };
  }

  /**
   * What the guard for a channel covers, or null if it has not been built yet.
   *
   * The guard is built on the first append, so a process that only reads - or a
   * `status` invocation - has no stats; that is the point of the lazy seed.
   * `estimateDedupCost` covers that case from the manifest alone.
   */
  getDedupStats(channelId: string): DedupStats | null {
    return this.dedupStats.get(channelId) ?? null;
  }

  /**
   * What indexing a channel would cost, straight from the manifest - no part is
   * read, so this is safe to call for every channel in `status`.
   */
  estimateDedupCost(channelId: string): DedupCostEstimate | null {
    const archive = this.loadArchive(channelId);
    if (!archive) return null;

    // Windowed mode never indexes more than the parts it reads.
    const window = Math.min(archive.totalMessages, this.dedupParts * this.chunkSize);
    const ids = this.exactDedup ? archive.totalMessages : window;

    return {
      ids,
      archived: archive.totalMessages,
      parts: archive.totalParts,
      bytes: ids * BYTES_PER_DEDUP_ID,
      exact: this.exactDedup,
      affordable: ids <= this.maxDedupIds,
      maxIds: this.maxDedupIds,
    };
  }

  /** Keep the "guard dropped these" message in one place. */
  private logSkippedDuplicates(channelId: string, skipped: number): void {
    if (skipped <= 0) return;

    const stats = this.dedupStats.get(channelId);
    if (stats) stats.skipped += skipped;

    this.log.debug(`Dropped ${skipped} already-archived message(s) for ${channelId}`);
  }

  /**
   * Record ids that are definitely on disk. Call this *after* the write
   * succeeded - adding an id that never reached the disk would make a later
   * append drop a message that is not actually archived.
   */
  private guardAdd(channelId: string, guard: Set<string> | null, rows: ExportRow[]): void {
    if (!guard || rows.length === 0) return;

    for (const row of rows) guard.add(row.id);

    const limit = this.guardLimit;
    const stats = this.dedupStats.get(channelId);

    if (guard.size <= limit) {
      if (stats) stats.ids = guard.size;
      return;
    }

    // Set iteration is insertion-ordered, so this drops the oldest ids and
    // leaves the guard tracking the most recently written messages.
    let excess = guard.size - limit;
    for (const id of guard) {
      if (excess-- <= 0) break;
      guard.delete(id);
    }

    if (stats) {
      stats.ids = guard.size;
      // Trimmed, so the guard no longer answers for every archived id. Saying
      // "exact" here would be the one thing this report must never do.
      if (!stats.truncated) {
        this.log.warn(
          `Duplicate index for ${channelId} passed ARCHIVE_DEDUP_MAX_IDS (${limit}); ` +
            'the oldest ids are no longer indexed, so duplicates from them can be written again',
        );
      }
      stats.exact = false;
      stats.truncated = true;
    }
  }

  /**
   * Append a batch of messages, continuing the channel's archive.
   *
   * Messages are appended to the open part and the manifest entry for that part
   * is updated, so the batch is durable when this returns. Once the part reaches
   * `chunkSize` it is compacted into a normal chunk and a new part begins. This
   * is what lets bulk extraction and real-time live capture write into the same
   * archive without ever creating a tiny file per message, and it keeps part
   * numbers monotonic across resumes and restarts.
   *
   * Messages the archive already holds are dropped rather than appended again,
   * which closes the crash window described on `dedupGuards`. `written` and
   * `skipped` are reported so callers can keep their own counters honest
   * instead of counting work that was not stored.
   */
  appendMessages(
    channelId: string,
    channelName: string,
    messages: ExportRow[],
  ): { partsWritten: number; archive: JsonArchive; written: number; skipped: number } {
    const archive = this.loadArchive(channelId) ?? this.emptyArchive(channelId, channelName);
    archive.channelName = archive.channelName || channelName;

    if (messages.length === 0) {
      return { partsWritten: 0, archive, written: 0, skipped: 0 };
    }

    const guard = this.getGuard(channelId, archive);
    const sorted = sortBySnowflake(messages);

    let skipped = 0;
    let fresh = sorted;
    if (guard && guard.size > 0) {
      fresh = sorted.filter((row) => {
        if (!guard.has(row.id)) return true;
        skipped++;
        return false;
      });
    }

    if (fresh.length === 0) {
      // Everything in this batch is already archived; nothing to write and no
      // need to touch the manifest.
      this.logSkippedDuplicates(channelId, skipped);
      return { partsWritten: 0, archive, written: 0, skipped };
    }

    let partsWritten = 0;

    for (let index = 0; index < fresh.length; ) {
      const tail = this.getTail(channelId, archive);

      if (tail.count === 0) {
        this.openPart(channelId, archive, tail);
        tail.startId = fresh[index].id;
      }

      // Fill the open part, then rotate so the rest of the batch goes into the
      // next one. `room` is never 0: `getTail` only returns a part that is
      // below `chunkSize`.
      const room = Math.max(1, this.chunkSize - tail.count);
      const batch = fresh.slice(index, index + room);

      this.appendPartRows(channelId, archive, tail, batch);
      tail.count += batch.length;
      tail.endId = batch[batch.length - 1].id;
      index += batch.length;

      // Guard and manifest only after the rows are on disk. An id in the guard
      // that never reached the disk would make a later append drop a real
      // message.
      this.guardAdd(channelId, guard, batch);
      this.upsertPartMeta(archive, this.openPartMeta(archive, tail));

      if (tail.count >= this.chunkSize) {
        const sealed = this.compactPart(channelId, archive, tail.part);
        if (sealed) {
          this.upsertPartMeta(archive, sealed);
          partsWritten++;
        }
        this.tails.delete(channelId);
      }
    }

    this.persistArchive(archive);
    this.logSkippedDuplicates(channelId, skipped);

    return { partsWritten, archive, written: fresh.length, skipped };
  }

  /**
   * Close out the open part for a channel. Data is already on disk (each append
   * appends to it); this compacts the part into the normal chunk format and
   * lets a later append start a clean one.
   *
   * The duplicate guard is released too: the caller finalizes only after the
   * resume state that describes this data has been persisted, so there is
   * nothing left to protect against - and keeping one guard per channel around
   * for the life of the process would grow with the number of channels seen.
   */
  finalizeChannel(channelId: string): JsonArchive | null {
    const tail = this.tails.get(channelId);
    if (tail && tail.count > 0) {
      const archive = this.loadArchive(channelId);
      if (archive) {
        // Compact, so a run that ends cleanly leaves every part in the normal
        // chunk format no matter how `PRETTY_JSON` is set.
        const sealed = this.compactPart(channelId, archive, tail.part);
        if (sealed) this.upsertPartMeta(archive, sealed);
        this.persistArchive(archive);
      }
    }
    this.tails.delete(channelId);
    this.dedupGuards.delete(channelId);
    return this.loadArchive(channelId);
  }

  // ==================== Reading ====================

  /**
   * Read one part. A completed chunk is a single JSON value; a part still being
   * appended to is JSON Lines. The object is tried first so the (far more
   * common) completed part pays nothing for the other shape.
   */
  loadChunk(channelId: string, filename: string): JsonChunk | null {
    const filepath = join(this.getChannelDir(channelId), filename);
    if (!existsSync(filepath)) return null;

    const text = readFileSync(filepath, 'utf-8');
    try {
      return JSON.parse(text) as JsonChunk;
    } catch {
      return this.parseOpenPart(text, filename);
    }
  }

  /**
   * Parse an open part: the first line names the part, every line after it is a
   * message.
   *
   * A line that does not parse is dropped rather than treated as fatal. The
   * only way to produce one is a crash in the middle of an append, and that
   * batch was never acknowledged to the caller - so the run that wrote it never
   * advanced its resume cursor past it, and the next run fetches those messages
   * again.
   */
  private parseOpenPart(text: string, filename: string): JsonChunk | null {
    const lines = text.split('\n').filter((line) => line.length > 0);
    if (lines.length === 0) {
      this.log.warn(`Part file ${filename} is empty; ignoring it`);
      return null;
    }

    const header = tryParseJson<Partial<JsonChunk> & { startedAt?: string }>(lines[0]);
    if (!header || typeof header.part !== 'number') {
      this.log.warn(`Part file ${filename} is not a chunk or an open part; ignoring it`);
      return null;
    }

    const messages: ExportRow[] = [];
    for (let i = 1; i < lines.length; i++) {
      const row = tryParseJson<ExportRow>(lines[i]);
      if (row && typeof row.id === 'string') messages.push(row);
      else this.log.debug(`Dropped an interrupted line ${i + 1} of ${filename}`);
    }

    return {
      channelId: header.channelId ?? '',
      channelName: header.channelName ?? '',
      part: header.part,
      startId: messages[0]?.id ?? '0',
      endId: messages[messages.length - 1]?.id ?? '0',
      messageCount: messages.length,
      extractedAt: header.startedAt ?? new Date().toISOString(),
      messages,
    };
  }

  listChunks(channelId: string): string[] {
    const dir = this.getChannelDir(channelId);
    if (!existsSync(dir)) return [];

    return readdirSync(dir)
      .filter((f) => f.endsWith('.json') && f !== MANIFEST_NAME && !f.includes('.tmp-'))
      .sort();
  }

  /** Iterate chunks one at a time so callers never hold the whole channel. */
  *iterMessages(channelId: string): Generator<ExportRow> {
    const archive = this.loadArchive(channelId);
    if (!archive) return;

    for (const part of archive.parts) {
      const chunk = this.loadChunk(channelId, part.filename);
      if (!chunk) continue;
      for (const message of chunk.messages) {
        yield message;
      }
    }
  }

  loadAllMessages(channelId: string): ExportRow[] {
    const all: ExportRow[] = [];
    for (const message of this.iterMessages(channelId)) {
      all.push(message);
    }
    return sortBySnowflake(all);
  }

  deleteAllChunks(channelId: string): void {
    const dir = this.getChannelDir(channelId);

    for (const chunk of this.listChunks(channelId)) {
      try {
        unlinkSync(join(dir, chunk));
      } catch (e) {
        this.log.error(`Failed to delete chunk ${chunk}: ${e instanceof Error ? e.message : e}`);
      }
    }

    // Clean up any temp files a crash left behind.
    if (existsSync(dir)) {
      for (const file of readdirSync(dir)) {
        if (file.includes('.tmp-')) {
          try { unlinkSync(join(dir, file)); } catch { /* best effort */ }
        }
      }
    }

    const manifestPath = this.getManifestPath(channelId);
    if (existsSync(manifestPath)) {
      try { unlinkSync(manifestPath); } catch (e) {
        this.log.error(`Failed to delete manifest: ${e instanceof Error ? e.message : e}`);
      }
    }

    this.archives.delete(channelId);
    this.tails.delete(channelId);
    // The guard describes the chunks that were just deleted; keeping it would
    // make the re-extraction that follows a reset drop every message as a
    // "duplicate" of data that no longer exists.
    this.dedupGuards.delete(channelId);
  }

  // ==================== Stats / export ====================

  getStorageStats(channelId: string): {
    totalMessages: number;
    totalParts: number;
    totalSizeBytes: number;
    isComplete: boolean;
  } | null {
    const archive = this.loadArchive(channelId);
    if (!archive) return null;

    let totalSize = 0;
    for (const part of archive.parts) {
      const filepath = join(this.getChannelDir(channelId), part.filename);
      try {
        totalSize += statSync(filepath).size;
      } catch {
        // Missing chunk - ignore for stats purposes.
      }
    }

    return {
      totalMessages: archive.totalMessages,
      totalParts: archive.totalParts,
      totalSizeBytes: totalSize,
      isComplete: archive.completedAt !== null,
    };
  }

  /**
   * Stream every message into a single JSON array while holding at most one
   * chunk in memory. Safe for channels with tens of millions of messages.
   */
  exportToSingleJson(channelId: string, outputPath: string): number {
    const archive = this.loadArchive(channelId);
    if (!archive) return 0;

    const fd = openSync(outputPath, 'w');
    let total = 0;
    let first = true;

    try {
      writeSync(fd, '[\n');
      for (const part of archive.parts) {
        const chunk = this.loadChunk(channelId, part.filename);
        if (!chunk) continue;
        for (const message of chunk.messages) {
          const prefix = first ? '' : ',\n';
          writeSync(fd, prefix + this.serialize(message));
          first = false;
          total++;
        }
      }
      writeSync(fd, '\n]\n');
    } finally {
      closeSync(fd);
    }

    this.log.info(`Exported ${total} messages to ${outputPath}`);
    return total;
  }

  /** Stream messages as newline-delimited JSON (constant memory, easy to pipe). */
  exportToJsonl(channelId: string, outputPath: string): number {
    const archive = this.loadArchive(channelId);
    if (!archive) return 0;

    const fd = openSync(outputPath, 'w');
    let total = 0;
    try {
      for (const part of archive.parts) {
        const chunk = this.loadChunk(channelId, part.filename);
        if (!chunk) continue;
        for (const message of chunk.messages) {
          writeSync(fd, `${this.serialize(message)}\n`);
          total++;
        }
      }
    } finally {
      closeSync(fd);
    }
    return total;
  }
}

/**
 * Chronological order by Discord snowflake id.
 *
 * Snowflakes are numeric, not fixed-width, so a string sort is wrong ('10' <
 * '9') and every comparison needs a numeric key. The key is computed once per
 * item: `BigInt()` inside the comparator would re-parse both operands on every
 * comparison, which at a chunk of 50K messages is a few hundred thousand
 * parses per flush - all of them redundant.
 */
export function sortBySnowflake<T extends { id: string }>(items: T[]): T[] {
  try {
    return items
      .map((item) => ({ item, key: BigInt(item.id) }))
      .sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))
      .map((entry) => entry.item);
  } catch {
    // One unparseable id must not make an append throw. Fall back to a
    // deterministic textual order rather than rejecting the batch.
    return [...items].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  }
}
