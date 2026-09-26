// Manifest recovery tests: part numbering must survive a rebuild.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { JsonStorage } from '../src/jsonStorage.js';
import { Logger } from '../src/utils.js';
import type { ExportRow } from '../src/types.js';

const quiet = new Logger('error');

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-manifest-'));
}

function exportRow(id: string): ExportRow {
  return {
    id,
    author: 'tester#0 (u1)',
    content: `msg-${id}`,
    timestamp: new Date().toISOString(),
    replyTo: null,
    replyToAuthor: null,
    attachments: 0,
    attachmentUrls: [],
    imageUrls: [],
    attachmentsDetailed: [],
    threadId: null,
  };
}

function rows(from: number, count: number): ExportRow[] {
  return Array.from({ length: count }, (_, i) => exportRow(String(from + i)));
}

test('a rebuilt manifest counts the highest part number, not the number of parts', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    // Three full parts of ten, on disk as normal chunks.
    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', rows(1, 30));
    first.finalizeChannel('c1');
    assert.equal(first.loadArchive('c1')!.totalParts, 3);

    // Lose the middle part and the manifest: the recovery path has to cope with
    // a gap in the part numbering.
    unlinkSync(join(base, 'c1', 'chan-000002.json'));
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 10 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.deepEqual(rebuilt!.parts.map((p) => p.part), [1, 3], 'expected the two surviving parts');
    assert.equal(
      rebuilt!.totalParts,
      3,
      'totalParts must be the highest part number, not how many parts exist',
    );

    // The next append must therefore open part 4, not reuse part 3 - reusing it
    // overwrites an archived chunk and loses its messages.
    const result = second.appendMessages('c1', 'chan', rows(31, 5));
    assert.equal(result.written, 5);

    const archive = second.loadArchive('c1')!;
    assert.equal(archive.totalParts, 4);
    assert.deepEqual(archive.parts.map((p) => p.part), [1, 3, 4]);

    const stored = new JsonStorage(base, quiet, { chunkSize: 10 })
      .loadAllMessages('c1')
      .map((m) => m.id);
    assert.equal(stored.length, 25, 'the surviving part 3 was overwritten');
    assert.deepEqual(
      stored.sort((a, b) => Number(a) - Number(b)),
      rows(1, 10).concat(rows(21, 10)).concat(rows(31, 5)).map((r) => r.id),
      'recovered messages were lost or duplicated',
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a rebuilt manifest reserves an unreadable higher-numbered part', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');
  const skippedPath = join(base, 'c1', 'chan-000004.json');
  const skippedBytes = 'not-json\n';

  try {
    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', rows(1, 10));
    first.finalizeChannel('c1');
    writeFileSync(skippedPath, skippedBytes, 'utf-8');
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 10 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.deepEqual(rebuilt!.parts.map((part) => part.part), [1]);
    assert.equal(rebuilt!.totalParts, 4, 'the unreadable part number was not reserved');

    second.appendMessages('c1', 'chan', rows(11, 10));

    const archive = second.loadArchive('c1')!;
    assert.deepEqual(archive.parts.map((part) => part.part), [1, 5]);
    assert.equal(archive.totalParts, 5);
    assert.equal(readFileSync(skippedPath, 'utf-8'), skippedBytes);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a gap before an open part does not lower the high-water mark', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    const first = new JsonStorage(base, quiet, { chunkSize: 10 });
    first.appendMessages('c1', 'chan', rows(1, 10));
    first.finalizeChannel('c1');
    writeFileSync(
      join(base, 'c1', 'chan-000003.json'),
      `${JSON.stringify({
        channelId: 'c1',
        channelName: 'chan',
        part: 3,
        open: true,
        startedAt: new Date().toISOString(),
      })}\n`,
      'utf-8',
    );
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 10 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.deepEqual(rebuilt!.parts.map((part) => part.part), [1, 3]);
    assert.equal(rebuilt!.totalParts, 3);
    assert.equal(rebuilt!.parts[1].open, true);

    second.appendMessages('c1', 'chan', rows(11, 10));

    const archive = second.loadArchive('c1')!;
    assert.deepEqual(archive.parts.map((part) => part.part), [1, 4]);
    assert.equal(archive.totalParts, 4, 'sealing the empty open part lowered the high-water mark');
    assert.deepEqual(
      second.loadAllMessages('c1').map((message) => message.id),
      rows(1, 20).map((row) => row.id),
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
