// Manifest recovery tests: an open JSON-Lines part must stay recognisable.
// Run with: npm test

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, unlinkSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { JsonStorage } from '../src/jsonStorage.js';
import { Logger } from '../src/utils.js';
import type { ExportRow } from '../src/types.js';

const quiet = new Logger('error');

function makeTmpDir(): string {
  return mkdtempSync(join(tmpdir(), 'apex-openpart-'));
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

test('a rebuilt manifest keeps an open part open so the next append seals it', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');

  try {
    // A part that was still being appended to when the process died: JSON Lines
    // on disk, and a manifest entry marked `open`.
    const first = new JsonStorage(base, quiet, { chunkSize: 1000 });
    first.appendMessages('c1', 'chan', Array.from({ length: 5 }, (_, i) => exportRow(String(1000 + i))));
    assert.equal(first.loadArchive('c1')!.parts[0].open, true);
    // Deliberately no finalizeChannel: the part stays open.

    // Lose only the manifest. The part file itself is intact JSON Lines.
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 1000 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.equal(rebuilt!.totalMessages, 5);
    assert.equal(
      rebuilt!.parts[0].open,
      true,
      'the rebuild forgot the part is still JSON Lines',
    );

    // The next append goes through `getTail`, which seals the stale open part
    // into a normal chunk before opening a new one.
    second.appendMessages('c1', 'chan', Array.from({ length: 3 }, (_, i) => exportRow(String(2000 + i))));

    const partPath = join(base, 'c1', 'chan-000001.json');
    const sealed = JSON.parse(readFileSync(partPath, 'utf-8'));
    assert.equal(sealed.open, undefined, 'the sealed part still carries the open flag');
    assert.equal(sealed.messageCount, 5);

    const archive = second.loadArchive('c1')!;
    assert.equal(archive.totalParts, 2);
    assert.equal(archive.parts[0].open, undefined);
    assert.equal(archive.totalMessages, 8);

    const stored = second.loadAllMessages('c1').map((m) => m.id);
    assert.equal(stored.length, 8);
    assert.equal(new Set(stored).size, 8);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a parsed non-chunk object is skipped without being unlinked by the next append', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');
  const skippedPath = join(base, 'c1', 'chan-000001.json');
  const skippedBytes = `${JSON.stringify({
    channelId: 'c1',
    channelName: 'chan',
    part: 1,
    startedAt: new Date().toISOString(),
  })}\n`;

  try {
    const first = new JsonStorage(base, quiet, { chunkSize: 1000 });
    first.appendMessages('c1', 'chan', [exportRow('1')]);
    first.finalizeChannel('c1');
    unlinkSync(skippedPath);
    writeFileSync(skippedPath, skippedBytes, 'utf-8');
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 1000 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.deepEqual(rebuilt!.parts, [], 'the parsed non-chunk object became an open part');

    second.appendMessages('c1', 'chan', [exportRow('2')]);

    const archive = second.loadArchive('c1')!;
    assert.deepEqual(archive.parts.map((part) => part.part), [2]);
    assert.equal(archive.totalParts, 2);
    assert.equal(readFileSync(skippedPath, 'utf-8'), skippedBytes);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a header-only open part is recognised and sealed without accepting a corrupt neighbour', () => {
  const dir = makeTmpDir();
  const base = join(dir, 'json');
  const headerPath = join(base, 'c1', 'chan-000002.json');
  const corruptPath = join(base, 'c1', 'chan-000003.json');
  const corruptBytes = `${JSON.stringify({
    channelId: 'c1',
    channelName: 'chan',
    part: 3,
    startedAt: new Date().toISOString(),
  })}\n`;

  try {
    const first = new JsonStorage(base, quiet, { chunkSize: 1000 });
    first.appendMessages('c1', 'chan', [exportRow('1')]);
    first.finalizeChannel('c1');
    writeFileSync(
      headerPath,
      `${JSON.stringify({
        channelId: 'c1',
        channelName: 'chan',
        part: 2,
        open: true,
        startedAt: new Date().toISOString(),
      })}\n`,
      'utf-8',
    );
    writeFileSync(corruptPath, corruptBytes, 'utf-8');
    unlinkSync(join(base, 'c1', '_archive.json'));

    const second = new JsonStorage(base, quiet, { chunkSize: 1000 });
    const rebuilt = second.loadArchive('c1');
    assert.ok(rebuilt, 'archive was not rebuilt');
    assert.deepEqual(rebuilt!.parts.map((part) => part.part), [1, 2]);
    assert.equal(rebuilt!.parts[1].open, true);
    assert.equal(rebuilt!.totalParts, 3);

    second.appendMessages('c1', 'chan', [exportRow('4')]);
    second.finalizeChannel('c1');

    const archive = second.loadArchive('c1')!;
    assert.deepEqual(archive.parts.map((part) => part.part), [1, 4]);
    assert.equal(archive.totalParts, 4);
    assert.equal(archive.parts[1].open, undefined);
    assert.equal(readFileSync(corruptPath, 'utf-8'), corruptBytes);
    assert.deepEqual(second.loadAllMessages('c1').map((message) => message.id), ['1', '4']);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
