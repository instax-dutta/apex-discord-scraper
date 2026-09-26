# Apex Discord Scraper

Headless multi-channel Discord message extractor that runs on any server. Extracts messages from channels you have access to using your own Discord user session - **no bot required**.

This tool uses the same authentication method as the Vencord FetchChannelMessages plugin - it piggybacks on your logged-in user session to access channels.

## Modes at a glance

Every mode writes into **one shared archive per channel**, so you can mix them
freely without ever duplicating or losing a message.

| Mode | What it does | Cost | Use it when |
|---|---|---|---|
| `extract` | Bulk history for a channel, newest to oldest, time-sharded and parallel | One request per 100 messages, once | First time you take a channel, or you want a full re-scan |
| `extract --incremental` | Fetches only messages newer than the stored baseline | ~1 request when idle | You want a cheap, explicit top-up |
| `extract` (again on a `done` channel) | **Automatically** becomes an incremental catch-up | ~1 request when idle | Day-to-day updates - the default behaviour |
| `live` | Discord Gateway listener that appends messages as they are posted | One persistent WebSocket | You want near-real-time capture |
| `watch` | Recurring incremental catch-up for **every** known channel | ~1 request per channel per cycle | You want the archive to stay current unattended |

### Choosing between them

- **Backfilling history** - `extract`. Run it once per channel. It is the only
  mode that walks backwards through the whole channel.
- **Keeping up to date periodically** - plain `extract` now does the right
  thing automatically (see [Automatic catch-up](#automatic-catch-up)).
- **Keeping up to date continuously** - `live` for the few channels you care
  about in real time, or `watch` if you would rather poll on a cadence than
  hold a WebSocket open.
- **Many channels, low effort** - `watch`, which fans the catch-up out over
  everything the tool has already extracted.

The three "update" modes (`--incremental`, automatic catch-up, `watch`) are all
the same operation underneath: fetch only what is newer than the channel'sbaseline. They differ only in *what triggers* them.

## Architecture

**Messages stored in JSON files** (chunked at `CHUNK_SIZE`, default 50K: appended while a part is open, written atomically once it is finalized) for easy management, backup, and analysis.

**SQLite used only for small metadata** (it stays a few KB no matter how many messages you extract):
- Per-channel progress and per-shard resume state
- Channel configuration

No per-message rows are written to SQLite; the JSON chunks are the source of truth.

## Use Case

You're a member of a Discord server (not an admin) and want to extract and analyze messages from one or more channels. This tool lets you:
- Extract all messages from any channel you can read
- Run headlessly on a server (no Discord app needed)
- Extract from multiple channels in parallel
- Store messages in chunked JSON files (easy to manage)
- Resume interrupted extractions without duplicates
- Catch up only new messages, on demand or on a schedule
- Capture new messages live, into the same archive
- Export to JSON for analysis

## Requirements

- Node.js 18+
- Your Discord user token (not a bot token)

## Setup

### 1. Get Your Discord User Token

1. Open [discord.com](https://discord.com) in a browser and log in
2. Press `F12` to open Developer Tools
3. Go to the **Network** tab
4. Click on any message or refresh the page
5. Find a request to `discord.com/api/v9/...`
6. In the request headers, find `authorization`
7. Copy the token value (it's a long base64 string like `MjM4Njxxxxxxxxx`)

**Important:** Your user token gives full access to your Discord account. Keep it secret.

### 2. Install Dependencies

```bash
cd message-extractor
npm install
```

### 3. Configure

Create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and add your token:

```
DISCORD_USER_TOKEN=your_token_here
```

### 4. Validate Your Token

```bash
npm run validate
```

You should see:
```
Using user token authentication (same method as Vencord plugin)
Token is valid!
User: YourUsername (123456789012345678)
```

## Usage

### List Accessible Channels

See all channels you can access:

```bash
npm run list
```

Output:
```
Fetching accessible channels...

=== My Server (123456789012345678) ===
  #general (987654321098765432)
  #random (987654321098765433)
  #investigation (987654321098765434)
```

### Extract Messages from a Channel

Extract all messages from a single channel:

```bash
npm run extract -- 987654321098765434
```

Output:
```
Using user token authentication (same method as Vencord plugin)

Extracting 1 channel(s) with concurrency 3...

  [OK] #investigation (My Server): 45230 messages in 2m 30s

=== Summary ===
Mode: auto (channels already done are caught up incrementally)
Total messages: 45230
Total time: 2m 30s
Database: ./data/apex-scraper.db
```

### Extract from Multiple Channels in Parallel

```bash
npm run extract -- 987654321098765432 987654321098765433 987654321098765434 --concurrency 3
```

### Incremental Catch-up (only new messages)

Once a channel has been extracted, fetch **only the messages that arrived
since** - no full re-scan, no duplicates:

```bash
npm run extract -- 987654321098765434 --incremental
```

On an idle channel this costs a single request (the fetch returns an empty
page). To override the starting point explicitly:

```bash
npm run extract -- 987654321098765434 --since 1234567890123456789
```

`--since` also implies `--incremental`.

#### How the baseline works

Every channel records two ids in the metadata database:

| Field | Meaning |
|---|---|
| `newest_message_id` | The **highest** message id durably written to disk for this channel. This is the catch-up baseline. |
| `last_message_id` | The last id processed by the most recent run (informational). |

The baseline is deliberately conservative: it only ever moves forward **after
the messages it describes have been flushed to disk**. If the process dies
mid-run, the baseline still points at the last fully-written page, so the next
catch-up re-fetches at most one page - never skips one.

The catch-up itself is one shard covering `(baseline, now]`:

1. A window is built from the baseline to `now + 60s` (the small forward skew
   avoids racing a message that arrives while the window is being computed).
2. Pages are fetched with `before` pagination, exactly like a bulk shard.
3. Messages are kept only when `id > baseline` (strictly greater), so the
   baseline message itself - which is already in the archive - is never
   re-fetched.
4. Because the window is a single shard, an interrupted catch-up resumes
   against the **same saved window** rather than recomputing it.

`newest_message_id` is the *maximum* id of the run, not the last one fetched:
pages arrive newest-to-oldest, so tracking "last" would have recorded the
oldest message of the final batch and made every catch-up re-fetch nearly the
whole channel. Both bulk extraction and live capture advance it.

#### Behaviour matrix

| Situation | What happens |
|---|---|
| `--incremental`, channel has a baseline | Fetches `(baseline, now]` only |
| `--incremental`, no baseline (never extracted) | Logs a warning and performs a normal full extraction |
| `--since <id>` | Same as `--incremental`, with the given lower bound |
| `--incremental`, interrupted mid-run | Resumes the identical window, no gaps or duplicates |
| `--incremental` on a channel mid-bulk-extraction | Reuses nothing; runs as its own single-shard window |
| Plain `extract`, channel already `done` | **Automatically becomes a catch-up** (see below) |
| Plain `extract`, channel not finished | Normal resume from per-shard cursors |
| `--full` | Disables the automatic promotion only |
| `AUTO_INCREMENTAL=false` | Same as `--full`, globally |

An explicit `--incremental` always wins over `--full`: `--full` switches off
only the *automatic* promotion, never an explicit request.

### Automatic catch-up

Re-running a plain `extract` on a channel that is already finished used to be a
**no-op** - the per-shard resume state was complete, so the tool had nothing to
do. That made the most common thing you actually want ("is there anything new?")
the one thing it would not do.

It now promotes that run to an incremental catch-up automatically:

```bash
npm run extract -- 987654321098765434            # catches up automatically
npm run extract -- 987654321098765434 --full      # resume only, no catch-up
AUTO_INCREMENTAL=false                             # disable globally
```

The promotion is deliberately conservative. It happens only when **all** of
these hold:

- `--full` was not passed and `AUTO_INCREMENTAL` is not `false`,
- `--incremental`/`--since` were not passed (an explicit mode is never overridden),
- the channel's status is `done` **and** its archive manifest has a
  `completedAt`, and
- a `newest_message_id` baseline exists.

A channel with no baseline is left alone, so an archive produced before
baselines existed does not silently trigger a full re-extraction.

Because this is the default, a routine `npm run extract -- <id>` on a fully
extracted, idle channel now makes **one** request and adds **zero** messages,
instead of doing nothing at all:

```
[INFO] Channel investigation is already extracted; catching up on messages newer than 1234567890123456789
[INFO] Incremental catch-up for 987654321098765434 since message 1234567890123456789
[INFO] Resuming investigation: 0/1 shards done, 0 partial, 1 pending
[INFO] Extracted 0 new messages from investigation in 143ms (12 JSON parts, 1/1 shards done, 0 partial, 0 pending)
```

The promotion is skipped entirely if the channel is not in the `done` state, so
interrupted work still resumes as interrupted work.

### Recurring Catch-up (`watch`)

`watch` keeps every known channel current without you having to think about it.
It loops: build a plan, run an incremental catch-up for everything due, sleep,
repeat.

```bash
npm run watch -- --interval 15          # every ~15 minutes (default)
npm run watch -- --once                  # a single cycle, then exit
npm run watch -- --concurrency 3         # catch up 3 channels at a time
npm run watch                              # uses WATCH_INTERVAL_MINUTES
```

The interval is in **minutes** (decimals are allowed: `--interval 0.5` runs
every 30 seconds). A small random jitter of up to 10% (capped at 60s) is added
to each interval so multiple instances do not wake in lockstep.

#### What a cycle does

1. **Plan.** Read the metadata table and decide which channels are due.
2. **Catch up.** Hand the due channels to the same extraction engine used by
   `extract`, in parallel up to `--concurrency`, each one running an
   *incremental* catch-up from its own baseline.
3. **Report.** Log per-channel results and the cycle total.
4. **Sleep.** Wait the interval (plus jitter), then repeat.

The scheduler reuses the normal extraction path, so every property documented
under [Resume & Durability](#resume--durability), [Rate Limiting & Backoff](#rate-limiting--backoff)
and [Failure Fallbacks](#failure-fallbacks) applies unchanged. In particular a
transient channel failure gets the standard single retry, and one channel
failing never stops the others.

#### What gets skipped, and why

`watch` runs unattended, so it refuses to do anything surprising. Three
categories are skipped each cycle, and the reason is printed:

| Reason | Meaning | Why |
|---|---|---|
| `no-baseline` | The channel was never extracted | A first-time extraction is a long, expensive, disk-hungry operation - it should be a deliberate decision, not something a background loop starts at 3am |
| `live` | The channel is currently being captured live in this process | The Gateway listener already owns that archive; two writers would fight over part numbers |
| `in-progress` | Channel status is `extracting` | Do not disturb a run that is already working |

To include never-extracted channels in an unattended run, pass
`--include-new`. They are then full-extracted, exactly as if you had run
`extract` on them:

```bash
npm run watch -- --include-new --interval 60
```

Be aware of what that means: `--include-new` turns the scheduler into something
that will start a multi-hour backfill on its own. Good for a controlled
cold-start; usually not what you want left running forever.

#### Shutdown

`Ctrl-C` (or `SIGTERM`) sets the stop flag, **cancels the pending sleep**, and
aborts any in-flight channel. The scheduler then returns, flushes what it has,
and exits. Because resume state is durable, stopping mid-cycle costs nothing -
the next run picks each channel up from its last flushed page.

#### Exit codes

- Continuous mode runs until interrupted; a failed cycle is logged and the
  loop continues.
- `--once` exits `1` if any channel in the cycle failed, `0` otherwise, which
  makes it usable as a cron job or a health check.

A one-shot cycle is often the easiest thing to put on a cron schedule instead
of running the loop -- see [Operations](#operations).

### Live Capture (new messages in real time)

Append new messages to the **same archive** as they are posted, using the
Discord Gateway:

```bash
npm run live -- 987654321098765434
```

Capture several channels at once and skip bot/webhook traffic:

```bash
npm run live -- 987654321098765434 987654321098765435 --exclude-bots
```

Live capture and bulk extraction share one archive: part numbers continue
where extraction stopped, so a later `--incremental` catch-up picks up exactlywhere live capture left off (and vice versa). `Ctrl-C` flushes everything
buffered before exiting.

The library-level `UserTokenExtractor.close()` stops live admission synchronously
and then releases the directory; it is not an asynchronous drain barrier. A flush
already in flight when it is called can finish after the lock is released. The CLI
signal path calls `capture.stop()` first so its normal shutdown drains before close.

A capture never writes history twice. Anything at or below the channel's
stored baseline - the newest id the archive already holds - is dropped, so
restarting `live`, or a replayed dispatch after a Gateway reconnect, cannot
duplicate messages that are already on disk.

### Check Extraction Status

```bash
npm run status
```

Output:
```
=== Extraction Status ===

  [OK] 987654321098765432: done (15234 messages) 1 parts (145 KB)
  [OK] 987654321098765433: done (8932 messages) 1 parts (89 KB)
  [...] 987654321098765434: extracting (45230 messages) 1 parts

=== Storage Info ===
SQLite (metadata): ./data/apex-scraper.db
JSON (messages): ./data/apex-scraper_json/
```

### Export to JSON

After extraction, export a channel to a single JSON file:

```bash
npm run dump -- 987654321098765434 investigation.json
```

`dump` streams one chunk at a time, so it works on channels with tens of
millions of messages without loading them into memory.

### Export to JSONL (streaming)

`--export-jsonl` writes every message a run processes, one JSON object per
line, while that run is happening:

```bash
npm run extract -- 987654321098765434 --export-jsonl ./data/investigation.jsonl
```

A JSONL file is append-oriented: a later catch-up adds only the new messages,
so the file stays a continuous log of everything the archive has ever received
and is trivial to stream (`grep`, `jq`, `wc -l`, BigQuery/Athena loaders) without
parsing the chunked archive.

```bash
# How many messages has this channel ever captured?
wc -l < ./data/investigation.jsonl

# Pull out one author, streaming
jq -c 'select(.author | startswith("someone#"))' ./data/investigation.jsonl
```

**Multiple channels.** Exports are per channel, so a shared path would mean two
concurrent writers interleaving into one file. If you extract more than one
channel, the channel id is inserted before the extension
(`out.jsonl` becomes `out-1234….jsonl`), or the path is treated as a directory
if it has no extension. To control the naming yourself, put `{channel}`
anywhere in the path:

```bash
npm run extract -- 111 222 --export-jsonl './exports/{channel}.jsonl'
```

**Behaviour worth knowing:**

- The lines are written on the **flush path**, not as messages arrive, so the
export shares the archive's durability and order rather than racing ahead of it.
- Writes are awaited in bounded batches, so a slow or full destination applies
backpressure instead of buffering the channel in memory.
- The export is **auxiliary**: if it fails (bad path, full disk), the error is
reported and extraction continues - the JSON archive is still the source of
truth.
- Lines are ordered as fetched (newest-first within a batch), not
chronologically. Sort by `id` if you need strict order.
- The file is a **log of what each run processed**, not a mirror of the archive.
`reset` deletes the archive chunks but leaves the JSONL alone, so a reset and
re-extract leaves the old lines in place and appends the messages again. Delete
the file yourself if you want it to track a fresh archive.

### Resume Interrupted Extraction

An interrupted extraction retains durable per-shard progress, so the same command resumes from where it left off with **no duplicates**:

```bash
npm run extract -- 987654321098765434
```

If the previous process exited without releasing `<DB_PATH>.lock`, verify that it is no longer active and follow the recovery procedure under [Limitations](#limitations) before rerunning.

### Reset Extraction Progress

To re-extract a channel from scratch:

```bash
npm run reset -- 987654321098765434
npm run extract -- 987654321098765434
```

Resetting clears the progress row, the per-shard resume state and the stored
baseline, and deletes every chunk file for the channel. The next extraction is
a genuine full run.

## Command Reference

Every command is also available directly as `node dist/cli.js <command>` or
`apex-scraper <command>` once installed.

| Command | Purpose |
|---|---|
| `validate` | Check the token works and show the logged-in account |
| `list` / `list-channels` | List every channel you can read, grouped by server |
| `extract <id...>` | Extract history, resume, or catch up (auto) |
| `live <id...>` | Capture new messages in real time via the Gateway |
| `watch` | Recurring incremental catch-up for all known channels |
| `status` | Per-channel progress, resume state, shard balance, rate limiter |
| `dump <id> [file]` | Stream a channel out to a single JSON file |
| `reset <id...>` | Forget progress and delete chunks for a channel |
| `query "<sql>"` | Run SQL against the metadata database while holding the writer lock |

### `extract` options

| Option | Default | Purpose |
|---|---|---|
| `--concurrency <n>` | `3` | Channels extracted in parallel |
| `-p`, `--parallelism <n>` | `6` | Time shards per channel (max 12) |
| `--retries <n>` | `8` | Retries per request after the first attempt |
| `--page-delay <ms>` | `100` | Pause between pages within a shard |
| `-i`, `--incremental` | off | Fetch only messages newer than the baseline |
| `--since <id>` | - | Explicit lower bound; implies `--incremental` |
| `--full` | off | Never auto-promote to a catch-up |
| `--no-balance` | balance on | Use equal-time shards instead of density-balanced ones |
| `--balance` | balance on | Force density balancing on |
| `--density-probes <n>` | derived | Probe count to pin (overrides `DENSITY_PROBES`) |
| `--export-jsonl <path>` | - | Also append every processed message as JSONL |

### `watch` options

| Option | Default | Purpose |
|---|---|---|
| `--interval <minutes>` | `15` | Time between catch-up cycles |
| `--concurrency <n>` | `1` | Channels caught up in parallel per cycle |
| `--once` | off | Run a single cycle then exit (exit `1` on any failure) |
| `--include-new` | off | Also full-extract channels with no baseline |

### `live` options

| Option | Default | Purpose |
|---|---|---|
| `--flush-interval <ms>` | `2000` | How often buffered messages are written to disk |
| `--exclude-bots` | off | Drop messages authored by bots or webhooks |

## How It Works

### Authentication

Uses Discord's API with your user session token, just like the Vencord plugin:
- Sends requests to `https://discord.com/api/v9/channels/{id}/messages`
- Includes your `authorization` header
- Access is limited to channels you can read

### Density-Balanced Shards

Equal-time shards are unbalanced because Discord traffic is bursty, so one
shard can hold many times the messages of another and the parallel phase ends
up waiting on it. Before a fresh extraction the tool samples the channel's
message rate with a handful of cheap probe pages and places boundaries so each
shard holds a similar **estimated** number of messages.

Probing is best-effort: if it fails, or the estimate is degenerate, it falls
back to equal-time windows, so it can only help. Disable it with `--no-balance`
or `BALANCE_SHARDS=false`.

The probe count defaults to 3 per shard (clamped to 4-36) and can be pinned:

```bash
npm run extract -- 987654321098765434 --parallelism 8 --density-probes 12
```

The estimate that produced a layout is persisted with the resume state, so
`status` can show it without re-probing:

```
  [OK] 987654321098765434: done (45230 messages) 1 parts (1.2 MB)
        resume: 6/6 shards done, 0 partial, 0 pending
        balance: density, 6 shards, ~7.1K-7.9K msgs/shard, imbalance 11%, 18 probes
        est. per shard: 7.4K | 7.6K | 7.1K | 7.9K | 7.5K | 7.3K
```

### Time-Sharded Parallelism

For large channels, messages are fetched in parallel using time segments:

1. Channel creation time is derived from the channel ID snowflake
2. The channel's lifetime is split into 5 segments
3. Each segment is fetched concurrently with `before` pagination
4. Results are merged and sorted

This is the same algorithm as the original Vencord plugin.

### Live Capture

`live` connects to the Discord Gateway (WebSocket, no extra dependency on
Node 22+), listens for `MESSAGE_CREATE`, and appends into the shared archive.
The Gateway client handles heartbeats, session resume after a drop, sequence
tracking, and bounded reconnect with backoff; fatal close codes (bad token,
bad intents) fail fast instead of reconnecting forever. Buffers are flushed on
an interval, on batch size, and on shutdown, and flushes are serialized per
channel so writes and part numbers stay ordered.

A socket that connects and then never finishes the handshake (a blackholed
proxy, a half-open TCP session) is dropped after 30 seconds instead of being
awaited forever, so the capture either reconnects or fails loudly - it never
sits there looking healthy while capturing nothing.

### Incremental Catch-up Internals

`src/userTokenExtractor.ts` decides the window; the same shard fetcher used for
full extractions does the work. A few properties are worth calling out because
they are what make repeated catch-ups safe:

- **One shard, not many.** A catch-up window is almost always small, so
  splitting it would spend more requests on bookkeeping than on messages. It
  also means resume is trivial: the whole window is one cursor.
- **Exclusive lower bound.** The filter is `id > baseline && id < windowEnd`.
  The baseline is a message already in the archive, so a `>=` comparison would
  duplicate it on every single run.
- **The baseline never runs ahead of the disk.** `newest_message_id` is updated
  inside the flush path, after the batch has been appended and the archive
  manifest persisted. A crash therefore causes the next catch-up to re-read a
  page - never to skip one. Re-reading is harmless too: the archive's duplicate
  guard drops the messages it already holds (see
  [Resume & Durability](#resume--durability)).
- **The baseline is the maximum id, not the last one.** Pages arrive
  newest-to-oldest, so the final element of a batch is the oldest message in
  it. Recording that would make each catch-up re-fetch almost the entire
  channel.
- **Catch-ups append to the existing archive.** They continue part numbering
  where the archive left off (`JsonStorage.appendMessages`), so a channel's
  files stay one continuous, ordered sequence no matter how many modes wrote
  to it.
- **Non-incremental runs still work.** If the saved resume state is complete,
  a resumed run is a no-op; if the state is partial, it resumes normally. The
  automatic promotion only changes what happens to *finished* channels.
- **Live capture and catch-up agree.** Both advance `newest_message_id` from the
  ids they actually write, so switching between `live` and `extract` never
  re-reads or skips a message.

### The Catch-Up Scheduler

`src/scheduler.ts` holds the scheduling policy; the CLI is a thin wrapper.
The split matters because the interesting logic is the eligibility rule, which
is a **pure function**:

```ts
planCatchUp(channels, { includeMissingBaseline, activeChannelIds })
  -> { due: CatchUpPlanItem[], skipped: SkippedChannel[] }
```

Given the metadata rows and the set of channels currently active in this
process, it returns exactly which channels to catch up and why the others were
left out. No database, no network, no clock - which is why it is directly
testable and why the skip reasons can be printed honestly.

A cycle then hands the due channels to `extractAll` with `incremental: true`
and the configured concurrency. There is no separate code path for "scheduled"
extraction, so scheduled runs get the same retry, backoff, resume and
fallback behaviour as manual ones.

The loop itself is deliberately simple:

```
while not stopped:
    try:    runCycle()          # never rejects
    except: log and continue
    sleep(intervalMs + jitter)  # cancellable
```

- **A failed cycle never kills the scheduler.** Exceptions are caught and
  logged; the next cycle runs on schedule.
- **The sleep is cancellable.** `stop()` resolves the pending timer directly
  rather than waiting it out, so `Ctrl-C` stops in milliseconds.
- **`stop()` also aborts in-flight work** through the extractor's abort path,
  so a cycle that is mid-channel returns promptly with its durable cursor
  intact.

### Memory Safety

Memory is bounded and does not grow with the number of messages extracted:

- Messages flow through a capped buffer sized to one chunk (`CHUNK_SIZE`).
- Shards pause via a backpressure hook once the buffer passes 2x that size,
  so a slow disk can never cause unbounded growth.
- Chunks are written to disk and dropped from memory. Appending to the open
  part writes only the new rows, so a flush costs O(new messages) rather than
  O(part) - the same total bytes are written once either way, but a growing
  part is no longer rewritten on every flush.
- Duplicate guards are bounded, not per-message: the in-run guard is capped at
  200k ids, and the archive index is capped at `ARCHIVE_DEDUP_MAX_IDS`
  (2M ids, roughly 100MB). Past the cap the newest ids are kept and the
  coverage is reported as not exact rather than growing without limit.
- `status` uses `statSync`, and `dump` streams one chunk at a time, so neither
  reads the whole channel into memory.

### Resume & Durability

Resume is per-shard and crash-safe:

- Each time shard keeps its own cursor (a channel split into 6 shards is
  resumed as 6 independent cursors). A single "last message id" cannot
  describe a parallel fetch, which is why the old approach could leave gaps.
- Cursors are snapshotted at **flush time** and only then persisted. A crash
  between flushes re-fetches that page instead of skipping it, so **a crash
  cannot lose messages**.
- That re-fetch used to be stored a second time. It no longer is: the archive
  indexes the ids it holds and drops a message it already has. The index is
  built lazily on the first append, so a run that writes nothing pays nothing,
  and it can only ever drop a message that is genuinely on disk.
- **The index is exact by default**: it reads every part of the channel, so an
  id is recognized no matter how old it is (`EXACT_ARCHIVE_DEDUP=false`
  restores the cheaper newest-parts window).
- **Exactness costs one entry per archived id**, which is exactly what the
  chunked design otherwise avoids: about 56 bytes of memory per id, plus the
  time to read the channel's parts once. An id index over 10M messages is
  ~560MB, so the memory is capped by `ARCHIVE_DEDUP_MAX_IDS` (default 2M ids,
  ~100MB). Past the cap the index keeps the *newest* ids - the ones a re-fetch
  can actually reach - stops reading, warns, and reports `exact: false`.
- `status` prints the projected cost straight from the manifest (ids, parts,
  approximate memory, and whether the cap covers it), so the price of indexing
  a channel is visible before a run pays it. The run log adds what was actually
  built: ids, parts read, seed time, exactness, duplicates dropped.
- When the index is not exact, a duplicate from the uncovered range can be
  written again. That is a duplicate, never a loss.
- Completed chunks, the manifest, and the metadata database are written to a
  temp file and atomically renamed. A crash never leaves a truncated chunk, and
  never leaves a truncated progress database either.
- The part being appended to is the one file that is *not* rewritten per flush:
  rows are appended as JSON Lines after a header line, and the manifest entry
  records how many of them are committed. So a crash can leave a half-written
  line at the end of that file, and nothing else. The line is dropped when the
  part is compacted, and because the batch that wrote it was never
  acknowledged, the next run fetches those messages again - it is re-fetchable
  by construction, which is what makes an append safe to use as the durability
  point.
- The open part is compacted into the normal single-object chunk format when it
  fills, when live capture stops, and at the end of an extraction. A part left
  open by a killed process is compacted by the next run that appends to it.
- If the manifest (`_archive.json`) is ever lost or corrupted, it is rebuilt
  automatically by scanning the chunk files.
- If the metadata database is unreadable, it is moved aside as
  `*.corrupt-<timestamp>` and a fresh one is created instead of refusing to
  start. Losing it only costs a re-extraction; it should not make every
  command fail until someone deletes a file by hand.
- Re-running the same `extract` command resumes exactly where each shard
  stopped. Completed shards are skipped entirely.

### Rate Limiting & Backoff

Every request goes through one rate-limit layer (`src/rateLimit.ts`), and the
API client is the only place that retries:

- **Classification first**: 429 / 408 / 425 / 5xx / timeouts / socket errors
  are retried; 401/403 (bad token), 404 (missing channel), and other 4xx fail
  fast with an actionable message instead of looping.
- **Bounded retries**: `MAX_RETRIES` attempts after the initial try, then a
  typed error. There is no `while (true)` retry path.
- **Server-guided delays**: `Retry-After`, `X-RateLimit-Reset-After` and
  `X-RateLimit-Reset` are honoured (including values only present in the 429
  JSON body), up to 15 minutes, so global/abuse limits are respected rather
  than hammered. `Reset-After` is preferred because it is unambiguously
  relative to now; the ambiguous `Reset` header is only used as a fallback.
- **Global limits**: `X-RateLimit-Global`, `X-RateLimit-Scope: global`, and
  `"global": true` in the body block every bucket until the reset.
- **Per-bucket pacing**: when `X-RateLimit-Remaining` hits 0, only that
  channel's bucket waits - other channels keep flowing.
- **Adaptive pacing**: the inter-request interval doubles after a 429 and
  decays back toward the configured floor after a success streak.
- **Circuit breaker**: repeated failures pause all requests briefly instead of
  producing a retry storm, then probe with half-open requests.
- **Jitter** on every delay avoids synchronized retry storms.

### Failure Fallbacks

Failures degrade instead of aborting the run:

1. A shard that fails is retried **serially** after the parallel phase (less
   pressure is usually what it needed).
2. If **every** shard fails before any data arrives, the fetcher abandons
   sharding and does a single **sequential sweep** - bounded to the pending
   shards' own windows and cursors, so it can only ever fetch work that is
   genuinely missing. A failed incremental catch-up therefore retries its own
   window instead of re-downloading (and re-appending) the whole channel, and
   a resume is never swept across shards that were already completed.
3. A channel failure never aborts other channels in the same run.
4. Transient channel failures get one automatic retry; auth/not-found/abort/
   disk-full failures do not (retrying those would be a debugging loop).
5. If the disk fills mid-run, extraction stops cleanly with the durable resume
   cursor intact - free space, rerun, and it continues.
6. A preflight free-space check refuses to start with under 256&nbsp;MB free
   and warns under 2&nbsp;GB.

## Storage Structure

```
./data/
├── apex-scraper.db              # SQLite (metadata only - stays tiny)
│   ├── channel_progress        # status, totals, baseline, per-shard resume_state
│   ├── channels                # Channel configuration
│   └── live_sessions           # Live capture session bookkeeping
│
└── apex-scraper_json/          # JSON message storage
    └── {channel_id}/
        ├── _archive.json            # Manifest (rebuilt automatically if lost)
        ├── channelName-000001.json  # Chunk 1 (up to CHUNK_SIZE messages)
        ├── channelName-000002.json  # Chunk 2 - the open part while it is being
        └── ...                      #   appended to: a header line, then one
                                     #   message per line; compacted to the
                                     #   normal single-object chunk when it
                                     #   fills, and on a clean stop
```

No per-message rows are written to SQLite, so the metadata database stays a
few KB regardless of how many messages are extracted.

### Metadata schema

`channel_progress` is the table every mode reads and writes. The columns that
matter for catch-up are:

| Column | Meaning |
|---|---|
| `status` | `pending` / `extracting` / `done` / `error` / `live` |
| `total_extracted` | Cumulative messages written for the channel |
| `newest_message_id` | **Catch-up baseline** - highest id durably written |
| `last_message_id` | Last id processed by the most recent run (informational) |
| `resume_state` | JSON blobs holding the per-shard cursors, the exact shard windows, and the shard-balance estimate |
| `error_message` | Most recent failure, shown by `status` |

`resume_state` is versioned. It looks like this (abridged):

```json
{
  "version": 2,
  "parallelism": 6,
  "segments": [
    { "index": 0, "after": "…", "before": "…", "cursor": null, "done": true }
  ],
  "balance": {
    "strategy": "density",
    "imbalance": 0.11,
    "loads": [7400, 7600, 7100, 7900, 7500, 7300],
    "minLoad": 7100, "maxLoad": 7900,
    "probes": 18
  },
  "updatedAt": "2024-01-15T10:30:00.000Z"
}
```

Storing the **windows** alongside the cursors is what makes resume exact:
boundaries are normally derived from the current time, so recomputing them a
day later would shift every boundary and could leave a gap in history.

Chunk files are written with a temp-file-then-rename dance, so a reader never
sees a partially written chunk, and a crash never truncates one.

### JSON Message Format

Chunks are compact JSON by default (roughly half the size and faster to write
than pretty-printed). Set `PRETTY_JSON=true` if you want indentation. Each
chunk file contains:

```json
{
  "channelId": "987654321098765434",
  "channelName": "investigation",
  "part": 1,
  "startId": "1234567890123456789",
  "endId": "1234567890123457900",
  "messageCount": 50000,
  "extractedAt": "2024-01-15T10:30:00.000Z",
  "messages": [
    {
      "id": "1234567890123456789",
      "author": "username#0 (123456789012345678)",
      "content": "Hello world",
      "timestamp": "2024-01-15T10:30:00.000Z",
      "replyTo": null,
      "replyToAuthor": null,
      "attachments": 0,
      "attachmentUrls": [],
      "imageUrls": [],
      "attachmentsDetailed": [],
      "threadId": null
    }
  ]
}
```

## Scale: 100M+ messages in one session

The extractor is designed to run for days without growing memory or losing
data. At 100M messages a single channel produces roughly:

- ~1,000,000 API requests (100 messages per page, the Discord max)
- ~2,000 chunk files at the default `CHUNK_SIZE=50000`
- ~20-40 GB on disk with compact JSON

Throughput is dominated by Discord's **per-channel** rate limit (not by this
tool), so the numbers to tune are:

| Knob | Effect |
|---|---|
| `PARALLELISM` | Time shards per channel (default 6, max 12). More shards = better utilization but no more than the rate limit allows. |
| `REQUESTS_PER_SECOND` | Sustained request budget across all channels (default 4). Raise carefully; adaptive pacing backs off on 429s. |
| `--concurrency` | Channels extracted in parallel. Separate channels have separate buckets, so this is how you use the global limit. |
| `CHUNK_SIZE` | Messages per chunk file. Larger = fewer files, more memory per write. |
| `PAGE_DELAY_MS` | Pause between pages within a shard (default 100). |

Interrupting never makes the archive unrecoverable: progress is per-shard,
durable, and validated at startup. A `Ctrl-C`, crash, or reboot can nevertheless
leave `<DB_PATH>.lock` behind. Before rerunning after a crash or reboot, verify
that no writer is active, then remove the lock or explicitly override it using
the procedure under [Limitations](#limitations).

### Environment variables

**Authentication**

| Variable | Default | Purpose |
|---|---|---|
| `DISCORD_USER_TOKEN` | - | User session token (recommended) |
| `DISCORD_BOT_TOKEN` | - | Alternative bot token |

**Storage**

| Variable | Default | Purpose |
|---|---|---|
| `DB_PATH` | `./data/apex-scraper.db` | Metadata DB; JSON lives in the sibling `_json` directory |
| `CHUNK_SIZE` | `50000` | Messages per JSON chunk |
| `PRETTY_JSON` | `false` | Indent chunk JSON (roughly doubles size and write time) |
| `ARCHIVE_DEDUP_PARTS` | `3` | Newest parts indexed when `EXACT_ARCHIVE_DEDUP=false` (`0` disables dedup) |
| `EXACT_ARCHIVE_DEDUP` | `true` | Index the whole archive, so a duplicate is caught no matter how old it is |
| `ARCHIVE_DEDUP_MAX_IDS` | `2000000` | Ceiling on the index, in ids (~100MB); past it the newest ids are kept |

**Throughput & retries**

| Variable | Default | Purpose |
|---|---|---|
| `PARALLELISM` | `6` | Time shards per channel (max 12) |
| `PAGE_DELAY_MS` | `100` | Delay between pages in a shard |
| `REQUESTS_PER_SECOND` | `4` | Sustained request budget |
| `REQUEST_TIMEOUT_MS` | `30000` | Per-request deadline |
| `MAX_RETRIES` | `8` | Retries per request after the first attempt |
| `BACKOFF_BASE_MS` | `1000` | Base for our own exponential backoff |
| `MAX_BACKOFF_MS` | `60000` | Cap for our own exponential backoff |

**Shard balancing**

| Variable | Default | Purpose |
|---|---|---|
| `BALANCE_SHARDS` | `true` | Place shard boundaries by observed message density |
| `DENSITY_PROBES` | derived | Explicit probe count (3 per shard, clamped 4-36) |

**Catch-up & scheduling**

| Variable | Default | Purpose |
|---|---|---|
| `AUTO_INCREMENTAL` | `true` | Plain `extract` catches up already-`done` channels |
| `WATCH_INTERVAL_MINUTES` | `15` | Minutes between `watch` cycles (a `--interval` flag overrides it) |

**Live capture**

| Variable | Default | Purpose |
|---|---|---|
| `LIVE_FLUSH_INTERVAL_MS` | `2000` | How often live buffers are flushed to disk |
| `LIVE_BATCH_MESSAGES` | `200` | Buffered messages that force a live flush |
| `LIVE_MAX_BUFFER` | `10000` | Hard cap on buffered live messages per channel |

**Logging**

| Variable | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

Resolution order is: **CLI flag > real environment variable > `.env` file**.
A real environment variable always beats `.env`, so Docker, systemd and
Kubernetes overrides work the way you would expect without editing files. For
example, all three of these are equivalent:

```bash
WATCH_INTERVAL_MINUTES=5 npm run watch
npm run watch -- --interval 5
# .env: WATCH_INTERVAL_MINUTES=5
```

## Operations

### Running the scheduler as a service

`watch` is designed to be supervised. Cycles never overlap - if a cycle takes
longer than the interval, the next one starts immediately after it finishes
rather than stacking up.

**systemd** (`/etc/systemd/system/apex-watch.service`):

```ini
[Unit]
Description=Apex Discord Scraper - catch-up scheduler
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/apex-scraper/message-extractor
EnvironmentFile=/opt/apex-scraper/message-extractor/.env
ExecStart=/usr/bin/npm run watch -- --interval 15 --concurrency 2
Restart=always
RestartSec=30
# SIGTERM cancels the pending sleep and aborts in-flight channels gracefully.
KillSignal=SIGTERM
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now apex-watch
journalctl -u apex-watch -f
```

**cron** (one-shot cycles instead of a long-running process):

```cron
*/15 * * * * cd /opt/apex-scraper/message-extractor && npm run watch -- --once >> /var/log/apex-watch.log 2>&1
```

`--once` exits `1` if any channel failed, so cron will mail you on failures if
`MAILTO` is set, and the run is naturally guarded against overlap by the
schedule itself.

**Docker Compose**:

```yaml
services:
  apex-watch:
    build: .
    command: npm run watch -- --interval 15
    env_file: .env
    volumes:
      - ./data:/app/data
    restart: unless-stopped
```

### Choosing a cadence

| Cadence | Suitable for |
|---|---|
| 1-5 min | Very active channels where you want minimal lag and accept the request cost |
| 15 min (default) | Most communities - a good balance of freshness and request volume |
| 1-6 h | Low-traffic archives or large channel sets |

Because each channel costs **one request when idle**, the practical cost of a
cycle is roughly `channels / concurrency` request round-trips. Raise
`--concurrency` when you track many channels; the rate limiter still governs
the global request budget.

### Monitoring

- Each cycle prints its plan: a `due:` line per channel to catch up, and a
  `skip <id>: <reason>` line for everything it deliberately left alone.
- `npm run status` shows per-channel state, including `[LIVE]` for channels
  currently captured and the persisted shard-balance estimate.
- The scheduler logs one summary line per cycle: due/skipped counts, then the
  message and failure totals.
- `LOG_LEVEL=debug` adds per-request detail for diagnosing rate limits and
  retries.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `watch` reports `nothing due` forever | The channels were never extracted, so they have no baseline | Run `extract` once per channel first, or use `--include-new` |
| A channel is skipped as `live` | It is being captured by `live` in the same process | Expected - stop `live`, or leave the scheduler to handle the others |
| A channel is skipped as `in-progress` | Another extraction is running for it | Wait for it to finish; the next cycle picks it up |
| `--once` exits `1` | At least one channel failed that cycle | Check the logged error; transient failures are already retried once |
| Catch-up re-fetches the same page every run | The baseline is not advancing | Confirm writes succeed (disk space, permissions) - the baseline only moves after a successful flush |
| First cycle after `--include-new` runs for hours | It is backfilling full history | Expected; `Ctrl-C` is safe and it resumes next cycle |

## Deployment

### Run on a VPS

```bash
# SSH into your server
ssh user@your-server

# Clone and setup
git clone https://github.com/your-repo/apex-scraper.git
cd apex-scraper
npm install

# Configure
cp .env.example .env
nano .env  # Add your token

# Run extraction
npm run extract -- <channel_id>
```

### Docker (Optional)

```dockerfile
FROM node:20-slim

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
CMD ["npm", "start"]
```

```bash
docker build -t apex-scraper .
docker run -v $(pwd)/data:/app/data -e DISCORD_USER_TOKEN=your_token apex-scraper extract <channel_id>
```

## Limitations

- **One process per data directory**: every command that opens the data directory, including
  `query`, takes an exclusive lock on `<DB_PATH>.lock`, so a second run against the same `DB_PATH`
  stops immediately instead of corrupting the archive. An existing lock is never taken or reclaimed
  automatically, even if its recorded process has died. Without the override, the error reports the
  recorded pid, host, and acquisition time but does not probe liveness. If you have verified that no
  writer is active, remove the lock file manually or set `APEX_SCRAPER_FORCE_UNLOCK=1` and retry;
  the override explicitly accepts the risk of racing another writer. If the lock file cannot be read
  or lacks usable owner metadata, its owner is unknown and the override cannot establish ownership,
  so remove it manually only after identifying the owner. Ownership tokens prevent a stale handle
  from releasing a normally reacquired lock, but that protection assumes the file is not manually
  replaced between the token check and removal. Manual replacement has the same race risk as the
  override and must never be done while a writer is active.
- **Access limited to your permissions**: You can only extract channels you can already read in Discord.
- **History starts when you joined**: Discord only serves message history from the point you
  gained access to the channel. No tool can retrieve what was there before you could see it -
  this is a server-side rule, not a scraper limitation.
- **Deletes and edits are invisible**: A catch-up sees the channel *as it is now*. If a message was
  deleted or edited between two runs, there is nothing left to fetch. Capture more often (`live`,
  or a short `watch` interval) if that matters to you.
- **No permissions interception**: If you lose access to a channel, catch-ups and live capture for
  it start failing with `403`. The failure is reported and does not affect other channels.
- **User tokens are against Discord's ToS**: The same gray area as every browser extension. Your
  token grants full account access - treat it like a password.
- **Bot tokens work but are rarely useful here**: `DISCORD_BOT_TOKEN` is supported as an
  alternative, but it requires a bot that is already in the server with read access, which is
  exactly what most users of this tool do not have.

## Getting Help

- Make sure your token is valid and not expired
- Check that you can see the channel in Discord (web or app)
- Try a smaller channel first to verify setup
- Use `LOG_LEVEL=debug` in .env for verbose output

## License

MIT
