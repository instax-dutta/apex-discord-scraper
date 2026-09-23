#!/usr/bin/env node
// Apex Discord Scraper - CLI
// Headless multi-channel Discord message extraction using user tokens.
// No bot required - uses the same auth as the Vencord plugin.

import { existsSync, readFileSync, writeFileSync, mkdirSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import type { ScraperConfig } from './types.js';
import { UserTokenExtractor, type ChannelInfo } from './userTokenExtractor.js';
import { Logger, formatDuration, formatBytes, resolveJsonlPath } from './utils.js';
import { parseResumeState, describeResumeState } from './resumable.js';
import { describeBalance, formatCount } from './segments.js';
import { CatchUpScheduler, DEFAULT_INTERVAL_MS } from './scheduler.js';
import { DEFAULT_DEDUP_PARTS, DEFAULT_MAX_DEDUP_IDS } from './jsonStorage.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Read `.env` if present. Values from the real environment win. */
function loadFileEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  const envPath = join(__dirname, '..', '.env');

  if (!existsSync(envPath)) return env;

  const content = readFileSync(envPath, 'utf-8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex > 0) {
      const key = trimmed.slice(0, eqIndex).trim();
      const value = trimmed.slice(eqIndex + 1).trim();
      env[key] = value;
    }
  }
  return env;
}

function createEnvResolver(): (key: string) => string | undefined {
  const fileEnv = loadFileEnv();
  return (key: string) => process.env[key] ?? fileEnv[key];
}

function parseNumber(value: string | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

function getConfig(): ScraperConfig {
  const env = createEnvResolver();

  const userToken = env('DISCORD_USER_TOKEN') || '';
  const botToken = env('DISCORD_BOT_TOKEN') || '';

  if (!userToken && !botToken) {
    console.error('ERROR: Neither DISCORD_USER_TOKEN nor DISCORD_BOT_TOKEN is set.');
    console.error('\nTo get your user token:');
    console.error('1. Open discord.com in a browser and log in');
    console.error('2. Press F12 to open DevTools');
    console.error('3. Go to the Network tab');
    console.error('4. Find any request to discord.com/api/v9/...');
    console.error('5. Copy the "authorization" header value');
    console.error('\nThen set it: DISCORD_USER_TOKEN=your_token_here (in .env or the environment)');
    process.exit(1);
  }

  console.log(
    userToken
      ? 'Using user token authentication (same method as the Vencord plugin)'
      : 'Using bot token authentication',
  );

  return {
    botToken,
    userToken: userToken || botToken,
    dbPath: env('DB_PATH') || './data/apex-scraper.db',
    parallelism: parseNumber(env('PARALLELISM'), 6),
    chunkSize: parseNumber(env('CHUNK_SIZE'), 50000),
    pageDelayMs: parseNumber(env('PAGE_DELAY_MS'), 100),
    maxRetries: parseNumber(env('MAX_RETRIES'), 8),
    backoffBaseMs: parseNumber(env('BACKOFF_BASE_MS'), 1000),
    liveMaxBuffer: parseNumber(env('LIVE_MAX_BUFFER'), 10000),
    liveBatchMessages: parseNumber(env('LIVE_BATCH_MESSAGES'), 200),
    liveFlushIntervalMs: parseNumber(env('LIVE_FLUSH_INTERVAL_MS'), 2000),
    isBot: !!botToken && botToken === (userToken || botToken),
    logLevel: (env('LOG_LEVEL') as ScraperConfig['logLevel']) || 'info',
    requestsPerSecond: parseNumber(env('REQUESTS_PER_SECOND'), 4),
    timeoutMs: parseNumber(env('REQUEST_TIMEOUT_MS'), 30000),
    maxBackoffMs: parseNumber(env('MAX_BACKOFF_MS'), 60000),
    prettyJson: (env('PRETTY_JSON') ?? 'false').toLowerCase() === 'true',
    balanceShards: (env('BALANCE_SHARDS') ?? 'true').toLowerCase() !== 'false',
    densityProbes: env('DENSITY_PROBES') ? parseNumber(env('DENSITY_PROBES'), 0) || undefined : undefined,
    autoIncremental: (env('AUTO_INCREMENTAL') ?? 'true').toLowerCase() !== 'false',
    archiveDedupParts: parseNumber(env('ARCHIVE_DEDUP_PARTS'), DEFAULT_DEDUP_PARTS),
    exactArchiveDedup: (env('EXACT_ARCHIVE_DEDUP') ?? 'true').toLowerCase() !== 'false',
    archiveDedupMaxIds: parseNumber(env('ARCHIVE_DEDUP_MAX_IDS'), DEFAULT_MAX_DEDUP_IDS),
    watchIntervalMs: parseNumber(env('WATCH_INTERVAL_MINUTES'), 15) * 60_000,
  };
}

async function cmdValidate(config: ScraperConfig) {
  const log = new Logger('info');
  const extractor = new UserTokenExtractor(config, log);

  console.log('Validating Discord token...\n');

  const result = await extractor.validateToken();

  if (result.valid) {
    const user = result.user as any;
    console.log('Token is valid!');
    console.log(`User: ${user.username} (${user.id})`);
    console.log(`Email: ${user.email || 'N/A'}`);
  } else {
    console.error(`Token validation failed: ${result.error}`);
    process.exit(1);
  }

  extractor.close();
}

async function cmdListChannels(config: ScraperConfig) {
  const log = new Logger('info');
  const extractor = new UserTokenExtractor(config, log);

  console.log('Fetching accessible channels...\n');

  const channelsByGuild = await extractor.listChannels();

  if (channelsByGuild.size === 0) {
    console.log('No channels found. Make sure you are a member of some servers.');
  } else {
    for (const [guildId, channels] of channelsByGuild) {
      const guildName = channels[0]?.guildName || guildId;
      console.log(`\n=== ${guildName} (${guildId}) ===`);
      for (const ch of channels) {
        console.log(`  #${ch.channelName} (${ch.channelId})`);
      }
    }
  }

  extractor.close();
}

interface ExtractArgs {
  channelIds: string[];
  concurrency: number;
  parallelism: number | null;
  retries: number | null;
  pageDelayMs: number | null;
  incremental: boolean;
  since: string | null;
  full: boolean;
  balanceShards: boolean | null;
  densityProbes: number | null;
  exportJsonl: string | null;
}

function parseExtractArgs(args: string[]): ExtractArgs {
  const out: ExtractArgs = {
    channelIds: [],
    concurrency: 3,
    parallelism: null,
    retries: null,
    pageDelayMs: null,
    incremental: false,
    since: null,
    full: false,
    balanceShards: null,
    densityProbes: null,
    exportJsonl: null,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const value = args[i + 1];

    if (arg === '--concurrency' && value) {
      out.concurrency = parseNumber(value, out.concurrency);
      i++;
    } else if ((arg === '--parallelism' || arg === '-p') && value) {
      out.parallelism = parseNumber(value, 6);
      i++;
    } else if (arg === '--retries' && value) {
      out.retries = parseNumber(value, 8);
      i++;
    } else if (arg === '--page-delay' && value) {
      out.pageDelayMs = parseNumber(value, 100);
      i++;
    } else if (arg === '--incremental' || arg === '-i') {
      out.incremental = true;
    } else if (arg === '--since' && value) {
      out.since = value;
      out.incremental = true;
      i++;
    } else if (arg === '--full') {
      out.full = true;
      out.incremental = false;
    } else if (arg === '--density-probes' && value) {
      out.densityProbes = parseNumber(value, 0) || null;
      i++;
    } else if (arg === '--export-jsonl' && value) {
      out.exportJsonl = value;
      i++;
    } else if (arg === '--no-balance') {
      out.balanceShards = false;
    } else if (arg === '--balance') {
      out.balanceShards = true;
    } else if (/^\d{17,19}$/.test(arg)) {
      out.channelIds.push(arg);
    }
  }

  return out;
}

async function cmdExtract(config: ScraperConfig, args: string[]) {
  const parsed = parseExtractArgs(args);

  if (parsed.channelIds.length === 0) {
    console.error('Please specify channel IDs to extract:');
    console.error('  apex-scraper extract <channel_id> [channel_id ...]');
    console.error('  apex-scraper list  (to see available channels)');
    process.exit(1);
  }

  // Apply CLI overrides before constructing the extractor.
  if (parsed.parallelism !== null) config.parallelism = Math.max(1, Math.min(parsed.parallelism, 12));
  if (parsed.retries !== null) config.maxRetries = Math.max(0, parsed.retries);
  if (parsed.pageDelayMs !== null) config.pageDelayMs = Math.max(0, parsed.pageDelayMs);
  if (parsed.balanceShards !== null) config.balanceShards = parsed.balanceShards;
  if (parsed.densityProbes !== null) config.densityProbes = parsed.densityProbes;

  const log = new Logger('info');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  // Channel names are a nicety. If listing fails (rate limit, permission), we
  // still extract - so never let it abort the run.
  const channelInfoMap = new Map<string, ChannelInfo>();
  try {
    const allChannels = await extractor.listChannels();
    for (const channels of allChannels.values()) {
      for (const ch of channels) channelInfoMap.set(ch.channelId, ch);
    }
  } catch (e: any) {
    log.warn(`Could not list channels (${e?.message ?? e}); continuing with provided IDs`);
  }

  const channelsToExtract: ChannelInfo[] = parsed.channelIds.map((id) => {
    const info = channelInfoMap.get(id);
    return info ?? { channelId: id, channelName: id, guildId: '', guildName: 'Unknown' };
  });

  console.log(
    `\nExtracting ${channelsToExtract.length} channel(s) with concurrency ${parsed.concurrency}...\n`,
  );

  const singleChannel = channelsToExtract.length === 1;
  const jsonlTemplate = parsed.exportJsonl;

  let totalMessages = 0;
  let totalTime = 0;
  let totalJsonl = 0;

  const results = await extractor.extractAll(channelsToExtract, {
    concurrency: parsed.concurrency,
    incremental: parsed.incremental,
    since: parsed.since,
    full: parsed.full,
    exportJsonl: jsonlTemplate
      ? (channel) => resolveJsonlPath(jsonlTemplate, channel.channelId, singleChannel)
      : null,
    onChannelProgress: (stats) => {
      process.stdout.write(
        `\r  ${stats.channelId}: ${stats.total} messages (${stats.rate} msg/s)   `,
      );
    },
    onChannelComplete: (result) => {
      process.stdout.write('\n');
      if (result.success) {
        const extras = [
          result.duplicatesSkipped > 0 ? `${result.duplicatesSkipped} dupes skipped` : null,
          result.usedFallback ? 'used fallback' : null,
        ].filter(Boolean).join(', ');
        console.log(
          `  [OK] #${result.channelName} (${result.guildName}): ` +
          `${result.messagesExtracted} messages in ${formatDuration(result.duration)}` +
          (extras ? ` (${extras})` : ''),
        );
      } else {
        console.log(`  [FAIL] #${result.channelName}: ${result.error}`);
        if (result.failedShards) {
          console.log(`         ${result.failedShards} shard(s) incomplete - rerun to resume`);
        }
      }
      if (result.jsonlError) {
        console.log(`         JSONL export failed: ${result.jsonlError}`);
      }
      totalMessages += result.messagesExtracted;
      totalTime += result.duration;
      totalJsonl += result.jsonlRows ?? 0;
    },
  });

  const failed = results.filter((r) => !r.success);

  console.log('\n=== Summary ===');
  if (parsed.incremental) {
    console.log(`Mode: incremental catch-up${parsed.since ? ` (since ${parsed.since})` : ''}`);
  } else if (!parsed.full) {
    console.log('Mode: auto (channels already done are caught up incrementally)');
  }
  console.log(`Total messages: ${totalMessages}`);
  console.log(`Total time: ${formatDuration(totalTime)}`);
  console.log(`Database: ${config.dbPath}`);
  if (jsonlTemplate) {
    const target = singleChannel
      ? resolveJsonlPath(jsonlTemplate, channelsToExtract[0].channelId, true)
      : `${channelsToExtract.length} files from ${jsonlTemplate}`;
    console.log(`JSONL export: ${target} (${totalJsonl} lines)`);
  }

  if (failed.length > 0) {
    console.log(`\n${failed.length} channel(s) incomplete - rerun the same command to resume.`);
    process.exitCode = 1;
  }

  extractor.close();
}

async function cmdLive(config: ScraperConfig, args: string[]) {
  const channelIds = args.filter((a) => /^\d{17,19}$/.test(a));
  const excludeBots = args.includes('--exclude-bots');

  let flushIntervalMs = config.liveFlushIntervalMs ?? 2000;
  const flushIdx = args.indexOf('--flush-interval');
  if (flushIdx >= 0 && args[flushIdx + 1]) {
    flushIntervalMs = parseNumber(args[flushIdx + 1], flushIntervalMs);
  }

  if (channelIds.length === 0) {
    console.error('Please specify channel IDs to capture:');
    console.error('  apex-scraper live <channel_id> [channel_id ...]');
    process.exit(1);
  }

  const log = new Logger('info');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  // Resolve names where we can; a naming failure must not stop the capture.
  const channelInfoMap = new Map<string, ChannelInfo>();
  try {
    const allChannels = await extractor.listChannels();
    for (const channels of allChannels.values()) {
      for (const ch of channels) channelInfoMap.set(ch.channelId, ch);
    }
  } catch (e: any) {
    log.warn(`Could not list channels (${e?.message ?? e}); continuing with provided IDs`);
  }

  const channels = channelIds.map(
    (id) =>
      channelInfoMap.get(id) ?? {
        channelId: id,
        channelName: id,
        guildId: '',
        guildName: 'Unknown',
      },
  );

  const capture = extractor.createLiveCapture(extractor.createLiveSource(), {
    flushIntervalMs,
    batchMessages: config.liveBatchMessages,
    excludeBots,
    onMessage: (channelId, count) => {
      process.stdout.write(`\r  ${channelId}: ${count} live message(s)   `);
    },
  });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n\nReceived ${signal}; flushing and shutting down...`);
    try {
      const summary = await capture.stop();
      console.log('\n=== Live Capture Summary ===');
      console.log(`Messages captured: ${summary.messages}`);
      for (const c of summary.channels) {
        console.log(
          `  #${c.channelName}: +${c.sessionMessages} ` +
          `(archive: ${c.totalMessages} in ${c.jsonParts} parts)`,
        );
      }
    } catch (e: any) {
      console.error(`Shutdown error: ${e?.message ?? e}`);
    } finally {
      extractor.close();
      process.exit(0);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  console.log(`\nStarting live capture for ${channels.length} channel(s)...\n`);
  await capture.start(channels);
  console.log('Listening for new messages. Press Ctrl+C to stop.\n');

  // Keep the process alive until a signal arrives.
  await new Promise<void>(() => {});
}

async function cmdWatch(config: ScraperConfig, args: string[]) {
  const once = args.includes('--once');
  const includeMissingBaseline = args.includes('--include-new');

  let intervalMs = config.watchIntervalMs ?? DEFAULT_INTERVAL_MS;
  const intervalIdx = args.indexOf('--interval');
  if (intervalIdx >= 0 && args[intervalIdx + 1]) {
    const minutes = Number.parseFloat(args[intervalIdx + 1]);
    if (Number.isFinite(minutes) && minutes > 0) intervalMs = Math.round(minutes * 60_000);
  }

  let concurrency = 1;
  const concurrencyIdx = args.indexOf('--concurrency');
  if (concurrencyIdx >= 0 && args[concurrencyIdx + 1]) {
    concurrency = Math.max(1, parseNumber(args[concurrencyIdx + 1], 1));
  }

  const log = new Logger('info');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  const minutesLabel = (intervalMs / 60_000).toFixed(intervalMs % 60_000 === 0 ? 0 : 1);

  const scheduler = new CatchUpScheduler(extractor, {
    intervalMs,
    concurrency,
    includeMissingBaseline,
    onCycleStart: (plan) => {
      for (const item of plan.due) console.log(`  due: #${item.channelName} (${item.channelId})`);
      for (const skip of plan.skipped) console.log(`  skip ${skip.channelId}: ${skip.reason}`);
    },
    onCycleComplete: (result) => {
      const time = result.finishedAt;
      console.log(
        `  cycle done at ${time}: +${result.messages} message(s), ` +
        `${result.failures} failure(s)`,
      );
      if (!once) console.log(`  next cycle in ~${minutesLabel} min\n`);
    },
  });

  let shuttingDown = false;
  const shutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\nReceived ${signal}; stopping catch-up scheduler...`);
    scheduler.stop();
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  console.log(`\nCatch-up scheduler: every ~${minutesLabel} min, concurrency ${concurrency}.`);
  if (!includeMissingBaseline) {
    console.log('Channels that were never extracted are skipped (use --include-new to include them).');
  }
  console.log('Press Ctrl+C to stop.\n');

  if (once) {
    const result = await scheduler.runCycle();
    extractor.close();
    if (result.failures > 0) process.exitCode = 1;
    return;
  }

  await scheduler.start();
  extractor.close();
}

async function cmdStatus(config: ScraperConfig) {
  const log = new Logger('warn');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  const status = await extractor.getStatus();

  if (status.length === 0) {
    console.log('No extraction history found.');
    console.log('Run "apex-scraper extract <channel_id>" to start extracting.');
  } else {
    console.log('\n=== Extraction Status ===\n');
    for (const s of status) {
      const stats = extractor.getChannelStats(s.channelId);
      const icons: Record<string, string> = {
        done: '[OK]',
        error: '[ERR]',
        extracting: '[...]',
        live: '[LIVE]',
        pending: '[  ]',
      };
      const icon = icons[s.status] ?? '[  ]';
      const parts = stats ? `${stats.jsonParts} parts` : '';
      const size = stats ? `(${formatBytes(stats.sizeBytes)})` : '';
      console.log(`  ${icon} ${s.channelId}: ${s.status} (${s.total} messages) ${parts} ${size}`);
      printDedupReport(extractor, s.channelId);

      const detail = await extractor.getProgressDetail(s.channelId);
      const resume = parseResumeState(detail?.resume_state ?? null);
      if (resume) {
        console.log(`        resume: ${describeResumeState(resume)}`);

        const balance = describeBalance(resume.balance);
        if (balance) {
          console.log(`        balance: ${balance}`);
        }

        // Show the actual per-shard estimate when there are few enough to read.
        const loads = resume.balance?.loads;
        if (loads && loads.length > 1 && loads.length <= 12) {
          console.log(`        est. per shard: ${loads.map((n) => formatCount(n)).join(' | ')}`);
        }
      }
      if (detail?.error_message) {
        console.log(`        last error: ${detail.error_message}`);
      }
    }
  }

  const rate = extractor.rateLimiter.getState();
  console.log('\n=== Rate Limiter ===');
  console.log(
    `  circuit: ${rate.circuitState}, in-flight: ${rate.inFlight}, ` +
    `pacing: ${rate.currentIntervalMs}ms, buckets: ${rate.buckets}`,
  );

  console.log('\n=== Storage Info ===');
  console.log(`SQLite (metadata): ${config.dbPath}`);
  console.log(`JSON (messages): ${config.dbPath.replace(/\.db$/, '_json')}/`);

  extractor.close();
}

/**
 * Report what the archive duplicate index costs for one channel.
 *
 * The index is built lazily on the first append, and `status` does not append,
 * so this is always the projection: how many ids an index would hold, how much
 * memory that is, and whether the configured ceiling can cover them. What a run
 * actually built - parts read, seed time, duplicates dropped - is logged by the
 * run itself, where those numbers exist.
 */
function printDedupReport(extractor: UserTokenExtractor, channelId: string): void {
  const cost = extractor.estimateDedupCost(channelId);
  if (!cost) return;

  const config = extractor.getDedupConfig();
  const scope = cost.exact ? 'whole channel' : `newest ${config.parts} part(s)`;
  const cap = cost.affordable
    ? ''
    : ` - over ARCHIVE_DEDUP_MAX_IDS (${cost.maxIds.toLocaleString('en-US')}), ` +
      'older ids are not indexed and duplicates from them can be written again';

  console.log(
    `        dedup: ${scope}, index ${cost.ids.toLocaleString('en-US')} of ` +
    `${cost.archived.toLocaleString('en-US')} archived id(s) across ${cost.parts} part(s) ` +
    `~${formatBytes(cost.bytes)}${cap}`,
  );

}

async function cmdDump(config: ScraperConfig, args: string[]) {
  if (args.length === 0) {
    console.error('Usage: apex-scraper dump <channel_id> [output_file.json]');
    process.exit(1);
  }

  const channelId = args[0];
  const outputFile = args[1] || `./data/${channelId}-export.json`;

  const log = new Logger('warn');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  const stats = extractor.getChannelStats(channelId);

  if (!stats) {
    console.log(`No data found for channel ${channelId}`);
    console.log('Make sure you have extracted this channel first:');
    console.log(`  apex-scraper extract ${channelId}`);
  } else {
    const count = extractor.exportChannelToJson(channelId, outputFile);
    console.log(`Exported ${count} messages to ${outputFile}`);
    if (existsSync(outputFile)) {
      console.log(`File size: ${formatBytes(statSync(outputFile).size)}`);
    }
  }

  extractor.close();
}

async function cmdReset(config: ScraperConfig, args: string[]) {
  if (args.length === 0) {
    console.error('Usage: apex-scraper reset <channel_id>');
    process.exit(1);
  }

  const log = new Logger('warn');
  const extractor = new UserTokenExtractor(config, log);
  await extractor.init();

  for (const channelId of args) {
    await extractor.resetChannel(channelId);
  }

  extractor.close();
}

async function cmdQuery(config: ScraperConfig, args: string[]) {
  if (args.length === 0) {
    console.error('Usage: apex-scraper query "<sql>"');
    console.error('Example: apex-scraper query "SELECT status, total_extracted FROM channel_progress"');
    process.exit(1);
  }

  const sql = args.join(' ');
  const log = new Logger('error');
  const { Storage } = await import('./storage.js');
  const storage = new Storage(config.dbPath, log);
  await storage.init();

  try {
    const stmt = (storage as any).db.prepare(sql);
    const results: any[] = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject());
    }
    stmt.free();
    console.log(JSON.stringify(results, null, 2));
  } catch (e: any) {
    console.error(`Query error: ${e.message}`);
    storage.close();
    process.exit(1);
  }

  storage.close();
}

function printUsage() {
  console.log(`
Apex Discord Scraper - Headless multi-channel message extraction

Usage:
  apex-scraper <command> [options]

Commands:
  validate              Validate your Discord token
  list, list-channels  List all accessible channels
  extract <channel_id>  Extract messages from channel(s) (resumes automatically)
  live <channel_id>     Capture new messages into the same archive in real time
  watch                Recurring incremental catch-up for all known channels
  status               Show extraction status + resume state
  dump <channel_id>     Export channel to a single JSON file
  reset <channel_id>    Reset extraction progress for channel(s)
  query "<sql>"        Run a SQL query on the metadata cache

Extract Options:
  --concurrency <n>    Channels extracted in parallel (default: 3)
  -p, --parallelism <n> Time shards per channel (default: 6, max: 12)
  --retries <n>        Retries per request after the first attempt (default: 8)
  --page-delay <ms>    Pause between pages within a shard (default: 100)
  -i, --incremental    Fetch only messages newer than the last run
  --since <id>         Explicit lower bound for an incremental fetch
  --full               Never auto-catch-up; resume/full behaviour as before
  --no-balance         Disable density-balanced shard boundaries
  --balance            Force density balancing on (overrides BALANCE_SHARDS)
  --density-probes <n> Number of density probes to sample before a fresh run
  --export-jsonl <p>   Also append every processed message as JSONL (one line
                       each). Use {channel} in the path for multiple channels.

Watch Options:
  --interval <minutes> Time between catch-up cycles (default: 15)
  --concurrency <n>    Channels caught up in parallel per cycle (default: 1)
  --once               Run a single cycle and exit
  --include-new        Also full-extract channels that were never extracted

Live Options:
  --flush-interval <ms> How often live buffers are written to disk (default: 2000)
  --exclude-bots        Skip messages authored by bots or webhooks

Examples:
  apex-scraper validate
  apex-scraper list
  apex-scraper extract 123456789012345678
  apex-scraper extract 123456789012345678 987654321098765432 --concurrency 2
  apex-scraper extract 123456789012345678 --parallelism 4 --page-delay 150
  apex-scraper extract 123456789012345678 --incremental
  apex-scraper extract 123456789012345678 --full
  apex-scraper extract 123456789012345678 --export-jsonl ./data/out.jsonl
  apex-scraper live 123456789012345678
  apex-scraper watch --interval 15
  apex-scraper status
  apex-scraper dump 123456789012345678 my-export.json

Environment (real env vars win over .env):
  DISCORD_USER_TOKEN    Your Discord user token
  DISCORD_BOT_TOKEN     Alternative: Discord bot token
  DB_PATH               SQLite metadata path (default: ./data/apex-scraper.db)
  PARALLELISM           Time shards per channel (default: 6)
  PAGE_DELAY_MS         Delay between pages (default: 100)
  MAX_RETRIES           Retries per request (default: 8)
  REQUESTS_PER_SECOND   Sustained request rate (default: 4)
  REQUEST_TIMEOUT_MS    Per-request deadline (default: 30000)
  LOG_LEVEL             debug | info | warn | error (default: info)
  LIVE_FLUSH_INTERVAL_MS How often live buffers are flushed to disk (default: 2000)
  LIVE_BATCH_MESSAGES   Buffered messages that trigger a live flush (default: 200)
  LIVE_MAX_BUFFER       Hard cap on buffered live messages per channel (default: 10000)
  AUTO_INCREMENTAL      Plain extract catches up already-done channels (default: true)
  ARCHIVE_DEDUP_PARTS   Recently written archive parts used to drop messages a
                        crash left written-but-unacknowledged (default: 3, 0 = off)
  EXACT_ARCHIVE_DEDUP   Index the whole archive, not just its newest parts, so
                        a duplicate is caught no matter how old it is (default: true)
  ARCHIVE_DEDUP_MAX_IDS Ceiling on that index, in ids; past it the newest ids are
                        kept and coverage is reported as not exact (default: 2000000)
  DENSITY_PROBES        Explicit density probe count (default: 3 per shard)
  WATCH_INTERVAL_MINUTES Minutes between watch catch-up cycles (default: 15)

Interrupted or partially failed runs can be resumed by re-running the same
extract command: per-shard cursors are stored in the database.
`);
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    printUsage();
    process.exit(0);
  }

  const command = args[0];
  const config = getConfig();

  try {
    switch (command) {
      case 'validate':
        await cmdValidate(config);
        break;
      case 'list':
      case 'list-channels':
        await cmdListChannels(config);
        break;
      case 'extract':
        await cmdExtract(config, args.slice(1));
        break;
      case 'live':
        await cmdLive(config, args.slice(1));
        break;
      case 'watch':
        await cmdWatch(config, args.slice(1));
        break;
      case 'status':
        await cmdStatus(config);
        break;
      case 'dump':
        await cmdDump(config, args.slice(1));
        break;
      case 'reset':
        await cmdReset(config, args.slice(1));
        break;
      case 'query':
        await cmdQuery(config, args.slice(1));
        break;
      default:
        console.error(`Unknown command: ${command}`);
        printUsage();
        process.exit(1);
    }
  } catch (error: any) {
    console.error(`\nError: ${error.message}`);
    if (error.hint) console.error(`Hint: ${error.hint}`);
    if (config.logLevel === 'debug') {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

main();
