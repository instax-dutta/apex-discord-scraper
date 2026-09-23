-- Apex Discord Scraper - SQLite Schema

-- Messages table: stores all extracted messages
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,                    -- Discord message snowflake
    channel_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    author_username TEXT,
    author_discriminator TEXT,
    content TEXT,
    timestamp TEXT NOT NULL,                -- ISO string from API
    reply_to_id TEXT,
    reply_to_author_username TEXT,
    attachment_count INTEGER DEFAULT 0,
    attachment_urls TEXT,                  -- JSON array of strings
    image_urls TEXT,                        -- JSON array of strings
    attachments_detail TEXT,                 -- JSON array of AttachmentDetail objects
    embed_image_urls TEXT,                  -- JSON array of strings
    thread_id TEXT,
    extracted_at TEXT NOT NULL,             -- when our service extracted it
    UNIQUE(channel_id, id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_messages_server_channel ON messages(server_id, channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);

-- Per-channel progress tracking
CREATE TABLE IF NOT EXISTS channel_progress (
    channel_id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL,
    channel_name TEXT,
    last_message_id TEXT,                  -- cursor for next fetch
    last_extracted_ts TEXT,                -- timestamp of last extracted message
    total_extracted INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',         -- pending | extracting | done | error | live
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);

-- Configuration for channels to extract
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL,
    server_name TEXT,
    channel_name TEXT,
    enabled INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Live capture sessions
CREATE TABLE IF NOT EXISTS live_sessions (
    session_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_message_id TEXT,
    message_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active'  -- active | stopped | error
);
