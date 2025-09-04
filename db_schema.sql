-- Drop tables if they exist to avoid conflicts
-- Users table
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    telegram_id BIGINT GENERATED ALWAYS AS (user_id) STORED,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    is_admin BOOLEAN DEFAULT FALSE,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Matches (Scorecards) table
CREATE TABLE IF NOT EXISTS matches (
    match_id TEXT PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    match_name TEXT,
    timestamp TIMESTAMP,
    match_result TEXT,
    game_mode TEXT,
    deleted BOOLEAN DEFAULT FALSE
);

-- Groups table
CREATE TABLE IF NOT EXISTS groups (
    group_id BIGINT PRIMARY KEY,
    group_name TEXT,
    added_by BIGINT REFERENCES users(user_id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Admins table
CREATE TABLE IF NOT EXISTS admins (
    user_id BIGINT PRIMARY KEY REFERENCES users(user_id),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Broadcasts table (optional, for broadcast history)
CREATE TABLE IF NOT EXISTS broadcasts (
    id SERIAL PRIMARY KEY,
    message TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_matches_user_id ON matches(user_id);
CREATE INDEX IF NOT EXISTS idx_matches_timestamp ON matches(timestamp);
