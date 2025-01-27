-- Drop existing tables if they exist (be careful with this in production!)
DROP TABLE IF EXISTS scorecards;
DROP TABLE IF EXISTS command_logs;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS player_stats;
DROP TABLE IF EXISTS match_performances;

-- Create users table
CREATE TABLE users (
    telegram_id BIGINT PRIMARY KEY,
    username VARCHAR(255),
    first_name VARCHAR(255),
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create command_logs table
CREATE TABLE command_logs (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT,
    command VARCHAR(50),
    chat_type VARCHAR(20),
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
);

-- Create scorecards table
CREATE TABLE scorecards (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) UNIQUE NOT NULL,
    user_id BIGINT REFERENCES users(telegram_id),
    game_mode VARCHAR(50),
    match_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create player_stats table
CREATE TABLE player_stats (
    telegram_id BIGINT PRIMARY KEY REFERENCES users(telegram_id),
    total_runs INT DEFAULT 0,
    total_wickets INT DEFAULT 0,
    total_matches INT DEFAULT 0,
    total_wins INT DEFAULT 0,
    total_boundaries INT DEFAULT 0,
    total_sixes INT DEFAULT 0,
    fifties INT DEFAULT 0,
    hundreds INT DEFAULT 0,
    best_score INT DEFAULT 0,
    best_wickets INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create match_performances table for individual match stats
CREATE TABLE match_performances (
    id SERIAL PRIMARY KEY,
    match_id VARCHAR(50) REFERENCES scorecards(match_id),
    telegram_id BIGINT REFERENCES users(telegram_id),
    runs_scored INT DEFAULT 0,
    wickets_taken INT DEFAULT 0,
    boundaries INT DEFAULT 0,
    sixes INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_users_telegram_id ON users(telegram_id);
CREATE INDEX idx_scorecards_user_id ON scorecards(user_id);
CREATE INDEX idx_scorecards_match_id ON scorecards(match_id);
CREATE INDEX idx_command_logs_telegram_id ON command_logs(telegram_id);
CREATE INDEX idx_scorecards_created_at ON scorecards(created_at);
CREATE INDEX idx_player_stats_runs ON player_stats(total_runs DESC);
CREATE INDEX idx_player_stats_wickets ON player_stats(total_wickets DESC);
CREATE INDEX idx_player_stats_wins ON player_stats(total_wins DESC);
CREATE INDEX idx_match_performances_player ON match_performances(telegram_id);

-- Create function to update player stats
CREATE OR REPLACE FUNCTION update_player_stats()
RETURNS TRIGGER AS $$
BEGIN
    -- Update total runs and boundaries
    UPDATE player_stats
    SET total_runs = total_runs + NEW.runs_scored,
        total_boundaries = total_boundaries + NEW.boundaries,
        total_sixes = total_sixes + NEW.sixes,
        fifties = CASE 
            WHEN NEW.runs_scored >= 50 AND NEW.runs_scored < 100 THEN fifties + 1 
            ELSE fifties 
        END,
        hundreds = CASE 
            WHEN NEW.runs_scored >= 100 THEN hundreds + 1 
            ELSE hundreds 
        END,
        best_score = GREATEST(best_score, NEW.runs_scored),
        updated_at = CURRENT_TIMESTAMP
    WHERE telegram_id = NEW.telegram_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for stats update
CREATE TRIGGER update_stats_trigger
AFTER INSERT ON match_performances
FOR EACH ROW
EXECUTE FUNCTION update_player_stats();

-- Grant necessary permissions (adjust according to your needs)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_username;
