import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from datetime import datetime
import json
from typing import Optional
from dotenv import load_dotenv
from constants import DB_CONFIG, DB_POOL_MIN, DB_POOL_MAX, REGISTERED_USERS, logger

# --- Database Handler Class ---
class DatabaseHandler:
    def __init__(self, db_config, minconn=1, maxconn=5):
        self.pool = SimpleConnectionPool(
            minconn,
            maxconn,
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"]
        )
        if not self._verify_tables():
            self._init_tables()
        self.load_registered_users()
        
    def load_registered_users(self):
        global REGISTERED_USERS
        try:
            conn = self.get_connection()
            if not conn:
                return
                
            with conn.cursor() as cur:
                cur.execute("SELECT telegram_id FROM users")
                users = cur.fetchall()
                for user in users:
                    REGISTERED_USERS.add(str(user[0]))
                logger.info(f"Loaded {len(REGISTERED_USERS)} registered users")
        except Exception as e:
            logger.error(f"Error loading users: {e}")
        finally:
            if conn:
                self.return_connection(conn)

    def check_connection(self) -> bool:
        if not self.pool:
            return False
            
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute('SELECT 1')
                    cur.fetchone()
                return True
            finally:
                self.pool.putconn(conn)
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        try:
            if not self.pool:
                if not self._init_pool():
                    return None
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Error getting connection: {e}")
            return None

    def return_connection(self, conn: psycopg2.extensions.connection):
        if self.pool:
            self.pool.putconn(conn)

    def close(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def register_user(self, telegram_id: int, username: str = None, first_name: str = None) -> bool:
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO users (telegram_id, username, first_name, last_active)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (telegram_id) 
                        DO UPDATE SET 
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_active = CURRENT_TIMESTAMP
                        RETURNING telegram_id
                    """, (telegram_id, username, first_name))
                    conn.commit()
                    
                    REGISTERED_USERS.add(str(telegram_id))
                    return True
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error registering user: {e}")
            return False

    def save_match(self, match_data: dict) -> bool:
        try:
            conn = self.get_connection()
            if not conn:
                return False

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (telegram_id, first_name)
                    VALUES (%s, %s)
                    ON CONFLICT (telegram_id) DO NOTHING
                """, (match_data['user_id'], match_data.get('user_name', 'Unknown')))

                cur.execute("""
                    INSERT INTO scorecards 
                    (match_id, user_id, match_name, game_mode, match_data, created_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
                    ON CONFLICT (match_id) 
                    DO UPDATE SET
                        match_data = EXCLUDED.match_data,
                        match_name = EXCLUDED.match_name,
                        game_mode = EXCLUDED.game_mode
                """, (
                    match_data['match_id'],
                    match_data['user_id'],
                    match_data.get('match_name', f"Match_{match_data['match_id']}"),
                    match_data.get('game_mode', 'classic'),
                    json.dumps(match_data)
                ))

                conn.commit()
                return True

        except Exception as e:
            logger.error(f"Database save error: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def delete_match(self, match_id: str, user_id: str) -> bool:
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM scorecards 
                    WHERE match_id = %s AND user_id = %s
                    RETURNING match_id
                """, (match_id, user_id))
                
                result = cur.fetchone()
                conn.commit()
                return result is not None
                
        except Exception as e:
            logger.error(f"Error deleting match: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def _init_tables(self) -> bool:
        try:
            conn = self.get_connection()
            if not conn:
                return False

            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        telegram_id BIGINT PRIMARY KEY,
                        username VARCHAR(255),
                        first_name VARCHAR(255),
                        registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE TABLE IF NOT EXISTS scorecards (
                        id SERIAL PRIMARY KEY,
                        match_id VARCHAR(50) UNIQUE NOT NULL,
                        user_id BIGINT REFERENCES users(telegram_id),
                        match_name VARCHAR(255),
                        game_mode VARCHAR(50),
                        match_data JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_scorecards_user_id ON scorecards(user_id);
                    CREATE INDEX IF NOT EXISTS idx_scorecards_match_id ON scorecards(match_id);
                """)

                conn.commit()
                logger.info("Database tables created successfully")
                return True

        except Exception as e:
            logger.error(f"Error initializing tables: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def _verify_tables(self) -> bool:
        try:
            conn = self.get_connection()
            if not conn:
                return False

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('users', 'scorecards');
                """)
                count = cur.fetchone()[0]
                return count == 2

        except Exception as e:
            logger.error(f"Error verifying tables: {e}")
            return False
        finally:
            if conn:
                self.return_connection(conn)
    
    def get_user(self, user_id):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT user_id, username, first_name, last_name, nickname FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "user_id": row[0],
                        "username": row[1],
                        "first_name": row[2],
                        "last_name": row[3],
                        "nickname": row[4]
                    }
        except Exception as e:
            # handle/log error
            pass
        return None

    def get_user_matches(self, user_id, limit=1000):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT match_result FROM matches WHERE user_id = %s AND deleted = FALSE ORDER BY timestamp DESC LIMIT %s",
                    (user_id, limit)
                )
                rows = cur.fetchall()
                return [{"match_result": row[0]} for row in rows]
        except Exception as e:
            return []

    def get_user_matches(self, user_id, limit=1000):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT match_result FROM matches WHERE user_id = %s AND deleted = FALSE ORDER BY timestamp DESC LIMIT %s",
                    (user_id, limit)
                )
                rows = cur.fetchall()
                return [{"match_result": row[0]} for row in rows]
        except Exception as e:
            return []
