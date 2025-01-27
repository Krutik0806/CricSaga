import os
import logging
from typing import Optional
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import DictCursor
import json

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self):
        self.pool = None
        self._init_pool()

    def _init_pool(self) -> bool:
        """Initialize connection pool with proper error handling"""
        try:
            # Get Supabase credentials from environment
            db_config = {
                'dbname': os.getenv('DB_NAME', 'postgres'),
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'host': os.getenv('DB_HOST'),
                'port': int(os.getenv('DB_PORT', '5432')),
                # 'SUPABASE_DB_HOST': os.getenv('DB_HOST'),
                # 'SUPABASE_DB_USER': os.getenv('DB_USER'),
                # 'SUPABASE_DB_PASSWORD': os.getenv('DB_PASSWORD'),
                'sslmode': 'require'
            }

            # Create connection pool
            self.pool = SimpleConnectionPool(
                1,  # Minimum connections
                20, # Maximum connections
                **db_config
            )
            
            logger.info("Database pool initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            self.pool = None
            return False

    def check_connection(self) -> bool:
        """Test database connection"""
        if not self.pool:
            return False
            
        try:
            # Get a connection from the pool
            conn = self.pool.getconn()
            try:
                # Test the connection
                with conn.cursor() as cur:
                    cur.execute('SELECT 1')
                    cur.fetchone()
                return True
            finally:
                # Always return the connection to the pool
                self.pool.putconn(conn)
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """Get a connection from the pool"""
        try:
            if not self.pool:
                if not self._init_pool():
                    return None
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Error getting connection: {e}")
            return None

    def return_connection(self, conn: psycopg2.extensions.connection):
        """Return a connection to the pool"""
        if self.pool:
            self.pool.putconn(conn)

    def close(self):
        """Close all database connections"""
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def register_user(self, telegram_id: int, username: str = None, first_name: str = None) -> bool:
        """Register a new user or update existing user"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            try:
                with conn.cursor() as cur:
                    # First make sure the users table exists
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            telegram_id BIGINT PRIMARY KEY,
                            username VARCHAR(255),
                            first_name VARCHAR(255),
                            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

                    # Insert or update user
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
                    return cur.fetchone() is not None
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error registering user: {e}")
            return False

    def log_command(self, telegram_id: int, command: str, chat_type: str, success: bool = True, error_message: str = None) -> bool:
        """Log command usage"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            try:
                with conn.cursor() as cur:
                    # First make sure the command_logs table exists
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS command_logs (
                            id SERIAL PRIMARY KEY,
                            telegram_id BIGINT,
                            command VARCHAR(50),
                            chat_type VARCHAR(20),
                            success BOOLEAN DEFAULT TRUE,
                            error_message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
                        )
                    """)

                    # Log the command
                    cur.execute("""
                        INSERT INTO command_logs 
                        (telegram_id, command, chat_type, success, error_message, timestamp)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """, (telegram_id, command, chat_type, success, error_message))
                    conn.commit()
                    return True
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error logging command: {e}")
            return False

    def save_match(self, match_data: dict) -> bool:
        """Save match data to database"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO scorecards 
                        (match_id, user_id, match_data, timestamp)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (match_id) 
                        DO UPDATE SET 
                            match_data = EXCLUDED.match_data,
                            timestamp = CURRENT_TIMESTAMP
                        RETURNING match_id
                    """, (
                        match_data.get('match_id'),
                        match_data.get('user_id'),
                        json.dumps(match_data.get('match_data'))
                    ))
                    conn.commit()
                    return cur.fetchone() is not None
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error saving match: {e}")
            return False

    def get_user_matches(self, user_id: str, limit: int = 10) -> list:
        """Get user's match history"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
                
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT match_id, match_data, timestamp
                        FROM scorecards 
                        WHERE user_id = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (user_id, limit))
                    
                    matches = []
                    for row in cur.fetchall():
                        match_data = json.loads(row[1]) if row[1] else {}
                        matches.append({
                            'match_id': row[0],
                            'timestamp': row[2].isoformat(),
                            **match_data
                        })
                    return matches
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error getting user matches: {e}")
            return []