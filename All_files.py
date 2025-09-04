# --- Standard Library Imports ---
import os
import random
import json
import logging
import asyncio
import time
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Set, List, Optional 
from collections import defaultdict
#from html import escape_html

# --- Third Party Imports ---
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.error import BadRequest
from telegram.constants import ParseMode, ChatType
from telegram.helpers import escape_markdown
import psycopg2
from psycopg2 import Error
from psycopg2.extras import DictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

# --- Configuration & Constants ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# Bot Configuration
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BOT_ADMINS: Set[str] = {os.getenv('BOT_ADMIN', '')}
DATA_DIR = Path("data")

# Game State
games: Dict[str, Dict] = {}
REGISTERED_USERS = set()
AUTHORIZED_GROUPS = set()
in_memory_scorecards = []

# Timing Constants (Optimized for speed)
ANIMATION_DELAY = 0.8  # Reduced from 0.8
BALL_ANIMATION_DELAY = 0.8  # Reduced from 0.5
OVER_BREAK_DELAY = 1.5  # Reduced from 2.0
BROADCAST_DELAY = 1  # Reduced from 1
MAX_MESSAGE_RETRIES = 3  # Reduced from 3
FLOOD_CONTROL_BACKOFF = [5, 10, 15]  # Reduced delays

# Game Constants
INFINITY_SYMBOL = "∞"
MATCH_SEPARATOR = "━━━━━━━━━━━━━━"
TEST_MODE = False  # Set to False for production
USE_FILE_STORAGE = False

# Database Configuration
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", 5))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

db_pool = None

# File Paths
MATCH_HISTORY_FILE = DATA_DIR / "match_history.json"

# Game Mode Display Format (Updated Style)
GAME_MODE_DISPLAY = {
    'classic': {
        'icon': '🏏',
        'title': 'Classic Cricket',
        'desc': 'Traditional format with limited overs & wickets'
    },
    'quick': {
        'icon': '⚡',
        'title': 'Quick Match', 
        'desc': 'Fast-paced action with unlimited wickets'
    },
    'survival': {
        'icon': '🎯',
        'title': 'Survival Mode',
        'desc': 'One wicket challenge - last man standing'
    }
}

# Commentary Phrases (Shortened for speed)
COMMENTARY_PHRASES = {
    'wicket': [
        "💥 *BOWLED!* {} *strikes!*",
        "🎯 *OUT!* {} *gets the wicket!*",
        "⚡ *CLEAN BOWLED!* {} *celebrates!*"
    ],
    'run_1': ["👌 *Good single by* {}", "🏃 *Quick run by* {}"],
    'run_2': ["🏃‍♂️ *Quick double by* {}", "⚡ *Good running by* {}"],
    'run_3': ["💪 *Excellent running by* {}", "🏃‍♂️ *Great effort by* {}"],
    'run_4': ["🏏 *FOUR!* *Great shot by* {}", "⚡ *Boundary by* {}"],
    'run_5': ["🔥 *FIVE RUNS!* *Smart cricket by* {}", "⭐ *Bonus run by* {}"],
    'run_6': ["💥 *SIX!* {} *goes big*", "🚀 *MAXIMUM!* {} *clears the ropes*"],
    'over_complete': ["🎯 *Over complete!*", "⏱️ *End of over!*"],
    'innings_end': ["🏁 *INNINGS COMPLETE!*", "🎊 *Innings over!*"]
}

ACTION_MESSAGES = {
    'batting': ["🏏 {} *takes guard*...", "⚡ {} *ready to face*..."],
    'bowling': ["🎯 {} *marks run-up*...", "⚡ {} *charging in*..."],
    'delivery': ["⚾ *Ball in the air*...", "🎯 *Delivery on its way*..."]
}

def escape_markdown_v2_custom(text: str) -> str:
    """Escape special characters for Markdown V2 format"""
    special_chars = ['_', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text

# --- Helper Functions ---
def check_admin(user_id: str) -> bool:
    return user_id in BOT_ADMINS

def is_registered(user_id: str) -> bool:
    return user_id in REGISTERED_USERS

def generate_match_id() -> str:
    timestamp = int(time.time())
    random_num = random.randint(1000, 9999)
    return f"M{timestamp}{random_num}"

def create_game(creator_id: str, creator_name: str, chat_id: int) -> str:
    game_id = str(random.randint(1000, 9999))
    while game_id in games:
        game_id = str(random.randint(1000, 9999))
        
    games[game_id] = {
        'chat_id': chat_id,
        'creator': creator_id,
        'creator_name': creator_name,
        'status': 'config',
        'score': {'innings1': 0, 'innings2': 0},
        'wickets': 0,
        'balls': 0,
        'current_innings': 1,
        'this_over': [],
        'match_id': generate_match_id(),
        'boundaries': {'innings1': 0, 'innings2': 0},
        'sixes': {'innings1': 0, 'innings2': 0},
        'dot_balls': 0,
        'over_scores': {}
    }
    return game_id

def should_end_innings(game: dict) -> bool:
    max_wickets = game.get('max_wickets', float('inf'))
    max_overs = game.get('max_overs', float('inf'))
    
    return (
        (max_wickets != float('inf') and game['wickets'] >= max_wickets) or 
        (max_overs != float('inf') and game['balls'] >= max_overs * 6) or
        (game['current_innings'] == 2 and game['score']['innings2'] >= game.get('target', float('inf')))
    )

def store_first_innings(game: dict):
    game['first_innings_wickets'] = game['wickets']
    game['first_innings_score'] = game['score']['innings1']
    game['first_innings_overs'] = f"{game['balls']//6}.{game['balls']%6}"
    game['first_innings_boundaries'] = game['boundaries']['innings1']
    game['first_innings_sixes'] = game['sixes']['innings1']
    game['target'] = game['score']['innings1'] + 1

def safe_division(numerator, denominator, default=0):
    try:
        if denominator == 0 or not denominator:
            return default
        return numerator / float(denominator)
    except (ValueError, TypeError):
        return default

async def safe_edit_message(message, text: str, keyboard=None, max_retries=MAX_MESSAGE_RETRIES):
    for attempt in range(max_retries):
        try:
            escaped_text = escape_markdown_v2_custom(text)
            if keyboard:
                return await message.edit_text(
                    escaped_text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            return await message.edit_text(
                escaped_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except telegram.error.RetryAfter as e:
            delay = FLOOD_CONTROL_BACKOFF[min(attempt, len(FLOOD_CONTROL_BACKOFF)-1)]
            await asyncio.sleep(delay)
        except telegram.error.TimedOut:
            await asyncio.sleep(0.5)  # Reduced delay
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            break
    return None

def save_to_file(match_data: dict):
    try:
        DATA_DIR.mkdir(exist_ok=True)
        
        existing_data = []
        if MATCH_HISTORY_FILE.exists():
            with open(MATCH_HISTORY_FILE, 'r', encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        
        existing_data.append(match_data)
        
        with open(MATCH_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, default=str)
            
    except Exception as e:
        logger.error(f"Error saving to file: {e}")
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
db = DatabaseHandler(DB_CONFIG, minconn=DB_POOL_MIN, maxconn=DB_POOL_MAX)

# --- Initialize Database ---
if not db.check_connection():
    logger.warning("Database connection failed, using file storage")
    USE_FILE_STORAGE = True

# --- Game Commands ---
async def gameon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = str(update.effective_user.id)
        
        if not is_registered(user_id):
            await update.message.reply_text(
                escape_markdown_v2_custom(
                    "❌ *You need to register first!*\n"
                    "Send /start to me in private chat to register."
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        if update.effective_chat.type == ChatType.PRIVATE:
            await update.message.reply_text(
                escape_markdown_v2_custom("❌ *Please add me to a group to play!*"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        game_id = create_game(
            creator_id=str(update.effective_user.id),
            creator_name=update.effective_user.first_name,
            chat_id=update.effective_chat.id
        )

        # Updated game mode display format
        keyboard = []
        mode_text = escape_markdown_v2_custom("🎮 *CRICKET GAME MODES*\n" + "═" * 20 + "\n\n")
        
        for mode, details in GAME_MODE_DISPLAY.items():
            keyboard.append([InlineKeyboardButton(
                f"{details['icon']} {escape_markdown_v2_custom(details['title'])}",
                callback_data=f"mode_{game_id}_{mode}"
            )])
            mode_text += f"{details['icon']} *{escape_markdown_v2_custom(details['title'])}*\n"
            mode_text += f"   ↳ {escape_markdown_v2_custom(details['desc'])}\n\n"
        
        mode_text += "═" * 20 + "\n"
        mode_text += escape_markdown_v2_custom("👆 *Select your preferred game mode*")

        await update.message.reply_text(
            mode_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in gameon: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ An error occurred. Please try again."),
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ *Please start me in private chat to register!*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    msg = await update.message.reply_text(escape_markdown_v2_custom("🎮 *Setting up your account*..."))
    

    try:
        success = db.register_user(
            telegram_id=user.id,
            username=user.username,
            first_name=user.first_name
        )
        
        if success:
            REGISTERED_USERS.add(str(user.id))
            await msg.edit_text(
                escape_markdown_v2_custom(
                    f"🏏 *Welcome to Cricket Bot, {user.first_name}!*\n\n"
                    "✅ *Registration Complete!*\n\n"
                    "📌 *Quick Guide:*\n"
                    "🏏 /gameon - Start a new match\n"
                    "📊 /scorecard - View match history\n"
                    "💾 /save - Save match results\n\n"
                    "🎮 *Join any group and type /gameon to play!*"
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            raise Exception("Registration failed")
            
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        REGISTERED_USERS.add(str(user.id))
        await msg.edit_text(
            escape_markdown_v2_custom(
                f"⚠️ *Welcome, {user.first_name}!*\n\n"
                "Registration partially completed.\n"
                "Some features may be limited.\n\n"
                "📌 *Available Commands:*\n"
                "🏏 /gameon - Start a new match"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )

# Updated keyboard layouts as requested
def get_batting_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("1️⃣", callback_data=f"bat_{game_id}_1"),
            InlineKeyboardButton("2️⃣", callback_data=f"bat_{game_id}_2"),
            InlineKeyboardButton("3️⃣", callback_data=f"bat_{game_id}_3")
        ],
        [
            InlineKeyboardButton("4️⃣", callback_data=f"bat_{game_id}_4"),
            InlineKeyboardButton("5️⃣", callback_data=f"bat_{game_id}_5"),
            InlineKeyboardButton("6️⃣", callback_data=f"bat_{game_id}_6")
        ]
    ]

def get_bowling_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("1️⃣", callback_data=f"bowl_{game_id}_1"),
            InlineKeyboardButton("2️⃣", callback_data=f"bowl_{game_id}_2"),
            InlineKeyboardButton("3️⃣", callback_data=f"bowl_{game_id}_3")
        ],
        [
            InlineKeyboardButton("4️⃣", callback_data=f"bowl_{game_id}_4"),
            InlineKeyboardButton("5️⃣", callback_data=f"bowl_{game_id}_5"),
            InlineKeyboardButton("6️⃣", callback_data=f"bowl_{game_id}_6")
        ]
    ]

# Updated wickets keyboard layout: (1,3) (5,11) and custom
def get_wickets_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("1 🎯", callback_data=f"wickets_{game_id}_1"),
            InlineKeyboardButton("3 🎯", callback_data=f"wickets_{game_id}_3")
        ],
        [
            InlineKeyboardButton("5 🎯", callback_data=f"wickets_{game_id}_5"),
            InlineKeyboardButton("11 🎯", callback_data=f"wickets_{game_id}_11")
        ],
        [InlineKeyboardButton("📝 Custom", callback_data=f"custom_{game_id}_wickets")]
    ]

# Updated overs keyboard layout: (1,5) (10,20) and custom
def get_overs_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    return [
        [
            InlineKeyboardButton("1 🎯", callback_data=f"overs_{game_id}_1"),
            InlineKeyboardButton("5 🎯", callback_data=f"overs_{game_id}_5")
        ],
        [
            InlineKeyboardButton("10 🎯", callback_data=f"overs_{game_id}_10"),
            InlineKeyboardButton("20 🎯", callback_data=f"overs_{game_id}_20")
        ],
        [InlineKeyboardButton("📝 Custom", callback_data=f"custom_{game_id}_overs")]
    ]

# --- Game Handlers ---
async def handle_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, mode = query.data.split('_')
        if game_id not in games:
            await query.edit_message_text("❌ Game not found!", parse_mode=ParseMode.MARKDOWN_V2)
            return
            
        game = games[game_id]
        game['mode'] = mode
        game['status'] = 'setup'
        
        if mode == 'survival':
            game['max_wickets'] = 1
            game['max_overs'] = float('inf')
            keyboard = [[InlineKeyboardButton("🤝 Join Game", callback_data=f"join_{game_id}")]]
            setup_text = f"🎯 *{GAME_MODE_DISPLAY[mode]['title']}*\n\n" \
                        f"⚙️ *Settings:*\n" \
                        f"• Wickets: 1\n" \
                        f"• Overs: Unlimited\n\n" \
                        f"👤 *Host:* {game['creator_name']}\n\n" \
                        f"🎮 *Ready to play!*"
        elif mode == 'quick':
            game['max_wickets'] = float('inf')
            keyboard = get_overs_keyboard(game_id)
            setup_text = f"⚡ *{GAME_MODE_DISPLAY[mode]['title']}*\n\n" \
                        f"⚙️ *Current Settings:*\n" \
                        f"• Wickets: Unlimited\n\n" \
                        f"🎯 *Select number of overs:*"
        else:  # classic
            keyboard = get_wickets_keyboard(game_id)
            setup_text = f"🏏 *{GAME_MODE_DISPLAY[mode]['title']}*\n\n" \
                        f"🎯 *Select number of wickets:*"
            
        await query.edit_message_text(
            escape_markdown_v2_custom(setup_text),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Error in handle_mode: {e}")

async def handle_wickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, wickets = query.data.split('_')
        game = games[game_id]
        game['max_wickets'] = int(wickets)
        
        keyboard = get_overs_keyboard(game_id)
        
        await query.edit_message_text(
            escape_markdown_v2_custom(
                f"🏏 *Classic Mode Setup*\n\n"
                f"⚙️ *Current Settings:*\n"
                f"• Wickets: {wickets}\n\n"
                f"🎯 *Select number of overs:*"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_wickets: {e}")

async def handle_vers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, overs = query.data.split('_')
        if game_id not in games:
            await query.edit_message_text("❌ Game not found!", parse_mode=ParseMode.MARKDOWN_V2)
            return
            
        game = games[game_id]
        game['max_overs'] = int(overs)
        game['status'] = 'waiting'
        
        keyboard = [[InlineKeyboardButton("🤝 Join Match", callback_data=f"join_{game_id}")]]
        
        wickets_display = str(game['max_wickets']) if game['max_wickets'] != float('inf') else INFINITY_SYMBOL
        
        await query.edit_message_text(
            escape_markdown_v2_custom(
                f"🏏 *Game Ready*\n\n"
                f"⚙️ *Final Settings:*\n"
                f"• Mode: {game['mode'].title()}\n"
                f"• Wickets: {wickets_display}\n"
                f"• Overs: {overs}\n"
                f"• Host: {game['creator_name']}\n\n"
                f"⏳ *Waiting for opponent...*"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_vers: {e}")

async def handle_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id = query.data.split('_')
        
        if game_id not in games:
            await query.answer("❌ Game not found!")
            return
            
        game = games[game_id]
        
        if user_id == game['creator']:
            await query.answer("❌ You can't join your own game!", show_alert=True)
            return
            
        if 'joiner' in game:
            await query.answer("❌ Game already has two players!", show_alert=True)
            return
            
        game['joiner'] = user_id
        game['joiner_name'] = query.from_user.first_name
        game['status'] = 'toss'
        
        keyboard = [
            [
                InlineKeyboardButton("ODD", callback_data=f"toss_{game_id}_odd"),
                InlineKeyboardButton("EVEN", callback_data=f"toss_{game_id}_even")
            ]
        ]
        
        game['choosing_player'] = game['joiner']
        game['choosing_player_name'] = game['joiner_name']
        
        await query.edit_message_text(
            escape_markdown_v2_custom(
                f"🏏 *Match Starting!*\n\n"
                f"👥 *Players:*\n"
                f"• Host: {game['creator_name']}\n"
                f"• Joined: {game['joiner_name']}\n\n"
                f"🎲 *{game['joiner_name']}, choose ODD or EVEN!*"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_join: {e}")

async def handle_toss(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, choice = query.data.split('_')
        game = games.get(game_id)
        
        if not game or user_id != game['choosing_player']:
            await query.answer("❌ Invalid action!", show_alert=True)
            return
            
        await query.answer()
        
        msg = query.message
        await msg.edit_text(escape_markdown_v2_custom("🎲 *Rolling dice*..."), parse_mode=ParseMode.MARKDOWN_V2)
        await asyncio.sleep(ANIMATION_DELAY)
        
        dice1 = random.randint(1, 6)
        dice2 = random.randint(1, 6)
        total = dice1 + dice2
        is_odd = total % 2 == 1
        
        choice_correct = (choice == 'odd' and is_odd) or (choice == 'even' and not is_odd)
        toss_winner = game['choosing_player'] if choice_correct else (
            game['creator'] if game['choosing_player'] == game['joiner'] else game['joiner']
        )
        toss_winner_name = game['creator_name'] if toss_winner == game['creator'] else game['joiner_name']
        
        game['toss_winner'] = toss_winner
        game['toss_winner_name'] = toss_winner_name
        game['status'] = 'choosing'
        
        keyboard = [
            [
                InlineKeyboardButton("🏏 BAT", callback_data=f"choice_{game_id}_bat"),
                InlineKeyboardButton("⚾ BOWL", callback_data=f"choice_{game_id}_bowl")
            ]
        ]
        
        await msg.edit_text(
            escape_markdown_v2_custom(
                f"🎲 *TOSS RESULT*\n\n"
                f"🎯 *Dice:* {dice1} + {dice2} = {total} ({choice.upper()})\n"
                f"🏆 *{toss_winner_name} wins the toss!*\n\n"
                f"🎮 *Choose your action:*"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_toss: {e}")

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, choice = query.data.split('_')
        game = games[game_id]
        
        if user_id != game['toss_winner']:
            await query.answer("❌ Only toss winner can choose!", show_alert=True)
            return
            
        await query.answer()
        
        if choice == 'bat':
            game['batsman'] = game['toss_winner']
            game['batsman_name'] = game['toss_winner_name']
            game['bowler'] = game['joiner'] if game['toss_winner'] == game['creator'] else game['creator']
            game['bowler_name'] = game['joiner_name'] if game['toss_winner'] == game['creator'] else game['creator_name']
        else:
            game['bowler'] = game['toss_winner']
            game['bowler_name'] = game['toss_winner_name']
            game['batsman'] = game['joiner'] if game['toss_winner'] == game['creator'] else game['creator']
            game['batsman_name'] = game['joiner_name'] if game['toss_winner'] == game['creator'] else game['creator_name']
        
        keyboard = get_batting_keyboard(game_id)
        
        await safe_edit_message(
            query.message,
            f"🏏 *Match Starting!*\n\n"
            f"🎯 *{game['toss_winner_name']}* chose to *{choice}* first\n\n"
            f"🎮 *{game['batsman_name']}'s turn to bat!*",
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_choice: {e}")

async def handle_bat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, runs_str = query.data.split('_')
        runs = int(runs_str)
        
        if game_id not in games:
            await query.answer("❌ Game not found!", show_alert=True)
            return
            
        game = games[game_id]
        
        if user_id != game['batsman']:
            await query.answer(f"❌ Not your turn! It's {game['batsman_name']}'s turn to bat!", show_alert=True)
            return
        
        await query.answer()
        
        game['batsman_choice'] = runs
        keyboard = get_bowling_keyboard(game_id)
        
        innings_key = f'innings{game["current_innings"]}'  
        score = game['score'][innings_key]  
        
        batting_msg = random.choice(ACTION_MESSAGES['batting']).format(game['batsman_name'])
        await safe_edit_message(
            query.message,
            f"🏏 *Over {game['balls']//6}.{game['balls']%6}*\n\n"
            f"📊 *Score:* {score}/{game['wickets']}\n\n"
            f"{batting_msg}\n\n"
            f"🎯 *{game['bowler_name']}'s turn to bowl!*",
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_bat: {e}")

async def handle_bowl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, bowl_num_str = query.data.split('_')
        bowl_num = int(bowl_num_str)
        
        if game_id not in games:
            await query.answer("❌ Game not found!", show_alert=True)
            return
            
        game = games[game_id]
        
        if user_id != game['bowler']:
            await query.answer(f"❌ Not your turn! It's {game['bowler_name']}'s turn to bowl!", show_alert=True)
            return
            
        await query.answer()
        
        runs = game['batsman_choice']
        current_innings = game['current_innings']
        
        # Show bowling action
        bowling_msg = random.choice(ACTION_MESSAGES['bowling']).format(game['bowler_name'])
        await safe_edit_message(query.message, bowling_msg)
        await asyncio.sleep(BALL_ANIMATION_DELAY)

        delivery_msg = random.choice(ACTION_MESSAGES['delivery'])
        await safe_edit_message(query.message, delivery_msg)
        await asyncio.sleep(BALL_ANIMATION_DELAY)
        
        # Process result
        if bowl_num == runs:
            game['wickets'] += 1
            result_text = random.choice(COMMENTARY_PHRASES['wicket']).format(f"*{game['bowler_name']}*")
            game['this_over'].append('W')
        else:
            game['score'][f'innings{current_innings}'] += runs
            game['boundaries'][f'innings{current_innings}'] += 1 if runs == 4 else 0
            game['sixes'][f'innings{current_innings}'] += 1 if runs == 6 else 0
            game['dot_balls'] += 1 if runs == 0 else 0
            game['this_over'].append(str(runs))
            
            if runs in COMMENTARY_PHRASES:
                result_text = random.choice(COMMENTARY_PHRASES[f'run_{runs}']).format(f"*{game['batsman_name']}*")
            else:
                result_text = f"*{runs} runs by {game['batsman_name']}*"

        game['balls'] += 1
        current_score = game['score'][f'innings{current_innings}']
        
        # Check for over completion
        if game['balls'] % 6 == 0:
            over_commentary = random.choice(COMMENTARY_PHRASES['over_complete'])
            current_over = ' '.join(game['this_over'])
            game['this_over'] = []
            
            await safe_edit_message(
                query.message,
                f"🏏 *Over Complete!*\n\n"
                f"📊 *Score:* {current_score}/{game['wickets']}\n"
                f"📋 *Last Over:* {current_over}\n\n"
                f"{over_commentary}\n\n"
                f"⏳ *Short break...*"
            )
            await asyncio.sleep(OVER_BREAK_DELAY)
        
        # Check for innings/game end
        if should_end_innings(game):
            if current_innings == 1:
                await handle_innings_change(query.message, game, game_id)
                return
            else:
                is_chase_successful = current_score >= game.get('target', float('inf'))
                await handle_game_end(query, game, current_score, is_chase_successful,game_id)
                return

        keyboard = get_batting_keyboard(game_id)
        
        status_text = (
            f"🏏 *Over {game['balls']//6}.{game['balls']%6}*\n\n"
            f"📊 *Score:* {current_score}/{game['wickets']}\n"
            f"🎯 *Batsman:* {runs} | *Bowler:* {bowl_num}\n\n"
            f"{result_text}\n\n"
        )
        
        if game['this_over']:
            status_text += f"📋 *This Over:* {' '.join(game['this_over'])}\n\n"
        
        if current_innings == 2:
            runs_needed = game['target'] - current_score
            balls_left = (game['max_overs'] * 6) - game['balls']
            if balls_left > 0:
                required_rate = (runs_needed * 6) / balls_left
                status_text += f"🎯 *Need {runs_needed} from {balls_left} balls (RRR: {required_rate:.1f})*\n\n"
        
        status_text += f"🎮 *{game['batsman_name']}'s turn to bat!*"
        
        await safe_edit_message(
            query.message,
            status_text,
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_bowl: {e}")

async def handle_innings_change(msg, game: dict, game_id: str):
    store_first_innings(game)
    game['current_innings'] = 2
    game['wickets'] = 0
    game['balls'] = 0
    game['this_over'] = []
    
    # Swap batsman and bowler
    temp_batsman = game['batsman']
    temp_batsman_name = game['batsman_name']
    
    game['batsman'] = game['bowler']
    game['batsman_name'] = game['bowler_name']
    game['bowler'] = temp_batsman
    game['bowler_name'] = temp_batsman_name
    
    innings_text = (
        f"🏁 *INNINGS COMPLETE!*\n\n"
        f"📊 *First Innings:* {game['first_innings_score']}/{game['first_innings_wickets']} ({game['first_innings_overs']})\n"
        f"🎯 *Target:* {game['target']} runs\n"
        f"📈 *Required Rate:* {game['target'] / game['max_overs']:.1f}\n\n"
        f"🔄 *Innings change!*\n\n"
        f"🎮 *{game['batsman_name']}'s turn to bat!*"
    )

    await safe_edit_message(msg, innings_text, keyboard=InlineKeyboardMarkup(get_batting_keyboard(game_id)))

async def handle_game_end(query, game: dict, current_score: int, is_chase_successful: bool,game_id: str):
    try:
        match_id = game.get('match_id', f"M{random.randint(1000, 9999)}")
        
        # Calculate match statistics
        total_boundaries = game['boundaries']['innings1'] + game['boundaries']['innings2']
        total_sixes = game['sixes']['innings1'] + game['sixes']['innings2']
        
        first_innings_rr = safe_division(game['first_innings_score'], float(game['first_innings_overs'].split('.')[0]) if '.' in game['first_innings_overs'] else game['first_innings_score'], 0)
        second_innings_rr = safe_division(current_score, game['balls']/6, 0)
        
        # Determine winner and margin
        if is_chase_successful:
            wickets_left = game['max_wickets'] - game['wickets'] if game['max_wickets'] != float('inf') else 'many'
            winner = game['batsman_name']
            margin = f"{wickets_left} wickets" if wickets_left != 'many' else "comfortably"
        elif current_score == game['target']-1:
            winner = "No one"
            margin = "Match tied"
        else:
            runs_short = game['target'] - current_score - 1
            winner = game['bowler_name']
            margin = f"{runs_short} runs"
        # Compact match result format
        final_message = (
            f"🏆 *MATCH COMPLETE* #{match_id}\n"
            f"══════════════════════\n\n"
            f"🏏 *{game['mode'].upper()} MODE*\n\n"
            f"📊 *SCORECARD*\n"
            f"*1st Innings:* {game['first_innings_score']}/{game['first_innings_wickets']} ({game['first_innings_overs']})\n"
            f"*2nd Innings:* {current_score}/{game['wickets']} ({game['balls']//6}.{game['balls']%6})\n\n"
            f"📈 *MATCH STATS*\n"
            f"• *Boundaries:* {total_boundaries} | *Sixes:* {total_sixes}\n"
            f"• *Run Rate:* {first_innings_rr:.1f} & {second_innings_rr:.1f}\n"
            f"• *Dot Balls:* {game.get('dot_balls', 0)}\n\n"
            f"🎉 *RESULT*\n"
            f"*{winner} won by {margin}!* 🏆\n\n"
            f"══════════════════════\n"
            f"💾 *Use /save (Name - Optional) to save this match*"
        )

        await query.message.edit_text(
            escape_markdown_v2_custom(final_message),
            parse_mode=ParseMode.MARKDOWN_V2
        )

        # Store match data for saving
        game['match_result'] = {
            'match_id': match_id,
            'winner': winner,
            'margin': margin,
            'first_innings': f"{game['first_innings_score']}/{game['first_innings_wickets']} ({game['first_innings_overs']})",
            'second_innings': f"{current_score}/{game['wickets']} ({game['balls']//6}.{game['balls']%6})",
            'boundaries': total_boundaries,
            'sixes': total_sixes,
            'dot_balls': game.get('dot_balls', 0),
            'mode': game['mode'],
            'date': datetime.now().strftime('%d %b %Y'),
            'full_result': final_message
        }

        # Cleanup
        if game_id in games:
            del games[game_id]

    except Exception as e:
        logger.error(f"Error in handle_game_end: {e}")
        await query.message.edit_text(
            "🏏 *Game Complete!*\n\n"
            "❗ Some details couldn't be displayed\n"
            "Use /save to save this match",
            parse_mode=ParseMode.MARKDOWN_V2
        )

# --- Custom Input Handler ---
async def handle_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, setting = query.data.split('_')
        
        context.user_data['awaiting_input'] = {
            'game_id': game_id,
            'setting': setting,
            'message_id': query.message.message_id
        }
        
        max_value = 50 if setting == 'overs' else 10
        
        await query.edit_message_text(
            escape_markdown_v2_custom(
                f"📝 *Enter custom {setting}:*\n\n"
                f"🎯 *Reply with a number (1-{max_value})*\n"
                f"⚠️ *Must be a reply to this message*"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_custom: {e}")

async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
        
    if 'awaiting_input' not in context.user_data:
        return
    
    try:
        input_data = context.user_data['awaiting_input']
        game_id = input_data['game_id']
        setting = input_data['setting']
        
        if game_id not in games:
            return
            
        game = games[game_id]
        
        input_value = update.message.text.strip()
        if not input_value.isdigit():
            await update.message.reply_text(
                "❌ *Please enter a valid number!*",
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        value = int(input_value)
        max_value = 50 if setting == 'overs' else 10
        
        if value < 1 or value > max_value:
            await update.message.reply_text(
                f"❌ *Please enter a number between 1-{max_value}!*",
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
        
        if setting == 'overs':
            game['max_overs'] = value
            game['status'] = 'waiting'
            keyboard = [[InlineKeyboardButton("🤝 Join", callback_data=f"join_{game_id}")]]
            message = (
                f"✅ *Custom Overs Set:* {value}\n\n"
                f"⚙️ *Final Settings:*\n"
                f"• Mode: {game['mode'].title()}\n"
                f"• Wickets: {game['max_wickets']}\n"
                f"• Overs: {value}\n\n"
                f"⏳ *Waiting for opponent...*"
            )
        else:
            game['max_wickets'] = value
            keyboard = get_overs_keyboard(game_id)
            message = (
                f"✅ *Custom Wickets Set:* {value}\n\n"
                f"🎯 *Now select number of overs:*"
            )
        
        await update.message.reply_text(
            escape_markdown_v2_custom(message),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        del context.user_data['awaiting_input']
        
    except Exception as e:
        logger.error(f"Error handling input: {e}")
    except Exception as e:
        logger.error(f"Error handling input: {e}")
    except Exception as e:
        logger.error(f"Error handling input: {e}")
# --- Scorecard Functions ---
async def save_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text(escape_markdown_v2_custom(
            "❌ *Please reply to a match result message with /save <name>*\n\n"
            "*Example:* Reply to match result + `/save (Name-Optional)`"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    try:
        # Get custom name from args or use default
        match_name = ' '.join(context.args) if context.args else "CricSaga Match"
        match_result = update.message.reply_to_message.text
        
        if not match_result or "MATCH COMPLETE" not in match_result:
            await update.message.reply_text(escape_markdown_v2_custom(
                "❌ *Invalid match result!*\n"
                "Please reply to a valid match result message"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        # Clean match name
        match_name = re.sub(r'[^a-zA-Z0-9\s_-]', '', match_name)[:50]
        if not match_name:
            match_name = "CricSaga Match"

        match_data = {
            'match_id': f"M{int(time.time())}_{random.randint(1000, 9999)}",
            'user_id': update.effective_user.id,
            'user_name': update.effective_user.first_name,
            'match_name': match_name,
            'timestamp': datetime.now().isoformat(),
            'match_result': match_result
        }

        # Try database save
        success_db = db.save_match(match_data)
        
        # Try file save as backup
        if not success_db:
            save_to_file(match_data)

        await update.message.reply_text(
            escape_markdown_v2_custom(
                f"✅ *Match saved successfully as:* {match_name}\n\n"
                f"📊 *View your matches with /scorecard*"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )

    except Exception as e:
        logger.error(f"Error in save_match: {e}")
        await update.message.reply_text(
            "❌ *Error saving match. Please try again.*",
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def view_scorecards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    msg_target = update.message or update.effective_message
    # Check if private chat
    if update.effective_chat.type != ChatType.PRIVATE:
        await msg_target.reply_text(escape_markdown_v2_custom(
            "❌ *Please use this command in bot DM!*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    # Get matches
    matches = db.get_user_matches(user_id, limit=20)
    
    if not matches:
        await msg_target.reply_text(
            escape_markdown_v2_custom("❌ *No saved matches found!*"),
            parse_mode="MarkdownV2"
        )
        return
    msg = "🏏 *Your Saved Matches:*\n\n"
    for match_id, match_name, created_at, game_mode in matches:
        match_name = match_name or "Unnamed Match"
        msg += f"• *{escape_markdown_v2_custom(match_name)}* \\(`{escape_markdown_v2_custom(str(match_id))}`\\)\n"
        msg += f"  _{escape_markdown_v2_custom(str(created_at))}_ | {escape_markdown_v2_custom(str(game_mode))}\n"
    await update.message.reply_text(
        msg,
        parse_mode="MarkdownV2"
    )

async def view_single_scorecard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    match_id = query.data.split('_', 1)[1]
    user_id = str(query.from_user.id)
    
    matches = db.get_user_matches(user_id, limit=50)
    match_data = None
    
    for match in matches:
        if match['match_id'] == match_id:
            match_data = match
            break

    if not match_data:
        await query.edit_message_text(
            escape_markdown_v2_custom("❌ *Match not found!*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    keyboard = [
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{match_id}")],
        [InlineKeyboardButton("◀️ Back", callback_data="list_matches")]
    ]

    try:
        # Format match details
        match_info = match_data.get('match_data', {})
        if isinstance(match_info, str):
            try:
                match_info = json.loads(match_info)
            except Exception:
                match_info = {}

        timestamp = datetime.fromisoformat(match_data['timestamp']).strftime('%d %b %Y, %H:%M')
        match_name = match_data.get('match_name', 'Unknown')
        game_mode = match_data.get('game_mode', 'classic').title()
        match_result = match_info.get('match_result', 'No details available')

        result_lines = str(match_result).splitlines()
        formatted_result = []
        stop_after_this = False
        for line in result_lines:
            if stop_after_this:
                break
            if 'match complete' in line.lower():
                continue
            if (('won by' in line.lower() or 'draw' in line.lower()) and not line.strip().startswith('*')):
                formatted_result.append(f"*{line.strip()}*")
                stop_after_this = True
            elif ('MODE' in line and not line.strip().startswith('*')) or ('SCORECARD' in line and not line.strip().startswith('*')) or ('STATS' in line and not line.strip().startswith('*')):
                formatted_result.append(f"*{line.strip()}*")
            else:
                formatted_result.append(line)
        match_result_formatted = '\n'.join(formatted_result)

        details_text = (
            f"📊 *Match Details*\n"
            f"═══════════════════════════\n\n"
            f"🏷️ *Name:* {match_name}\n"
            f"📅 *Date:* {timestamp}\n"
            f"📋 *Match Result:*\n"
            f"{match_result_formatted}"
        )

        await query.edit_message_text(
            escape_markdown_v2_custom(details_text),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error formatting match: {e}")
        await query.edit_message_text(
            escape_markdown_v2_custom("❌ *Error displaying match details!*"),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def delete_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    match_id = query.data.split('_', 1)[1]
    user_id = str(query.from_user.id)
    
    try:
        success = db.delete_match(match_id, user_id)
        
        if success:
            await query.message.edit_text(escape_markdown_v2_custom(
                "✅ *Match deleted successfully!*\n\n"
                "🔄 *Refreshing list...*"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            await asyncio.sleep(1)
            await view_scorecards(update, context)
        else:
            await query.message.edit_text(escape_markdown_v2_custom(
                "❌ *Failed to delete match!*"),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back", callback_data="list_matches")
                ]])
            )
    except Exception as e:
        logger.error(f"Error deleting match: {e}")
        await query.message.edit_text(escape_markdown_v2_custom(
            "❌ *Error occurred while deleting!*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def back_to_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = str(query.from_user.id)
    matches = db.get_user_matches(user_id, limit=20)

    if not matches:
        await query.edit_message_text(
            escape_markdown_v2_custom(
                "📊 *No saved matches found!*\n\n"
                "🎮 Play some matches and save them with `/save`"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    keyboard = []
    for match in matches[:10]:
        match_date = datetime.fromisoformat(match['timestamp']).strftime('%d/%m')
        button_text = f"📅 {match_date} - {match['match_name']}"
        keyboard.append([
            InlineKeyboardButton(button_text, callback_data=f"view_{match['match_id']}")
        ])
    if len(matches) > 10:
        keyboard.append([
            InlineKeyboardButton("➡️ More Matches", callback_data="page_next")
        ])

    await query.edit_message_text(
        escape_markdown_v2_custom(
            f"📊 *Your Match History* ({len(matches)} total)\n"
            f"═══════════════════════════\n\n"
            f"📋 *Select a match to view details:*"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN_V2
    )

# --- Admin Commands ---
async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text(escape_markdown_v2_custom("❌ Unauthorized"), parse_mode=ParseMode.MARKDOWN_V2)
        return
        
    if not context.args:
        await update.message.reply_text(escape_markdown_v2_custom("Usage: /addadmin <user_id>"), parse_mode=ParseMode.MARKDOWN_V2)
        return
        
    BOT_ADMINS.add(context.args[0])
    await update.message.reply_text(escape_markdown_v2_custom("✅ Admin added successfully"), parse_mode=ParseMode.MARKDOWN_V2)

async def bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text(escape_markdown_v2_custom("❌ Unauthorized"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    active_games = len(games)
    unique_users = set()
    for game in games.values():
        unique_users.add(game['creator'])
        if 'joiner' in game:
            unique_users.add(game['joiner'])
    
    stats = (
        f"📊 *Bot Statistics*\n"
        f"{escape_markdown_v2_custom(MATCH_SEPARATOR)}\n\n"
        f"👥 Total Users: {escape_markdown_v2_custom(len(REGISTERED_USERS))}\n"
        f"🎮 Active Games: {escape_markdown_v2_custom(active_games)}\n"
        f"🎯 Current Players: {escape_markdown_v2_custom(len(unique_users))}\n"
        f"💾 Saved Matches: {escape_markdown_v2_custom(len(in_memory_scorecards))}\n"
        f"👥 Authorized Groups: {escape_markdown_v2_custom(len(AUTHORIZED_GROUPS))}\n"
        f"🔐 Test Mode: {escape_markdown_v2_custom('Enabled' if TEST_MODE else 'Disabled')}\n\n"
        f"_Last updated: {escape_markdown_v2_custom(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}_"
    )
    
    await update.message.reply_text(stats, parse_mode=ParseMode.MARKDOWN_V2)

async def add_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text(escape_markdown_v2_custom("❌ Unauthorized"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    if not context.args:
        await update.message.reply_text(escape_markdown_v2_custom("Usage: /addgroup <group_id>"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    AUTHORIZED_GROUPS.add(int(context.args[0]))
    await update.message.reply_text(escape_markdown_v2_custom("✅ Group added to authorized list"), parse_mode=ParseMode.MARKDOWN_V2)

async def remove_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text(escape_markdown_v2_custom("❌ Unauthorized"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    if not context.args:
        await update.message.reply_text(escape_markdown_v2_custom("Usage: /removegroup <group_id>"), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    try:
        AUTHORIZED_GROUPS.remove(int(context.args[0]))
        await update.message.reply_text(escape_markdown_v2_custom("✅ Group removed from authorized list"), parse_mode=ParseMode.MARKDOWN_V2)
    except KeyError:
        await update.message.reply_text(escape_markdown_v2_custom("❌ Group not found in authorized list"), parse_mode=ParseMode.MARKDOWN_V2)

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send broadcast message to all users/games"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return

    # Determine message to broadcast
    if update.message.reply_to_message:
        msg = update.message.reply_to_message
        is_forward = True
        message = None
    elif context.args:
        message = ' '.join(context.args)
        is_forward = False
        msg = None
    else:
        await update.message.reply_text(
            "Usage:\n"
            "• Reply to a message with /broadcast\n"
            "• Or use /broadcast <message>"
        )
        return

    status_msg = await update.message.reply_text("📢 Broadcasting message...")

    # Collect unique chat IDs from REGISTERED_USERS, games, and in_memory_scorecards
    unique_chats = set()

    # Add all registered users
    for uid in REGISTERED_USERS:
        try:
            unique_chats.add(int(uid))
        except Exception:
            continue

    # Add all active game chat_ids
    for game in games.values():
        chat_id = game.get('chat_id')
        if chat_id:
            unique_chats.add(chat_id)

    # Add all in_memory_scorecards user_ids
    if in_memory_scorecards:
        for card in in_memory_scorecards:
            uid = card.get('user_id')
            if uid:
                unique_chats.add(int(uid))

    # Try to fetch all user_ids from DB if possible
    try:
        db_users = db.get_all_user_ids() if hasattr(db, "get_all_user_ids") else []
        for uid in db_users:
            unique_chats.add(int(uid))
    except Exception:
        pass

    failed = 0
    success = 0

    for chat_id in unique_chats:
        try:
            if is_forward and msg:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=update.effective_chat.id,
                    message_id=msg.message_id
                )
            elif message:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=escape_markdown_v2_custom(f"📢 Broadcast\n{message}"),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            success += 1
            await asyncio.sleep(BROADCAST_DELAY)
        except Exception as e:
            logger.error(f"Broadcast failed for {chat_id}: {e}")
            failed += 1

    await status_msg.edit_text(
        escape_markdown_v2_custom(
            f"📢 *Broadcast Complete*\n"
            f"{MATCH_SEPARATOR}\n"
            f"✅* Success:* {success}\n"
            f"❌* Failed: *{failed}\n"
            f"*📊 Total:* {success + failed}"
        ),
        parse_mode=ParseMode.MARKDOWN_V2
    )

async def stop_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop all active games"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text(escape_markdown_v2_custom("❌ Unauthorized"), parse_mode=ParseMode.MARKDOWN_V2)
        return
        
    games.clear()
    await update.message.reply_text(escape_markdown_v2_custom("*🛑 All games stopped*"), parse_mode=ParseMode.MARKDOWN_V2)

async def test_db_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Test database connection and schema"""
        if not check_admin(str(update.effective_user.id)):
            await update.message.reply_text("❌ Unauthorized")
            return
            
        try:
            connection = db.get_connection()
            if not connection:
                await update.message.reply_text("❌ Database connection failed!")
                return
    
                
            with connection.cursor() as cursor:
                # Test users table
                cursor.execute("SELECT COUNT(*) FROM users")
                users_count = cursor.fetchone()[0]
                
                # Test scorecards table
                cursor.execute("SELECT COUNT(*) FROM scorecards")
                scorecards_count = cursor.fetchone()[0]
                
                await update.message.reply_text(
                    f"*✅ Database connected successfully!*👍\n"
                    f"*Users:* {users_count}\n"
                    f"*Scorecards:* {scorecards_count}"
                )
                
        except Exception as e:
            await update.message.reply_text(
                f"*❌ Database test failed:*\n{str(e)}"
            )
        finally:
            if connection:
                connection.close()
# --- Main Function ---
def main():
    load_dotenv()

    if not BOT_TOKEN:
        logger.error(escape_markdown_v2_custom("Bot token not found!"))
        return

    # Optimized application settings
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(4)  # Reduced pool size
        .connect_timeout(15.0)    # Reduced timeout
        .read_timeout(15.0)
        .write_timeout(15.0)
        .build()
    )

    # Game handlers
    application.add_handler(CommandHandler("gameon", gameon))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("save", save_match))
    application.add_handler(CommandHandler("scorecard", view_scorecards))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(handle_mode, pattern="^mode_"))
    application.add_handler(CallbackQueryHandler(handle_wickets, pattern="^wickets_"))
    application.add_handler(CallbackQueryHandler(handle_vers, pattern="^overs_"))
    application.add_handler(CallbackQueryHandler(handle_custom, pattern="^custom_"))
    application.add_handler(CallbackQueryHandler(handle_join, pattern="^join_"))
    application.add_handler(CallbackQueryHandler(handle_toss, pattern="^toss_"))
    application.add_handler(CallbackQueryHandler(handle_choice, pattern="^choice_"))
    application.add_handler(CallbackQueryHandler(handle_bat, pattern="^bat_"))
    application.add_handler(CallbackQueryHandler(handle_bowl, pattern="^bowl_"))
    application.add_handler(CallbackQueryHandler(view_single_scorecard, pattern="^view_"))
    application.add_handler(CallbackQueryHandler(delete_match, pattern="^delete_"))
    application.add_handler(CallbackQueryHandler(back_to_list, pattern="^list_matches"))
    
    # Admin handlers
    application.add_handler(CommandHandler("addadmin", add_admin))
    application.add_handler(CommandHandler("botstats", bot_stats))
    application.add_handler(CommandHandler("addgroup", add_group))
    application.add_handler(CommandHandler("removegroup", remove_group))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("stopgames", stop_games))
    application.add_handler(CommandHandler("testdb", test_db_connection))
    
    # Message handler for custom input
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_input))

    logger.info("🏏 Cricket Bot starting...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        if db:
            db.close()

if __name__ == '__main__':
    main()