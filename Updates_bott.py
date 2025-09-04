# --- Standard Library Imports ---
import os
import random
import json
import logging
import asyncio
import time
import re
import html
import asyncpg
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Set, List, DefaultDict, Optional 
from collections import defaultdict
from html import escape as escape_markdown_v2_custom

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
import aiofiles
import async_timeout

# --- Initialization & Configuration ---
from db_handlerr import DatabaseHandler  # Import DatabaseHandler

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# Constants
DATA_DIR = Path("data")
MATCH_HISTORY_FILE = DATA_DIR / "match_history.json"
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BOT_ADMINS: Set[str] = {os.getenv('BOT_ADMIN', '')}
games: Dict[str, Dict] = {}

UI_THEMES = {
    'primary': {
        'separator': "┏━━━━━━━━━━━━━━━━━━━━━┓",
        'section_sep': "┣━━━━━━━━━━━━━━━━━━━━━┫",
        'footer': "┗━━━━━━━━━━━━━━━━━━━━━┛",
        'bullet': "•",
        'frames': {
            'top': "╔══════════════════════╗",
            'middle': "╠══════════════════════╣",
            'bottom': "╚══════════════════════╝"
        }
    },
    'accents': {
        'match': "🎯",
        'score': "📊",
        'bat': "🏏",
        'ball': "⚾",
        'win': "🏆",
        'stats': "📈",
        'alert': "⚠️",
        'error': "❌",
        'success': "✅"
    },
    'animations': {
        'loading': ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
        'progress': ["▰▱▱▱▱", "▰▰▱▱▱", "▰▰▰▱▱", "▰▰▰▰▱", "▰▰▰▰▰"]
    }
}

MESSAGE_STYLES = {
    'game_start': (
        "{ui[separator]}\n"
        "🎮 *CRICKET SAGA* 🏏\n"
        "{ui[section_sep]}\n"
        "*Mode:* {mode}\n"
        "*Host:* {host}\n"
        "{ui[footer]}"
    ),
    'match_status': (
        "{ui[separator]}\n"
        "📊 *LIVE MATCH STATUS*\n"
        "{ui[section_sep]}\n"
        "*Score:* {score}/{wickets}\n"
        "*Overs:* {overs}.{balls}\n"
        "*Run Rate:* {run_rate:.2f}\n"
        "{target_info}\n"
        "{ui[section_sep]}\n"
        "{commentary}\n"
        "{ui[footer]}"
    ),
    'innings_complete': (
        "{ui[separator]}\n"
        "🏁 *INNINGS COMPLETE*\n"
        "{ui[section_sep]}\n"
        "*Score:* {score}/{wickets}\n"
        "*Overs:* {overs}\n"
        "*Run Rate:* {run_rate:.2f}\n"
        "*Target:* {target}\n"
        "{ui[footer]}"
    ),
    'match_result': (
        "{ui[separator]}\n"
        "🏆 *MATCH COMPLETE*\n"
        "{ui[section_sep]}\n"
        "*First Innings:* {score1}/{wickets1} ({overs1})\n"
        "*Second Innings:* {score2}/{wickets2} ({overs2})\n"
        "{ui[section_sep]}\n"
        "*Boundaries:* {boundaries} | *Sixes:* {sixes}\n"
        "*Best Over:* {best_over} runs\n"
        "*Run Rate:* {run_rate:.2f}\n"
        "{ui[section_sep]}\n"
        "{result}\n"
        "{ui[footer]}"
    )
}

# Update GAME_MODES with properly escaped descriptions
GAME_MODES = {
    'classic': {
        'icon': "🏏",
        'title': "Classic Cricket",
        'description': [
            "Traditional Format",
            "Limited Overs & Wickets",
            "Strategic Gameplay"
        ],
        'max_wickets': 10,
        'max_overs': 20,
        'style': 'elegant'
    },
    'quick': {
        'icon': "⚡",
        'title': "Quick Match",
        'description': [
            "Fast\\-Paced Action",
            "Unlimited Wickets",
            "Quick Games"
        ],
        'max_wickets': float('inf'),
        'max_overs': 5,
        'style': 'dynamic'
    },
    'survival': {
        'icon': "🎯",
        'title': "Survival Mode",
        'description': [
            "One Wicket Challenge",
            "Unlimited Overs",
            "Last Man Standing"
        ],
        'max_wickets': 1,
        'max_overs': float('inf'),
        'style': 'intense'
    }
}

# Animation and timing constants
ANIMATION_DELAY = 0.8
TRANSITION_DELAY = 0.5
BALL_ANIMATION_DELAY = 0.5
BUTTON_COOLDOWN = 2.0
BUTTON_PRESS_COOLDOWN = 3
FLOOD_CONTROL_LIMIT = 24
MAX_RETRIES = 5
RETRY_DELAY = 1.0
SPLIT_ERROR_DELAY = 0.5
MAX_BUTTON_RETRIES = 2
TURN_WAIT_TIME = 5
FLOOD_WAIT_TIME = 5
ERROR_DISPLAY_TIME = 3
RETRY_WAIT_TIME = 5
MAX_AUTO_RETRIES = 3
OVER_BREAK_DELAY = 2.0
INFINITY_SYMBOL = "∞"
TIMEOUT_RETRY_DELAY = 0.5
MAX_MESSAGE_RETRIES = 3
# Add near the top with other constants
MAINTENANCE_MODE = False
BLACKLISTED_USERS = set()

# Add new constants
USER_DATA_FILE = DATA_DIR / "user_data.json"
GAME_DATA_FILE = DATA_DIR / "game_data.json"
ANIMATION_FRAMES = {
    'progress': ['▰▱▱▱▱', '▰▰▱▱▱', '▰▰▰▱▱', '▰▰▰▰▱', '▰▰▰▰▰']
}

# Game state tracking
last_button_press = {}
user_last_click = {}
user_scorecards = {}

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),  # Make sure this is correct
    'port': int(os.getenv('DB_PORT', '5432')),
    'connect_timeout': 30,  # Increased timeout
    'sslmode': 'require',  # Required for most cloud hosting
    'keepalives': 1,
    'keepalives_idle': 30,
    'keepalives_interval': 10,
    'keepalives_count': 5,
    'client_encoding': 'utf8',
    'application_name': 'cricket_bot',  # Add application name
    'options': '-c timezone=UTC'  # Set timezone
}

# Add near the top with other constants
DB_POOL_MIN = 1
DB_POOL_MAX = 20
db_pool = None


def init_db_pool():
    """Initialize database connection pool with better error handling"""
    global db_pool
    try:
        if not all([DB_CONFIG['user'], DB_CONFIG['password'], DB_CONFIG['host']]):
            logger.error("Database configuration missing. Please check your .env file")
            return False

        # Log connection attempt
        logger.info(f"Attempting to connect to database at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            
        # Create connection pool with retry logic
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                db_pool = SimpleConnectionPool(
                    DB_POOL_MIN,
                    DB_POOL_MAX,
                    **DB_CONFIG
                )
                
                # Test the connection
                conn = db_pool.getconn()
                with conn.cursor() as cursor:
                    cursor.execute('SELECT 1')
                db_pool.putconn(conn)
                
                logger.info("Database connection pool created successfully")
                return True
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection attempt {retry_count} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(5)  # Wait 5 seconds before retrying
                    continue
                break
                
        logger.error("Failed to create connection pool after all retries")
        return False
        
    except Exception as e:
        logger.error(f"Failed to create connection pool: {e}")
        return False


def check_admin(user_id: str) -> bool:
    """Check if user is an admin"""
    return user_id in BOT_ADMINS


def escape_markdown_v2_custom(text: str) -> str:
    """Escape special characters for Markdown V2 format with custom handling"""
    special_chars = ['_', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text

def format_text(text: str) -> str:
    """Escape special characters for Markdown V2 format"""
    return escape_markdown_v2_custom(text)

def get_db_connection():
    """Get a connection from the pool"""
    global db_pool
    if db_pool is None:
        if not init_db_pool():
            return None
    try:
        conn = db_pool.getconn()
        # Test if connection is alive
        with conn.cursor() as cursor:
            cursor.execute('SELECT 1')
        return conn
    except Exception as e:
        logger.error(f"Error getting connection from pool: {e}")
        if db_pool is not None:
            try:
                db_pool.putconn(conn)
            except:
                pass
        return None

def return_db_connection(conn):
    """Return a connection to the pool"""
    global db_pool
    if db_pool is not None and conn is not None:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")

# Add new function to check connection status
def is_connection_alive(connection):
    """Check if PostgreSQL connection is alive"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            return True
    except (psycopg2.Error, AttributeError):
        return False

# Initialize database connection
db = DatabaseHandler()  # Create an instance of DatabaseHandler
if not db.check_connection():  # Check if the connection is successful
    logger.error("Database connection failed!")
    exit(1)  # Exit if the connection fails

# Add in-memory fallback storage
in_memory_scorecards = []

# Error message templates
ERROR_MESSAGES = {
    'turn_wait': "⏳ *Please wait* {} *seconds for your turn*...",
    'flood_control': "🚫 *Telegram limit reached!* Wait *{}* seconds...",  # Added bold for seconds
    'invalid_turn': "❌ *Not your turn!* It's *{}*'s turn to *{}*",
    'game_state': "⚠️ *Game state error:* *{}*",  # Added bold for error message
    'recovery': "🔄 *Attempting to recover game state*..."
}

# Commentary phrases
COMMENTARY_PHRASES = {
    'wicket': [
        "💥 *BOWLED!* {} *gets the breakthrough!*",
        "🎯 *CLEAN BOWLED!* {} *destroys the stumps!*",
        "⚡ *OUT!* *Masterful bowling by* {}!",
        "🌟 *THAT'S OUT!* {} *celebrates the wicket!*",
        "💫 *DESTROYED!* *Perfect delivery by* {}!",
        "🔥 *INCREDIBLE!* {} *breaks through!*",
        "🎯 *GENIUS!* {} *outsmarts the batsman!*",
        "⚡ *SENSATIONAL!* {} *strikes gold!*",
        "🌟 *WHAT A BALL!* {} *does the trick!*",
        "💫 *BRILLIANT!* {} *gets the breakthrough!*",
        "🎱 *PERFECT DELIVERY!* {} *hits the target!*",
        "🎯 *MASTERCLASS!* {} *shows pure skill!*",
        "⭐ *SPECTACULAR!* {} *makes it happen!*",
        "🔮 *MAGICAL!* {} *weaves their magic!*",
        "💎 *PRECIOUS WICKET!* {} *strikes gold!*"
    ],
    'run_1': [
        "👌 *Good single by* {}",
        "🏃 *Quick running by* {}",
        "✨ *Smart cricket from* {}",
        "🎯 *Well placed by* {}",
        "🎯 *Precise placement by* {}",
        "💫 *Clever running by* {}",
        "🌟 *Good judgment by* {}",
        "⚡ *Sharp single from* {}",
        "✨ *Excellent awareness by* {}",
        "🎭 *Tactical single by* {}",
        "🎪 *Smart cricket from* {}",
        "🎯 *Perfect timing by* {}",
        "🌠 *Calculated single by* {}",
        "⭐ *Well executed by* {}"
    ],
    'run_2': [
        "🏃‍♂️ *Quick double by* {}",
        "⚡ *Good running between wickets by* {}",
        "💫 *Smart cricket from* {}",
        "🎯 *Well played by* {}",
        "🏃‍♂️ *Excellent running by* {}",
        "⚡ *Perfect coordination by* {}",
        "🎯 *Sharp doubles from* {}",
        "💫 *Brilliant running by* {}",
        "✨ *Quick between wickets by* {}",
        "🎭 *Great understanding shown by* {}",
        "🌟 *Perfect judgment by* {}",
        "⭐ *Aggressive running by* {}",
        "🎪 *Fantastic doubles from* {}",
        "🌠 *Professional running by* {}"
    ],
    'run_3': [
        "💪 *Excellent running by* {}",
        "🏃‍♂️ *Great effort for three by* {}",
        "⚡ *Aggressive running by* {}",
        "✨ *Brilliant running between wickets by* {}"
    ],
    'run_4': [
        "🏏 *FOUR!* *Beautiful shot by* {}",
        "⚡ *Cracking boundary by* {}",
        "🎯 *Elegant four from* {}",
        "💫 *Fantastic placement by* {}",
        "🎯 *Masterful boundary by* {}",
        "💫 *Textbook four from* {}",
        "🌟 *Classic boundary by* {}",
        "⚡ *Perfect placement by* {}",
        "✨ *Brilliant shot by* {}",
        "🎭 *Graceful four from* {}",
        "⭐ *Magnificent stroke by* {}",
        "🎪 *Expert placement by* {}",
        "🌠 *Superb timing by* {}",
        "💎 *Delightful boundary by* {}"
    ],
    'run_6': [
        "💥 *MASSIVE SIX!* {} *clears the ropes*",
        "🚀 *HUGE HIT!* {} *goes big*",
        "⚡ *MAXIMUM!* {} *shows the power*",
        "🎆 *SPECTACULAR!* {} *into the crowd*",
        "🎯 *PURE POWER!* {} *launches it*",
        "💫 *WHAT A HIT!* {} *goes downtown*",
        "🌟 *MASSIVE SIX!* {} *dominates*",
        "⚡ *INCREDIBLE!* {} *clears the roof*",
        "✨ *HUGE HIT!* {} *makes it look easy*",
        "🎭 *GIGANTIC!* {} *shows who's boss*",
        "⭐ *MAGNIFICENT!* {} *goes big again*",
        "🎪 *ENORMOUS!* {} *launches it miles*",
        "🌠 *SPECTACULAR!* {} *sends it flying*",
        "💎 *COLOSSAL!* {} *hits a monster*"
    ],
    'over_complete': [
        "🎯 *End of the over!* Time for a bowling change.",
        "⏱️ *That's the over completed!* Teams regrouping.",
        "🔄 *Over completed!* Players taking positions.",
        "📊 *Over finished!* Time for fresh tactics."
    ],
    'innings_end': [
        "🏁 *INNINGS COMPLETE!* What a performance!",
        "🎊 *That's the end of the innings!* Time to switch sides!",
        "🔚 *INNINGS OVER!* Get ready for the chase!",
        "📢 *And that concludes the innings!* Let's see what happens next!"
    ],
    'chase_complete': [
        "🏆 *GAME OVER!* The chase is successful!",
        "🎉 *Victory achieved!* What a chase!",
        "💫 *Target achieved!* Brilliant batting!",
        "🌟 *Chase completed!* Fantastic performance!"
    ],
    'chase_failed': [
        "🎯 *GAME OVER!* The chase falls short!",
        "🏁 *That's it!* Defense wins the day!",
        "🔚 *Chase unsuccessful!* What a bowling performance!",
        "📢 *All over!* The target proves too much!"
    ],
    'run_5': [
        "🔥 *FIVE RUNS!* *Smart cricket by* {}",
        "⭐ *Bonus run taken by* {}",
        "💫 *Extra run grabbed by* {}",
        "✨ *Quick thinking by* {}"
    ]
}

MATCH_SEPARATOR = "━━━━━━━━━━━━━━"

# Add near the top with other constants
AUTHORIZED_GROUPS = set()  # Store authorized group IDs
TEST_MODE = True  # Flag to enable/disable group restriction

# Add these new animation messages
ACTION_MESSAGES = {
    'batting': [
        "🏏 *Power Stance!* {} *takes guard*...",
        "⚡ *Perfect Balance!* {} *ready to face the delivery*...",
        "🎯 *Focused!* {} *watches the bowler intently*...",
        "💫 *Calculating!* {} *reads the field placement*...",
        "✨ *Ready!* {} *grips the bat tightly*...",
        "🎯 *Mental Focus!* {} *visualizes the shot*...",
        "💫 *Perfect Stance!* {} *looks determined*...",
        "🌟 *Battle Ready!* {} *adjusts the gloves*...",
        "⚡ *Game Face!* {} *taps the crease*...",
        "✨ *Pure Focus!* {} *eyes on the bowler*...",
        "🎭 *Alert Mode!* {} *checks the field*...",
        "⭐ *Lightning Ready!* {} *takes center*...",
        "🎪 *Warrior Mode!* {} *marks the guard*...",
        "🌠 *Zen Mode!* {} *deep breath*...",
        "💎 *Power Pose!* {} *ready to face*..."
    ],
    'bowling': [
        "🎯 *Strategic Setup!* {} *marks the run-up*...",
        "⚡ *Perfect Grip!* {} *adjusts the seam position*...",
        "💫 *Building Momentum!* {} *starts the approach*...",
        "🌟 *Full Steam!* {} *charging in to bowl*...",
        "🎭 *Masterful!* {} *about to release*...",
        "🎯 *Perfect Rhythm!* {} *measures the run-up*...",
        "💫 *Focused Attack!* {} *plans the delivery*...",
        "🌟 *Strategic Setup!* {} *checks the field*...",
        "⚡ *Power Stance!* {} *ready to strike*...",
        "✨ *Battle Ready!* {} *eyes the target*...",
        "🎭 *Precision Mode!* {} *adjusts the field*...",
        "⭐ *Strike Ready!* {} *locks the target*...",
        "🎪 *Perfect Setup!* {} *final check*...",
        "🌠 *Attack Mode!* {} *starts the run*...",
        "💎 *Lethal Mode!* {} *ready to deliver*..."
    ],
    'delivery': [
        "⚾ *RELEASE!* The ball flies through the air...",
        "🎯 *BOWLED!* The delivery is on its way...",
        "⚡ *PERFECT!* Beautiful release from the hand...",
        "💫 *INCOMING!* The ball curves through the air...",
        "✨ *BRILLIANT!* What a delivery this could be...",
        "🎯 *PRECISION!* What a delivery path...",
        "💫 *MASTERFUL!* The ball dances in air...",
        "🌟 *PERFECT!* Beautifully executed...",
        "⚡ *LIGHTNING!* The ball zips through...",
        "✨ *MAGICAL!* What a delivery this...",
        "🎭 *BRILLIANT!* The ball curves nicely...",
        "⭐ *STUNNING!* Perfect line and length...",
        "🎪 *AMAZING!* Great trajectory...",
        "🌠 *SUPERB!* The ball moves perfectly...",
        "💎 *CLINICAL!* Excellent execution..."
    ]
}
# Add near other constants
BROADCAST_DELAY = 1  # Delay between messages to avoid flood limits

# Add to constants section
REGISTERED_USERS = set()  # Store registered user IDs

# --- Helper Functions ---
def get_active_game_id(chat_id: int) -> str:
    """Get active game ID for a given chat"""
    for game_id, game in games.items():
        if game.get('chat_id') == chat_id:
            return game_id
    return None

async def check_button_cooldown(msg, user_id: str, text: str, keyboard=None) -> bool:
    """Check if user can click button again"""
    current_time = time.time()
    if user_id in user_last_click:
        try:
            if keyboard:
                return await msg.edit_text(
                    text,
                    reply_markup=keyboard
                )
            return await msg.edit_text(text)
        except Exception as e:
            logger.error(f"Failed to edit message: {e}")
            return None

async def recover_game_state(game_id: str, chat_id: int) -> bool:
    """Try to recover game state if possible"""
    try :
        if game_id in games:
            game = games[game_id]
            if 'status' not in game:
                game['status'] = 'config'
            if 'score' not in game:
                game['score'] = {'innings1': 0, 'innings2': 0}
            if 'wickets' not in game:
                game['wickets'] = 0
            if 'balls' not in game:
                game['balls'] = 0
            return True
        return False
    except Exception as e:
        logger.error(f"Error recovering game state: {e}")
        return False

def safe_split_callback(data: str, expected_parts: int = 3) -> tuple:
    """Safely split callback data and ensure correct number of parts"""
    parts = data.split('_', expected_parts - 1)
    if len(parts) != expected_parts:
        raise ValueError(f"Invalid callback data format: {data}")
    return tuple(parts)

async def show_error_message(query, message: str, show_alert: bool = True, delete_after: float = None):
    """Show error message to user with optional auto-delete"""
    try:
        await query.answer(message, show_alert=show_alert)
        if delete_after:
            await asyncio.sleep(delete_after)
    except Exception as e:
        logger.error(f"Error showing message: {e}")

def should_end_innings(game: dict) -> bool:
    """Check if innings should end based on wickets or overs"""
    max_wickets = game.get('max_wickets', float('inf'))
    max_overs = game.get('max_overs', float('inf'))
    
    return (
        (max_wickets != float('inf') and game['wickets'] >= max_wickets) or 
        (max_overs != float('inf') and game['balls'] >= max_overs * 6) or
        (game['current_innings'] == 2 and game['score']['innings2'] >= game.get('target', float('inf')))
    )

def store_first_innings(game: dict):
    """Store first innings details before resetting"""
    game['first_innings_wickets'] = game['wickets']
    game['first_innings_score'] = game['score']['innings1']
    game['first_innings_overs'] = f"{game['balls']//6}.{game['balls']%6}"
    game['target'] = game['score']['innings1'] + 1

def generate_match_summary(game: dict, current_score: int) -> dict:
    """Generate enhanced match summary"""
    try:
        match_id = escape_markdown_v2_custom(game.get('match_id', ''))
        date = escape_markdown_v2_custom(datetime.now().strftime('%d %b %Y'))
        team1 = escape_markdown_v2_custom(game['creator_name'])
        team2 = escape_markdown_v2_custom(game['joiner_name'])
        
        # First innings details
        first_batting = escape_markdown_v2_custom(game['creator_name'])
        first_score = game['first_innings_score']
        first_wickets = game['first_innings_wickets']
        first_overs = game['first_innings_overs']
        first_balls = game.get('first_innings_balls', 0)
        
        # Second innings details
        second_batting = escape_markdown_v2_custom(game['batsman_name'])
        
        # Calculate various statistics
        total_boundaries = game.get('first_innings_boundaries', 0) + game.get('second_innings_boundaries', 0)
        total_sixes = game.get('first_innings_sixes', 0) + game.get('second_innings_sixes', 0)
        dot_balls = game.get('dot_balls', 0)
        best_over = (0, 0)  # Default value
        if game.get('over_scores'):
            best_over = max(game.get('over_scores', {0: 0}).items(), key=lambda x: x[1])
        
        return {
            'match_id': match_id,
            'timestamp': date,
            'game_mode': game['mode'],
            'teams': {
                'batting_first': team1,
                'bowling_first': team2
            },
            'innings1': {
                'score': first_score,
                'wickets': first_wickets,
                'overs': first_overs,
                'run_rate': first_score / float(first_overs),
                'boundaries': game.get('first_innings_boundaries', 0),
                'sixes': game.get('first_innings_sixes', 0)
            },
            'innings2': {
                'score': current_score,
                'wickets': game['wickets'],
                'overs': f"{game['balls']//6}.{game['balls']%6}",
                'run_rate': current_score / (game['balls']/6) if game['balls'] > 0 else 0,
                'boundaries': game.get('second_innings_boundaries', 0),
                'sixes': game.get('second_innings_sixes', 0)
            },
            'stats': {
                'dot_balls': dot_balls,
                'total_boundaries': total_boundaries,
                'average_rr': safe_division(
                    first_score + current_score,
                    float(first_overs) + float(f"{game['balls']//6}.{game['balls']%6}")
                ),
                'best_over_runs': best_over[1],
                'best_over_number': best_over[0]
            },
            'winner_id': game['batsman'] if current_score >= game.get('target', float('inf')) else game['bowler'],
            'win_margin': calculate_win_margin(game, current_score),
            'match_result': format_match_result(game, current_score)
        }
    except Exception as e:
        logger.error(f"Error generating match summary: {e}")
        return {
            'stats': {
                'total_boundaries': 0,
                'total_sixes': 0,
                'average_rr': 0,
                'best_over_runs': 0,
                'best_over_number': 0
            }
        }

def calculate_win_margin(game: dict, current_score: int) -> str:
    """Calculate the margin of victory"""
    if game['current_innings'] == 2:
        if current_score >= game.get('target', float('inf')):
            return f"{game['max_wickets'] - game['wickets']} wickets"
        else:
            return f"{game['target'] - current_score - 1} runs"
    return ""

def format_match_result(game: dict, current_score: int) -> str:
    """Format the match result string with proper escaping"""
    if game['score']['innings1'] == game['score']['innings2']:
        return "*Match Drawn\\!*"
    
    winner_name = escape_markdown_v2_custom(
        game['batsman_name'] if current_score >= game.get('target', float('inf')) 
        else game['bowler_name']
    )
    margin = calculate_win_margin(game, current_score)
    return f"*{winner_name} won by {margin}\\!*"

# --- Database Handler Class ---
class DatabaseHandler:
    def __init__(self):
        self.pool = None
        self._init_pool()
        if not self._verify_tables():
            self._init_tables()
        self.load_registered_users()

    def load_registered_users(self):
        """Load registered users from database into memory"""
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
                logger.info(f"Loaded {len(REGISTERED_USERS)} registered users from database")
        except Exception as e:
            logger.error(f"Error loading registered users: {e}")
        finally:
            if conn:
                self.return_connection(conn)

    def _init_pool(self) -> bool:
        """Initialize connection pool with proper error handling"""
        try:
            if not all([DB_CONFIG['user'], DB_CONFIG['password'], DB_CONFIG['host']]):
                logger.error("Database configuration missing. Check your .env file")
                return False
                
            # Create connection pool with retry logic
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    self.pool = SimpleConnectionPool(
                        DB_POOL_MIN,
                        DB_POOL_MAX,
                        **DB_CONFIG
                    )
                    
                    # Test the connection
                    test_conn = self.pool.getconn()
                    with test_conn.cursor() as cur:
                        cur.execute('SELECT 1')
                    self.pool.putconn(test_conn)
                    
                    logger.info("Database pool created successfully")
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    logger.error(f"Connection attempt {retry_count} failed: {e}")
                    if retry_count < max_retries:
                        time.sleep(5)  # Wait 5 seconds before retrying
                        continue
                    break
                    
            logger.error("Failed to create connection pool after all retries")
            return False
            
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
                    
                    # Add to in-memory set
                    REGISTERED_USERS.add(str(telegram_id))
                    return True
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
                        (telegram_id, command, chat_type, success, error_message, created_at)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """, (telegram_id, command, chat_type, success, error_message))
                    conn.commit()
                    return True
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error logging command: {e}")
            return False

    async def save_match_async(self, match_data: dict) -> bool:
        """Async version of save match with proper error handling"""
        try:
            connection = self.get_connection()
            if not connection:
                return False

            match_summary = {
                'match_id': match_data.get('match_id'),
                'user_id': match_data.get('user_id'),
                'game_mode': match_data.get('mode', 'classic'),
                'timestamp': datetime.now().isoformat(),
                'teams': {
                    'team1': match_data.get('creator_name', ''),
                    'team2': match_data.get('joiner_name', '')
                },
                'innings': {
                    'first': match_data.get('first_innings_score', 0),
                    'second': match_data.get('score', {}).get('innings2', 0)
                },
                'result': match_data.get('result', ''),
                'stats': {
                    'boundaries': match_data.get('boundaries', 0),
                    'sixes': match_data.get('sixes', 0),
                    'dot_balls': match_data.get('dot_balls', 0),
                    'best_over': match_data.get('best_over', 0)
                }
            }

            with connection.cursor() as cur:
                # Ensure user exists first
                cur.execute("""
                    INSERT INTO users (telegram_id, first_name)
                    VALUES (%s, %s)
                    ON CONFLICT (telegram_id) DO NOTHING
                """, (match_data.get('user_id'), match_data.get('user_name', 'Unknown')))

                # Fixed INSERT statement that matches table structure
                cur.execute("""
                    INSERT INTO scorecards 
                    (match_id, user_id, game_mode, match_data)
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (match_id) 
                    DO UPDATE SET
                        match_data = EXCLUDED.match_data,
                        game_mode = EXCLUDED.game_mode
                """, (
                    match_summary['match_id'],
                    match_summary['user_id'],
                    match_summary['game_mode'],
                    json.dumps(match_summary)
                ))

                connection.commit()
                return True

        except Exception as e:
            logger.error(f"Database save error: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                self.return_connection(connection)

    def get_user_matches(self, user_id: str, limit: int = 10) -> list:
        """Get user's match history"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
                
            try:
                with conn.cursor() as cur:
                    # Use match_data instead of direct columns
                    cur.execute("""
                        SELECT 
                            match_id,
                            match_data->>'timestamp' as timestamp,
                            match_data->>'teams' as teams,
                            match_data->>'innings1' as innings1,
                            match_data->>'innings2' as innings2,
                            match_data->>'result' as result
                        FROM scorecards 
                        WHERE user_id = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (user_id, limit))
                    
                    matches = []
                    for row in cur.fetchall():
                        match_data = row[1] if row[1] else {}
                        matches.append({
                            'match_id': row[0],
                            'timestamp': row[1],
                            'teams': row[2],
                            'innings1': row[3],
                            'innings2': row[4],
                            'result': row[5]
                        })
                    return matches
            finally:
                self.return_connection(conn)
                
        except Exception as e:
            logger.error(f"Error getting user matches: {e}")
            return []
    def _init_tables(self) -> bool:
        """Initialize database tables only if they don't exist"""
        try:
            conn = self.get_connection()
            if not conn:
                return False

            with conn.cursor() as cur:
                # Check if tables exist first
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'users'
                    ), EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'scorecards'
                    );
                """)
                users_exist, scorecards_exist = cur.fetchone()

                # Only create tables if they don't exist
                if not users_exist:
                    logger.info("Creating users table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            telegram_id BIGINT PRIMARY KEY,
                            username VARCHAR(255),
                            first_name VARCHAR(255),
                            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            is_admin BOOLEAN DEFAULT FALSE,
                            is_banned BOOLEAN DEFAULT FALSE
                        )
                    """)

                if not scorecards_exist:
                    logger.info("Creating scorecards table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS scorecards (
                            id SERIAL PRIMARY KEY,
                            match_id VARCHAR(50) UNIQUE NOT NULL,
                            user_id BIGINT REFERENCES users(telegram_id),
                            game_mode VARCHAR(50),
                            match_data JSONB,
                            first_innings_score INTEGER DEFAULT 0,
                            first_innings_wickets INTEGER DEFAULT 0,
                            second_innings_score INTEGER DEFAULT 0,
                            second_innings_wickets INTEGER DEFAULT 0,
                            boundaries INTEGER DEFAULT 0,
                            sixes INTEGER DEFAULT 0,
                            dot_balls INTEGER DEFAULT 0,
                            best_over_score INTEGER DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            result TEXT
                        )
                    """)

                # Create other tables
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS match_stats (
                        id SERIAL PRIMARY KEY,
                        match_id VARCHAR(50) REFERENCES scorecards(match_id),
                        total_runs INTEGER DEFAULT 0,
                        total_wickets INTEGER DEFAULT 0,
                        total_overs DECIMAL(5,1) DEFAULT 0,
                        boundaries INTEGER DEFAULT 0,
                        sixes INTEGER DEFAULT 0,
                        dot_balls INTEGER DEFAULT 0,
                        best_over_score INTEGER DEFAULT 0,
                        required_run_rate DECIMAL(5,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS player_stats (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(telegram_id),
                        matches_played INTEGER DEFAULT 0,
                        matches_won INTEGER DEFAULT 0,
                        total_runs_scored INTEGER DEFAULT 0,
                        total_wickets_taken INTEGER DEFAULT 0,
                        highest_score INTEGER DEFAULT 0,
                        best_bowling VARCHAR(10),
                        total_boundaries INTEGER DEFAULT 0,
                        total_sixes INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS authorized_groups (
                        group_id BIGINT PRIMARY KEY,
                        group_name VARCHAR(255),
                        added_by BIGINT REFERENCES users(telegram_id),
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    );

                    CREATE TABLE IF NOT EXISTS bot_admins (
                        admin_id BIGINT PRIMARY KEY REFERENCES users(telegram_id),
                        added_by BIGINT REFERENCES users(telegram_id),
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_super_admin BOOLEAN DEFAULT FALSE
                    );

                    CREATE TABLE IF NOT EXISTS command_logs (
                        id SERIAL PRIMARY KEY,
                        telegram_id BIGINT REFERENCES users(telegram_id),
                        command VARCHAR(50),
                        chat_type VARCHAR(20),
                        success BOOLEAN DEFAULT TRUE,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS game_modes (
                        id SERIAL PRIMARY KEY,
                        mode_name VARCHAR(50) UNIQUE,
                        max_overs INTEGER,
                        max_wickets INTEGER,
                        description TEXT,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS achievements (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(telegram_id),
                        achievement_type VARCHAR(50),
                        achievement_data JSONB,
                        unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS bot_stats (
                        id SERIAL PRIMARY KEY,
                        total_users INTEGER DEFAULT 0,
                        total_games_played INTEGER DEFAULT 0,
                        active_games INTEGER DEFAULT 0,
                        total_commands_used INTEGER DEFAULT 0,
                        uptime_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE INDEX IF NOT EXISTS idx_scorecards_user_id ON scorecards(user_id);
                    CREATE INDEX IF NOT EXISTS idx_scorecards_match_id ON scorecards(match_id);
                    CREATE INDEX IF NOT EXISTS idx_match_stats_match_id ON match_stats(match_id);
                    CREATE INDEX IF NOT EXISTS idx_player_stats_user_id ON player_stats(user_id);
                    CREATE INDEX IF NOT EXISTS idx_command_logs_telegram_id ON command_logs(telegram_id);
                    CREATE INDEX IF NOT EXISTS idx_achievements_user_id ON achievements(user_id);
                """)

                conn.commit()
                logger.info("Database tables verified/created successfully")
                return True

        except Exception as e:
            logger.error(f"Error initializing database tables: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def save_match(self, match_data: dict) -> bool:
        """Save match with proper error handling"""
        try:
            conn = self.get_connection()
            if not conn:
                return False

            with conn.cursor() as cur:
                # Ensure user exists first
                cur.execute("""
                    INSERT INTO users (telegram_id, first_name)
                    VALUES (%s, %s)
                    ON CONFLICT (telegram_id) DO NOTHING
                """, (match_data['user_id'], match_data.get('user_name', 'Unknown')))

                # Fixed INSERT statement for regular save
                cur.execute("""
                    INSERT INTO scorecards 
                    (match_id, user_id, game_mode, match_data)
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (match_id) 
                    DO UPDATE SET
                        match_data = EXCLUDED.match_data,
                        game_mode = EXCLUDED.game_mode,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    match_data['match_id'],
                    match_data['user_id'],
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

    def _verify_tables(self) -> bool:
        """Check if required tables exist"""
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

    def get_player_stats(self, user_id: str) -> dict:
        """Get player statistics"""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
                
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT matches_played, matches_won, total_runs_scored, total_wickets_taken, 
                           highest_score, best_bowling, total_boundaries, total_sixes
                    FROM player_stats
                    WHERE user_id = %s
                """, (user_id,))
                
                stats = cur.fetchone()
                if stats:
                    return {
                        'matches_played': stats[0],
                        'matches_won': stats[1],
                        'total_runs_scored': stats[2],
                        'total_wickets_taken': stats[3],
                        'highest_score': stats[4],
                        'best_bowling': stats[5],
                        'total_boundaries': stats[6],
                        'total_sixes': stats[7]
                    }
                return {}
        except Exception as e:
            logger.error(f"Error getting player stats: {e}")
            return {}
        finally:
            if conn:
                self.return_connection(conn)

    def get_bot_stats(self) -> dict:
        """Get bot statistics"""
        try:
            conn = self.get_connection()
            if not conn:
                return {}
                
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_users, total_games_played, active_games, total_commands_used, uptime_start, last_updated
                    FROM bot_stats
                    ORDER BY last_updated DESC
                    LIMIT 1
                """)
                
                stats = cur.fetchone()
                if stats:
                    return {
                        'total_users': stats[0],
                        'total_games_played': stats[1],
                        'active_games': stats[2],
                        'total_commands_used': stats[3],
                        'uptime_start': stats[4],
                        'last_updated': stats[5]
                    }
                return {}
        except Exception as e:
            logger.error(f"Error getting bot stats: {e}")
            return {}
        finally:
            if conn:
                self.return_connection(conn)

    def get_authorized_groups(self) -> list:
        """Get all authorized groups"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
                
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT group_id, group_name, added_by, added_at, is_active
                    FROM authorized_groups
                    WHERE is_active = TRUE
                """)
                
                groups = cur.fetchall()
                return [
                    {
                        'group_id': group[0],
                        'group_name': group[1],
                        'added_by': group[2],
                        'added_at': group[3],
                        'is_active': group[4]
                    }
                    for group in groups
                ]
        except Exception as e:
            logger.error(f"Error getting authorized groups: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)

    def get_admins(self) -> list:
        """Get all bot admins"""
        try:
            conn = self.get_connection()
            if not conn:
                return []
                
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT admin_id, added_by, added_at, is_super_admin
                    FROM bot_admins
                """)
                
                admins = cur.fetchall()
                return [
                    {
                        'admin_id': admin[0],
                        'added_by': admin[1],
                        'added_at': admin[2],
                        'is_super_admin': admin[3]
                    }
                    for admin in admins
                ]
        except Exception as e:
            logger.error(f"Error getting admins: {e}")
            return []
        finally:
            if conn:
                self.return_connection(conn)

    def add_admin(self, admin_id: int, added_by: int, is_super_admin: bool = False) -> bool:
        """Add a new admin"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_admins (admin_id, added_by, is_super_admin)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (admin_id) DO NOTHING
                """, (admin_id, added_by, is_super_admin))
                
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding admin: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def remove_admin(self, admin_id: int) -> bool:
        """Remove an admin"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM bot_admins
                    WHERE admin_id = %s
                """, (admin_id,))
                
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error removing admin: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def add_group(self, group_id: int, group_name: str, added_by: int) -> bool:
        """Add a new authorized group"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO authorized_groups (group_id, group_name, added_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (group_id) DO NOTHING
                """, (group_id, group_name, added_by))
                
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error adding group: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

    def remove_group(self, group_id: int) -> bool:
        """Remove an authorized group"""
        try:
            conn = self.get_connection()
            if not conn:
                return False
                
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE authorized_groups
                    SET is_active = FALSE
                    WHERE group_id = %s
                """, (group_id,))
                
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error removing group: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                self.return_connection(conn)

# Update the database initialization to use telegram_id instead of user_id
def init_database():
    """Initialize database tables if they don't exist"""
    connection = get_db_connection()
    if not connection:
        logger.error("Could not connect to database for initialization")
        return False
        
    try:
        with connection.cursor() as cursor:
            # Read and execute the SQL file
            sql_file_path = Path(__file__).parent / "SETUP.sql"
            if not sql_file_path.exists():
                logger.error("setup_database.sql file not found")
                return False
                
            with open(sql_file_path, 'r') as sql_file:
                # Remove any comments and empty lines
                sql_commands = []
                for line in sql_file:
                    line = line.strip()
                    if line and not line.startsWith('--'):
                        sql_commands.append(line)
                
                # Join commands and split by semicolon
                sql_script = ' '.join(sql_commands)
                commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]
                
                # Execute each command separately
                for command in commands:
                    try:
                        cursor.execute(command)
                    except psycopg2.Error as e:
                        logger.error(f"Error executing SQL command: {e}")
                        logger.error(f"Failed command: {command}")
                        connection.rollback()
                        return False
                        
            connection.commit()
            logger.info("Database initialized successfully")
            return True
            
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            return_db_connection(connection)

# --- Game Commands ---
def is_registered(user_id: str) -> bool:
    """Check if user is registered"""
    return user_id in REGISTERED_USERS

async def gameon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info(f"User {update.effective_user.id} initiated game")
        db.log_command(
            telegram_id=update.effective_user.id,
            command="gameon",
            chat_type=update.effective_chat.type
        )
        user_id = str(update.effective_user.id)
        
        if not is_registered(user_id):
            await update.message.reply_text(
                escape_markdown_v2_custom(f"{UI_THEMES['accents']['error']} You need to register first!\nSend /start to me in private chat to register."),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        if update.effective_chat.type == ChatType.PRIVATE:
            await update.message.reply_text(
                escape_markdown_v2_custom(f"{UI_THEMES['accents']['error']} Please add me to a group to play!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        game_id = create_game(
            creator_id=str(update.effective_user.id),
            creator_name=update.effective_user.first_name,
            chat_id=update.effective_chat.id
        )

        # Use the UI themes for game mode selection
        modes_text = (
            f"{escape_markdown_v2_custom(UI_THEMES['primary']['separator'])}\n"
            f"🎮 *SELECT GAME MODE*\n"
            f"{escape_markdown_v2_custom(UI_THEMES['primary']['section_sep'])}\n\n"
        )

        for mode, details in GAME_MODES.items():
            modes_text += (
                f"{details['icon']} *{escape_markdown_v2_custom(details['title'])}*\n"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['bullet'])} " + 
                f"\n{escape_markdown_v2_custom(UI_THEMES['primary']['bullet'])} ".join(
                    escape_markdown_v2_custom(desc) for desc in details['description']
                ) +
                f"\n\n"
            )

        keyboard = [
            [InlineKeyboardButton(
                f"{GAME_MODES['classic']['icon']} Classic Mode", 
                callback_data=f"mode_{game_id}_classic"
            )],
            [InlineKeyboardButton(
                f"{GAME_MODES['quick']['icon']} Quick Mode", 
                callback_data=f"mode_{game_id}_quick"
            )],
            [InlineKeyboardButton(
                f"{GAME_MODES['survival']['icon']} Survival Mode", 
                callback_data=f"mode_{game_id}_survival"
            )]
        ]

        modes_text += escape_markdown_v2_custom(UI_THEMES['primary']['footer'])

        await update.message.reply_text(
            escape_markdown_v2_custom(modes_text),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        logger.info(f"Game {game_id} created by user {update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error in gameon: {e}")
        db.log_command(
            telegram_id=update.effective_user.id,
            command="gameon",
            chat_type=update.effective_chat.type,
            success=False,
            error_message=str(e)
        )
        raise

# --- Game State Management ---
# Add this new helper function near the top
def generate_match_id() -> str:
    """Generate a unique match ID"""
    timestamp = int(time.time())
    random_num = random.randint(1000, 9999)
    return f"M{timestamp}{random_num}"

# Update create_game function
def create_game(creator_id: str, creator_name: str, chat_id: int) -> str:
    """Create a new game with proper initialization"""
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
        'match_id': generate_match_id(),  # Add match_id when creating game
        'first_innings_boundaries': 0,
        'first_innings_sixes': 0,
        'second_innings_boundaries': 0,
        'second_innings_sixes': 0,
        'over_scores': {},
        'dot_balls': 0
    }
    return game_id

# --- Game Mechanics ---
# Update handle_mode for better classic mode setup
async def handle_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, mode = query.data.split('_')
        if game_id not in games:
            await query.edit_message_text(
                escape_markdown_v2_custom("❌ Game not found!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        game = games[game_id]
        game['mode'] = mode
        game['status'] = 'setup'
        game['current_innings'] = 1
        game['score'] = {'innings1': 0, 'innings2': 0}
        game['wickets'] = 0
        game['balls'] = 0
        game['this_over'] = []
        
        if mode == 'survival':
            game['max_wickets'] = 1
            game['max_overs'] = float('inf')
            keyboard = [[InlineKeyboardButton("🤝 Join Game", callback_data=f"join_{game_id}")]]
            mode_info = "🎯 Survival Mode (1 wicket)"
        elif mode == 'quick':
            game['max_wickets'] = float('inf')
            keyboard = get_overs_keyboard(game_id)
            mode_info = f"⚡ Quick Mode ({INFINITY_SYMBOL} wickets)"
        else:  # classic
            keyboard = get_wickets_keyboard(game_id)
            mode_info = "🏏 Classic Mode"
            
        mode_message = MESSAGE_STYLES['game_start'].format(
            ui=UI_THEMES['primary'],
            mode=game['mode'].title(),
            host=escape_markdown_v2_custom(game['creator_name'])
        )

        await query.edit_message_text(
            mode_message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Error in handle_mode: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

async def handle_wickets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, wickets = query.data.split('_')
        game = games[game_id]
        
        game['max_wickets'] = int(wickets)
        
        keyboard = [
            [
                InlineKeyboardButton("5 🎯", callback_data=f"overs_{game_id}_5"),
                InlineKeyboardButton("10 🎯", callback_data=f"overs_{game_id}_10"),
            ],
            [
                InlineKeyboardButton("15 🎯", callback_data=f"overs_{game_id}_15"),
                InlineKeyboardButton("20 🎯", callback_data=f"overs_{game_id}_20"),
            ],
            [InlineKeyboardButton("📝 Custom Overs", callback_data=f"custom_{game_id}_overs")]
        ]
        
        await query.edit_message_text(
            escape_markdown_v2_custom(
                f"*🏏 Classic Mode Setup*\n"
                f"{MATCH_SEPARATOR}\n"
                f"Current Settings:\n"
                f"• Wickets: {wickets}\n\n"
                f"Now select number of overs (1-50):"
            ),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_wickets: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

# Update handle_vers to support both regular and team matches
async def handle_vers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, match_id, overs = query.data.split('_')
        game = None
        
        # Check in both games and team_matches dictionaries
        if match_id in games:
            game = games[match_id]
            
        if not game:
            await query.edit_message_text(
                escape_markdown_v2_custom("❌ Match not found!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        game['max_overs'] = int(overs)
        if 'settings' in game:  # For team matches
            game['settings']['overs'] = int(overs)
            
        game['status'] = 'waiting'
        
        # Create appropriate keyboard based on game type
        keyboard = [[InlineKeyboardButton("🤝 Join Match", callback_data=f"join_{match_id}")]]
        message = (
            f"*🏏 Game Ready!*\n"
            f"{MATCH_SEPARATOR}\n"
            f"*Mode:* {game['mode'].title()}\n"
            f"*Wickets:* {game.get('max_wickets', '∞')}\n"
            f"*Overs:* {overs}\n"
            f"*Host:* {game['creator_name']}\n\n"
            f"*Waiting for opponent...*"
        )
        
        await query.edit_message_text(
            escape_markdown_v2_custom(message),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_vers: {e}")
        await handle_error(query, match_id if 'match_id' in locals() else None)

# Update handle_custom function with proper escaping
async def handle_custom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle custom input request with improved UI and proper escaping"""
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id, setting = query.data.split('_')
        game = games[game_id]
        
        context.user_data['awaiting_input'] = {
            'game_id': game_id,
            'setting': setting,
            'chat_id': query.message.chat_id,
            'message_id': query.message.message_id
        }
        
        setting_title = "OVERS" if setting == "overs" else "WICKETS"
        max_value = GAME_MODES[game['mode']]['max_overs'] if setting == "overs" else GAME_MODES[game['mode']]['max_wickets']
        
        if max_value == float('inf'):
            max_value = 50 if setting == "overs" else 10
        
        # Properly escape the message components
        separator = escape_markdown_v2_custom(UI_THEMES['primary']['separator'])
        section_sep = escape_markdown_v2_custom(UI_THEMES['primary']['section_sep'])
        footer = escape_markdown_v2_custom(UI_THEMES['primary']['footer'])
        mode_title = escape_markdown_v2_custom(game['mode'].title())
        
        message_text = (
            f"{separator}\n"
            f"📝 *{setting_title}*\n"
            f"{section_sep}\n\n"
            f"{UI_THEMES['accents']['alert']} Reply with a number *\\(1\\-{max_value}\\)*\n"
            f"{escape_markdown_v2_custom(UI_THEMES['primary']['bullet'])} Mode: *{mode_title}*\n"
            f"{footer}"
        )
        
        sent_msg = await query.message.edit_text(
            message_text,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        context.user_data['awaiting_input']['prompt_message_id'] = sent_msg.message_id
        logger.info(f"Custom {setting} input requested for game {game_id}")
        
    except Exception as e:
        logger.error(f"Error in handle_custom: {e}")
        error_msg = escape_markdown_v2_custom(
            f"{UI_THEMES['accents']['error']} An error occurred\\. Please try again\\."
        )
        await query.message.edit_text(
            error_msg,
            parse_mode=ParseMode.MARKDOWN_V2
        )

# Update handle_join function to properly store player names
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
            
        # Store full player details
        game['joiner'] = user_id
        game['joiner_name'] = query.from_user.first_name
        if query.from_user.username:
            game['joiner_username'] = query.from_user.username
            
        game['status'] = 'toss'
        
        # Store initial game state
        game.update({
            'current_innings': 1,
            'score': {'innings1': 0, 'innings2': 0},
            'wickets': 0,
            'balls': 0,
            'boundaries': 0,
            'sixes': 0,
            'this_over': []
        })
        
        keyboard = [
            [
                InlineKeyboardButton("ODD", callback_data=f"toss_{game_id}_odd"),
                InlineKeyboardButton("EVEN", callback_data=f"toss_{game_id}_even")
            ]
        ]
        
        # Set who should choose odd/even
        game['choosing_player'] = game['joiner']
        game['choosing_player_name'] = game['joiner_name']
        
        await query.edit_message_text(escape_markdown_v2_custom(
            f"*🏏 Game Starting!*\n"  # Escaped exclamation mark
            f"{escape_markdown_v2_custom(MATCH_SEPARATOR)}\n"
            f"*Players:*\n"
            f"• Host: {escape_markdown_v2_custom(game['creator_name'])}\n"
            f"• Joined: {escape_markdown_v2_custom(game['joiner_name'])}\n\n"
            f"🎲 {escape_markdown_v2_custom(game['joiner_name'])}, choose ODD or EVEN!"),  # Escaped exclamation mark
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_join: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

async def handle_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, choice = query.data.split('_')
        game = games[game_id]
        
        if user_id != game['toss_winner']:
            await query.answer("❌ Only toss winner can choose\\!", show_alert=True)
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
            f"*🏏 Match Starting!*\n"
            f"{MATCH_SEPARATOR}\n"
            f"{game['toss_winner_name']} chose to {choice} first\n\n"
            f"🎮 {game['batsman_name']}'s turn to bat!",
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_choice: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

# Update handle_bat function to match working version from Backup.py
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
            await query.answer(f"❌ Not your turn! It's {escape_markdown_v2_custom(game['batsman_name'])}'s turn to bat!", show_alert=True)
            return
        
        await query.answer()
        
        game['batsman_choice'] = runs
        
        keyboard = get_bowling_keyboard(game_id)
        
        innings_key = f'innings{game["current_innings"]}'  
        score = game['score'][innings_key]  
        
        # Use random batting message with player name
        game_status = MESSAGE_STYLES['match_status'].format(
            ui=UI_THEMES['primary'],
            score=score,
            wickets=game['wickets'],
            overs=game['balls']//6,
            balls=game['balls']%6,
            run_rate=safe_division(score, (game['balls']/6)),
            target_info=get_target_info(game) if game['current_innings'] == 2 else "",
            commentary=random.choice(ACTION_MESSAGES['batting']).format(
                escape_markdown_v2_custom(game['batsman_name'])
            )
        )

        await safe_edit_message(
            query.message,
            game_status,
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_bat: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

# Update handle_bowl function to match working version from Backup.py
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
        
        if 'this_over' not in game:
            game['this_over'] = []
            
        current_score = game['score'][f'innings{game["current_innings"]}']
        
        if user_id != game['bowler']:
            await query.answer(f"❌ Not your turn! It's {escape_markdown_v2_custom(game['bowler_name'])}'s turn to bowl!", show_alert=True)
            return
            
        await query.answer()
        
        runs = game['batsman_choice']

        # Determine result text early
        if bowl_num == runs:
            result_text = random.choice(COMMENTARY_PHRASES['wicket']).format(f"*{escape_markdown_v2_custom(game['bowler_name'])}*")
        else:
            if runs == 4:
                result_text = random.choice(COMMENTARY_PHRASES['run_4']).format(f"*{escape_markdown_v2_custom(game['batsman_name'])}*")
            elif runs == 6:
                result_text = random.choice(COMMENTARY_PHRASES['run_6']).format(f"*{escape_markdown_v2_custom(game['batsman_name'])}*")
            else:
                result_text = random.choice(COMMENTARY_PHRASES[f'run_{runs}']).format(f"*{escape_markdown_v2_custom(game['batsman_name'])}*")
        
        # Use random bowling message with player name
        bowling_msg = random.choice(ACTION_MESSAGES['bowling']).format(escape_markdown_v2_custom(game['bowler_name']))
        await safe_edit_message(query.message, bowling_msg)
        await asyncio.sleep(1)

        # Add delivery message
        delivery_msg = random.choice(ACTION_MESSAGES['delivery'])
        await safe_edit_message(query.message, delivery_msg)
        await asyncio.sleep(1)
        
        if bowl_num == runs:
            game['wickets'] += 1
            commentary = result_text
            game['this_over'].append('W')
            if should_end_innings(game):
                game['this_over'] = []
        else:
            game['score'][f'innings{game["current_innings"]}'] += runs
            current_score = game['score'][f'innings{game["current_innings"]}']
            game['this_over'].append(str(runs))
            if runs == 4:
                if game['current_innings'] == 1:
                    game['first_innings_boundaries'] = game.get('first_innings_boundaries', 0) + 1
                else:
                    game['second_innings_boundaries'] = game.get('second_innings_boundaries', 0) + 1
            elif runs == 6:
                if game['current_innings'] == 1:
                    game['first_innings_sixes'] = game.get('first_innings_sixes', 0) + 1
                else:
                    game['second_innings_sixes'] = game.get('second_innings_sixes', 0) + 1

            # Track current over score
            current_over = game['balls'] // 6
            if 'over_scores' not in game:
                game['over_scores'] = {}
            game['over_scores'][current_over] = game['over_scores'].get(current_over, 0) + runs
            commentary = result_text

        game['balls'] += 1
        
        if game['balls'] % 6 == 0:
            over_commentary = random.choice(COMMENTARY_PHRASES['over_complete'])
            current_over = ' '.join(game['this_over'])
            game['this_over'] = []
            
            await safe_edit_message(
                query.message,
                f"*🏏 Over Complete!*\n"
                f"{MATCH_SEPARATOR}\n"
                f"Score: {current_score}/{game['wickets']}\n"
                f"Last Over: {current_over}\n\n"
                f"{over_commentary}\n\n"
                f"*Taking a short break between overs...*",
                keyboard=None
            )
            await asyncio.sleep(OVER_BREAK_DELAY)
        else:
            over_commentary = ""

        current_score = game['score'][f'innings{game["current_innings"]}']
        
        if should_end_innings(game):
            if game['current_innings'] == 1:
                await handle_innings_change(query.message, game, game_id)
                return
            else:
                is_chase_successful = current_score >= game.get('target', float('inf'))
                await handle_game_end(query, game, current_score, is_chase_successful)
                return

        keyboard = get_batting_keyboard(game_id)
        
        status_text = (
            f"🏏 Over {game['balls']//6}.{game['balls']%6}\n"
            f"{MATCH_SEPARATOR}\n"
            f"*Score:* {current_score}/{game['wickets']}\n"
            f"*Batsman played: *{runs} | *Bowler bowled: *{bowl_num}\n\n"
            f"{commentary}\n"
            f"{over_commentary}\n\n"
            f"*This Over: {' '.join(game['this_over'])}*\n\n"
            f"🎮 {escape_markdown_v2_custom(game['batsman_name'])}'s turn to bat!"
        )
        
        if game['current_innings'] == 2:
            runs_needed = game['target'] - current_score
            balls_left = (game['max_overs'] * 6) - game['balls']
            if balls_left > 0:
                required_rate = (runs_needed * 6) / max(balls_left, 1)  # Prevent division by zero
                status_text += f"\nNeed {runs_needed} from {balls_left} balls (RRR: {required_rate:.2f})"
        
        await safe_edit_message(
            query.message,
            status_text,
            keyboard=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error in handle_bowl: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

# Update handle_innings_change to use MESSAGE_STYLES
async def handle_innings_change(msg, game: dict, game_id: str):
    store_first_innings(game)
    game['current_innings'] = 2
    game['wickets'] = 0
    game['balls'] = 0
    game['this_over'] = []
    
    temp_batsman = game['batsman']
    temp_batsman_name = game['batsman_name']
    
    game['batsman'] = game['bowler']
    game['batsman_name'] = game['bowler_name']
    game['bowler'] = temp_batsman
    game['bowler_name'] = temp_batsman_name
    
    innings_text = MESSAGE_STYLES['innings_complete'].format(
        ui=UI_THEMES['primary'],
        score=game['first_innings_score'],
        wickets=game['first_innings_wickets'],
        overs=game['first_innings_overs'],
        run_rate=safe_division(game['first_innings_score'], float(game['first_innings_overs'].split('.')[0])),
        target=game['target']
    )

    await safe_edit_message(msg, innings_text,
        keyboard=InlineKeyboardMarkup(get_batting_keyboard(game_id)))

# Update handle_game_end to format match summary properly
async def handle_game_end(query, game: dict, current_score: int, is_chase_successful: bool):
    """Handle game end with improved match summary format"""
    try:
        match_id = game.get('match_id', f"M{random.randint(1000, 9999)}")
        date = datetime.now().strftime('%d %b %Y')

        # Calculate innings stats safely
        first_innings_overs = f"{game.get('first_innings_balls', 0)//6}.{game.get('first_innings_balls', 0)%6}"
        second_innings_overs = f"{game['balls']//6}.{game['balls']%6}"
        
        # Calculate boundaries and sixes for both innings
        first_innings_boundaries = game.get('first_innings_boundaries', 0)
        first_innings_sixes = game.get('first_innings_sixes', 0)
        second_innings_boundaries = game.get('second_innings_boundaries', 0)
        second_innings_sixes = game.get('second_innings_sixes', 0)
        
        total_boundaries = first_innings_boundaries + second_innings_boundaries
        total_sixes = first_innings_sixes + second_innings_sixes

        # Calculate run rates safely
        first_innings_rr = safe_division(game['first_innings_score'], game.get('first_innings_balls', 0)/6, 0)
        second_innings_rr = safe_division(current_score, game['balls']/6, 0)
        avg_rr = (first_innings_rr + second_innings_rr) / 2

        # Find best over score
        best_over_score = max(game.get('over_scores', {0: 0}).values(), default=0)
        
        # Determine result
        if is_chase_successful:
            wickets_left = game['max_wickets'] - game['wickets']
            result = f"*🎉 {game['batsman_name']} won by {wickets_left} wickets! *🏆"
        else:
            runs_short = game['target'] - current_score - 1
            result = f"*🎉 {game['bowler_name']} won by {runs_short} runs! 🏆*"

        # Format with proper line breaks and separators
        final_message = (
            f"*🏏 MATCH COMPLETE* #{match_id}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 {game['mode'].upper()} MODE | {date}\n\n"
            f"*👥 TEAM LINEUPS*\n"
            f"🔵 {game['creator_name']} (Batting First)\n"
            f"🔴 {game['joiner_name']} (Bowling First)\n\n"
            f"📝* SCORECARD*\n"
            f"┌─* INNINGS 1*\n"
            f"│ {game['first_innings_score']}/{game['first_innings_wickets']} ({first_innings_overs})\n"
            f"│ 📈* RR: *{first_innings_rr:.2f}\n"
            f"│ 🎯* 4s: *{first_innings_boundaries} | 💥* 6s: *{first_innings_sixes}\n"
            f"└─ *Total Runs: *{game['first_innings_score']}\n\n"
            f"┌─ *INNINGS 2*\n"
            f"│ {current_score}/{game['wickets']} ({second_innings_overs})\n"
            f"│ 📈 *RR: *{second_innings_rr:.2f}\n"
            f"│ 🎯* 4s: *{second_innings_boundaries} | 💥* 6s: *{second_innings_sixes}\n"
            f"└─ *Total Runs: *{current_score}\n\n"
            f"📊* MATCH STATS*\n"
            f"• 📈* Average RR: *{avg_rr:.2f}\n"
            f"• ⭕* Dot Balls: *{game.get('dot_balls', 0)}\n"
            f"• 🎯* Total Boundaries: *{total_boundaries}\n"
            f"• 💥* Total Sixes: *{total_sixes}\n"
            f"• ⚡* Best Over: *{best_over_score} runs\n\n"
            f"🏆* RESULT*\n"
            f"*{result}*"
        )

        # Send message with proper escaping
        await query.message.edit_text(
            escape_markdown_v2_custom(final_message),
            parse_mode=ParseMode.MARKDOWN_V2
        )

        # Update the match data to include the new stats
        match_data = {
            'match_id': match_id,
            'date': date,
            'mode': game['mode'],
            'teams': {
                'team1': game['creator_name'],
                'team2': game['joiner_name']
            },
            'innings1': {
                'score': game['first_innings_score'],
                'wickets': game['first_innings_wickets'],
                'overs': first_innings_overs,
                'run_rate': first_innings_rr,
                'boundaries': first_innings_boundaries,
                'sixes': first_innings_sixes
            },
            'innings2': {
                'score': current_score,
                'wickets': game['wickets'],
                'overs': second_innings_overs,
                'run_rate': second_innings_rr,
                'boundaries': second_innings_boundaries,
                'sixes': second_innings_sixes
            },
            'stats': {
                'total_boundaries': total_boundaries,
                'total_sixes': total_sixes,
                'best_over': best_over_score,
                'dot_balls': game.get('dot_balls', 0),
                'average_rr': avg_rr
            },
            'result': result
        }

        # Save match data
        try:
            await db.save_match_async(match_data)
        except Exception as save_error:
            logger.error(f"Error saving match: {save_error}")
            save_to_file(match_data)

        # Cleanup game state
        if str(game['chat_id']) in games:
            del games[str(game['chat_id'])]

    except Exception as e:
        logger.error(f"Error in handle_game_end: {e}", exc_info=True)
        # Send simplified error message if formatting fails
        await query.message.edit_text(
            "🏏 *Game Complete!*\n\n"
            f"• {result}\n\n"
            "❗ Some details couldn't be displayed\n"
            "Use /save to save this match",
            parse_mode=ParseMode.MARKDOWN_V2
        )

# --- Admin Commands ---
async def add_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /addgroup <group_id>")
        return
    
    AUTHORIZED_GROUPS.add(int(context.args[0]))
    await update.message.reply_text("✅ Group added to authorized list")

async def remove_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a group from authorized list with improved error handling"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not context.args:
        await update.message.reply_text(
            escape_markdown_v2_custom("*Usage:* /removegroup <group_id>"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    try:
        group_id = int(context.args[0])
        if group_id not in AUTHORIZED_GROUPS:
            await update.message.reply_text(
                escape_markdown_v2_custom("❌ Group not found in authorized list!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        AUTHORIZED_GROUPS.remove(group_id)
        await update.message.reply_text(
            escape_markdown_v2_custom("✅ Group removed successfully!"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except ValueError:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Invalid group ID format!"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Error removing group: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Error removing group!"),
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def toggle_test_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
    
    global TEST_MODE
    TEST_MODE = not TEST_MODE
    status = "enabled" if TEST_MODE else "disabled"
    await update.message.reply_text(f"✅ Test mode {status}")

# Merge broadcast_message() and broadcast() into one function
async def get_bot_members() -> dict:
    """Get all users and groups where bot is present"""
    try:
        members = {
            'users': set(),  # Store user IDs and names
            'groups': set(), # Store group IDs and names
            'details': {}    # Store detailed info about users/groups
        }
        
        # Get users from database
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cur:
                # Get registered users
                cur.execute("""
                    SELECT telegram_id, username, first_name 
                    FROM users
                    WHERE is_banned = FALSE
                """)
                for user in cur.fetchall():
                    members['users'].add(user[0])
                    members['details'][user[0]] = {
                        'type': 'user',
                        'username': user[1],
                        'name': user[2],
                        'link': f"@{user[1]}" if user[1] else f"tg://user?id={user[0]}"
                    }
                
                # Get authorized groups
                cur.execute("""
                    SELECT group_id, group_name
                    FROM authorized_groups 
                    WHERE is_active = TRUE
                """)
                for group in cur.fetchall():
                    members['groups'].add(group[0])
                    members['details'][group[0]] = {
                        'type': 'group',
                        'name': group[1],
                        'link': f"tg://group?id={group[0]}"
                    }
                    
        return members
    except Exception as e:
        logger.error(f"Error getting bot members: {e}")
        return {'users': set(), 'groups': set(), 'details': {}}

# Update broadcast function to use new member list
async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast a tagged message to all users and groups"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return

    # Check if message is tagged/replied to
    if not update.message.reply_to_message:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Please reply to a message to broadcast it"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    status_msg = await update.message.reply_text("📢 Broadcasting message...")
    msg = update.message.reply_to_message
    success_count = 0
    fail_count = 0

    try:
        # Get all users from database
        conn = get_db_connection()
        if not conn:
            await status_msg.edit_text("❌ Database connection failed")
            return

        with conn.cursor() as cur:
            cur.execute("SELECT telegram_id FROM users WHERE is_banned = FALSE")
            users = [row[0] for row in cur.fetchall()]

            # Broadcast to each user
            for user_id in users:
                try:
                    await context.bot.copy_message(
                        chat_id=user_id,
                        from_chat_id=update.effective_chat.id,
                        message_id=msg.message_id
                    )
                    success_count += 1
                    await asyncio.sleep(0.05)  # Rate limiting
                except Exception as e:
                    logger.error(f"Broadcast failed for {user_id}: {e}")
                    fail_count += 1

        # Send broadcast report
        report = (
            f"📊 *Broadcast Complete*\n"
            f"{MATCH_SEPARATOR}\n"
            f"✅ Success: {success_count}\n"
            f"❌ Failed: {fail_count}\n"
        )
        
        await status_msg.edit_text(
            escape_markdown_v2_custom(report),
            parse_mode=ParseMode.MARKDOWN_V2
        )

    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await status_msg.edit_text(
            escape_markdown_v2_custom("❌ Broadcast failed. Please try again."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    finally:
        if conn:
            return_db_connection(conn)

async def bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
    
    active_games = len(games)
    unique_users = set()
    for game in games.values():
        unique_users.add(game['creator'])
        if 'joiner' in game:
            unique_users.add(game['joiner'])
    
    stats = (
        f"📊 *Bot Statistics*\n"
        f"{MATCH_SEPARATOR}\n\n"
        f"👥 Total Users: {len(REGISTERED_USERS)}\n"
        f"🎮 Active Games: {active_games}\n"
        f"🎯 Current Players: {len(unique_users)}\n"
        f"💾 Saved Matches: {len(in_memory_scorecards)}\n"
        f"👥 Authorized Groups: {len(AUTHORIZED_GROUPS)}\n"
        f"🔐 Test Mode: {'Enabled' if TEST_MODE else 'Disabled'}\n\n"
        f"_Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
    )
    
    await update.message.reply_text(
        escape_markdown_v2_custom(stats),
        parse_mode=ParseMode.MARKDOWN_V2
    )

# Add persistent data storage
class DataManager:
    def __init__(self):
        self.users: DefaultDict[str, dict] = defaultdict(dict)
        self.games: DefaultDict[str, dict] = defaultdict(dict)
        self.load_data()

    async def save_data(self):
        """Save data to files asynchronously"""
        async with aiofiles.open(USER_DATA_FILE, 'w') as f:
            await f.write(json.dumps(self.users, default=str))
        async with aiofiles.open(GAME_DATA_FILE, 'w') as f:
            await f.write(json.dumps(self.games, default=str))

    def load_data(self):
        """Load data from files"""
        try:
            if (USER_DATA_FILE.exists()):
                with open(USER_DATA_FILE, 'r') as f:
                    self.users.update(json.load(f))
            if (GAME_DATA_FILE.exists()):
                with open(GAME_DATA_FILE, 'r') as f:
                    self.games.update(json.load(f))
        except Exception as e:
            logger.error(f"Error loading data: {e}")

    async def register_user(self, user_id: str, user_data: dict):
        """Register or update user data"""
        self.users[user_id].update(user_data)
        self.users[user_id]['last_active'] = datetime.now().isoformat()
        await self.save_data()

# Initialize data manager
data_manager = DataManager()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    if update.effective_chat.type != ChatType.PRIVATE:
        await update.message.reply_text(
            escape_markdown_v2_custom(
                "❌ Please start me in private chat to register!\n"
                "Click here: @Cric_Saga_Bot"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    # Show initial message
    msg = await update.message.reply_text("🎮 Setting up your account...")

    try:
        # Try database registration first
        if db and not USE_FILE_STORAGE:
            success = db.register_user(
                telegram_id=user.id,
                username=user.username,
                first_name=user.first_name
            )
        else:
            # Fallback to in-memory registration
            success = True
            REGISTERED_USERS.add(str(user.id))
            # Also save to file
            user_data = {
                'telegram_id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'registered_at': datetime.now().isoformat()
            }
            await data_manager.register_user(str(user.id), user_data)
        
        if success:
            REGISTERED_USERS.add(str(user.id))
            await msg.edit_text(
                escape_markdown_v2_custom(
                    f"*🏏 Welcome to Cricket Bot, {user.first_name}!*🏏\n\n"
                    "*✅ Registration Complete!*👍\n\n"
                    "*📌 Quick Guide:*📚\n"
                    "🏏 /gameon - Start a new match\n"
                    "📊 /scorecards - View match history\n"
                    "❓ /help - View detailed commands\n\n"
                    "🎮 Join any group and type /gameon to play!👋"
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            raise Exception("Registration failed")
            
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        # Add user to in-memory storage as fallback
        REGISTERED_USERS.add(str(user.id))
        await msg.edit_text(
            escape_markdown_v2_custom(
                f"*⚠️ Welcome, {user.first_name}!*👋\n\n"
                "*Registration partially completed.*🤔\n"
                "*Some features may be limited.*🚫\n\n"
                "*📌 Available Commands:*📚\n"
                "🏏 /gameon - Start a new match\n"
                "❓ /help - View commands"
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )


# --- Admin Functions ---
async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new admin user"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not context.args:
        await update.message.reply_text("Usage: /addadmin <user_id>")
        return
        
    BOT_ADMINS.add(context.args[0])
    await update.message.reply_text("✅ Admin added successfully")


async def stop_games(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop all active games"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    games.clear()
    await update.message.reply_text(escape_markdown_v2_custom("*🛑 All games stopped*"))

# --- Scorecard Functions ---
# Add save_match function improvements
async def save_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save match result with custom name"""
    if not update.message.reply_to_message:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Please reply to a match result message with /save <match_name>"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    try:
        # Get custom name from args or generate default
        match_name = ' '.join(context.args) if context.args else f"Match_{int(time.time())}"
        match_result = update.message.reply_to_message.text
        
        # Validate match result format
        if not match_result or "MATCH COMPLETE" not in match_result:
            await update.message.reply_text(
                escape_markdown_v2_custom(
                    "❌ Invalid match result!\n"
                    "Please reply to a valid match result message\n"
                    "Usage: Reply to result + /save <optional_name>"
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        # Validate match name length and characters
        if len(match_name) > 100:
            await update.message.reply_text(
                escape_markdown_v2_custom("❌ Match name too long! Maximum 100 characters."),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        # Remove special characters from match name
        match_name = re.sub(r'[^a-zA-Z0-9\s_-]', '', match_name)
        if not match_name:
            match_name = f"Match_{int(time.time())}"

        # Parse match result and create match data
        match_data = {
            'match_id': f"M{int(time.time())}_{random.randint(1000, 9999)}",
            'user_id': update.effective_user.id,
            'match_name': match_name,  # Add custom name
            'game_mode': 'classic',
            'timestamp': datetime.now().isoformat(),
            'match_data': json.dumps({
                'full_text': match_result,
                'saved_at': datetime.now().isoformat(),
                'saved_by': update.effective_user.id,
                'match_name': match_name  # Include in match data
            })
        }

        # Try database save first
        success_db = False
        try:
            connection = get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    # First ensure user exists
                    cursor.execute("""
                        INSERT INTO users (telegram_id, first_name)
                        VALUES (%s, %s)
                        ON CONFLICT (telegram_id) DO NOTHING
                    """, (update.effective_user.id, update.effective_user.first_name))

                    # Save match with custom name
                    cursor.execute("""
                        INSERT INTO scorecards 
                        (match_id, user_id, match_name, match_data, created_at)
                        VALUES (%s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
                        RETURNING match_id
                    """, (
                        match_data['match_id'],
                        match_data['user_id'],
                        match_data['match_name'],
                        match_data['match_data']
                    ))
                    
                    connection.commit()
                    success_db = True
                    
        except Exception as e:
            logger.error(f"Database save error: {e}")
            if connection:
                connection.rollback()
        finally:
            if connection:
                return_db_connection(connection)

        # Try file save as backup
        success_file = False
        try:
            DATA_DIR.mkdir(exist_ok=True)
            
            # Load existing data
            existing_data = []
            if MATCH_HISTORY_FILE.exists():
                with open(MATCH_HISTORY_FILE, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            
            # Add new match data
            existing_data.append(match_data)
            
            # Save updated data
            with open(MATCH_HISTORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, default=str)
            
            success_file = True
            
        except Exception as e:
            logger.error(f"File save error: {e}")
            success_file = False

        if success_db or success_file:
            storage_type = "Database" if success_db else "Backup file"
            await update.message.reply_text(
                escape_markdown_v2_custom(
                    f"*✅ Match saved successfully as:*\n"
                    f"*Name:* {match_name}\n"
                    f"*Storage:* {storage_type}\n"
                    "*View your matches with /scorecard*"
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await update.message.reply_text(
                escape_markdown_v2_custom("❌ Failed to save match. Please try again."),
                parse_mode=ParseMode.MARKDOWN_V2
            )

    except Exception as e:
        logger.error(f"Error in save_match: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Error saving match. Please try again."),
            parse_mode=ParseMode.MARKDOWN_V2
        )

# Update view_scorecards query to show custom names
async def view_scorecards(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's match history with custom names"""
    user_id = str(update.effective_user.id)
    
    try:
        conn = get_db_connection()
        if not conn:
            return []
            
        with conn.cursor() as cur:
            # Updated query to include match_name
            cur.execute("""
                SELECT 
                    match_id,
                    created_at,
                    match_data,
                    match_name
                FROM scorecards 
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 10
            """, (user_id,))
            
            matches = cur.fetchall()
            
        if not matches:
            message_text = escape_markdown_v2_custom("❌ No saved matches found!")
            if update.callback_query:
                await update.callback_query.message.edit_text(
                    message_text,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            else:
                await update.message.reply_text(
                    message_text,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            return

        # Create paginated keyboard
        keyboard = []
        matches_per_page = 5
        total_pages = (len(matches) + matches_per_page - 1) // matches_per_page
        current_page = context.user_data.get('scorecard_page', 0)
        
        start_idx = current_page * matches_per_page
        end_idx = start_idx + matches_per_page
        page_matches = matches[start_idx:end_idx]
        
        for match in page_matches:
            match_id = match[0]
            created_at = match[1]
            match_data = match[2]
            saved_name = match[3]  # Get the saved match name
            
            # Format date
            match_date = created_at.strftime('%d/%m/%Y')
            
            # Get match name - first try saved name, then from match data, then fallback
            match_name = saved_name
            if not match_name and isinstance(match_data, dict):
                match_name = match_data.get('match_name')
            if not match_name and match_data and isinstance(match_data, str):
                try:
                    match_data_dict = json.loads(match_data)
                    match_name = match_data_dict.get('match_name')
                except:
                    pass
            if not match_name:
                match_name = f"Match #{match_id}"
            
            # Create button with match name
            keyboard.append([
                InlineKeyboardButton(
                    f"📅 {match_date} - {match_name}",
                    callback_data=f"view_{match_id}"
                )
            ])

        # Add navigation buttons
        nav_buttons = []
        if current_page > 0:
            nav_buttons.append(
                InlineKeyboardButton("⬅️ Previous", callback_data="page_prev")
            )
        if current_page < total_pages - 1:
            nav_buttons.append(
                InlineKeyboardButton("➡️ Next", callback_data="page_next")
            )
        if nav_buttons:
            keyboard.append(nav_buttons)

        # Send or edit message
        message_text = escape_markdown_v2_custom(
            f"*🏏 Match History Page {current_page + 1}/{total_pages}*\n"
            f"{MATCH_SEPARATOR}\n"
            f"*Select a match to view details:*"
        )
        
        if update.callback_query:
            await update.callback_query.message.edit_text(
                message_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await update.message.reply_text(
                message_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            
    except Exception as e:
        logger.error(f"Error in view_scorecards: {e}")
        error_text = escape_markdown_v2_custom("❌ Error loading matches. Please try again.")
        if update.callback_query:
            await update.callback_query.message.edit_text(
                error_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            await update.message.reply_text(
                error_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
    finally:
        if conn:
            return_db_connection(conn)

# Add new function to delete match
async def delete_match(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a match from both database and file storage"""
    query = update.callback_query
    await query.answer()
    
    _, match_id, _2 = query.data.split('_')
    match_id = match_id + '_'+_2
    user_id = str(query.from_user.id)
    
    try:
        # Delete from database
        success_db = False
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM scorecards 
                        WHERE match_id = %s AND user_id = %s
                        RETURNING match_id
                    """, (match_id, user_id))
                    success_db = cur.fetchone() is not None
                    conn.commit()
            
            success_db = True
        except Error as e:
            logger.error(f"Database delete error: {e}")

        # Delete from file storage
        success_file = False
        try:
            if MATCH_HISTORY_FILE.exists():
                with open(MATCH_HISTORY_FILE, 'r', encoding='utf-8') as f:
                    matches = json.load(f)
                
                # Filter out the match to delete
                matches = [m for m in matches if not (
                    m['match_id'] == match_id and str(m['user_id']) == user_id
                )]
                
                with open(MATCH_HISTORY_FILE, 'w', encoding='utf-8') as f:
                    json.dump(matches, f, indent=2)
                success_file = True
                success_file = True
        except Exception as e:
            logger.error(f"File delete error: {e}")

        if success_db or success_file:
            # Show success message before refreshing list
            await query.message.edit_text(
                escape_markdown_v2_custom(
                    "*✅ Match deleted successfully*!👍\n"
                    "*Refreshing list*...."
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            await asyncio.sleep(1)  # Short delay for user feedback
            
            # Refresh the scorecards view
            await view_scorecards(update, context)
        else:
            await query.message.edit_text(
                escape_markdown_v2_custom(
                    "*❌ Failed to delete match!*😐\n"
                    "*Please try again later.*"
                ),
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back to List", callback_data="list_matches")
                ]])
            )
    except Exception as e:
        logger.error(f"Error in delete_match: {e}")
        await query.message.edit_text(
            escape_markdown_v2_custom(
                "*❌ An error occurred while deleting the match*.😐\n"
                "*Please try again later*."
            ),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back to List", callback_data="list_matches")
            ]])
        )

# Update view_single_scorecard function
async def view_single_scorecard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show details of a single match"""
    query = update.callback_query
    await query.answer()
    
    _,match_id,_2  = query.data.split('_')
    match_id = match_id + '_'+_2
    user_id = str(query.from_user.id)
    
    connection = get_db_connection()
    card = None
    
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT *
                    FROM scorecards 
                    WHERE user_id = %s AND match_id = %s
                """, (user_id, match_id))
                db_result = cursor.fetchone()
                if db_result:
                    card = db_result

        except Error as e:
            logger.error(f"Database error: {e}")
            # Fallback to in-memory
            for saved_card in in_memory_scorecards:
                if (str(saved_card['user_id']) == user_id and 
                    saved_card['match_data']['match_id'] == match_id):
                    card = saved_card['match_data']
                    break
        finally:
            connection.close()
    else:
        # Use in-memory storage
        for saved_card in in_memory_scorecards:
            if (str(saved_card['user_id']) == user_id and 
                saved_card['match_data']['match_id'] == match_id):
                card = saved_card['match_data']
                break

    if not card:
        await query.edit_message_text(
            escape_markdown_v2_custom("*❌ Match not found\!*😐"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    # Create keyboard with delete button
    keyboard = [
        [InlineKeyboardButton("🗑️ Delete Match", callback_data=f"delete_{match_id}")],
        [InlineKeyboardButton("◀️ Back to List", callback_data="list_matches")]
    ]

    try:
        # Handle potential data formats with proper null checks
        res_view_score = ""
        
        # Safely get match_id, using card[1] or a fallback from the card dict
        match_id_str = str(card[1] if isinstance(card, tuple) else card.get('match_id', 'Unknown'))
        res_view_score += "*Match id *- " + match_id_str + "\n"
        
        # Safely get game mode
        game_mode = str(card[3] if isinstance(card, tuple) else card.get('game_mode', 'Classic'))
        res_view_score += "*Mode* - " + game_mode + "\n"
        
        # Safely handle match data
        if isinstance(card, tuple) and card[4]:
            match_data = card[4]
        else:
            match_data = card if isinstance(card, dict) else {}
        
        # Get timestamp with fallback
        timestamp = (match_data.get('saved_at', '') if isinstance(match_data, dict) else 
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if timestamp:
            formatted_time = timestamp.replace("T", ", ").split(".")[0]
            res_view_score += "*Time* - " + formatted_time + "\n\n"
        
        # Get match summary with fallback
        summary = (match_data.get('full_text', '') if isinstance(match_data, dict) else 
                  str(match_data) if match_data else 'No match summary available')
        if summary:
            res_view_score += "*Summary*\n*" + summary + "*\n"
        
        # Escape the final formatted string
        res_view_score = escape_markdown_v2_custom(res_view_score)

        await query.edit_message_text(
            res_view_score,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error formatting match data: {e}")
        await query.edit_message_text(
            escape_markdown_v2_custom(
                "❌ Error displaying match details!\n"
                "Please try again or contact support."
            ),
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def back_to_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Return to the match list view"""
    query = update.callback_query
    await query.answer()
    
    # Reset page number when returning to list
    context.user_data['scorecard_page'] = 0
    await view_scorecards(update, context)

# Update handle_input function
async def handle_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle numeric input with improved UI and flow"""
    if not update.message or not update.message.text:
        return
        
    # Check if this is a reply to our prompt
    if (not update.message.reply_to_message or 
        'awaiting_input' not in context.user_data or
        update.message.reply_to_message.message_id != context.user_data['awaiting_input'].get('prompt_message_id')):
        return
    
    try:
        input_data = context.user_data['awaiting_input']
        game_id = input_data['game_id']
        setting = input_data['setting']
        game = games[game_id]
        
        input_value = update.message.text.strip()
        if not input_value.isdigit():
            await update.message.reply_text(
                escape_markdown_v2_custom(f"{UI_THEMES['accents']['error']} Please enter a valid number!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        value = int(input_value)
        max_value = 50 if setting == 'overs' else 10
        
        if value < 1 or value > max_value:
            await update.message.reply_text(
                escape_markdown_v2_custom(f"{UI_THEMES['accents']['error']} Please enter a number between 1-{max_value}!"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
        
        if setting == 'overs':
            game['max_overs'] = value
            game['status'] = 'waiting'
            keyboard = [[InlineKeyboardButton("🤝 Join Game", callback_data=f"join_{game_id}")]]
            message = (
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['separator'])}\n"
                f"✅ GAME SETTINGS\n"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['section_sep'])}\n"
                f"Mode: {escape_markdown_v2_custom(game['mode'].title())}\n"
                f"Overs: {value}\n"
                f"Wickets: {game['max_wickets']}\n"
                f"Host: {escape_markdown_v2_custom(game['creator_name'])}\n"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['section_sep'])}\n"
                f"{escape_markdown_v2_custom("Waiting for opponent...\n")}"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['footer'])}"
            )
        else:  # wickets
            game['max_wickets'] = value
            keyboard = get_overs_keyboard(game_id)
            message = (
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['separator'])}\n"
                f"✅ WICKETS SET: {value}\n"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['section_sep'])}\n"
                f"Select number of overs:\n"
                f"{escape_markdown_v2_custom(UI_THEMES['primary']['footer'])}"
            )
        
        # Clean up old messages
        try:
            await context.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=input_data['prompt_message_id']
            )
        except:
            pass
        
        # Send new message
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        del context.user_data['awaiting_input']
        logger.info(f"Custom {setting} set to {value} for game {game_id}")
        
    except Exception as e:
        logger.error(f"Error in handle_input: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom(f"{UI_THEMES['accents']['error']} An error occurred. Please try again or start a new game with /gameon"),
            parse_mode=ParseMode.MARKDOWN_V2
        )

# Remove duplicate handle_error() functions - Keep only the enhanced version
async def handle_error(query: CallbackQuery, game_id: str = None):
    """Enhanced error handler with game state recovery"""
    try:
        if not query or not query.message:
            logger.error("Invalid query object in error handler")
            return
            
        if game_id and game_id in games:
            game = games[game_id]
            if not validate_game_state(game):
                game['status'] = game.get('status', 'error')
                game['current_innings'] = game.get('current_innings', 1)
                game['score'] = game.get('score', {'innings1': 0, 'innings2': 0})
                game['wickets'] = game.get('wickets', 0)
                game['balls'] = game.get('balls', 0)
        
        keyboard = [[InlineKeyboardButton("🔄 Retry", callback_data=f"retry_{game_id}")]] if game_id else None
        
        error_msg = escape_markdown_v2_custom(
            "*⚠️ An error occurred!*😐\n\n"
            "*• The game state has been preserved*\n"
            "*• Click Retry to continue*\n"
            "*• Or start a new game with /gameon*"
        )
        
        await query.message.edit_text(
            error_msg,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in error handler: {e}")
        try:
            await query.answer(
                escape_markdown_v2_custom("Failed to handle error. Please start a new game."),
                show_alert=True
            )
        except:
            pass


# Add near other helper functions
def get_current_overs(game: dict) -> str:
    """Get formatted overs string"""
    if 'max_overs' not in game or game['max_overs'] == float('inf'):
        return INFINITY_SYMBOL
    return str(game['max_overs'])

# Merge safe_edit_message() and safe_edit_with_retry()
async def safe_edit_message(message, text: str, keyboard=None, max_retries=MAX_MESSAGE_RETRIES):
    """Edit message with retry logic and flood control"""
    for attempt in range(max_retries):
        try:
            # Don't double escape if text already contains escape sequences
            escaped_text = text if '\\' in text else escape_markdown_v2_custom(text)
            if keyboard:
                return await message.edit_text(
                    text=escaped_text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            return await message.edit_text(
                text=escaped_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except telegram.error.RetryAfter as e:
            delay = FLOOD_CONTROL_BACKOFF[min(attempt, len(FLOOD_CONTROL_BACKOFF)-1)]
            logger.warning(f"Flood control hit, waiting {delay}s")
            await asyncio.sleep(delay)
        except telegram.error.TimedOut:
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            break
    return None

async def handle_auto_retry(msg, game: dict, retries: int = 0):
    """Auto retry mechanism for failed actions"""
    if retries >= MAX_AUTO_RETRIES:
        await safe_edit_message(msg, escape_markdown_v2_custom(ERROR_MESSAGES['recovery']))
        return False
        
    try:
        await asyncio.sleep(RETRY_WAIT_TIME)
        return True
    except Exception as e:
        logger.error(f"Auto retry failed: {e}")
        return await handle_auto_retry(msg, game, retries + 1)

# Update the keyboard generation to avoid duplicates
def get_batting_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    """Generate batting keyboard with unique buttons"""
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
    """Generate bowling keyboard with unique buttons"""
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

# Add at top of file with other constants
USE_FILE_STORAGE = False

async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle pagination for scorecards"""
        query = update.callback_query
        await query.answer()
        
        direction = query.data.split('_')[1]
        current_page = context.user_data.get('scorecard_page', 0)
        
        if direction == 'prev':
            context.user_data['scorecard_page'] = max(0, current_page - 1)
        else:  # next
            context.user_data['scorecard_page'] = current_page + 1
            
        await view_scorecards(update, context)

# Add these helper functions near the top after imports
def get_game_state_message(game: dict) -> str:
    """Generate formatted game state message"""
    batting_team = escape_markdown_v2_custom(game['batting_team'])
    bowling_team = escape_markdown_v2_custom(game['bowling_team'])
    score = game['score']
    wickets = game['wickets']
    overs = game.get('overs', 0)
    balls = game.get('balls', 0)
    target = game.get('target')
    
    state = escape_markdown_v2_custom(
        f"🏏 *{batting_team}* vs *{bowling_team}*\n"
        f"📊 *Score:* {score}/{wickets}\n"
        f"🎯 *Overs:* {overs}.{balls}\n"
    )
    
    if target:
        runs_needed = target - score
        balls_left = (game['total_overs'] * 6) - (overs * 6 + balls)
        if balls_left > 0:
            state += escape_markdown_v2_custom(
                f"🎯 *Target:* {target}\n"
                f"📈 *Need {runs_needed} runs from {balls_left} balls*"
            )
    
    return state

def validate_game_state(game: dict) -> bool:
    """Validate all required game state fields exist"""
    required_fields = [
        'status', 'mode', 'current_innings', 'score', 'wickets', 'balls',
        'max_wickets', 'max_overs', 'batsman', 'bowler'
    ]
    return all(field in game for field in required_fields)

# Add keyboard generator functions
def get_wickets_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    """Generate wickets selection keyboard with 1 wicket option"""
    return [
        [
            InlineKeyboardButton(f"1 🎯", callback_data=f"wickets_{game_id}_1"),
            InlineKeyboardButton(f"2 🎯", callback_data=f"wickets_{game_id}_2")
        ],
        [
            InlineKeyboardButton(f"5 🎯", callback_data=f"wickets_{game_id}_5"),
            InlineKeyboardButton(f"7 🎯", callback_data=f"wickets_{game_id}_7")
        ],
        [InlineKeyboardButton("📝 Custom (1-10)", callback_data=f"custom_{game_id}_wickets")]
    ]

def get_overs_keyboard(game_id: str) -> List[List[InlineKeyboardButton]]:
    """Generate overs selection keyboard with 1 over option"""
    return [
        [
            InlineKeyboardButton(f"1 🎯", callback_data=f"overs_{game_id}_1"),
            InlineKeyboardButton(f"2 🎯", callback_data=f"overs_{game_id}_2")
        ],
        [
            InlineKeyboardButton(f"5 🎯", callback_data=f"overs_{game_id}_5"),
            InlineKeyboardButton(f"10 🎯", callback_data=f"overs_{game_id}_10")
        ],
        [InlineKeyboardButton("📝 Custom (1-50)", callback_data=f"custom_{game_id}_overs")]
    ]

# Update the toss handling
async def handle_toss(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    try:
        _, game_id, choice = query.data.split('_')
        game = games.get(game_id)
        
        if not game:
            await query.answer(
                escape_markdown_v2_custom("❌ Game not found!"),
                show_alert=True,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        if user_id != game['choosing_player']:
            await query.answer(
                escape_markdown_v2_custom(f"❌ Only {game['choosing_player_name']} can choose!"),
                show_alert=True,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        await query.answer()
        
        # Show dice rolling animation
        msg = query.message
        await msg.edit_text(
            escape_markdown_v2_custom("🎲 Rolling first dice..."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await asyncio.sleep(1)
        
        dice1 = random.randint(1, 6)
        await msg.edit_text(
            escape_markdown_v2_custom(f"First roll: {dice1}"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await asyncio.sleep(1)
        
        await msg.edit_text(
            escape_markdown_v2_custom("🎲 Rolling second dice..."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await asyncio.sleep(1)
        
        dice2 = random.randint(1, 6)
        total = dice1 + dice2
        is_odd = total % 2 == 1
        
        # Determine toss winner
        choice_correct = (choice == 'odd' and is_odd) or (choice == 'even' and not is_odd)
        toss_winner = game['choosing_player'] if choice_correct else (
            game['creator'] if game['choosing_player'] == game['joiner'] else game['creator']
        )
        toss_winner_name = game['creator_name'] if toss_winner == game['creator'] else game['joiner_name']
        
        # Update game state
        game['toss_winner'] = toss_winner
        game['toss_winner_name'] = toss_winner_name
        game['status'] = 'choosing'
        
        # Create properly escaped message text
        toss_msg = (
            f"{escape_markdown_v2_custom('🎲 TOSS RESULT')}\n"
            f"{escape_markdown_v2_custom(MATCH_SEPARATOR)}\n"
            f"{escape_markdown_v2_custom('First Roll:')} {dice1}\n"
            f"{escape_markdown_v2_custom('Second Roll:')} {dice2}\n"
            f"{escape_markdown_v2_custom('Total:')} {total}\n\n"
            f"{escape_markdown_v2_custom('🏆')} {escape_markdown_v2_custom(f'{toss_winner_name} wins the toss!')}"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("🏏 BAT", callback_data=f"choice_{game_id}_bat"),
                InlineKeyboardButton("⚾ BOWL", callback_data=f"choice_{game_id}_bowl")
            ]
        ]
        
        await msg.edit_text(
            toss_msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_toss: {e}")
        await handle_error(query, game_id if 'game_id' in locals() else None)

# Add function to handle game retry
async def handle_retry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle game retry attempts"""
    query = update.callback_query
    await query.answer()
    
    try:
        _, game_id = query.data.split('_')
        if game_id not in games:
            await query.edit_message_text(
                escape_markdown_v2_custom("❌ Game not found! Please start a new game."),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        game = games[game_id]
        if not validate_game_state(game):
            await query.edit_message_text(
                escape_markdown_v2_custom("❌ Game state corrupted. Please start a new game."),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        # Restore last valid game state
        status = game['status']
        if status == 'batting':
            keyboard = get_batting_keyboard(game_id)
            message = get_game_state_message(game)
        elif status == 'bowling':
            keyboard = get_bowling_keyboard(game_id)
            message = get_game_state_message(game)
        else:
            # Can't recover, start new game
            await query.edit_message_text(
                escape_markdown_v2_custom("❌ Cannot recover game state. Please start a new game."),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
            
        await query.edit_message_text(
            escape_markdown_v2_custom(message),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
    except Exception as e:
        logger.error(f"Error in handle_retry: {e}")
        await query.edit_message_text(
            escape_markdown_v2_custom("❌ Retry failed. Please start a new game."),
            parse_mode=ParseMode.MARKDOWN_V2
        )

def init_database_connection():
    """Initialize database connection with better error handling"""
    global db
    try:
        # Add retries for initial connection
        max_retries = 5
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Database connection attempt {attempt + 1}")
                logger.info(f"Connecting to: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
                
                db = DatabaseHandler()
                if db.check_connection():
                    logger.info("Successfully connected to database")
                    return True
                    
                logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                continue
                
        logger.error("All database connection attempts failed")
        return False
        
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        return False
    
async def test_db_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Test database connection and schema"""
        if not check_admin(str(update.effective_user.id)):
            await update.message.reply_text("❌ Unauthorized")
            return
            
        try:
            connection = get_db_connection()
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
                    escape_markdown_v2_custom(
                        f"*✅ Database connected successfully!*👍\n"
                        f"*Users:* {users_count}\n"
                        f"*Scorecards:* {scorecards_count}"
                    ),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                
        except Exception as e:
            await update.message.reply_text(
                escape_markdown_v2_custom(
                    f"*❌ Database test failed:*\n{str(e)}"
                ),
                parse_mode=ParseMode.MARKDOWN_V2
            )
        finally:
            if connection:
                connection.close()

# Add near the top with other constants
FLOOD_CONTROL_DELAY = 21  # Seconds to wait when flood control is hit
MAX_MESSAGE_RETRIES = 3
FLOOD_CONTROL_BACKOFF = [5, 10, 21]  # Progressive backoff delays

# Add new helper function
async def safe_edit_with_retry(message, text: str, keyboard=None, max_retries=MAX_MESSAGE_RETRIES):
    """Edit message with flood control handling and retry logic"""
    for attempt in range(max_retries):
        try:
            if keyboard:
                return await message.edit_text(
                    text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            return await message.edit_text(
                text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except telegram.error.RetryAfter as e:
            delay = FLOOD_CONTROL_BACKOFF[min(attempt, len(FLOOD_CONTROL_BACKOFF)-1)]
            logger.warning(f"Flood control hit, waiting {delay}s")
            await asyncio.sleep(delay)
        except telegram.error.TimedOut:
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            break
    return None

def save_to_file(match_data: dict):
    """Save match data to backup file"""
    try:
        DATA_DIR.mkdir(exist_ok=True)
        
        # Load existing data
        existing_data = []
        if MATCH_HISTORY_FILE.exists():
            with open(MATCH_HISTORY_FILE, 'r', encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                except json.JSONDecodeError:
                    existing_data = []
        
        # Add new match data
        existing_data.append(match_data)
        
        # Save updated data
        with open(MATCH_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, default=str)
            
    except Exception as e:
        logger.error(f"Error saving to file: {e}")

# Add auto-save functionality
async def auto_save_match(game: dict, user_id: int):
    """Auto save match after key events"""
    try:
        match_data = {
            'match_id': game.get('match_id', generate_match_id()),
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'teams': {
                'batting_first': game['creator_name'],
                'bowling_first': game['joiner_name']
            },
            'innings1': {
                'score': game['score']['innings1'],
                'wickets': game.get('first_innings_wickets', game['wickets']),
                'overs': f"{game['balls']//6}.{game['balls']%6}"
            },
            'innings2': {
                'score': game['score'].get('innings2', 0),
                'wickets': game['wickets'],
                'overs': f"{game['balls']//6}.{game['balls']%6}"
            },
            'game_mode': game['mode'],
            'match_data': json.dumps(game)
        }
        
        
        # Try database save first
        if not await db.save_match_async(match_data):
            # Fallback to file storage
            save_to_file(match_data)
            
    except Exception as e:
        logger.error(f"Error auto-saving match: {e}")

# Add function to properly format messages

# Add function to properly format messages
def format_game_message(text: str) -> str:
    """Format game messages with proper escaping and bold text"""
    # Bold important numbers and text
    bold_patterns = [
        (r'(\d+)/(\d+)', r'*\1/\2*'),  # Score/wickets
        (r'Over (\d+\.\d+)', r'Over *\1*'),  # Overs
        (r'(\d+) runs', r'*\1* runs'),  # Run counts
        (r'(\d+) wickets', r'*\1* wickets'),  # Wicket counts
        (r'Target: (\d+)', r'Target: *\1*'),  # Target
        (r'RRR: ([\d.]+)', r'RRR: *\1*')  # Required run rate
    ]
    
    for pattern, replacement in bold_patterns:
        text = re.sub(pattern, replacement, text)
    
    # Escape special characters for Markdown V2
    return escape_markdown(text, version=2)

def validate_config():
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# Add this helper function near the top with other helper functions
def safe_division(numerator, denominator, default=0):
    """Safely perform division with fallback to default value"""
    try:
        if denominator == 0 or not denominator:
            return default
        return numerator / float(denominator)
    except (ValueError, TypeError):
        return default

def get_target_info(game: dict) -> str:
    """Get formatted target information string"""
    if game['current_innings'] != 2 or 'target' not in game:
        return ""
        
    target = game['target']
    current_score = game['score']['innings2']
    runs_needed = target - current_score
    balls_left = (game['max_overs'] * 6) - game['balls']
    
    if balls_left > 0:
        required_rate = (runs_needed * 6) / max(balls_left, 1)  # Prevent division by zero
        return f"\n*Target:* {target}\n*Need:* {runs_needed} from {balls_left} balls\n*RRR:* {required_rate:.2f}"
    
    return f"\n*Target:* {target}\n*Need:* {runs_needed} runs"

async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all authorized groups"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not AUTHORIZED_GROUPS:
        await update.message.reply_text(
            escape_markdown_v2_custom("*📝 No authorized groups*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    groups_list = "*📝 Authorized Groups:*\n" + MATCH_SEPARATOR + "\n\n"
    for group_id in AUTHORIZED_GROUPS:
        try:
            chat = await context.bot.get_chat(group_id)
            groups_list += f"• *ID:* `{group_id}`\n  *Name:* {escape_markdown_v2_custom(chat.title)}\n\n"
        except Exception:
            groups_list += f"• *ID:* `{group_id}`\n  *Name:* Unknown\n\n"
    
    await update.message.reply_text(
        groups_list,
        parse_mode=ParseMode.MARKDOWN_V2
    )

async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all bot admins"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not BOT_ADMINS:
        await update.message.reply_text(
            escape_markdown_v2_custom("*📝 No admins configured*"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    admins_list = "*👑 Bot Administrators:*\n" + MATCH_SEPARATOR + "\n\n"
    for admin_id in BOT_ADMINS:
        try:
            user = await context.bot.get_chat(admin_id)
            admins_list += (f"• *ID:* `{admin_id}`\n"
                          f"  *Name:* {escape_markdown_v2_custom(user.first_name)}\n\n")
        except Exception:
            admins_list += f"• *ID:* `{admin_id}`\n  *Name:* Unknown\n\n"
    
    await update.message.reply_text(
        admins_list,
        parse_mode=ParseMode.MARKDOWN_V2
    )

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove an admin"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not context.args:
        await update.message.reply_text(
            escape_markdown_v2_custom("*Usage:* /removeadmin <user_id>"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    admin_to_remove = context.args[0]
    
    if admin_to_remove not in BOT_ADMINS:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ User is not an admin!"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    if len(BOT_ADMINS) <= 1:
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Cannot remove the last admin!"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    BOT_ADMINS.remove(admin_to_remove)
    await update.message.reply_text(
        escape_markdown_v2_custom("✅ Admin removed successfully!"),
        parse_mode=ParseMode.MARKDOWN_V2
    )

async def blacklist_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Blacklist a user from using the bot"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not context.args:
        await update.message.reply_text(
            escape_markdown_v2_custom("*Usage:* /blacklist <user_id> [reason]"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    user_id = context.args[0]
    reason = ' '.join(context.args[1:]) if len(context.args) > 1 else "No reason provided"
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection error")
            return
            
        with conn.cursor() as cur:
            # Update user's banned status
            cur.execute("""
                UPDATE users 
                SET is_banned = TRUE,
                    ban_reason = %s,
                    banned_at = CURRENT_TIMESTAMP,
                    banned_by = %s
                WHERE telegram_id = %s
                RETURNING telegram_id
            """, (reason, update.effective_user.id, user_id))
            
            if cur.fetchone():
                BLACKLISTED_USERS.add(user_id)
                conn.commit()
                
                await update.message.reply_text(
                    escape_markdown_v2_custom(f"✅ User {user_id} has been blacklisted\nReason: {reason}"),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            else:
                await update.message.reply_text(
                    escape_markdown_v2_custom("❌ User not found in database"),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                
    except Exception as e:
        logger.error(f"Error blacklisting user: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Error blacklisting user"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    finally:
        if conn:
            return_db_connection(conn)

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a user from the blacklist"""
    if not check_admin(str(update.effective_user.id)):
        await update.message.reply_text("❌ Unauthorized")
        return
        
    if not context.args:
        await update.message.reply_text(
            escape_markdown_v2_custom("*Usage:* /unban <user_id>"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
        
    user_id = context.args[0]
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("❌ Database connection error")
            return
            
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users 
                SET is_banned = FALSE,
                    ban_reason = NULL,
                    banned_at = NULL,
                    banned_by = NULL
                WHERE telegram_id = %s
                RETURNING telegram_id
            """, (user_id,))
            
            if cur.fetchone():
                BLACKLISTED_USERS.discard(user_id)
                conn.commit()
                
                await update.message.reply_text(
                    escape_markdown_v2_custom(f"✅ User {user_id} has been unbanned"),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            else:
                await update.message.reply_text(
                    escape_markdown_v2_custom("❌ User not found in database"),
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                
    except Exception as e:
        logger.error(f"Error unbanning user: {e}")
        await update.message.reply_text(
            escape_markdown_v2_custom("❌ Error unbanning user"),
            parse_mode=ParseMode.MARKDOWN_V2
        )

# --- Main Function ---
def main():
    # Add this at the start of main()
    load_dotenv()  # Load environment variables

    if not BOT_TOKEN:
        logger.error("Bot token not found!")
        return

    # Initialize database
    if not init_database_connection():
        logger.warning("Running in file storage mode due to database initialization failure")
        global USE_FILE_STORAGE
        USE_FILE_STORAGE = True
    
    # Initialize application with proxy settings removed
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(8)  # Adjust pool size as needed
        .connect_timeout(30.0)    # Increase timeout if needed
        .read_timeout(30.0)
        .write_timeout(30.0)
        .build()
    )

    # Game handlers
    application.add_handler(CommandHandler("gameon", gameon))
    application.add_handler(CommandHandler("testdb", test_db_connection))
    application.add_handler(CommandHandler("savefile", save_to_file))
    application.add_handler(CommandHandler("listgroups", list_groups))
    application.add_handler(CommandHandler("listadmins", list_admins))
    application.add_handler(CommandHandler("removeadmin", remove_admin))
    application.add_handler(CommandHandler("removegroup", remove_group))
    application.add_handler(CommandHandler("blacklist", blacklist_user))
    application.add_handler(CommandHandler("addadmin", add_admin))
    application.add_handler(CommandHandler("stopgames", stop_games))
    application.add_handler(CommandHandler("broadcast", broadcast_message))
    application.add_handler(CommandHandler("addgroup", add_group))
    application.add_handler(CommandHandler("removegroup", remove_group))
    application.add_handler(CommandHandler("toggletest", toggle_test_mode))
    application.add_handler(CommandHandler("botstats", bot_stats))
    application.add_handler(CommandHandler("unban", unban_user))
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("save", save_match))
    application.add_handler(CommandHandler("scorecard", view_scorecards))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_input))
    application.add_handler(CallbackQueryHandler(handle_wickets, pattern="^wickets_"))
    application.add_handler(CallbackQueryHandler(view_single_scorecard, pattern="^view_"))
    application.add_handler(CallbackQueryHandler(back_to_list, pattern="^list_matches"))
    application.add_handler(CallbackQueryHandler(delete_match, pattern="^delete_"))
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^page\\_"))
    application.add_handler(CallbackQueryHandler(handle_input, pattern="^manual_"))
    application.add_handler(CallbackQueryHandler(handle_retry, pattern="^retry_"))
    application.add_handler(CallbackQueryHandler(handle_mode, pattern="^mode_"))
    application.add_handler(CallbackQueryHandler(handle_vers, pattern="^overs_"))
    application.add_handler(CallbackQueryHandler(handle_join, pattern="^join_"))
    application.add_handler(CallbackQueryHandler(handle_bat, pattern="^bat_"))
    application.add_handler(CallbackQueryHandler(handle_bowl, pattern="^bowl_"))
    application.add_handler(CallbackQueryHandler(handle_custom, pattern="^custom_"))
    application.add_handler(CallbackQueryHandler(handle_toss, pattern="^toss_"))
    application.add_handler(CallbackQueryHandler(handle_choice, pattern="^choice_"))
    application.add_handler(CallbackQueryHandler(handle_error, pattern="^retry_"))

    logger.info("Bot starting...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        if db:
            db.close() 


if __name__ == '__main__':
    main()
