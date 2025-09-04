import os
from pathlib import Path
from typing import Dict, Set
import logging
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

