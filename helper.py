import time
import random
import json
import asyncio
import logging
import telegram
from telegram.constants import ParseMode
from constants import BOT_ADMINS, REGISTERED_USERS,games, DATA_DIR, MATCH_HISTORY_FILE, INFINITY_SYMBOL, GAME_MODE_DISPLAY, DB_CONFIG, DB_POOL_MIN, DB_POOL_MAX, USE_FILE_STORAGE, ANIMATION_DELAY, BALL_ANIMATION_DELAY, OVER_BREAK_DELAY, BROADCAST_DELAY, MAX_MESSAGE_RETRIES, FLOOD_CONTROL_BACKOFF, ACTION_MESSAGES, COMMENTARY_PHRASES, MATCH_SEPARATOR, AUTHORIZED_GROUPS, TEST_MODE,logger

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
