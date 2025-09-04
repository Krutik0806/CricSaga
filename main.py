# --- Standard Library Imports ---
import json
from datetime import datetime
from typing import Optional

# --- Third Party Imports ---
from telegram import Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

# --- Local Imports ---
from constants import REGISTERED_USERS, DB_CONFIG, DB_POOL_MIN, DB_POOL_MAX, BOT_TOKEN,logger
from helper import escape_markdown_v2_custom
from commands import gameon, start
from handlers import handle_mode, handle_wickets, handle_bat,handle_bowl,handle_choice,handle_vers,handle_join,handle_toss,handle_custom,handle_input
from admin import add_admin, bot_stats, add_group,remove_group,broadcast_message,stop_games,test_db_connection
from scorecard import save_match, view_scorecards, view_single_scorecard, delete_match, back_to_list
from db_instance import db

# --- Initialize Database ---
if not db.check_connection():
    logger.warning("Database connection failed, using file storage")
    USE_FILE_STORAGE = True

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