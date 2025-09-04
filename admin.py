import asyncio
from datetime import datetime
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from constants import (
    BOT_ADMINS, REGISTERED_USERS, games, AUTHORIZED_GROUPS, TEST_MODE,
    MATCH_SEPARATOR, BROADCAST_DELAY, in_memory_scorecards
)
from helper import escape_markdown_v2_custom, check_admin, logger
from db_instance import db

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
