import json
import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import ContextTypes
from constants import REGISTERED_USERS, games ,GAME_MODE_DISPLAY,logger
from helper import escape_markdown_v2_custom, is_registered, create_game
from db_instance import db

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
