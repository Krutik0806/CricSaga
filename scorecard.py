import re
import time
import random
import json
import asyncio
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import ContextTypes
from helper import escape_markdown_v2_custom, logger, save_to_file
from db_instance import db

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

