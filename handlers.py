import asyncio
import random
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from constants import games, GAME_MODE_DISPLAY, logger,INFINITY_SYMBOL,ANIMATION_DELAY,ACTION_MESSAGES,BALL_ANIMATION_DELAY,COMMENTARY_PHRASES,OVER_BREAK_DELAY
from helper import escape_markdown_v2_custom, should_end_innings, store_first_innings, safe_edit_message,safe_division
from keys import get_batting_keyboard, get_bowling_keyboard, get_wickets_keyboard, get_overs_keyboard

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
