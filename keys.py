import os
from pathlib import Path
from typing import List
from telegram import InlineKeyboardButton

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
