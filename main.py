import requests 
import os
import io
import re
import math 
import json
import csv
from werkzeug.security import generate_password_hash
import time
import zipfile
import sqlite3
import pymongo
from bson.objectid import ObjectId
import asyncio
import random
from helper import edit_command_handler, edit_quiz_cb_handler, edit_flow_handler, parse_html_quiz # ➕ ADD THIS LINE
import traceback
import string
import glob
import uuid
import shutil
import tempfile
import pytz
from functools import wraps
from datetime import datetime, timedelta
from typing import Dict, Tuple, Any
from telegram.helpers import escape_markdown
from pyrogram import Client as PyroClient
from pyrogram.errors import FloodWait

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Poll,
    InlineQueryResultArticle,
    InputTextMessageContent,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    PollAnswerHandler,
    PollHandler,
    filters,
    JobQueue,
    InlineQueryHandler,
)
from telegram.constants import ParseMode

# --- BOT CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = 8572815322
POLL_LOG_CHANNEL_ID = -1003496956566
DB_PATH = "quizzes.db"
DB_NAME = "quizbot"
MONGO_URI = os.environ.get("mon") # 
FORCESUB_CHANNEL_ID = -1003496956566
SESSION_DIR = "sessions"
FORCESUB_CHANNEL_LINK = "https://t.me/studyhelppdf" 
os.makedirs(SESSION_DIR, exist_ok=True)

UB_API_ID =5074166
UB_API_HASH = "3cb93a9a9345592f5e6a42020687cdbe"

# List of session strings from environment variables
USERBOT_SESSIONS = [
    os.environ.get("ses1"),
    os.environ.get("ses2"),
    os.environ.get("ses3"),
    os.environ.get("ses4"),
    os.environ.get("ses5"),
]
# Filter out empty sessions (in case you only set 2 or 3)
USERBOT_SESSIONS = [s for s in USERBOT_SESSIONS if s]

# Global pool to store active Pyrogram clients
active_userbots = []
 
# +++ ADD THESE LINES +++
8572815322
POLL_QUESTION_MAX_LENGTH = 250
PLACEHOLDER_QUESTION = "⬆️ LOOK AT THE MESSAGE ABOVE FOR THE QUESTION ⬆️"


if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN env var required. Please set it in your environment.")


# --- DATABASE INITIALIZATION (MONGODB) ---
# Establish a connection to the MongoDB server
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Get handles to the collections (equivalent to tables)
    creators_collection = db["creators"]
    quizzes_collection = db["quizzes"]
    attempts_collection = db["attempts"]
    schedule_collection = db["schedule"]
    banned_users_collection = db["banned_users"]  

    # Create indexes to optimize queries
    creators_collection.create_index("tg_user_id", unique=True)
    quizzes_collection.create_index("creator_id")
    attempts_collection.create_index("quiz_id")
    attempts_collection.create_index("user_id")
    schedule_collection.create_index("status")
    banned_users_collection.create_index("user_id", unique=True)  

    print("Successfully connected to MongoDB.")
except Exception as e:
    print(f"FATAL: Could not connect to MongoDB: {e}")
    raise SystemExit(e)

# --- UTILITY FUNCTIONS & STATE MANAGEMENT ---
# NOTE: The db_execute function has been removed.
# All database operations will now use the collection objects directly.

running_private_tasks: Dict[Tuple[int,int], asyncio.Task] = {} 
running_group_tasks: Dict[Tuple[int,str], asyncio.Task] = {}
private_session_locks: Dict[Tuple[int,int], asyncio.Lock] = {}
group_session_locks: Dict[Tuple[int,str], asyncio.Lock] = {}
ongoing_sessions = {}
POLL_ID_TO_SESSION_MAP: Dict[str, Dict[str, Any]] = {}

def check_ban(func):
    """Decorator to check if user is banned before executing any command."""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Check if user is banned
        banned_user = banned_users_collection.find_one({"user_id": user_id})
        if banned_user:
            reason = banned_user.get("reason", "No reason provided")
            await update.message.reply_text(
                f"🚫 *You are banned from using this bot.*\n\n*Reason:* {reason}",
                parse_mode=ParseMode.MARKDOWN,
                reply_to_message_id=update.message.message_id  # This makes it a quote
            )
            return  # Stop execution of the command
        
        # If not banned, proceed with original function
        return await func(update, context, *args, **kwargs)
    return wrapped
    
# +++ NEW: INTERNAL POLL RESOLVER +++
async def _fetch_poll_with_client(client: PyroClient, chat_id: int, message_id: int):
    """Helper to fetch a single poll using a specific client."""
    msg = await client.get_messages(chat_id, message_id)
    if not msg or not msg.poll:
        raise ValueError("Message not found or not a poll")

    poll = msg.poll
    correct_index = None

    # Try to vote if answer is unknown
    if not getattr(poll, "correct_option_id", None) and not poll.is_closed:
        try:
            await client.vote_poll(chat_id, message_id, [0])
            await asyncio.sleep(1.5) # Short wait for update
            
            updated_msg = await client.get_messages(chat_id, message_id)
            if updated_msg and updated_msg.poll:
                poll = updated_msg.poll # Update reference
                correct_index = getattr(poll, "correct_option_id", None)
                
                # Fallback: check voter counts
                if correct_index is None:
                    for i, option in enumerate(poll.options):
                        if getattr(option, "voter_count", 0) > 0:
                            correct_index = i
                            break
        except Exception:
            pass # Voting failed (maybe already voted), proceed to check existing data
            
    # Final check for answer
    if correct_index is None:
        correct_index = getattr(poll, "correct_option_id", None)

    # Default to 0 if quiz mode and still unknown (common fallback)
    if correct_index is None and poll.type.value == "quiz":
        correct_index = 0

    if correct_index is None:
        raise ValueError("Could not determine correct answer")

    # Clean text
    cleaned_question = re.sub(r'^\s*\[\d+/\d+\]\s*', '', poll.question)

    return {
        "text": cleaned_question,
        "options": [opt.text for opt in poll.options],
        "correctIndex": correct_index,
        "explanation": getattr(poll, "explanation", "") or ""
    }

async def resolve_poll_multi_client(chat_id: int, message_id: int):
    """
    Tries to resolve poll using the pool of userbots.
    If one hits FloodWait or fails, it immediately switches to the next.
    """
    if not active_userbots:
        raise Exception("No active userbots available")

    # Shuffle list to load balance
    clients = list(active_userbots)
    random.shuffle(clients)

    last_error = None

    for i, client in enumerate(clients):
        try:
            # Attempt fetch
            return await _fetch_poll_with_client(client, chat_id, message_id)
        except FloodWait as e:
            print(f"⚠️ Userbot {i} hit FloodWait ({e.value}s). Switching...")
            continue # Try next bot immediately
        except Exception as e:
            print(f"⚠️ Userbot {i} failed: {e}. Switching...")
            last_error = e
            continue # Try next bot

    raise last_error or Exception("All userbots failed to resolve the poll")
# +++ END NEW FUNCTIONS +++
    
# +++ ADD THIS ENTIRE NEW FUNCTION +++
async def add_auth_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Owner Only) Grants premium status to a user."""
    user = update.effective_user
    if user.id != OWNER_ID:
        return # Silently ignore if not owner

    if not context.args:
        await update.message.reply_text("Usage: /addauth <user_id>")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid numerical Telegram User ID.")
        return

    # Use upsert to create the user if they don't exist, or update them if they do
    result = creators_collection.update_one(
        {"tg_user_id": target_user_id},
        {"$set": {"is_premium": 1}},
        upsert=True
    )

    if result.upserted_id or result.modified_count > 0:
        await update.message.reply_text(f"✅ User `{target_user_id}` has been granted premium access.")
    else:
        await update.message.reply_text("Could not update user. No changes were made.")

async def poll_import_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles forwarded polls using the internal Userbot Pool (Free for all)."""
    user = update.effective_user
    msg = update.message

    # 1. Basic checks (Session exists and user is in 'questions' step)
    create_key = (user.id, "create")
    if create_key not in ongoing_sessions or ongoing_sessions[create_key].get("step") != "questions":
        return

    state = ongoing_sessions[create_key]

    # 2. Check question limit
    if len(state.get("questions", [])) >= 100:
        await msg.reply_text("❌ You have reached the maximum limit of 100 questions.")
        return

    if not msg.poll:
        return

    processing_msg = await msg.reply_text("🔄 Processing poll...")

    try:
        # 3. Forward to Log Channel (So userbots can see it)
        logged_message = await context.bot.forward_message(
            chat_id=POLL_LOG_CHANNEL_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id
        )

        # 4. Resolve Locally using Multi-Userbot Pool
        question_obj = await resolve_poll_multi_client(
            chat_id=POLL_LOG_CHANNEL_ID,
            message_id=logged_message.message_id
        )

        # 5. Save
        state["questions"].append(question_obj)
        total_q = len(state['questions'])
        await processing_msg.edit_text(f"✅ Poll imported! Total questions: {total_q}. Send more or /done.")

    except Exception as e:
        error_message = str(e)
        print(f"Error in poll import: {error_message}")
        await processing_msg.edit_text(f"❌ Failed. Error: `{error_message}`")
            
@check_ban
async def set_password_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows a creator to set their web panel password."""
    user = update.effective_user
    
    if not context.args:
        await update.message.reply_text("Usage: /set_password <your_new_password>")
        return
        
    new_password = context.args[0]
    if len(new_password) < 8:
        await update.message.reply_text("❌ Password must be at least 8 characters long.")
        return
        
    password_hash = generate_password_hash(new_password)
    
    # Update the creator's document in MongoDB
    result = creators_collection.update_one(
        {"tg_user_id": user.id},
        {"$set": {"password_hash": password_hash}}
    )
    
    if result.matched_count > 0:
        await update.message.reply_text("✅ Your web panel password has been set! You can now log in using your Telegram ID and this password.")
    else:
        # This can happen if the user has never created a quiz
        await update.message.reply_text("❌ Could not find your creator profile. Please create a quiz first.")

async def _update_progress_task(progress_msg, stop_event: asyncio.Event):
    """A helper task to show a fake progress bar during calculations."""
    percentages = [10, 25, 40, 65, 80, 95]
    try:
        for p in percentages:
            if stop_event.is_set():
                break
            try:
                await progress_msg.edit_text(f"🏁 The quiz has finished! Processing results... {p}%")
            except Exception:
                pass # Ignore potential "message is not modified" errors
            await asyncio.sleep(2) # Wait for 2 seconds
    except asyncio.CancelledError:
        pass # Task was cancelled, which is expected
    except Exception as e:
        print(f"Error in progress task: {e}")

@check_ban
async def set_promo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows a creator to set or remove their promotional message."""
    user = update.effective_user
    
    # Use the text after the command as the promo message
    promo_message = update.message.text.partition(' ')[2].strip()
    
    # Ensure the user has a profile before we try to update it
    get_or_create_creator_by_tg(user)
    
    # If the user provided a message, we SET it.
    if promo_message:
        if len(promo_message) > 2000: # Optional: Add a length limit
            await update.message.reply_text("❌ Your promo message is too long (max 2000 characters).")
            return
            
        # Update the creator's document in MongoDB to add/update the promo message
        creators_collection.update_one(
            {"tg_user_id": user.id},
            {"$set": {"promo_message": promo_message}}
        )
        
        reply_text = f"✅ Your promo message has been set to:\n\n```\n{promo_message}\n```"
        
        await update.message.reply_text(reply_text, parse_mode=ParseMode.MARKDOWN_V2)

    # If the user sent the command with NO message, we REMOVE it.
    else:
        creators_collection.update_one(
            {"tg_user_id": user.id},
            {"$unset": {"promo_message": ""}}
        )
        
        await update.message.reply_text("✅ Your promotional message has been successfully removed. use /set_promo urmessage to set it again ")

# --- REPLACE your entire create_paginated_keyboard function with this final version ---

def create_paginated_keyboard(quizzes: list, page: int, page_size: int, command_base: str):
    if not quizzes:
        return "You haven't created any quizzes yet\\. Use /create to start\\.", None

    total_quizzes = len(quizzes)
    total_pages = math.ceil(total_quizzes / page_size)
    page = max(0, min(page, total_pages - 1))

    start_index = page * page_size
    end_index = start_index + page_size
    page_quizzes = quizzes[start_index:end_index]

    header = [
        f"📝 *Your Quizzes \\(Page {page + 1}/{total_pages}\\)*",
        f"📊 *Total Quizzes:* {total_quizzes}"
    ]
    
    quiz_details_list = []
    for i, r in enumerate(page_quizzes, start=start_index + 1):
        # The question count is now just the length of the embedded array
        q_count = len(r.get("questions", []))
        
        title = escape_markdown(r['title'], version=2)
        quiz_id = escape_markdown(r['_id'], version=2) # MongoDB uses _id

        neg_mark_str = str(r['negative_mark']).replace('.', '\\.')

        details = (
            f"{i}\\. *{title}*\n"
            f"    └ 🆔 *ID:* `{quiz_id}`\n"
            f"    └ ❓ *Questions:* {q_count}\n"
            f"    └ ⏰ *Timer:* {r['time_per_question_sec']}s per question\n"
            f"    └ 📉 *Negative Mark:* {neg_mark_str}"
        )
        quiz_details_list.append(details)

    separator = "\n\n────────────────\n\n"
    final_text = "\n".join(header) + "\n\n" + separator.join(quiz_details_list)

    kb = []
    for r in page_quizzes:
        title = r['title']
        if len(title) > 40:
            title = title[:37] + "..."
            
        if command_base == 'myquizzes':
            callback_action = f"postcard:{r['_id']}"
        else: # 'manage'
            callback_action = f"viewquiz:{r['_id']}"
        
        kb.append([InlineKeyboardButton(f"{title}", callback_data=callback_action)])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Back", callback_data=f"{command_base}_page:{page - 1}"))
    
    nav_row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop")) 

    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"{command_base}_page:{page + 1}"))
    
    if len(nav_row) > 1:
        kb.append(nav_row)
        
    return final_text, InlineKeyboardMarkup(kb)

async def paginated_quiz_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles 'next' and 'back' buttons for paginated quiz lists."""
    query = update.callback_query
    await query.answer()
    
    command_base, page_str = query.data.split('_page:')
    page = int(page_str)
    
    uid = query.from_user.id
    c = get_creator_by_tg(uid)
    if not c:
        await query.edit_message_text("Could not find your creator profile.")
        return
        
    cursor = quizzes_collection.find({"creator_tg_id": c["tg_user_id"]}).sort("created_at", -1)
    rows = list(cursor)
    
    text, kb = create_paginated_keyboard(rows, page=page, page_size=5, command_base=command_base)
    
    try:
        await query.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        print(f"Error updating paginated list: {e}")

async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Answers the callback query for buttons that should do nothing."""
    await update.callback_query.answer()

# --- REPLACE your old generate_quiz_html function with this one ---

async def generate_quiz_html(quiz_settings: dict, questions: list) -> str | None:
    """
    Generates a standalone, interactive HTML file from quiz data for the new template.

    Args:
        quiz_settings: A dictionary with quiz settings like total time and negative marks.
        questions: A list of question objects from the session.

    Returns:
        The file path to the generated temporary HTML file, or None on failure.
    """
    try:
        # 1. Read the HTML template
        with open("format.html", "r", encoding="utf-8") as f:
            html_template = f.read()

        # 2. Build the full quiz object required by the new HTML template.
        #    The `questions` list from the session already has the correct format
        #    ('text', 'options', 'correctIndex', etc.), so we don't need to convert it.
        full_quiz_object = {
            "settings": {
                "totalTimeSec": quiz_settings.get("totalTimeSec", 600),
                "negativeMarkPerWrong": quiz_settings.get("negativeMark", 0.0)
            },
            "questions": questions
        }
        
        # 3. Convert the entire object to a JSON string
        quiz_data_json = json.dumps(full_quiz_object, ensure_ascii=False, indent=2)

        # 4. Replace the new placeholder in the template
        final_html = html_template.replace(
            "/* QUIZ_DATA_PLACEHOLDER */",
            quiz_data_json
        )

        # 5. Save to a temporary file and return the path
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".html", encoding="utf-8") as temp_f:
            temp_f.write(final_html)
            return temp_f.name

    except FileNotFoundError:
        print("CRITICAL ERROR: The 'format.html' template file was not found.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred in generate_quiz_html: {e}")
        traceback.print_exc()
        return None


def get_private_lock(key: Tuple[int,int]):
    if key not in private_session_locks:
        private_session_locks[key] = asyncio.Lock()
    return private_session_locks[key]

def get_group_lock(key: Tuple[int,str]):
    if key not in group_session_locks:
        group_session_locks[key] = asyncio.Lock()
    return group_session_locks[key]

def get_private_session_path(user_id, attempt_id):
    return os.path.join(SESSION_DIR, f"private_{user_id}_{attempt_id}.json")

def get_group_session_path(chat_id, quiz_id):
    safe_quiz_id = re.sub(r'[^a-zA-Z0-9_]', '', str(quiz_id))
    return os.path.join(SESSION_DIR, f"group_{chat_id}_{safe_quiz_id}.json")

async def read_session_file(path, lock: asyncio.Lock):
    async with lock:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if 'started_at' in data and isinstance(data['started_at'], str):
                    try:
                        data['started_at'] = datetime.fromisoformat(data['started_at'])
                    except Exception:
                        data['started_at'] = datetime.utcnow()
                return data
        except Exception:
            traceback.print_exc()
            return None

async def write_session_file(path, session_data, lock: asyncio.Lock):
    async with lock:
        data_to_write = session_data.copy()
        if 'started_at' in data_to_write and isinstance(data_to_write['started_at'], datetime):
            data_to_write['started_at'] = data_to_write['started_at'].isoformat()
        data_to_write.pop('auto_task', None)
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data_to_write, f, ensure_ascii=False, indent=2)
        except Exception:
            traceback.print_exc()

async def delete_session_file(path, key, lock_dict, task_dict=None):
    lock = lock_dict.get(key)
    if lock:
        async with lock:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Error removing session file {path}: {e}")
    elif os.path.exists(path):
        try:
            os.remove(path)
        except Exception as e:
            print(f"Error removing orphaned session file {path}: {e}")

    if task_dict is not None:
        task = task_dict.pop(key, None)
        if task:
            try:
                task.cancel()
            except Exception:
                pass
    lock_dict.pop(key, None)

# --- PARSING FUNCTIONS ---
# +++ THIS IS THE NEW, FIXED CODE +++
def parse_format2_enhanced(txt: str):
    questions = []
    blocks = re.split(r'(?m)^\s*(?=\d+\.)', txt.strip())
    option_regex = re.compile(r'^\s*\([a-zA-Z]\)\s*', re.MULTILINE)
    ex_regex = re.compile(r'(?i)^ex:\s*', re.MULTILINE)
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        option_match = option_regex.search(block)
        ex_match = ex_regex.search(block)
        split_pos = -1
        if option_match:
            split_pos = option_match.start()
        if ex_match:
            if split_pos == -1 or ex_match.start() < split_pos:
                split_pos = ex_match.start()
        if split_pos == -1:
            continue
        raw_question_part = block[:split_pos]
        options_part = block[split_pos:]
        qtext_no_num = re.sub(r'^\s*\d+\.\s*', '', raw_question_part, 1)
        qtext_final = re.sub(r'^\s*(\[\d+/\d+\]\s*)+', '', qtext_no_num).strip()
        if not qtext_final:
            continue
        opts = []
        correct = -1
        explanation_buffer = []
        parsing_explanation = False
        for l in options_part.splitlines():
            l_stripped = l.strip()
            if parsing_explanation:
                explanation_buffer.append(l_stripped)
                continue
            if ex_regex.match(l_stripped):
                parsing_explanation = True
                first_ex_line = ex_regex.sub('', l_stripped).strip()
                if first_ex_line:
                    explanation_buffer.append(first_ex_line)
            elif option_regex.match(l_stripped):
                opt_text = re.sub(r'^\s*\([a-zA-Z]\)\s*', '', l_stripped).strip()
                if "✅" in opt_text or "✅️" in opt_text:
                    opt_text = opt_text.replace("✅", "").replace("✅️", "").strip()
                    opts.append(opt_text)
                    correct = len(opts) - 1
                else:
                    opts.append(opt_text)
            elif l_stripped and opts:
                opts[-1] = opts[-1] + "\n" + l_stripped
        explanation = "\n".join(explanation_buffer).strip()
        if not opts or len(opts) < 2 or correct == -1:
            continue
        questions.append({
            "text": qtext_final,
            "options": opts,
            "correctIndex": correct,
            "explanation": explanation,
            "reference": ""
        })
    return questions

# +++ ADD THIS ENTIRE NEW FUNCTION +++
def parse_format_capital_dot(txt: str):
    """Parses formats like '1. Question\nA. Option 1\nB. Option 2 ✅\nEx: Explanation'"""
    questions = []
    # This regex splits before "1.", "2.", etc.
    blocks = re.split(r'(?m)^\s*(?=\d+\.)', txt.strip()) 
    
    # This is the new regex for options like "A."
    option_regex = re.compile(r'^\s*[A-Z]\.\s*', re.MULTILINE)
    # This regex for "Ex:" is the same as in other functions
    ex_regex = re.compile(r'(?i)^ex:\s*', re.MULTILINE) 
    
    for block in blocks:
        block = block.strip()
        if not block:
            continue
            
        option_match = option_regex.search(block)
        ex_match = ex_regex.search(block)
        
        # Find where the options/explanation starts
        split_pos = -1
        if option_match:
            split_pos = option_match.start()
        if ex_match:
            if split_pos == -1 or ex_match.start() < split_pos:
                split_pos = ex_match.start()
        if split_pos == -1:
            continue # Not a valid block, skip it
            
        # Split the block into question part and options part
        raw_question_part = block[:split_pos]
        options_part = block[split_pos:]
        
        # Clean up the question text (remove "1." and "[1/50]")
        qtext_no_num = re.sub(r'^\s*\d+\.\s*', '', raw_question_part, 1)
        qtext_final = re.sub(r'^\s*\[\d+/\d+\]\s*', '', qtext_no_num, 1).strip()
        if not qtext_final:
            continue
            
        opts = []
        correct = -1
        explanation_buffer = []
        parsing_explanation = False
        
        # Parse the options part line by line
        for l in options_part.splitlines():
            l_stripped = l.strip()
            
            if parsing_explanation:
                explanation_buffer.append(l_stripped)
                continue
                
            if ex_regex.match(l_stripped):
                parsing_explanation = True
                first_ex_line = ex_regex.sub('', l_stripped).strip()
                if first_ex_line:
                    explanation_buffer.append(first_ex_line)
            elif option_regex.match(l_stripped):
                # This is the key change: use the new regex to strip the prefix
                opt_text = re.sub(r'^\s*[A-Z]\.\s*', '', l_stripped).strip()
                if "✅" in opt_text or "✅️" in opt_text:
                    opt_text = opt_text.replace("✅", "").replace("✅️", "").strip()
                    opts.append(opt_text)
                    correct = len(opts) - 1
                else:
                    opts.append(opt_text)
            elif l_stripped and opts:
                # This handles multi-line options
                opts[-1] = opts[-1] + "\n" + l_stripped
                
        explanation = "\n".join(explanation_buffer).strip()
        
        if not opts or len(opts) < 2 or correct == -1:
            continue # Skip if no options, or no correct answer
            
        questions.append({
            "text": qtext_final,
            "options": opts,
            "correctIndex": correct,
            "explanation": explanation,
            "reference": ""
        })
    return questions

def parse_format_dash(txt: str):
    questions = []
    blocks = re.split(r'(?m)^Q\d+:\s*', txt)
    for block in blocks:
        if not block.strip():
            continue
        lines = [l.strip() for l in block.strip().splitlines() if l.strip()]
        if not lines:
            continue
        qtext = lines[0]
        opts = []
        correct = -1
        explanation = ""
        for i, l in enumerate(lines[1:], start=1):
            if l.startswith("-"):
                option_text = l.lstrip("-").strip()
                has_tick = "✅" in option_text
                option_text = option_text.replace("✅", "").strip()
                opts.append(option_text)
                if has_tick:
                    correct = len(opts) - 1
            elif l.lower().startswith("ex:"):
                explanation = re.sub(r'(?i)^ex:\s*', '', l).strip()
        if not opts or correct == -1:
            continue
        questions.append({
            "text": qtext,
            "options": opts,
            "correctIndex": correct,
            "explanation": explanation,
            "reference": ""
        })
    return questions

def parse_format1(txt: str):
    questions = []
    chunks = re.split(r'(?m)^\s*\d+\.\s*', txt)
    chunks = [c.strip() for c in chunks if c.strip()]
    for chunk in chunks:
        m_def = re.split(r'\([a-zA-Z]\)', chunk, maxsplit=1)
        if len(m_def) < 2:
            continue
        definition = m_def[0].strip()
        opts = []
        correct = -1
        for match in re.finditer(r'\(([a-zA-Z])\)\s*(.*?)(?=(\([a-zA-Z]\)|Ex:|$))', chunk, flags=re.IGNORECASE | re.DOTALL):
            raw = match.group(2).strip()
            has_tick = '✅' in raw
            raw = raw.replace('✅','').strip()
            opts.append(raw)
            if has_tick:
                correct = len(opts)-1
        m_ex = re.search(r'Ex\s*:\s*[“"]?(.*?)[”"]?\s*$', chunk, flags=re.IGNORECASE | re.DOTALL | re.MULTILINE)
        explanation = m_ex.group(1).strip() if m_ex else ""
        if not opts or correct == -1:
            continue
        questions.append({
            "text": definition,
            "options": opts,
            "correctIndex": correct,
            "explanation": explanation,
            "reference": ""
        })
    return questions

def parse_format2_simple(txt: str):
    questions = []
    blocks = re.split(r'(?m)^\d+\.\s*', txt)
    for block in blocks:
        if not block.strip(): continue
        lines = [l.strip() for l in block.strip().splitlines() if l.strip()]
        if not lines: continue
        qtext = lines[0]
        opts = []; correct = -1
        explanation = ""
        parsing_explanation = False
        for i, l in enumerate(lines[1:]):
            if l.lower().startswith("ex:"):
                parsing_explanation = True
                explanation = re.sub(r'(?i)^ex:\s*', '', l).strip()
                continue
            if parsing_explanation:
                explanation += "\n" + l
                continue
            has_tick = '✅' in l
            l = l.replace('✅','').strip()
            if re.match(r'^[a-zA-Z]\)\s*', l.lower()):
                l = re.sub(r'^[a-zA-Z]\)\s*', '', l).strip()
            elif re.match(r'^\([a-zA-Z]\)\s*', l.lower()):
                 l = re.sub(r'^\([a-zA-Z]\)\s*', '', l).strip()
            opts.append(l)
            if has_tick:
                correct = len(opts)-1
        if not opts or correct == -1:
            continue
        questions.append({"text":qtext,"options":opts,"correctIndex":correct,"explanation":explanation.strip(),"reference":""})
    return questions

def parse_format3(txt: str):
    try:
        m = re.search(r'const\s+quizData\s*=\s*(\[.*\]);', txt, flags=re.S)
        if not m:
             m = re.search(r'const\s+quizData\s*=\s*({.*});', txt, flags=re.S)
             if not m:
                 return []
             obj = json.loads(m.group(1))
             return obj.get("questions",[])
        return json.loads(m.group(1))
    except Exception:
        return []

def parse_format4(txt: str):
    questions=[]
    blocks = re.split(r'\n\s*\n', txt.strip())
    for block in blocks:
        lines=[l.strip() for l in block.splitlines() if l.strip()]
        if len(lines) < 3: continue
        qtext=lines[0]
        opts=[];correct=-1
        explanation = ""
        opt_lines = lines[1:]
        ex_line_index = -1
        for i, l in enumerate(opt_lines):
             if l.lower().startswith("ex:"):
                 explanation = re.sub(r'(?i)^ex:\s*', '', l).strip()
                 ex_line_index = i
                 if i + 1 < len(opt_lines):
                     explanation += "\n" + "\n".join(opt_lines[i+1:])
                 break
        if ex_line_index != -1:
            opt_lines = opt_lines[:ex_line_index]
        for i,l in enumerate(opt_lines):
            has_tick='✅' in l
            l=l.replace('✅','').strip()
            opts.append(l)
            if has_tick: correct=i
        if not opts or correct == -1:
            continue
        questions.append({"text":qtext,"options":opts,"correctIndex":correct,"explanation":explanation,"reference":""})
    return questions

def parse_csv(path: str):
    questions = []
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                opts = []
                for i in range(1, 11):
                    val = row.get(f"Option {i}", "")
                    if val and val.strip():
                        opts.append(val.strip())
                try:
                    correct_idx = int(row.get("Correct Index", 0)) - 1
                except:
                    correct_idx = 0
                if correct_idx < 0 or correct_idx >= len(opts):
                    correct_idx = 0
                q_text = row.get("Question (Exam Info)", "") or row.get("Question", "")
                if not q_text.strip() or not opts:
                    continue
                questions.append({
                    "text": q_text.strip(),
                    "options": opts,
                    "correctIndex": correct_idx,
                    "explanation": row.get("Explanation", "").strip(),
                    "reference": ""
                })
    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        return []
    return questions

def detect_and_parse_strict(txt: str):
    res_f2_enhanced = parse_format2_enhanced(txt)
    if res_f2_enhanced:
        return res_f2_enhanced
    res_f_cap_dot = parse_format_capital_dot(txt)
    if res_f_cap_dot:
        return res_f_cap_dot    
    res_f4 = parse_format4(txt)
    if res_f4:
        return res_f4
    res_f_dash = parse_format_dash(txt)
    if res_f_dash:
        return res_f_dash
    res_f1 = parse_format1(txt)
    if res_f1:
        return res_f1
    res_f2_simple = parse_format2_simple(txt)
    if res_f2_simple:
        return res_f2_simple
    res_f3 = parse_format3(txt)
    if res_f3:
        return res_f3
    return []

# --- JSON & DB HELPER FUNCTIONS ---
def questions_to_json(qs):
    return json.dumps(qs, ensure_ascii=False)

def questions_from_json(s):
    return json.loads(s)

def get_creator_by_tg(tg_user_id):
    """Fetches a creator document from MongoDB by their Telegram User ID."""
    return creators_collection.find_one({"tg_user_id": tg_user_id})

def get_or_create_creator_by_tg(user):
    """
    Finds a creator by TG ID, or creates a new one if they don't exist.
    This uses an 'upsert' operation for efficiency.
    """
    username = user.username or ""
    display_name = ((user.first_name or "") + " " + (user.last_name or "")).strip()

    # Find the document and update it, or insert it if it doesn't exist (upsert)
    # $setOnInsert ensures we only set these values when the document is first created.
    creators_collection.update_one(
        {"tg_user_id": user.id},
        {
            "$set": {"username": username, "display_name": display_name},
            "$setOnInsert": {"is_admin": 0}
        },
        upsert=True
    )
    # Return the newly created or found document
    return creators_collection.find_one({"tg_user_id": user.id})

def generate_quiz_id(length=8):
    """
    Generates a unique random ID for a quiz, ensuring it doesn't already exist.
    The custom ID will be used as the primary key (_id) for the quizzes collection.
    """
    chars = string.ascii_letters + string.digits
    while True:
        quiz_id = ''.join(random.choices(chars, k=length))
        # Check if a document with this _id already exists
        if not quizzes_collection.find_one({"_id": quiz_id}):
            return quiz_id

def ensure_owner_exists(tg_user_id, username=None, display_name=None):
    """Ensures the bot owner is registered as an admin in the database."""
    global OWNER_ID
    if OWNER_ID == 0:
        OWNER_ID = tg_user_id
    
    # Use an upsert to create the owner if they don't exist, or update their admin status
    creators_collection.update_one(
        {"tg_user_id": OWNER_ID},
        {
            "$set": {
                "username": username or "",
                "display_name": display_name or "",
                "is_admin": 1
            }
        },
        upsert=True
    )

# --- COMMAND HANDLERS ---
@check_ban
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles the /start command in both private and group chats.
    - In private with payload: Starts a private quiz.
    - In groups with payload: Starts a group quiz.
    - Otherwise: Shows a welcome message.
    """
    chat_type = update.effective_chat.type
    user = update.effective_user

    # Check if the command has arguments (a payload like a quiz_id)
    if context.args:
        quiz_id = context.args[0]
        
        # Scenario 1: Deep link used in a group chat
        if chat_type in ['group', 'supergroup']:
            await start_quiz_in_group(context.bot, update.effective_chat.id, quiz_id, starter_id=user.id)
            return
            
        # Scenario 2: Deep link used in a private chat
        if chat_type == 'private':
            await take_quiz_private(context.bot, user.id, quiz_id)
            return

    # Scenario 3: Normal /start command with no deep link payload
    # This part will now only run if the command is not a deep link.
    uid = user.id
    uname = user.username or ""
    fullname = ((user.first_name or "") + " " + (user.last_name or "")).strip()
    if OWNER_ID == 0:
        ensure_owner_exists(uid, uname, fullname)
    
    is_owner = uid == OWNER_ID
    
    text = (
        "👋 <b>Welcome to our bot !</b> 🤖\n\n"
        "Your ultimate quiz creation and playing companion 🎯\n\n"
        "Here's everything you can do:\n\n"
        
        "────────────────────\n"
        "<b>🎨 QUIZ CREATION & MANAGEMENT</b>\n"
        "&#9433; /create - Create a new quiz interactively\n"
        "&#128218; /myquizzes - View all your quizzes  \n"
        "&#9881; /manage - Manage quizzes (delete, export)\n"
        "<blockquote>&#9999; /edit quiz_id - Edit quiz details and questions</blockquote>\n"
        "<blockquote>&#128260; /post quiz_id - Share quiz card in this chat</blockquote>\n"
        "<blockquote>&#9200; /schedule quiz_id - Schedule quiz for groups</blockquote>\n\n"

        "────────────────────\n"
        "<b>🎮 PLAYING & SHARING</b>\n"
        "<blockquote>🎯 /take quiz_id - Start a quiz in private chat</blockquote>\n"
        "<blockquote>&#128260; /post quiz_id - Share quiz card in this chat</blockquote>\n"
        "<blockquote>&#9200; /schedule quiz_id - Schedule quiz for groups</blockquote>\n\n"

        "────────────────────\n"
        "<b>🔧 CONTROLS & SETTINGS</b> \n"
        "&#127937; /finish - End your active private quiz\n"
        "&#9889; /fast or &#128034; /slow - Adjust timer (group admins)\n"
        "<blockquote>&#128272; /set_password password - Set web panel password</blockquote>\n"
        "❌ /cancel - Cancel current operation\n"
        "<blockquote>📝 /set_promo message - Set promotional message</blockquote>\n"
    )

    # Conditionally add the admin section if the user is the owner
    if is_owner:
        admin_section = (
            "\n────────────────────\n"
            "<b>🛡️ ADMIN COMMANDS (Owner Only)</b>\n"
            "<blockquote>&#128081; /addauth user_id - Grant premium status</blockquote>\n"
            "<blockquote>&#128721; /rmauth user_id - Remove premium status</blockquote>\n"
            "<blockquote>&#9940; /ban user_id reason - Ban user from bot</blockquote>\n"
            "<blockquote>✅ /unban user_id - Unban user</blockquote>\n"
            "📊 /status - View bot statistics\n"
            "&#128260; /backup - Create database backup\n"
            "&#128229; /restore - Restore from backup\n"
        )
        text += admin_section

    # Add the final tip and closing line
    text += (
        "\n────────────────────\n"
        "💡 Tip: Use /create to build your first quiz in minutes!\n"
        "🔧 Need help? Just type any command for guided assistance.\n\n"
        "🎲 Ready to challenge your friends? Let's get started! 🚀"
    )   
    # DEFINE THE KEYBOARD WITH YOUR CHANNEL LINK
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📩join for more updates📩", url="https://t.me/studyhelppdf")]
    ])
    
    # SEND THE MESSAGE WITH THE KEYBOARD
    await update.effective_message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

async def check_membership(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """Checks if a user is a member of the force-sub channel."""
    try:
        member = await context.bot.get_chat_member(chat_id=FORCESUB_CHANNEL_ID, user_id=user_id)
        if member.status in ['creator', 'administrator', 'member']:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking membership for {user_id} in {FORCESUB_CHANNEL_ID}: {e}")
        # If the bot is not an admin in the channel, it will fail.
        # Fail safely by assuming they are not a member.
        return False    

@check_ban
async def cancel_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /cancel command to abort any ongoing flows."""
    uid = update.effective_user.id
    create_key = (uid, "create")
    edit_key = (uid, "edit") # Added this line
    schedule_key = (uid, update.effective_chat.id, "schedule")

    if create_key in ongoing_sessions:
        del ongoing_sessions[create_key]
        await update.effective_message.reply_text("✅ Quiz creation has been cancelled.")
    elif edit_key in ongoing_sessions: # Added this block
        del ongoing_sessions[edit_key]
        await update.effective_message.reply_text("✅ Quiz editing has been cancelled.")
    elif schedule_key in ongoing_sessions:
        del ongoing_sessions[schedule_key]
        await update.effective_message.reply_text("✅ Quiz scheduling has been cancelled.")
    else:
        await update.effective_message.reply_text("You have no ongoing process to cancel.")

@check_ban
async def create_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    
    is_member = await check_membership(context, uid)
    
    if is_member:        
        state = {"flow": "create_quiz", "step": "title"}
        ongoing_sessions[(uid, "create")] = state
        
        html_text = "<blockquote>✅ You are verified! Let's create a new quiz.</blockquote>\n\nSend the <b>Quiz Title</b>:"
        await update.effective_message.reply_text(html_text, parse_mode=ParseMode.HTML)
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➡️ Join Channel", url=FORCESUB_CHANNEL_LINK)],
            [InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")]
        ])
        await update.effective_message.reply_text(
            "⚠️ **You must join our channel to use this command.**\n\n"
            "Please join the channel and then click 'Verify Join'.",
            reply_markup=keyboard
        )


@check_ban
async def done_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /done command during the questions step of quiz creation."""
    if update.effective_chat.type != 'private':
        return

    uid = update.effective_user.id
    key = (uid, "create")
    
    if key not in ongoing_sessions:
        return

    state = ongoing_sessions[key]

    if state.get("step") == "questions":
        if not state.get("questions"):
            await update.effective_message.reply_text("❌ No questions found. Send at least one question in the required format or upload a .txt file.")
            return
        
        state["step"] = "images"
        await update.effective_message.reply_text(
            "✅ Questions saved.\n\n"
            "Do you want to add images to the questions?\n"
            "If yes, send an image with the **question number** as the caption (e.g., caption `1` for the first question).\n\n"
            "Send /no_images when you are finished adding images or to skip this step."
        )

@check_ban
async def no_images_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /no_images command, finalizing quiz creation and saving to MongoDB."""
    if update.effective_chat.type != 'private':
        return

    uid = update.effective_user.id
    key = (uid, "create")
    
    if key not in ongoing_sessions:
        return

    state = ongoing_sessions[key]

    if state.get("step") == "images":
        creator = get_or_create_creator_by_tg(update.effective_user)
        quiz_id = generate_quiz_id()

        # Prepare the quiz document to be inserted into MongoDB
        quiz_document = {
            "_id": quiz_id,
            "title": state["title"],
            "creator_tg_id": creator["tg_user_id"], # Link to creator by their telegram ID
            "time_per_question_sec": state.get("time_per_question_sec", 30),
            "negative_mark": state.get("negative", 0.0),
            "created_at": datetime.utcnow(),
            "questions": state["questions"] # Embed the questions array directly
        }
        
        # Insert the single document into the collection
        quizzes_collection.insert_one(quiz_document)
        
        del ongoing_sessions[key]
        await post_quiz_card(context.bot, update.effective_chat.id, quiz_id, context)

async def create_flow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != 'private':
        return
    uid = update.effective_user.id
    key = (uid, "create")
    if key not in ongoing_sessions:
        return
    state = ongoing_sessions[key]
    if update.message and (update.message.document or update.message.photo) and state.get("step") == "questions":
        return
    text = (update.message.text or "").strip() if update.message and update.message.text else ""
    if not text:
        return
    
    # State: Title
    if state["step"] == "title":
        state["title"] = text
        state["step"] = "time_per_q"
        # UPDATED: Use HTML blockquote and ParseMode.HTML
        html_text = "<blockquote>Saved title.</blockquote>\n\nNow send <b>time per question in seconds</b> (integer):"
        await update.effective_message.reply_text(html_text, parse_mode=ParseMode.HTML)
        return
    
    # State: Time per Question
    if state["step"] == "time_per_q":
        try:
            secs = int(text)
            if secs <= 0:
                raise ValueError
            state["time_per_question_sec"] = secs
        except:
            await update.effective_message.reply_text("❌ Please send a valid positive integer for seconds.")
            return
        state["step"] = "negative"
        # UPDATED: Use HTML blockquote and ParseMode.HTML
        html_text = "<blockquote>Saved time.</blockquote>\n\nNow send <b>negative marks per wrong answer</b> (e.g., <code>0.25</code> or <code>0</code> for none):"
        await update.effective_message.reply_text(html_text, parse_mode=ParseMode.HTML)
        return

    # State: Negative Marking
    if state["step"] == "negative":
        try:
            neg = float(text)
            if neg < 0:
                raise ValueError
            state["negative"] = neg
        except:
            await update.effective_message.reply_text("❌ Please send a valid non-negative number for negative marks (e.g., 0.25).")
            return
        state["step"] = "questions"
        state["questions"] = []
        # UPDATED: Use HTML blockquote and ParseMode.HTML for instructions
        html_instructions = (
            "<blockquote>"
            "Now send questions one by one in this exact format OR upload a <code>.txt</code> file with many questions in the same format:\n\n"
            "1. Question text (can be multiple lines)\n"
            "(a) option1 (can be multiple lines)\n"
            "(b) option2 ✅\n"
            "(c) option3\n"
            "Ex: Optional explanation text (can be multiple lines)\n\n"
            "Send /done when finished."
            "</blockquote>"
        )
        # Note: We must use ParseMode.HTML here as well
        await update.effective_message.reply_text(html_instructions, parse_mode=ParseMode.HTML)
        return

        # --- REPLACE THE BLOCK ABOVE WITH THIS ---

    # State: Questions
    if state["step"] == "questions":
        parsed = detect_and_parse_strict(text) # <-- THE FIX
        if not parsed:
            # Updated error message
            await update.effective_message.reply_text("❌ Could not parse the question. Make sure it matches one of the supported formats (e.g., (a) options, or A. options).")
            return
        state["questions"].extend(parsed)
        await update.effective_message.reply_text(f"✅ Saved {len(parsed)} question(s). Total so far: {len(state['questions'])}. Send next or /done.")
        return

    # State: Images
    if state["step"] == "images":
        # This part now only handles unexpected text. /no_images is a command.
        await update.effective_message.reply_text("Please send an image with a question number as the caption, or send /no_images to finish creating the quiz.")
        return

@check_ban
async def private_conversation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Routes all private text messages to the correct flow handler based on the user's session."""
    user_id = update.effective_user.id

    # Check for an ongoing CREATE session
    create_key = (user_id, "create")
    if create_key in ongoing_sessions:
        await create_flow_handler(update, context)
        return

    # Check for an ongoing EDIT session
    edit_key = (user_id, "edit")
    if edit_key in ongoing_sessions:
        # Calls the imported edit_flow_handler from helper.py
        await edit_flow_handler(
            update,
            context,
            quizzes_collection,
            ongoing_sessions,
            detect_and_parse_strict
        )
        return

async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.photo:
        return
    uid = update.effective_user.id
    key = (uid, "create")

    if key in ongoing_sessions and ongoing_sessions[key].get("step") == "images":
        state = ongoing_sessions[key]
        caption = (msg.caption or "").strip()
        if not caption.isdigit():
            await msg.reply_text("❌ Invalid caption. Please send a valid question number (e.g., '1', '2', etc.).")
            return
        
        q_num = int(caption)
        if not (1 <= q_num <= len(state.get("questions", []))):
            await msg.reply_text(f"❌ Question number out of range. You have {len(state.get('questions',[]))} questions. Please provide a number between 1 and {len(state.get('questions',[]))}.")
            return
        
        file_id = msg.photo[-1].file_id
        state["questions"][q_num - 1]["image_file_id"] = file_id
        await msg.reply_text(f"✅ Image saved for question {q_num}. Send more images or /no_images to finish.")

@check_ban
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.document:
        return
    
    if msg.chat.type != 'private':
        return
    
    uid = update.effective_user.id
    create_key = (uid, "create")
    fname = (msg.document.file_name or "").lower()

    # Handle file upload during the /create flow (step: questions)
    if create_key in ongoing_sessions and ongoing_sessions[create_key].get("step") == "questions":
        
        # Check file type *before* downloading
        if not (fname.endswith(".txt") or fname.endswith(".html") or fname.endswith(".csv")):
            await msg.reply_text("❌ During quiz creation, only `.txt`, `.html`, or `.csv` files are supported.")
            return

        # File type is valid, now we can download
        file_obj = await msg.document.get_file()
        path = await file_obj.download_to_drive()
        
        try:
            parsed = []
            error_message = ""
            
            # Read the file data
            with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
                data = f.read()

            if fname.endswith(".txt"):
                parsed = detect_and_parse_strict(data)
                if not parsed: error_message = "❌ The .txt file did not match any formats."
            elif fname.endswith(".html"):
                parsed = parse_html_quiz(data)
                if not parsed: error_message = "❌ The .html file did not contain a valid 'quizData' object."
            elif fname.endswith(".csv"):
                # parse_csv expects a path, so we pass the path
                parsed = parse_csv(path)
                if not parsed: error_message = "❌ CSV parsing failed."
            
            if error_message:
                await msg.reply_text(f"{error_message} Please fix and resend.")
                return

            if parsed:
                ongoing_sessions[create_key].setdefault("questions", []).extend(parsed)
                total_q = len(ongoing_sessions[create_key]['questions'])
                await msg.reply_text(f"✅ Imported {len(parsed)} questions from the file. Total so far: {total_q}. Send more or /done.")

        finally:
            try: os.remove(path)
            except: pass
        return # We are done with the /create flow

    # Standalone upload logic (Only .csv files)
    
    # Check file type *before* downloading
    if not fname.endswith(".csv"):
        # It's not a /create session, and it's not a .csv file.
        # Ignore it silently.
        return

    # Now we know it's a standalone CSV, so we can proceed to download.
    file_obj = await msg.document.get_file()
    path = await file_obj.download_to_drive()
    creator = get_or_create_creator_by_tg(update.effective_user)
    
    try:
        parsed_questions = []
        error_msg = ""
        
        if fname.endswith(".csv"):
            parsed_questions = parse_csv(path)
            if not parsed_questions: error_msg = f"❌ Failed to parse {fname}: CSV format error or empty."
        
        if error_msg:
            await msg.reply_text(error_msg)
            return

        if parsed_questions:
            title = os.path.splitext(os.path.basename(fname))[0]
            quiz_id = generate_quiz_id()
            
            quiz_document = {
                "_id": quiz_id, "title": title, "creator_tg_id": creator["tg_user_id"],
                "time_per_question_sec": 30, "negative_mark": 0.0,
                "created_at": datetime.utcnow(), "questions": parsed_questions
            }
            
            quizzes_collection.insert_one(quiz_document)
            await msg.reply_text(f"✅ File Upload: Created 1 quiz ('{title}') with {len(parsed_questions)} questions. ID: `{quiz_id}`")
            return
            
    except Exception as e:
        await msg.reply_text(f"❌ Error processing document: {e}")
    finally:
        try: os.remove(path)
        except: pass

            
# --- REPLACE your old myquizzes_handler function with this one ---

@check_ban
async def myquizzes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    c = get_creator_by_tg(uid)
    if not c:
        await update.effective_message.reply_text("You haven't created any quizzes yet. Use /create to start.")
        return
        
    # Find all quizzes created by this user's tg_user_id
    cursor = quizzes_collection.find({"creator_tg_id": c["tg_user_id"]}).sort("created_at", -1)
    rows = list(cursor) # Convert cursor to a list
    
    text, kb = create_paginated_keyboard(rows, page=0, page_size=5, command_base="myquizzes")
    
    if kb:
        await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.effective_message.reply_text(text)

@check_ban
async def manage_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /manage command with a paginated list for quiz management."""
    uid = update.effective_user.id
    c = get_creator_by_tg(uid)
    if not c:
        await update.effective_message.reply_text("You haven't created any quizzes yet.")
        return
        
    cursor = quizzes_collection.find({"creator_tg_id": c["tg_user_id"]}).sort("created_at", -1)
    rows = list(cursor)
    
    text, kb = create_paginated_keyboard(rows, page=0, page_size=5, command_base="manage")
    
    if kb:
        await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.effective_message.reply_text(text)

# --- CALLBACK QUERY HANDLERS ---
async def verify_join_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the 'Verify Join' button click."""
    query = update.callback_query
    uid = query.from_user.id
    
    is_member = await check_membership(context, uid)
    
    if is_member:
        await query.answer("Verification successful!", show_alert=False)
        # Edit the message to confirm and then start the create flow
        await query.edit_message_text("✅ Thank you for joining! Let's continue.")
        
        state = {"flow": "create_quiz", "step": "title"}
        ongoing_sessions[(uid, "create")] = state
        await query.message.reply_text("✍️ Creating a new quiz. Send the *Quiz Title*:")
    else:
        await query.answer("You haven't joined the channel yet. Please join and try again.", show_alert=True)

async def view_quiz_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    quiz_id = query.data.split(":")[1]
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        await query.answer("Quiz not found", show_alert=True)
        return

    creator = creators_collection.find_one({"tg_user_id": q_doc["creator_tg_id"]})
    questions = q_doc.get("questions", [])
    
    preview = []
    for i, qobj in enumerate(questions[:10], start=1):
        preview.append(f"{i}. {qobj.get('text','')}\n  a) {qobj.get('options',[None])[0] if qobj.get('options') else ''}")
        
    creator_name = "N/A"
    if creator:
        creator_name = creator.get('username') or creator.get('display_name', 'N/A')

    txt = (f"Quiz ID: {quiz_id}\nTitle: {q_doc['title']}\n"
           f"Creator: {creator_name}\nQuestions: {len(questions)}\n\nPreview:\n" + "\n".join(preview))
           
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Delete 🚮", callback_data=f"deletequiz:{quiz_id}"), InlineKeyboardButton("Export♻️✅️", callback_data=f"exportquiz:{quiz_id}")],
        [InlineKeyboardButton("Share (post card)↗️➡️↘️", callback_data=f"postcard:{quiz_id}")]
    ])
    await query.message.reply_text(txt, reply_markup=kb)

async def delete_quiz_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    quiz_id = query.data.split(":")[1]
    
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        await query.answer("Not found", show_alert=True)
        return
        
    creator = get_creator_by_tg(uid)
    if not creator or (creator["is_admin"] != 1 and creator["tg_user_id"] != q_doc["creator_tg_id"]):
        await query.answer("No permission", show_alert=True)
        return
        
    # Deleting the single quiz document also deletes all its questions
    quizzes_collection.delete_one({"_id": quiz_id})
    
    await query.answer("Deleted", show_alert=True)
    await query.message.reply_text(f"✅ Quiz {quiz_id} deleted.")

async def export_quiz_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    quiz_id = query.data.split(":")[1]
    
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    questions = q_doc.get("questions", [])
    
    if not questions:
        await query.answer("No questions", show_alert=True)
        return
        
    lines = []
    for i, qobj in enumerate(questions, start=1):
        lines.append(f"{i}. {qobj.get('text','')}")
        for opt_idx, opt in enumerate(qobj.get("options", [])):
            mark = " ✅" if opt_idx == qobj.get("correctIndex", -1) else ""
            lines.append(f"({chr(97+opt_idx)}) {opt}{mark}")
        if qobj.get("explanation"):
            lines.append(f"Ex: {qobj.get('explanation')}")
        lines.append("")
        
    content = "\n".join(lines)
    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"quiz_{quiz_id}.txt"
    await query.message.reply_document(bio, caption=f"Exported quiz {quiz_id}")

async def postcard_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    quiz_id = query.data.split(":")[1]
    
    # FIXED: We now pass the 'context' object to post_quiz_card
    await post_quiz_card(context.bot, query.message.chat.id, quiz_id, context)

async def _generate_quiz_card_content(quiz_id: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, InlineKeyboardMarkup, int, int] | Tuple[None, None, None, None]:
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        return None, None, None, None
        
    total_q = len(q_doc.get("questions", []))
    # Escape user-provided data like the title
    title = escape_markdown(q_doc["title"] or f"Quiz {quiz_id}", version=2)
    time_per_q = q_doc.get('time_per_question_sec', 30)
    negative_marking = q_doc.get('negative_mark', 0.0)
    # Escape the period for MarkdownV2 compatibility
    negative_marking_str = str(negative_marking).replace('.', '\\.')

    base_lines = [
        f'💳 *Quiz Name:* {title}',
        f'\\#️⃣ *Questions:* {total_q}',
        f'⏰ *Timer:* {time_per_q} seconds',
        f'🆔 *Quiz ID:* `{quiz_id}`', # Code blocks don't need escaping
        f'🏴‍☠️ *\\-ve Marking:* {negative_marking_str}',
        '💰 *Type:* free'
    ]
    
    creator_mention_line = ""
    creator_tg_id = q_doc.get('creator_tg_id')
    if creator_tg_id:
        creator_doc = creators_collection.find_one({"tg_user_id": creator_tg_id})
        if creator_doc:
            # THE CRITICAL FIX: Escape the username and display name to handle
            # special characters (e.g., underscores in 'test_user') which
            # were causing the unclosed italic entity error.
            if creator_doc.get('username'):
                escaped_username = escape_markdown(creator_doc['username'], version=2)
                creator_mention_line = f"*Created by:* @{escaped_username}"
            elif creator_doc.get('display_name'):
                 escaped_name = escape_markdown(creator_doc['display_name'], version=2)
                 creator_mention_line = f"*Created by:* {escaped_name}"
    
    if creator_mention_line:
        base_lines.append(creator_mention_line)

    base_lines.append("\nTap a button below to start or share\\!")
    text = "\n".join(base_lines)

    bot_username = context.bot.username
    startgroup_link = f"https://t.me/{bot_username}?startgroup={quiz_id}"
    startprivate_link = f"https://t.me/{bot_username}?start={quiz_id}"
    
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚀 Start in a Group", url=startgroup_link),
            InlineKeyboardButton("👤 Start in Private", url=startprivate_link)
        ],
        [
            InlineKeyboardButton("🔗 Share Quiz", switch_inline_query=quiz_id)
        ]
    ])
    
    return text, kb, total_q, time_per_q

async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.inline_query.query
    if not query: return
    quiz_id = query.strip()
    results = []

    text, kb, total_q, time_per_q = await _generate_quiz_card_content(quiz_id, context)
    
    quiz_title = "Quiz"
    if text:
        q_doc = quizzes_collection.find_one({"_id": quiz_id}, {"title": 1})
        if q_doc: quiz_title = q_doc.get("title", "Quiz")

    if text and kb:
        results.append(
            InlineQueryResultArticle(
                id=quiz_id,
                title=quiz_title,
                description=f"{total_q} Questions | {time_per_q}s Timer | ID: {quiz_id}",
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=ParseMode.MARKDOWN_V2,
                ),
                reply_markup=kb,
            )
        )
    else:
        results.append(
            InlineQueryResultArticle(
                id=str(uuid.uuid4()),
                title="Quiz Not Found",
                description=f"No quiz exists with the ID: {quiz_id}",
                input_message_content=InputTextMessageContent(
                    f"❌ Could not find a quiz with ID: `{quiz_id}`"
                ),
            )
        )
    await update.inline_query.answer(results, cache_time=5)

async def post_quiz_card(bot, chat_id, quiz_id, context: ContextTypes.DEFAULT_TYPE):
    """
    Generates and sends a quiz card. Now requires the 'context' object
    to pass to the content generator for creating deep links.
    """
    # Pass the entire 'context' object to the helper function
    text, kb, _, __ = await _generate_quiz_card_content(quiz_id, context)
    if text and kb:
        await bot.send_message(chat_id, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        try:
            await bot.send_message(chat_id, "Quiz not found.")
        except Exception:
            pass

async def post_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /post command."""
    args = update.effective_message.text.split()
    if len(args) < 2:
        await update.effective_message.reply_text("Usage: /post <quiz_id>")
        return
    quiz_id = args[1]
    # This call passes the required 'context' to post_quiz_card
    await post_quiz_card(context.bot, update.effective_chat.id, quiz_id, context)

# --- PRIVATE QUIZ LOGIC ---
@check_ban
async def take_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.effective_message.text.split()
    if len(args) < 2:
        await update.effective_message.reply_text("Usage: /take <quiz_id>"); return
    quiz_id = args[1]
    await take_quiz_private(context.bot, update.effective_user.id, quiz_id)

async def take_quiz_private(bot, user_id, quiz_id):
    active_sessions = glob.glob(os.path.join(SESSION_DIR, f"private_{user_id}_*.json"))
    if active_sessions:
        try:
            await bot.send_message(user_id, "❌ You already have a quiz in progress. Please finish it with /finish before starting a new one.")
        except: pass
        return

    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        try: await bot.send_message(user_id, "Quiz not found.")
        except: pass
        return
        
    questions = q_doc.get("questions", [])
    if not questions:
        try: await bot.send_message(user_id, "No questions in quiz.")
        except: pass
        return

    username = ""
    started_at = datetime.utcnow()
    
    attempt_doc = {
        "quiz_id": quiz_id,
        "user_id": user_id,
        "username": username,
        "started_at": started_at
    }
    result = attempts_collection.insert_one(attempt_doc)
    attempt_id = str(result.inserted_id) # MongoDB's ID is an ObjectId

    session = {
        "quiz_id": quiz_id,
        "user_id": user_id,
        "attempt_id": attempt_id,
        "questions": questions,
        "answers": [-1]*len(questions),
        "current_q": 0,
        "started_at": datetime.utcnow(),
        "time_per_question_sec": int(q_doc.get("time_per_question_sec", 30)),
        "message_id": None,
        "chat_id": user_id,
    }
    session_key = (user_id, attempt_id)
    session_path = get_private_session_path(user_id, attempt_id)
    lock = get_private_lock(session_key)
    await write_session_file(session_path, session, lock)
    try:
        await bot.send_message(user_id, f"✅ Quiz started: {q_doc['title']}\nTime per question: {session['time_per_question_sec']} seconds.\nAnswer by tapping an option.")
    except: pass
    await send_question_for_session_private(bot, session_key)

async def send_question_for_session_private(bot, session_key):
    path = get_private_session_path(*session_key)
    lock = get_private_lock(session_key)
    session = await read_session_file(path, lock)
    if not session:
        return
    qidx = session["current_q"]
    if qidx < 0 or qidx >= len(session["questions"]):
        await finalize_attempt(bot, session_key, session)
        return
    q = session["questions"][qidx]

    # +++ THIS IS THE NEW LOGIC FOR PROMO MESSAGES +++
    # Check if the question number (qidx + 1) is a multiple of 10 and not the first question
    if (qidx + 1) % 10 == 0 and qidx > 0:
        quiz_doc = quizzes_collection.find_one({"_id": session["quiz_id"]})
        if quiz_doc and 'creator_tg_id' in quiz_doc:
            creator = creators_collection.find_one({"tg_user_id": quiz_doc['creator_tg_id']})
            if creator and 'promo_message' in creator:
                try:
                    await bot.send_message(
                        chat_id=session["chat_id"],
                        text=creator['promo_message']
                    )
                    await asyncio.sleep(1) # Small delay after sending the promo
                except Exception as e:
                    print(f"Failed to send promo message in private quiz: {e}")
    # +++ END OF NEW LOGIC +++

    total_questions = len(session["questions"])
    prefix = f"[{qidx + 1}/{total_questions}] "
    original_question_text = q.get("text", "")
    question_text_with_prefix = prefix + original_question_text

    image_file_id = q.get("image_file_id")
    if image_file_id:
        try:
            await bot.send_photo(chat_id=session["chat_id"], photo=image_file_id)
        except Exception as e:
            print(f"Failed to send photo for private quiz (will continue): {e}")

    explanation = q.get("explanation") or None
    poll_question_text = question_text_with_prefix

    if len(poll_question_text) > POLL_QUESTION_MAX_LENGTH:
        await bot.send_message(chat_id=session["chat_id"], text=question_text_with_prefix)
        poll_question_text = PLACEHOLDER_QUESTION

    sent = None
    for attempt in range(3):
        try:
            sent = await bot.send_poll(
                chat_id=session["chat_id"],
                question=poll_question_text,
                options=q.get("options"),
                type=Poll.QUIZ,
                correct_option_id=q.get("correctIndex", 0),
                open_period=session["time_per_question_sec"],
                is_anonymous=False,
                explanation=explanation,
                protect_content=True
            )
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to send private poll: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                print(f"All retries failed for private quiz question {qidx}. Skipping.")
                session["current_q"] += 1
                await write_session_file(path, session, lock)

                await send_question_for_session_private(bot, session_key)
                return

    if not sent:
        return

    session['poll_id'] = sent.poll.id
    session['message_id'] = sent.message_id if hasattr(sent, 'message_id') else None
    POLL_ID_TO_SESSION_MAP[sent.poll.id] = {"type": "private", "key": session_key}
    await write_session_file(path, session, lock)

    old_task = running_private_tasks.pop(session_key, None)
    if old_task:
        old_task.cancel()

    async def per_question_timeout_private():
        try:
            await asyncio.sleep(session["time_per_question_sec"] + 2)
            fresh_session = await read_session_file(path, lock)
            if fresh_session and fresh_session["current_q"] == qidx:
                await reveal_correct_and_advance_private(bot, session_key, qidx, timed_out=True)
        except asyncio.CancelledError:
            pass

    running_private_tasks[session_key] = asyncio.create_task(per_question_timeout_private())

    # For last question: Cancel +2s task and use +3s safety net (auto-finalize if still unanswered)
    if qidx == len(session["questions"]) - 1:
        # Cancel the standard +2s timeout for last question to avoid race
        main_task = running_private_tasks.pop(session_key, None)
        if main_task:
            main_task.cancel()

        async def auto_finalize_last_question():
            try:
                await asyncio.sleep(session["time_per_question_sec"] + 3)
                fresh_session = await read_session_file(path, lock)
                if (fresh_session and
                    fresh_session["current_q"] == qidx and
                    fresh_session["answers"][qidx] == -1):  # Explicitly check unanswered
                    await reveal_correct_and_advance_private(bot, session_key, qidx, timed_out=True)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Error in auto_finalize_last_question for {session_key}: {e}")

        asyncio.create_task(auto_finalize_last_question())
        
# --- POLL HANDLERS ---
async def poll_answer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    user = answer.user
    user_id = user.id
    poll_id = answer.poll_id
    option_ids = answer.option_ids
    if not option_ids:
        return
    chosen = option_ids[0]

    mapping = POLL_ID_TO_SESSION_MAP.get(poll_id)
    if not mapping:
        return

    session_type = mapping.get("type")
    session_key = mapping.get("key")

    if session_type == "private":
        try:
            user_id_key, attempt_id = session_key
            if user_id_key != user_id:
                 return
            
            lock = get_private_lock(session_key)
            session_path = get_private_session_path(user_id, attempt_id)
            session = await read_session_file(session_path, lock)
            
            if not session:
                return
            qidx = session["current_q"]
            if qidx < 0 or qidx >= len(session["questions"]):
                return
            if session["answers"][qidx] != -1:
                return
            
            session["answers"][qidx] = chosen
            await write_session_file(session_path, session, lock)
            
            await reveal_correct_and_advance_private(context.bot, session_key, qidx, chosen_idx=chosen)
        except Exception as e:
            print(f"Error handling private poll answer: {e}")
            traceback.print_exc()
        return

    elif session_type == "group":
        try:
            chat_id, quiz_id = session_key
            lock = get_group_lock(session_key)
            session_path = get_group_session_path(chat_id, quiz_id)
            session = await read_session_file(session_path, lock)

            if not session:
                return
            qidx = session["current_q"]
            if qidx < 0 or qidx >= len(session["questions"]):
                return

            p_data = session["participants"].get(str(user_id))
            if p_data is None:
                user_full_name = ((user.first_name or "") + " " + (user.last_name or "")).strip()
                p_data = {
                    "answers": [-1] * len(session["questions"]),
                    "start_time": time.time(),
                    "username": user_full_name or str(user_id)
                }
                session["participants"][str(user_id)] = p_data
            
            if p_data["answers"][qidx] != -1:
                return
            
            p_data["answers"][qidx] = chosen
            p_data["end_time"] = time.time()
            await write_session_file(session_path, session, lock)
        except Exception as e:
            print(f"Error handling group poll answer: {e}")
            traceback.print_exc()
        return

async def poll_update_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    poll = update.poll
    if not poll.is_closed:
        return
    
    mapping = POLL_ID_TO_SESSION_MAP.get(poll.id)
    if not mapping or mapping.get("type") != "private":
        return

    session_key = mapping.get("key")
    if not session_key:
        return

    path = get_private_session_path(*session_key)
    lock = get_private_lock(session_key)
    session = await read_session_file(path, lock)

    if not session:
        return

    if session.get("poll_id") == poll.id:
        await reveal_correct_and_advance_private(context.bot, session_key, session["current_q"], timed_out=True)

# --- ADVANCEMENT & FINALIZATION LOGIC ---
# +++ REPLACEMENT CODE +++
async def reveal_correct_and_advance_private(bot, session_key, qidx, chosen_idx=None, timed_out=False):
    path = get_private_session_path(*session_key)
    lock = get_private_lock(session_key)
    session = await read_session_file(path, lock)
    if not session:
        return

    # +++ ADD THIS CRITICAL CHECK +++
    # This prevents a race condition. If the user answers AND the poll times out,
    # this function might be called twice. We check if the quiz has already
    # advanced past the question index (`qidx`) this call was for.
    if session.get("current_q") != qidx:
        return # Do nothing, we've already moved on.
    
    if session.get("poll_id"):
        POLL_ID_TO_SESSION_MAP.pop(session["poll_id"], None)

    session["current_q"] += 1
    await write_session_file(path, session, lock)
    
    if session["current_q"] >= len(session["questions"]):
        await finalize_attempt(bot, session_key, session)
        return
        
    await send_question_for_session_private(bot, session_key)

async def finalize_attempt(bot, session_key, session_data):
    task = running_private_tasks.pop(session_key, None)
    if task: task.cancel()

    quiz_doc = quizzes_collection.find_one({"_id": session_data["quiz_id"]})
    negative = quiz_doc.get("negative_mark", 0.0) if quiz_doc else 0.0
    
    total_questions = len(session_data["questions"])
    correct, wrong, skipped = 0, 0, 0
    maxscore = total_questions

    for idx, q in enumerate(session_data["questions"]):
        correct_idx = q.get("correctIndex", -1)
        user_ans = session_data["answers"][idx]
        if user_ans == -1: skipped += 1
        elif user_ans == correct_idx: correct += 1
        else: wrong += 1

    score = max(0, correct - (wrong * negative))
    quiz_title = quiz_doc['title'] if quiz_doc else 'Quiz'

    results_text = f"""
📊 *Quiz Results: {quiz_title}*
✅ *Correct:* {correct}
❌ *Wrong:* {wrong}
⏭️ *Skipped:* {skipped}
📝 *Total Questions:* {total_questions}
🏆 *Score:* {score:.2f}/{maxscore}
📉 *Accuracy:* {(correct/total_questions)*100:.1f}%
⚖️ *Negative Marking:* {negative} per wrong answer
{"🎉 Excellent!" if correct/total_questions >= 0.8 else "👍 Good job!" if correct/total_questions >= 0.6 else "💪 Keep practicing!"}
"""

    finished_at = datetime.utcnow()
    # Update the attempt document in MongoDB
    attempts_collection.update_one(
        {"_id": ObjectId(session_data["attempt_id"])},
        {
            "$set": {
                "finished_at": finished_at,
                "answers_json": json.dumps(session_data["answers"]),
                "score": score,
                "max_score": maxscore
            }
        }
    )
    
    try:
        await bot.send_message(session_data["user_id"], results_text, parse_mode=ParseMode.MARKDOWN)
        total_quiz_time_sec = len(session_data["questions"]) * session_data.get("time_per_question_sec", 30)
        quiz_settings = {"totalTimeSec": total_quiz_time_sec, "negativeMark": negative}
        html_path = await generate_quiz_html(quiz_settings, session_data["questions"])
        if html_path:
            try:
                await bot.send_document(
                    session_data["user_id"],
                    document=open(html_path, "rb"),
                    caption="Here is an HTML file for you to practice this quiz again.",
                    filename=f"{session_data['quiz_id']}_practice.html"
                )
            finally: os.remove(html_path)
    except Exception as e:
        print(f"Error sending message or HTML file in finalize_attempt: {e}")
    
    path = get_private_session_path(*session_key)
    await delete_session_file(path, session_key, private_session_locks)

# --- REPLACE THIS ENTIRE FUNCTION ---

async def finish_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user_id = update.effective_user.id
    
    if chat.type == 'private':
        active_sessions = glob.glob(os.path.join(SESSION_DIR, f"private_{user_id}_*.json"))
        if not active_sessions:
            await update.effective_message.reply_text("You have no active quiz to finish.")
            return
        
        session_path = active_sessions[0]
        filename = os.path.basename(session_path)
        
        try:
            # --- THIS IS THE FIX ---
            # We split the filename to get the user ID and the attempt ID.
            parts = filename.replace("private_", "").replace(".json", "").split("_", 1) # Split only once
            
            # The first part is the user ID, which should be an integer.
            uid = int(parts[0])
            
            # The second part is the attempt_id, which is now a string from MongoDB.
            # We no longer try to convert it to an integer.
            attempt_id = parts[1] 
            
            session_key = (uid, attempt_id)
            # --- END OF FIX ---

        except (ValueError, IndexError): # Catch potential errors more specifically
            await update.effective_message.reply_text("Error identifying your session file. Could not finish.")
            if os.path.exists(session_path): 
                try:
                    os.remove(session_path)
                except Exception as e:
                    print(f"Failed to remove corrupt session file {session_path}: {e}")
            return
        
        lock = get_private_lock(session_key)
        session_data = await read_session_file(session_path, lock)
        if not session_data:
            await update.effective_message.reply_text("Could not read your session data. Cleaning up.")
            await delete_session_file(session_path, session_key, private_session_locks)
            return
        
        await update.effective_message.reply_text("Finishing your quiz now and calculating results...")
        await finalize_attempt(context.bot, session_key, session_data)
        
    else: # Group chat logic remains the same
        chat_id = chat.id
        quiz_id = None
        session_path = None
        
        if context.args:
            quiz_id = context.args[0]
            session_path = get_group_session_path(chat_id, quiz_id)
            if not os.path.exists(session_path):
                await update.effective_message.reply_text(f"No active quiz found with ID: `{quiz_id}`", parse_mode=ParseMode.MARKDOWN)
                return
        else:
            active_group_sessions = glob.glob(os.path.join(SESSION_DIR, f"group_{chat_id}_*.json"))
            if not active_group_sessions:
                await update.effective_message.reply_text("There is no active quiz in this group to finish.")
                return
            
            session_path = active_group_sessions[0]
            filename = os.path.basename(session_path)
            quiz_id = filename.replace(f"group_{chat_id}_", "").replace(".json", "")

        session_key = (chat_id, quiz_id)
        try:
            admins = await context.bot.get_chat_administrators(chat_id)
            admin_ids = {admin.user.id for admin in admins}
        except Exception:
            admin_ids = set()

        lock = get_group_lock(session_key)
        session_data = await read_session_file(session_path, lock)
        starter_id = session_data.get("starter_id") if session_data else None
        
        if user_id == starter_id or user_id in admin_ids or user_id == OWNER_ID:
            await update.effective_message.reply_text(f"Force-finishing quiz `{quiz_id}` and calculating results...", parse_mode=ParseMode.MARKDOWN)
            await group_finalize_and_export(context.bot, session_key)
        else:
            await update.effective_message.reply_text("You do not have permission. Only chat admins or the person who started the quiz can finish it.")

async def _adjust_quiz_speed(update: Update, context: ContextTypes.DEFAULT_TYPE, adjustment: int):
    """A helper function to adjust the quiz timer during a group session."""
    chat = update.effective_chat
    user_id = update.effective_user.id

    # 1. This command is for groups only.
    if chat.type not in ['group', 'supergroup']:
        return

    # 2. Check if a quiz is actually running in this group.
    chat_id = chat.id
    active_group_sessions = glob.glob(os.path.join(SESSION_DIR, f"group_{chat_id}_*.json"))
    if not active_group_sessions:
        return # No active quiz, so do nothing.
        
    session_path = active_group_sessions[0]
    filename = os.path.basename(session_path)
    quiz_id = filename.replace(f"group_{chat_id}_", "").replace(".json", "")
    session_key = (chat_id, quiz_id)

    # 3. Verify that the user is a chat admin.
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        admin_ids = {admin.user.id for admin in admins}
    except Exception:
        admin_ids = set() # Fail safely if bot can't get admin list.

    if user_id not in admin_ids and user_id != OWNER_ID:
        return # User is not an admin, so do nothing as requested.

    # 4. If we're here, the user is an admin and a quiz is running. Let's modify the session.
    lock = get_group_lock(session_key)
    session_data = await read_session_file(session_path, lock)
    if not session_data:
        return

    current_time = session_data.get("time_per_question_sec", 30)
    
    # Define min/max time to prevent issues
    MIN_TIME = 15 
    MAX_TIME = 100

    new_time = current_time + adjustment
    new_time = max(MIN_TIME, min(new_time, MAX_TIME)) # Clamp the value between min and max

    # If the time didn't change (e.g., it was already at the limit), inform the admin.
    if new_time == current_time:
        await update.effective_message.reply_text(f"Timer is already at its {'minimum' if adjustment < 0 else 'maximum'} limit of {current_time} seconds.")
        return

    # Update the session file with the new time
    session_data["time_per_question_sec"] = new_time
    await write_session_file(session_path, session_data, lock)

    await update.effective_message.reply_text(f"✅ Timer updated! The next question will have a **{new_time} second** timer.", parse_mode=ParseMode.MARKDOWN)

async def fast_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Decreases the timer for the current group quiz by 3 seconds."""
    await _adjust_quiz_speed(update, context, adjustment=-3)

async def slow_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Increases the timer for the current group quiz by 3 seconds."""
    await _adjust_quiz_speed(update, context, adjustment=+3)            

async def start_quiz_in_group(bot, chat_id: int, quiz_id: str, starter_id: int = None):
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        try: await bot.send_message(chat_id, "Quiz not found.")
        except: pass
        return
        
    questions = q_doc.get("questions", [])
    if not questions:
        try: await bot.send_message(chat_id, "No questions in quiz.")
        except: pass
        return

    active_group_sessions = glob.glob(os.path.join(SESSION_DIR, f"group_{chat_id}_*.json"))
    if active_group_sessions:
        await bot.send_message(chat_id, f"❌ A quiz is already running in this group. Please finish it with /finish before starting a new one.")
        return

    session_key = (chat_id, quiz_id)
    session_path = get_group_session_path(*session_key)
    session = {
        "quiz_id": quiz_id,
        "chat_id": chat_id,
        "questions": questions,
        "current_q": 0,
        "time_per_question_sec": int(q_doc.get("time_per_question_sec", 30)),
        "participants": {},
        "message_id": None,
        "starter_id": starter_id,
        "negative": float(q_doc.get("negative_mark", 0.0)),
        "title": q_doc["title"]
    }
    lock = get_group_lock(session_key)
    await write_session_file(session_path, session, lock)
    
    escaped_title = escape_markdown(q_doc['title'], version=2)
    message_text = (
        f"🎯 Quiz starting now: *{escaped_title}*\n"
        f"Time per question: {session['time_per_question_sec']}s\n"
        f"Everyone can answer using the quiz options\\. Results will be shown at the end\\."
    )
    
    await bot.send_message(chat_id, message_text, parse_mode=ParseMode.MARKDOWN_V2)
    await group_send_question(bot, session_key)
    
async def group_send_question(bot, session_key):
    path = get_group_session_path(*session_key)
    lock = get_group_lock(session_key)
    session = await read_session_file(path, lock)
    if not session:
        return
    qidx = session["current_q"]
    if qidx < 0 or qidx >= len(session["questions"]):
        await group_finalize_and_export(bot, session_key)
        return
    q = session["questions"][qidx]
    
    # +++ THIS IS THE NEW LOGIC FOR PROMO MESSAGES +++
    # Check if the question number (qidx + 1) is a multiple of 10 and not the first question
    if (qidx + 1) % 10 == 0 and qidx > 0:
        quiz_doc = quizzes_collection.find_one({"_id": session["quiz_id"]})
        if quiz_doc and 'creator_tg_id' in quiz_doc:
            creator = creators_collection.find_one({"tg_user_id": quiz_doc['creator_tg_id']})
            if creator and 'promo_message' in creator:
                try:
                    await bot.send_message(
                        chat_id=session["chat_id"],
                        text=creator['promo_message']
                    )
                    await asyncio.sleep(1) # Small delay after sending the promo
                except Exception as e:
                    print(f"Failed to send promo message in group quiz: {e}")
    # +++ END OF NEW LOGIC +++
    
    total_questions = len(session["questions"])
    prefix = f"[{qidx + 1}/{total_questions}] "
    original_question_text = q.get("text", "")
    question_text_with_prefix = prefix + original_question_text

    image_file_id = q.get("image_file_id")
    if image_file_id:
        try:
            await bot.send_photo(chat_id=session["chat_id"], photo=image_file_id)
        except Exception as e:
            print(f"Failed to send photo for group quiz (will continue): {e}")

    explanation = q.get("explanation") or None
    poll_question_text = question_text_with_prefix

    if len(poll_question_text) > POLL_QUESTION_MAX_LENGTH:
        await bot.send_message(chat_id=session["chat_id"], text=question_text_with_prefix)
        poll_question_text = PLACEHOLDER_QUESTION
        
    sent = None
    for attempt in range(3):
        try:
            sent = await bot.send_poll(
                chat_id=session["chat_id"],
                question=poll_question_text,
                options=q.get("options"),
                type=Poll.QUIZ,
                correct_option_id=q.get("correctIndex", 0),
                open_period=session["time_per_question_sec"],
                is_anonymous=False,
                explanation=explanation,
                protect_content=True
            )
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to send group poll: {e}")
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                print(f"All retries failed for group quiz question {qidx}. Skipping.")
                session["current_q"] += 1
                await write_session_file(path, session, lock)

                await group_send_question(bot, session_key)
                return
    
    if not sent:
        return

    session["poll_id"] = sent.poll.id
    session["message_id"] = sent.message_id if hasattr(sent, 'message_id') else None
    POLL_ID_TO_SESSION_MAP[sent.poll.id] = {"type": "group", "key": session_key}
    await write_session_file(path, session, lock)
    
    old_task = running_group_tasks.pop(session_key, None)
    if old_task:
        old_task.cancel()
    async def per_question_timeout():
        try:
            await asyncio.sleep(session["time_per_question_sec"] + 2)
            fresh_session = await read_session_file(path, lock)
            if fresh_session and fresh_session["current_q"] == qidx:
                await group_reveal_and_advance(bot, session_key, qidx, timed_out=True)
        except asyncio.CancelledError:
            pass
    running_group_tasks[session_key] = asyncio.create_task(per_question_timeout())

async def group_reveal_and_advance(bot, session_key, qidx, timed_out=False):
    path = get_group_session_path(*session_key)
    lock = get_group_lock(session_key)
    session = await read_session_file(path, lock)
    if not session:
        return
    
    if session.get("poll_id"):
        POLL_ID_TO_SESSION_MAP.pop(session["poll_id"], None)

    session["current_q"] += 1
    await write_session_file(path, session, lock)
    if session["current_q"] >= len(session["questions"]):
        await group_finalize_and_export(bot, session_key)
        return
    await group_send_question(bot, session_key)

async def group_finalize_and_export(bot, session_key):
    path = get_group_session_path(*session_key)
    lock = get_group_lock(session_key)
    session = await read_session_file(path, lock)
    if not session:
        return
        
    chat_id, quiz_id = session_key
    progress_msg = None
    stop_progress_event = asyncio.Event()
    
    try:
        progress_msg = await bot.send_message(chat_id, "🏁 The quiz has finished! Processing results... 0%")
        progress_task = asyncio.create_task(_update_progress_task(progress_msg, stop_progress_event))

        def format_duration(seconds):
            if seconds < 0: seconds = 0
            minutes, seconds_rem = divmod(int(seconds), 60)
            return f"{minutes}m {seconds_rem}s"
            
        participants = session["participants"]
        results = []
        negative = float(session.get("negative", 0.0))
        total_questions = len(session['questions'])

        for user_id_str, p_data in participants.items():
            correct, wrong, skipped = 0, 0, 0
            
            for idx, ans in enumerate(p_data["answers"]):
                if idx >= len(session["questions"]): continue
                correct_idx = session["questions"][idx].get("correctIndex", -1)
                if correct_idx == -1: continue
                
                if ans == -1:
                    skipped += 1
                elif ans == correct_idx:
                    correct += 1
                else:
                    wrong += 1

            score = (correct * 1.0) - (wrong * negative)
            duration = p_data.get("end_time", p_data.get("start_time", 0)) - p_data.get("start_time", 0)
            accuracy = (correct / total_questions) * 100 if total_questions > 0 else 0
            total_attempted = correct + wrong
            strike_rate = (correct / total_attempted) * 100 if total_attempted > 0 else 0
            
            results.append({
                "name": p_data.get("username", str(user_id_str)),
                "score": score,
                "duration": duration,
                "correct": correct,
                "wrong": wrong,
                "skipped": skipped,
                "accuracy": accuracy,
                "strike_rate": strike_rate
            })
        
        results.sort(key=lambda x: (x["score"], -x["duration"]), reverse=True)
        
        stop_progress_event.set()
        await progress_task

        quiz_title = escape_markdown(session.get("title", quiz_id), version=2)
        medals = ["🥇", "🥈", "🥉"]
        
        if not results:
            await bot.send_message(chat_id, f"🏁 The quiz '{quiz_title}' has finished\\!\n\nNo one participated in the quiz\\.", parse_mode=ParseMode.MARKDOWN_V2)
        else:
            CHUNK_SIZE = 20
            
            for i in range(0, len(results), CHUNK_SIZE):
                chunk = results[i:i + CHUNK_SIZE]
                msg_lines = []
                
                if i == 0:
                    msg_lines.append(f"🏆 Quiz *{quiz_title}* has ended\\!")
                    msg_lines.append(f"\n🎯 Top Performers:")

                separator = "\n" + "─" * 20 + "\n"

                for rank_in_chunk, res in enumerate(chunk):
                    total_rank = i + rank_in_chunk
                    prefix = medals[total_rank] if total_rank < len(medals) else f"{total_rank + 1}\\."
                    
                    participant_name = escape_markdown(res['name'], version=2)
                    
                    correct_str = f"✅ {res['correct']}"
                    wrong_str = f"❌ {res['wrong']}"
                    score_str = f"🎯 {f'{res['score']:.2f}'.replace('.', '\\.').replace('-', '\\-')}"
                    duration_str = f"⏱️ {format_duration(res['duration'])}"
                    accuracy_str = f"📊 {f'{res['accuracy']:.2f}'.replace('.', '\\.')}%"
                    strike_rate_str = f"🚀 {f'{res['strike_rate']:.2f}'.replace('.', '\\.')}%"
                    
                    skipped_str = ""
                    if res['skipped'] > 0:
                        # --- THIS IS THE FINAL FIX ---
                        skipped_str = f" \\| ⏯️ {res['skipped']}"
                    
                    line = (f"{prefix} {participant_name} \\| {correct_str} \\| {wrong_str} \\| {score_str} \\| "
                            f"{duration_str} \\| {accuracy_str} \\| {strike_rate_str}{skipped_str}")
                    
                    msg_lines.append(line)

                if i == 0:
                    header_and_first_line = msg_lines[0] + "\n\n" + msg_lines[2]
                    remaining_lines = msg_lines[3:]
                    final_chunk_message = header_and_first_line + separator + separator.join(remaining_lines)
                else:
                    final_chunk_message = separator.join(msg_lines)

                await bot.send_message(chat_id, final_chunk_message, parse_mode=ParseMode.MARKDOWN_V2)
                await asyncio.sleep(1)

        total_quiz_time_sec = len(session["questions"]) * session.get("time_per_question_sec", 30)
        quiz_settings = {"totalTimeSec": total_quiz_time_sec, "negativeMark": session.get("negative", 0.0)}
        html_path = await generate_quiz_html(quiz_settings, session["questions"])
        if html_path:
            try:
                await bot.send_document(
                    chat_id,
                    document=open(html_path, "rb"),
                    caption="Practice this quiz again with the attached HTML file.",
                    filename=f"{quiz_id}_practice.html"
                )
            finally:
                os.remove(html_path)

    except Exception as e:
        print(f"Error during group finalization in chat {chat_id}: {e}")
        error_details = traceback.format_exc()
        print(error_details)
        try:
            await bot.send_message(chat_id, "Sorry, an unexpected error occurred while generating results. The developer has been notified.")
        except Exception:
            pass 
        try:
            owner_report = (
                f"⚠️ **Quiz Bot Error Report** ⚠️\n\n"
                f"An error occurred in chat `{chat_id}` while finalizing quiz `{quiz_id}`.\n\n"
                f"**Error:**\n`{str(e)}`\n\n"
                f"**Full Traceback:**\n"
                f"```{error_details}```"
            )
            if len(owner_report) > 4096:
                owner_report = owner_report[:4090] + "...\n```"
            if OWNER_ID:
                await bot.send_message(OWNER_ID, owner_report, parse_mode=ParseMode.MARKDOWN)
        except Exception as owner_send_err:
            print(f"CRITICAL: Failed to send error report to owner: {owner_send_err}")
            
    finally:
        if progress_msg:
            try:
                await progress_msg.delete()
            except Exception:
                pass
        await delete_session_file(path, session_key, group_session_locks, running_group_tasks)
        
# --- BACKUP & RESTORE (MONGODB VERSION) ---
async def backup_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return
    msg = await update.effective_message.reply_text("Starting MongoDB backup process...")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # 1. Export collections to JSON files
            collections_to_backup = {
                "creators": creators_collection,
                "quizzes": quizzes_collection,
                "attempts": attempts_collection,
                "schedule": schedule_collection
            }
            for name, coll in collections_to_backup.items():
                with open(os.path.join(tmpdir, f"{name}.json"), "w") as f:
                    # We need a custom encoder for ObjectId and datetime
                    class MongoEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, ObjectId):
                                return {"$oid": str(obj)}
                            if isinstance(obj, datetime):
                                return {"$date": obj.isoformat()}
                            return json.JSONEncoder.default(self, obj)
                    
                    docs = list(coll.find({}))
                    json.dump(docs, f, cls=MongoEncoder)

            # 2. Backup images (logic is similar to before)
            q_docs = quizzes_collection.find({"questions.image_file_id": {"$exists": True}})
            unique_file_ids = set()
            for doc in q_docs:
                for q in doc.get("questions", []):
                    if "image_file_id" in q:
                        unique_file_ids.add(q["image_file_id"])

            image_map = {}
            if unique_file_ids:
                images_dir = os.path.join(tmpdir, "images")
                os.makedirs(images_dir)
                await msg.edit_text(f"Backing up DB collections and {len(unique_file_ids)} images...")
                for i, file_id in enumerate(unique_file_ids):
                    try:
                        file = await context.bot.get_file(file_id)
                        filename = f"{i}.jpg"
                        await file.download_to_drive(os.path.join(images_dir, filename))
                        image_map[file_id] = filename
                    except Exception as e:
                        print(f"Could not download file_id {file_id}: {e}")
            if image_map:
                with open(os.path.join(tmpdir, "image_map.json"), "w") as f:
                    json.dump(image_map, f)
            
            # 3. Create zip file and send
            zip_filename = f"mongo_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            zip_path = shutil.make_archive(zip_filename, 'zip', tmpdir)
            await update.effective_message.reply_document(
                document=open(zip_path, "rb"), caption="Here is your MongoDB backup file."
            )
            await msg.delete()
        except Exception as e:
            await msg.edit_text(f"❌ Backup failed: {e}")
            traceback.print_exc()
        finally:
            if 'zip_path' in locals() and os.path.exists(zip_path):
                os.remove(zip_path)

async def restore_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text("Please use this command when replying to a backup `.zip` file.")
        return
    doc = msg.reply_to_message.document
    if not doc.file_name.lower().endswith(".zip"):
        await msg.reply_text("The replied file is not a `.zip` file.")
        return
    
    status_msg = await msg.reply_text("Starting MongoDB restore... This will wipe existing data. Do not interrupt.")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            # 1. Download and extract zip
            zip_file = await doc.get_file()
            zip_path = os.path.join(tmpdir, "backup.zip")
            await zip_file.download_to_drive(zip_path)
            extract_dir = os.path.join(tmpdir, "extracted")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            # 2. Restore collections from JSON
            collections_to_restore = {
                "creators": creators_collection,
                "quizzes": quizzes_collection,
                "attempts": attempts_collection,
                "schedule": schedule_collection
            }
            await status_msg.edit_text("Wiping and restoring collections...")
            
            # Custom hook to parse MongoDB's extended JSON format
            def mongo_json_hook(dct):
                if "$oid" in dct: return ObjectId(dct["$oid"])
                if "$date" in dct: return datetime.fromisoformat(dct["$date"])
                return dct

            for name, coll in collections_to_restore.items():
                json_path = os.path.join(extract_dir, f"{name}.json")
                if os.path.exists(json_path):
                    coll.delete_many({}) # Wipe the collection
                    with open(json_path, 'r') as f:
                        docs = json.load(f, object_hook=mongo_json_hook)
                    if docs:
                        coll.insert_many(docs)

            # 3. Re-upload images and update DB
            await status_msg.edit_text("Collections restored. Now re-uploading images...")
            image_map_path = os.path.join(extract_dir, "image_map.json")
            if os.path.exists(image_map_path):
                with open(image_map_path, 'r') as f:
                    image_map = json.load(f)
                new_id_map = {}
                total = len(image_map)
                for i, (old_id, filename) in enumerate(image_map.items()):
                    await status_msg.edit_text(f"Re-uploading image {i+1}/{total}...")
                    image_path = os.path.join(extract_dir, "images", filename)
                    if os.path.exists(image_path):
                        with open(image_path, "rb") as pf:
                            sent = await context.bot.send_photo(OWNER_ID, pf)
                            new_id_map[old_id] = sent.photo[-1].file_id

                if new_id_map:
                    await status_msg.edit_text("Updating image references...")
                    for old_id, new_id in new_id_map.items():
                        # This update query is complex but efficient for nested arrays
                        quizzes_collection.update_many(
                            {"questions.image_file_id": old_id},
                            {"$set": {"questions.$[elem].image_file_id": new_id}},
                            array_filters=[{"elem.image_file_id": old_id}]
                        )

            await status_msg.edit_text("✅ MongoDB restore complete!")

        except Exception as e:
            await status_msg.edit_text(f"❌ Restore failed: {e}")
            traceback.print_exc()

def schedule_quiz_jobs(job_queue: JobQueue, schedule_id: int, chat_id: int, quiz_id: str, scheduled_time_utc: datetime):
    now_utc = datetime.now(pytz.utc)

    # Schedule Start Job
    start_delta = (scheduled_time_utc - now_utc).total_seconds()
    if start_delta > 0:
        job_queue.run_once(
            schedule_start_callback,
            start_delta,
            data={"chat_id": chat_id, "quiz_id": quiz_id, "schedule_id": schedule_id},
            name=f"start_{schedule_id}"
        )

    # Schedule Alert Job (5 mins before)
    alert_time_utc = scheduled_time_utc - timedelta(minutes=5)
    alert_delta = (alert_time_utc - now_utc).total_seconds()
    if alert_delta > 0:
        job_queue.run_once(
            schedule_alert_callback,
            alert_delta,
            data={"chat_id": chat_id, "quiz_id": quiz_id, "schedule_id": schedule_id},
            name=f"alert_{schedule_id}"
        )
async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == 'private':
        await update.message.reply_text("Scheduling only works in groups.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/schedule <quiz_id>`")
        return
    
    quiz_id = context.args[0]
    q_doc = quizzes_collection.find_one({"_id": quiz_id}, {"_id": 1})
    if not q_doc:
        await update.message.reply_text(f"Quiz with ID `{quiz_id}` not found.", parse_mode=ParseMode.MARKDOWN)
        return
    
    chat_id = chat.id
    user_id = update.effective_user.id
    key = (chat_id, user_id, "schedule")
    ongoing_sessions[key] = {"quiz_id": quiz_id, "step": "time"}
    await update.message.reply_text("Please provide the start time in **24-hour HH:MM format** (Indian Standard Time).", parse_mode=ParseMode.MARKDOWN)

async def schedule_flow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type == 'private': return
    
    user_id, chat_id = update.effective_user.id, chat.id
    key = (chat_id, user_id, "schedule")

    if key in ongoing_sessions:
        state = ongoing_sessions[key]
        if state.get("step") == "time":
            time_str = update.message.text.strip()
            try:
                hour, minute = map(int, time_str.split(':'))
                IST = pytz.timezone('Asia/Kolkata')
                now_ist = datetime.now(IST)
                scheduled_time_ist = now_ist.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if scheduled_time_ist <= now_ist:
                    scheduled_time_ist += timedelta(days=1)
                
                scheduled_time_utc = scheduled_time_ist.astimezone(pytz.utc)
                quiz_id = state['quiz_id']
                
                schedule_doc = {
                    "chat_id": chat_id,
                    "quiz_id": quiz_id,
                    "creator_id": user_id,
                    "scheduled_time_utc": scheduled_time_utc,
                    "status": "pending"
                }
                result = schedule_collection.insert_one(schedule_doc)
                schedule_id = str(result.inserted_id)
                
                schedule_quiz_jobs(context.job_queue, schedule_id, chat_id, quiz_id, scheduled_time_utc)
                
                await update.message.reply_text(f"✅ Quiz `{quiz_id}` scheduled for {scheduled_time_ist.strftime('%Y-%m-%d %H:%M')} IST.")
                
            except ValueError:
                await update.message.reply_text("Invalid time format. Please use HH:MM (e.g., 14:30).")
            except Exception as e:
                await update.message.reply_text(f"An error occurred: {e}")
            finally:
                del ongoing_sessions[key]

async def schedule_alert_callback(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id, quiz_id = job.data['chat_id'], job.data['quiz_id']
    q_doc = quizzes_collection.find_one({"_id": quiz_id}, {"title": 1})
    title = q_doc['title'] if q_doc else quiz_id
    await context.bot.send_message(chat_id, f"⏰ Reminder: Quiz '{title}' will start in 5 minutes!")

async def schedule_start_callback(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id, quiz_id, schedule_id = job.data['chat_id'], job.data['quiz_id'], job.data['schedule_id']
    
    schedule_collection.update_one({"_id": ObjectId(schedule_id)}, {"$set": {"status": "triggered"}})
    await start_quiz_in_group(context.bot, chat_id, quiz_id, starter_id=None)

async def load_schedules_on_startup(application: Application):
    print("Loading pending schedules from MongoDB...")
    job_queue = application.job_queue
    now_utc = datetime.now(pytz.utc)
    count = 0
    
    for row in schedule_collection.find({"status": "pending"}):
        try:
            scheduled_utc = row['scheduled_time_utc'].replace(tzinfo=pytz.utc)
            if scheduled_utc > now_utc:
                schedule_quiz_jobs(job_queue, str(row['_id']), row['chat_id'], row['quiz_id'], scheduled_utc)
                count += 1
            else:
                schedule_collection.update_one({"_id": row['_id']}, {"$set": {"status": "missed"}})
        except Exception as e:
            print(f"Failed to load schedule {row['_id']}: {e}")
            schedule_collection.update_one({"_id": row['_id']}, {"$set": {"status": "error"}})
    print(f"Successfully re-scheduled {count} pending jobs.")

def get_human_readable_size(size_bytes):
    """Converts a size in bytes to a human-readable format."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# ...

async def rmauth_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Owner Only) Removes premium status from a user."""
    user = update.effective_user
    if user.id != OWNER_ID:
        return # Silently ignore if not owner

    if not context.args:
        await update.message.reply_text("Usage: /rmauth <user_id>")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid numerical Telegram User ID.")
        return

    # Set is_premium to 0 to revoke access
    result = creators_collection.update_one(
        {"tg_user_id": target_user_id},
        {"$set": {"is_premium": 0}}
    )

    if result.matched_count > 0:
        await update.message.reply_text(f"✅ Premium access has been revoked for user `{target_user_id}`.")
    else:
        await update.message.reply_text(f"❌ Could not find a user with ID `{target_user_id}`.")


async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Owner Only) Displays detailed statistics about the bot and database."""
    if update.effective_user.id != OWNER_ID:
        return
        
    msg = await update.effective_message.reply_text("🔄 Gathering statistics, please wait...")

    try:
        # --- Database Stats ---
        db_stats = db.command('dbstats')
        db_size_bytes = db_stats.get('dataSize', 0)
        db_size_readable = get_human_readable_size(db_size_bytes)
        
        # --- Collection Stats ---
        total_users = creators_collection.count_documents({})
        total_quizzes = quizzes_collection.count_documents({})
        
        # --- Active Session Stats ---
        # We count the session files as they represent active quizzes
        active_private_quizzes = len(glob.glob(os.path.join(SESSION_DIR, "private_*.json")))
        active_group_quizzes = len(glob.glob(os.path.join(SESSION_DIR, "group_*.json")))
        
        # --- Formatting the Message ---
        status_text = (
            f"📊 **Quiz Bot Status Report** 📊\n\n"
            f"**Database Info**:\n"
            f"  - Database Name: `{DB_NAME}`\n"
            f"  - Data Size: `{db_size_readable}`\n\n"
            
            f"**Usage Statistics**:\n"
            f"  - Total Creators: `{total_users}`\n"
            f"  - Total Quizzes Created: `{total_quizzes}`\n\n"

            f"**Live Activity**:\n"
            f"  - Active Private Quizzes: `{active_private_quizzes}`\n"
            f"  - Active Group Quizzes: `{active_group_quizzes}`"
        )
        
        await msg.edit_text(status_text, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await msg.edit_text(f"❌ An error occurred while fetching stats: {e}")
        print(f"Error in /status handler: {e}")

async def ban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Owner Only) Bans a user from using the bot."""
    user = update.effective_user
    if user.id != OWNER_ID:
        return  # Silently ignore if not owner

    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /ban <user_id> <reason>")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid numerical Telegram User ID.")
        return

    reason = ' '.join(context.args[1:])
    
    # Add user to banned list
    result = banned_users_collection.update_one(
        {"user_id": target_user_id},
        {"$set": {"reason": reason, "banned_at": datetime.utcnow()}},
        upsert=True
    )

    if result.upserted_id or result.modified_count > 0:
        await update.message.reply_text(f"✅ User `{target_user_id}` has been banned.\nReason: {reason}")
    else:
        await update.message.reply_text("Could not ban user. No changes were made.")
        
async def unban_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Owner Only) Unbans a user."""
    user = update.effective_user
    if user.id != OWNER_ID:
        return  # Silently ignore if not owner

    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid numerical Telegram User ID.")
        return

    # Remove user from banned list
    result = banned_users_collection.delete_one({"user_id": target_user_id})

    if result.deleted_count > 0:
        await update.message.reply_text(f"✅ User `{target_user_id}` has been unbanned.")
    else:
        await update.message.reply_text(f"❌ User `{target_user_id}` was not found in the ban list.")        

async def on_startup(application: Application):
    """Starts userbots and forces a dialog refresh to fix 'Peer id invalid'."""
    print(f"🚀 Initializing {len(USERBOT_SESSIONS)} Userbots...")
    
    # Ensure ID is an integer
    try:
        target_id = int(POLL_LOG_CHANNEL_ID)
    except ValueError:
        print("❌ FATAL: POLL_LOG_CHANNEL_ID must be an integer.")
        return

    for i, session in enumerate(USERBOT_SESSIONS):
        try:
            client = PyroClient(
                f"ub_session_{i}",
                api_id=UB_API_ID,
                api_hash=UB_API_HASH,
                session_string=session,
                in_memory=True,
                no_updates=True
            )
            await client.start()
            
            # --- THE MAGIC FIX: FORCE DIALOG REFRESH ---
            print(f"   🔄 Userbot {i+1}: Refreshing chat list to find Log Channel...")
            found = False
            
            # Method 1: Direct fetch (Fastest)
            try:
                await client.get_chat(target_id)
                found = True
            except Exception:
                # Method 2: Iterate dialogs (Slower but guaranteed fix)
                try:
                    async for dialog in client.get_dialogs(limit=200):
                        if dialog.chat.id == target_id:
                            found = True
                            break
                except Exception as e:
                    print(f"      Error scanning dialogs: {e}")

            if found:
                print(f"   ✅ Userbot {i+1}: Access confirmed!")
                active_userbots.append(client)
            else:
                print(f"   ❌ Userbot {i+1}: Could NOT find channel {target_id} in chat list.")
                print("      👉 Please send a message to the Log Channel so it appears at the top!")
                # We add it anyway, but it might fail until it sees the channel
                active_userbots.append(client)
            # -------------------------------------------

        except Exception as e:
            print(f"❌ Failed to start Userbot {i+1}: {e}")

    if not active_userbots:
        print("⚠️ WARNING: No Userbots were started. Poll import will fail.")

    # Load schedules
    await load_schedules_on_startup(application)

async def on_shutdown(application: Application):
    """Stops all userbots."""
    print("🛑 Stopping Userbots...")
    for client in active_userbots:
        try:
            await client.stop()
        except:
            pass
# +++ END LIFECYCLE HANDLERS +++

def main():
    job_queue = JobQueue()
    
    # +++ MODIFIED: Register the startup/shutdown hooks here +++
    app = Application.builder() \
        .token(BOT_TOKEN) \
        .job_queue(job_queue) \
        .post_init(on_startup) \
        .post_shutdown(on_shutdown) \
        .build()
    
    # Command Handlers
    app.add_handler(CommandHandler(["start", "help"], start_handler))
    edit_cmd_handler = CommandHandler("edit", lambda upd, ctx: edit_command_handler(upd, ctx, quizzes_collection, ongoing_sessions, get_creator_by_tg))
    app.add_handler(edit_cmd_handler)
    app.add_handler(CommandHandler("create", create_command))
    app.add_handler(CommandHandler("done", done_command_handler))
    app.add_handler(CommandHandler("no_images", no_images_command_handler))
    app.add_handler(CommandHandler("myquizzes", myquizzes_handler))
    app.add_handler(CommandHandler("take", take_handler))
    app.add_handler(CommandHandler("manage", manage_handler))
    app.add_handler(CommandHandler("cancel", cancel_command_handler))
    app.add_handler(CommandHandler("post", post_command))
    app.add_handler(CommandHandler("addauth", add_auth_handler))
    app.add_handler(CommandHandler("rmauth", rmauth_handler))
    app.add_handler(CommandHandler("status", status_handler))
    app.add_handler(CommandHandler("fast", fast_command_handler))
    app.add_handler(CommandHandler("slow", slow_command_handler))
    app.add_handler(CommandHandler(["finish", "stop"], finish_command_handler))
    app.add_handler(CommandHandler("backup", backup_handler))
    app.add_handler(CommandHandler("set_password", set_password_handler))
    app.add_handler(CommandHandler("ban", ban_handler))
    app.add_handler(CommandHandler("unban", unban_handler))
    app.add_handler(CommandHandler("set_promo", set_promo_handler))
    app.add_handler(CommandHandler("restore", restore_handler))
    app.add_handler(CommandHandler("schedule", schedule_command))
    app.add_handler(InlineQueryHandler(inline_query_handler))
    
    # Message Handler
    app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, document_handler))
    app.add_handler(MessageHandler(filters.POLL & filters.ChatType.PRIVATE, poll_import_handler))
    app.add_handler(MessageHandler(filters.PHOTO & filters.ChatType.PRIVATE, photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND, private_conversation_handler))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, schedule_flow_handler))

    # Callback Query Handlers
    app.add_handler(CallbackQueryHandler(view_quiz_cb, pattern=r"^viewquiz:"))
    app.add_handler(CallbackQueryHandler(delete_quiz_cb, pattern=r"^deletequiz:"))
    app.add_handler(CallbackQueryHandler(paginated_quiz_list_cb, pattern=r"^(myquizzes_page:|manage_page:)"))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern=r"^noop$"))
    edit_cb_handler = CallbackQueryHandler(lambda upd, ctx: edit_quiz_cb_handler(upd, ctx, quizzes_collection, ongoing_sessions), pattern=r"^edit_quiz:")
    app.add_handler(edit_cb_handler)
    app.add_handler(CallbackQueryHandler(verify_join_cb, pattern=r"^verify_join$"))
    app.add_handler(CallbackQueryHandler(export_quiz_cb, pattern=r"^exportquiz:"))
    app.add_handler(CallbackQueryHandler(postcard_cb, pattern=r"^postcard:"))
    
    # Poll Handlers
    app.add_handler(PollAnswerHandler(poll_answer_handler))
    app.add_handler(PollHandler(poll_update_handler))

    print("Starting PTB Quiz Bot...")
    app.run_polling()

if __name__ == "__main__":
    main()
