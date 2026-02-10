import re 
import json 
import random
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown

async def edit_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, quizzes_collection, ongoing_sessions, get_creator_by_tg):
    """Starts the quiz editing flow."""
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text("Usage: /edit <quiz_id>")
        return

    quiz_id = context.args[0]
    
    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    if not q_doc:
        await update.message.reply_text("❌ Quiz not found.")
        return

    if q_doc.get("creator_tg_id") != user_id:
        creator = get_creator_by_tg(user_id)
        if not creator or creator.get("is_admin") != 1:
            await update.message.reply_text("❌ You don't have permission to edit this quiz.")
            return

    edit_key = (user_id, "edit")
    ongoing_sessions[edit_key] = {"flow": "edit", "quiz_id": quiz_id, "step": "menu"}
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📝 Edit Quiz Name", callback_data=f"edit_quiz:name:{quiz_id}"),
            InlineKeyboardButton("⏰ Edit Timer", callback_data=f"edit_quiz:timer:{quiz_id}")
        ],
        [
            InlineKeyboardButton("📉 Edit -ve Marking", callback_data=f"edit_quiz:negative:{quiz_id}"),
            InlineKeyboardButton("🔀 Shuffle Questions", callback_data=f"edit_quiz:shuffle:{quiz_id}")
        ],
        [InlineKeyboardButton("📖 Edit Questions", callback_data=f"edit_quiz:edit_q_select:{quiz_id}")],
        [
            InlineKeyboardButton("➕ Add Question", callback_data=f"edit_quiz:add_q:{quiz_id}"),
            InlineKeyboardButton("➖ Delete Question", callback_data=f"edit_quiz:del_q_select:{quiz_id}")
        ],
        [InlineKeyboardButton("Done Editing ✅", callback_data=f"edit_quiz:cancel:{quiz_id}")]
    ])
    
    await update.message.reply_text(
    f"✏️ Editing Quiz: *{escape_markdown(q_doc['title'], 2)}* (`{quiz_id}`)\n\n" # <-- Note the \ before ( and )
    "Select what you want to edit:",
    reply_markup=keyboard,
    parse_mode=ParseMode.MARKDOWN_V2
    )

async def edit_quiz_cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, quizzes_collection, ongoing_sessions):
    """Handles callbacks from the main quiz edit menu."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    _, action, quiz_id = query.data.split(":")
    
    edit_key = (user_id, "edit")
    if edit_key not in ongoing_sessions or ongoing_sessions[edit_key].get("quiz_id") != quiz_id:
        await query.edit_message_text("This editing session has expired. Please start again with /edit.")
        return

    state = ongoing_sessions[edit_key]

    if action == "shuffle":
        q_doc = quizzes_collection.find_one({"_id": quiz_id})
        questions = q_doc.get("questions", [])
        if questions:
            random.shuffle(questions)
            quizzes_collection.update_one({"_id": quiz_id}, {"$set": {"questions": questions}})
            await query.edit_message_text("✅ Questions have been shuffled!")
        else:
            await query.edit_message_text("❌ No questions to shuffle.")
        if edit_key in ongoing_sessions: del ongoing_sessions[edit_key]
        return
        
    if action == "cancel":
        if edit_key in ongoing_sessions: del ongoing_sessions[edit_key]
        await query.edit_message_text("✅ Editing finished.")
        return

    q_doc = quizzes_collection.find_one({"_id": quiz_id})
    q_count = len(q_doc.get("questions", []))
    
    prompts = {
        "name": "Please send the new name for the quiz.",
        "timer": "Please send the new time per question in seconds (e.g., 30).",
        "negative": "Please send the new negative mark per wrong answer (e.g., 0.25).",
        "add_q": "Please send the new question in the standard format. Send /done_editing when you are finished.",
        "edit_q_select": f"📚 There are {q_count} questions.\n\nSend the Question Number you want to edit:",
        "del_q_select": f"🗑️ There are {q_count} questions.\n\nSend the Question Number you want to delete:"
    }
    
    step_map = {
        "name": "editing_name", "timer": "editing_timer", "negative": "editing_negative",
        "add_q": "adding_question", "edit_q_select": "editing_question_num", "del_q_select": "deleting_question_num"
    }

    if action in prompts:
        state["step"] = step_map[action]
        await query.edit_message_text(prompts[action])

async def edit_flow_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, quizzes_collection, ongoing_sessions, detect_and_parse_strict):
    """Handles text-based replies during the quiz editing flow."""
    user_id = update.effective_user.id
    key = (user_id, "edit")
    if key not in ongoing_sessions: return
        
    state = ongoing_sessions[key]
    quiz_id = state["quiz_id"]
    text = (update.message.text or "").strip()

    async def finish_and_confirm(message_text):
        await update.message.reply_text(message_text)
        if key in ongoing_sessions: del ongoing_sessions[key]

    if state["step"] == "editing_name":
        quizzes_collection.update_one({"_id": quiz_id}, {"$set": {"title": text}})
        await finish_and_confirm(f"✅ Quiz name updated to: {text}")
    
    elif state["step"] == "editing_timer":
        try:
            new_time = int(text)
            if new_time < 10: raise ValueError("Timer too short")
            quizzes_collection.update_one({"_id": quiz_id}, {"$set": {"time_per_question_sec": new_time}})
            await finish_and_confirm(f"✅ Timer updated to {new_time} seconds per question.")
        except ValueError:
            await update.message.reply_text("❌ Invalid number. Please send a positive integer (e.g., 30).")

    elif state["step"] == "editing_negative":
        try:
            new_mark = float(text)
            if new_mark < 0: raise ValueError("Mark cannot be negative")
            quizzes_collection.update_one({"_id": quiz_id}, {"$set": {"negative_mark": new_mark}})
            await finish_and_confirm(f"✅ Negative marking updated to {new_mark}.")
        except ValueError:
            await update.message.reply_text("❌ Invalid number. Please send a non-negative number (e.g., 0 or 0.25).")
            
    elif state["step"] == "adding_question":
        if text.lower() == '/done_editing':
            return await finish_and_confirm("✅ Finished adding questions.")
        
        parsed = detect_and_parse_strict(text)
        if not parsed:
            return await update.message.reply_text("❌ Could not parse question. Please check the format.")
            
        quizzes_collection.update_one({"_id": quiz_id}, {"$push": {"questions": {"$each": parsed}}})
        q_doc = quizzes_collection.find_one({"_id": quiz_id})
        q_count = len(q_doc.get("questions", []))
        await update.message.reply_text(f"✅ Added {len(parsed)} question(s). Total is now {q_count}. Send more or /done_editing.")

    elif state["step"] == "editing_question_num":
        try:
            q_num = int(text)
            q_doc = quizzes_collection.find_one({"_id": quiz_id})
            questions = q_doc.get("questions", [])
            if not (1 <= q_num <= len(questions)):
                return await update.message.reply_text(f"❌ Invalid number. Please enter a number between 1 and {len(questions)}.")
            
            state["step"] = "editing_question_submit"
            state["q_index_to_edit"] = q_num - 1
            await update.message.reply_text(f"Please send the full, corrected text for question {q_num}.")
        except ValueError:
            await update.message.reply_text("❌ That's not a valid number. Please try again.")
            
    elif state["step"] == "editing_question_submit":
        parsed = detect_and_parse_strict(text)
        if not parsed or len(parsed) != 1:
            return await update.message.reply_text("❌ Could not parse question or format is incorrect. Please send one single, complete question.")
        
        q_index = state["q_index_to_edit"]
        new_question_obj = parsed[0]
        
        quizzes_collection.update_one({"_id": quiz_id}, {"$set": {f"questions.{q_index}": new_question_obj}})
        await finish_and_confirm(f"✅ Question {q_index + 1} has been updated.")
        
    elif state["step"] == "deleting_question_num":
        try:
            q_num = int(text)
            q_doc = quizzes_collection.find_one({"_id": quiz_id})
            questions = q_doc.get("questions", [])
            if not (1 <= q_num <= len(questions)):
                return await update.message.reply_text(f"❌ Invalid number. Please enter a number between 1 and {len(questions)}.")
            
            q_index = q_num - 1
            quizzes_collection.update_one({"_id": quiz_id}, {"$unset": {f"questions.{q_index}": 1}})
            quizzes_collection.update_one({"_id": quiz_id}, {"$pull": {"questions": None}})
            await finish_and_confirm(f"✅ Question {q_num} has been deleted.")
        except ValueError:
            await update.message.reply_text("❌ That's not a valid number. Please try again.")
            
def parse_html_quiz(html_content: str):
    """
    Parses an HTML file to extract quiz questions from an embedded 'quizData' JavaScript object.
    """
    try:
        # Regex to find the quizData object, capturing the JSON part.
        # It looks for 'const quizData =' and captures everything from '{' to the closing '};'
        match = re.search(r'const\s+quizData\s*=\s*({.*?});', html_content, flags=re.DOTALL)
        
        if not match:
            return [] # Return empty list if the pattern is not found

        # Extract the captured JSON string
        json_string = match.group(1)
        
        # Parse the JSON string into a Python dictionary
        data = json.loads(json_string)
        
        # Return the list of questions, or an empty list if 'questions' key doesn't exist
        return data.get("questions", [])
        
    except (json.JSONDecodeError, IndexError):
        # Handle cases where the regex matches but the content is not valid JSON,
        # or if the regex group doesn't exist.
        return []
    except Exception:
        # Catch any other unexpected errors
        return []
