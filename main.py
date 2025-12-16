import os
import time
import math
import asyncio
import re
import aiofiles
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified
from motor.motor_asyncio import AsyncIOMotorClient
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "0")) 
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
OWNER_ID = int(os.environ.get("OWNER_ID", "0")) # Only this ID can grant access

# --- DATABASE SETUP ---
mongo = AsyncIOMotorClient(MONGO_URL)
db = mongo["RenameBot"]
users_col = db["users"]

# --- BOT SETUP ---
app = Client("rename_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- GLOBAL VARS ---
DEFAULT_SUFFIX = " 🦋Vaiᡣ𐭩Su×@pglinsan2"
DEFAULT_MODE = "filename"
QUEUE = {} 
CURRENT_TASK = {} 

if not os.path.isdir("downloads"):
    os.makedirs("downloads")

# --- PROGRESS BAR & HELPERS ---

def humanbytes(size):
    if not size: return ""
    power = 2**10
    n = 0
    dic_powerN = {0: ' ', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + dic_powerN[n] + 'B'

def time_formatter(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {seconds}s"

async def progress_bar(current, total, status_msg, action_text, start_time):
    try:
        now = time.time()
        if (now - progress_bar.last_update_time) < 60 and current != total:
            return

        percentage = current * 100 / total
        elapsed_time = now - start_time
        speed = current / elapsed_time if elapsed_time > 0 else 1
        eta = (total - current) / speed if speed > 0 else 0
        
        bar_length = 10
        filled_length = int(bar_length * percentage / 100)
        bar = '■' * filled_length + '□' * (bar_length - filled_length)
        
        text = f"{action_text}\n\n" \
               f"**[{bar}]** {round(percentage, 2)}%\n" \
               f"⚡ **Speed:** {humanbytes(speed)}/s\n" \
               f"⏱️ **ETA:** {time_formatter(eta)}\n" \
               f"📦 **Size:** {humanbytes(current)} / {humanbytes(total)}"
               
        await status_msg.edit(text)
        progress_bar.last_update_time = now
    except MessageNotModified:
        pass
    except Exception:
        pass

progress_bar.last_update_time = 0

async def get_user(user_id):
    user = await users_col.find_one({"_id": user_id})
    if not user:
        user = {
            "_id": user_id, 
            "suffix": DEFAULT_SUFFIX, 
            "mode": DEFAULT_MODE, 
            "thumb": None, 
            "removal_words": [],
            "authorized": (user_id == OWNER_ID) # Owner is auto-authorized
        }
        await users_col.insert_one(user)
    return user

async def update_user(user_id, key, value):
    await users_col.update_one({"_id": user_id}, {"$set": {key: value}}, upsert=True)

# Helper to check auth
async def is_authorized(user_id):
    if user_id == OWNER_ID: return True
    user = await get_user(user_id)
    return user.get("authorized", False)



# --- ROBUST UPLOAD WRAPPER ---
async def upload_file(client, message, file_path, thumb_path, caption, duration, width, height, status_msg, start_time):
    retries = 3
    attempt = 0
    while attempt < retries:
        try:
            attempt += 1
            if file_path.lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".webm")):
                await client.send_video(
                    chat_id=message.chat.id,
                    video=file_path,
                    caption=caption,
                    thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None,
                    duration=duration,
                    width=width,
                    height=height,
                    supports_streaming=True,
                    progress=progress_bar,
                    progress_args=(status_msg, "⬆️ **Uploading...**", start_time)
                )
            else:
                await client.send_document(
                    chat_id=message.chat.id,
                    document=file_path,
                    thumb=thumb_path if thumb_path and os.path.exists(thumb_path) else None,
                    caption=caption,
                    force_document=True,
                    progress=progress_bar,
                    progress_args=(status_msg, "⬆️ **Uploading...**", start_time)
                )
            return 
        except FloodWait as e:
            await status_msg.edit(f"😴 **Sleeping for {e.value}s (FloodWait)...**")
            await asyncio.sleep(e.value)
            attempt -= 1
            continue
        except Exception as e:
            if attempt < retries:
                await status_msg.edit(f"⚠️ **Upload Failed. Retrying...**\nError: `{e}`")
                await asyncio.sleep(5)
            else:
                raise e

# --- HELPER: FILE NAME CLEANING ---
def get_metadata(file_path):
    try:
        parser = createParser(file_path)
        if not parser: return 0, 0, 0
        metadata = extractMetadata(parser)
        if not metadata: return 0, 0, 0
        duration = metadata.get("duration").seconds if metadata.has("duration") else 0
        width = metadata.get("width") if metadata.has("width") else 0
        height = metadata.get("height") if metadata.has("height") else 0
        return duration, width, height
    except:
        return 0, 0, 0

def clean_filename_text(text, removal_list):
    if not text: return "File"
    if removal_list:
        for word in removal_list:
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            text = pattern.sub("", text)
    text = re.sub(r'[\u0900-\u097F]+', '', text)
    text = re.sub(r'[\U0001f600-\U0001f64f]', '', text) 
    text = re.sub(r'[\U0001f300-\U0001f5ff]', '', text) 
    text = re.sub(r'[\U0001f680-\U0001f6ff]', '', text) 
    text = re.sub(r'[\U0001f1e0-\U0001f1ff]', '', text) 
    text = re.sub(r'[\U00002700-\U000027bf]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    if not text: text = "Media_File"
    return text

# --- NEW OWNER COMMANDS ---

@app.on_message(filters.command("auth") & filters.user(OWNER_ID))
async def auth_user(client, message):
    if len(message.command) != 2:
        return await message.reply_text("ℹ️ Usage: `/auth UserID`")
    try:
        user_id = int(message.command[1])
        await update_user(user_id, "authorized", True)
        await message.reply_text(f"✅ User `{user_id}` has been authorized.")
    except:
        await message.reply_text("❌ Invalid ID.")

@app.on_message(filters.command("unauth") & filters.user(OWNER_ID))
async def unauth_user(client, message):
    if len(message.command) != 2:
        return await message.reply_text("ℹ️ Usage: `/unauth UserID`")
    try:
        user_id = int(message.command[1])
        if user_id == OWNER_ID:
            return await message.reply_text("❌ Cannot unauthorize Owner.")
        await update_user(user_id, "authorized", False)
        await message.reply_text(f"🚫 User `{user_id}` access revoked.")
    except:
        await message.reply_text("❌ Invalid ID.")

# --- SETTINGS COMMANDS ---

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await is_authorized(message.from_user.id):
        return await message.reply_text(f"⛔ **Access Denied.**\nYour ID: `{message.from_user.id}`\nContact the owner to get access.")
    await message.reply_text("👋 **Welcome Back!**\nSend me a file to rename it.")

@app.on_message(filters.command("autoname") & filters.private)
async def set_autoname(client, message):
    if not await is_authorized(message.from_user.id): return
    await update_user(message.from_user.id, "mode", "filename")
    await message.reply_text("✅ **Mode Set:** Filename + Suffix")

@app.on_message(filters.command("autocaption") & filters.private)
async def set_autocaption(client, message):
    if not await is_authorized(message.from_user.id): return
    await update_user(message.from_user.id, "mode", "caption")
    await message.reply_text("✅ **Mode Set:** Caption as Filename")

@app.on_message(filters.command("suffix") & filters.private)
async def set_suffix(client, message):
    if not await is_authorized(message.from_user.id): return
    if len(message.command) < 2: return
    new_suffix = " " + message.text.split(None, 1)[1]
    await update_user(message.from_user.id, "suffix", new_suffix)
    await message.reply_text(f"✅ Suffix set to: `{new_suffix}`")

@app.on_message(filters.command("setremove") & filters.private)
async def set_remove_words(client, message):
    if not await is_authorized(message.from_user.id): return
    if len(message.command) < 2:
        await message.reply_text("ℹ️ Usage: `/setremove word1, word2`")
        return
    words_raw = message.text.split(None, 1)[1]
    words_list = [w.strip() for w in words_raw.split(",")]
    await update_user(message.from_user.id, "removal_words", words_list)
    await message.reply_text(f"✅ **Removal List Updated:**\n`{words_list}`")

@app.on_message(filters.command("resetremove") & filters.private)
async def reset_remove_words(client, message):
    if not await is_authorized(message.from_user.id): return
    await update_user(message.from_user.id, "removal_words", [])
    await message.reply_text("🗑️ **Removal List Cleared.**")

@app.on_message(filters.command("delthumb") & filters.private)
async def delete_thumbnail(client, message):
    if not await is_authorized(message.from_user.id): return
    await update_user(message.from_user.id, "thumb", None)
    await message.reply_text("🗑️ **Thumbnail Deleted.**")

@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client, message):
    if not await is_authorized(message.from_user.id): return
    await update_user(message.from_user.id, "thumb", message.photo.file_id)
    await message.reply_text("✅ **Thumbnail Saved!**")

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_process(client, message):
    if not await is_authorized(message.from_user.id): return
    user_id = message.from_user.id
    if user_id in CURRENT_TASK:
        try:
            CURRENT_TASK[user_id].cancel()
            del CURRENT_TASK[user_id]
            await message.reply_text("⏭️ **Skipped current file!**")
        except: pass
    else:
        await message.reply_text("No active process.")

@app.on_message(filters.command("clear") & filters.private)
async def clear_queue(client, message):
    if not await is_authorized(message.from_user.id): return
    user_id = message.from_user.id
    if user_id in QUEUE: QUEUE[user_id] = []
    await message.reply_text("🗑️ **Queue Cleared!**")

# --- QUEUE MANAGER ---
async def queue_handler(client, message):
    user_id = message.from_user.id
    if user_id not in QUEUE: QUEUE[user_id] = []
    QUEUE[user_id].append(message)
    if user_id not in CURRENT_TASK:
        task = asyncio.create_task(process_queue(client, user_id))
        CURRENT_TASK[user_id] = task
    else:
        q_len = len(QUEUE[user_id])
        if q_len % 5 == 0:
            temp_msg = await message.reply_text(f"🗂️ Added to Queue: **#{q_len}**")
            await asyncio.sleep(3)
            await temp_msg.delete()

async def process_queue(client, user_id):
    while user_id in QUEUE and len(QUEUE[user_id]) > 0:
        message = QUEUE[user_id][0] 
        try:
            await process_file_logic(client, message)
        except Exception as e:
            print(f"Task Failed: {e}")
            try: await message.reply_text(f"❌ Failed to process file: {e}")
            except: pass
        finally:
            if user_id in QUEUE and len(QUEUE[user_id]) > 0: QUEUE[user_id].pop(0) 
            await asyncio.sleep(1)

    if user_id in CURRENT_TASK: del CURRENT_TASK[user_id]
    if user_id in QUEUE: del QUEUE[user_id]
    await client.send_message(user_id, "✅ **All files in queue processed!**")

# --- CORE LOGIC ---

async def process_file_logic(client, message):
    user_id = message.from_user.id
    status_msg = await message.reply_text(f"⬇️ **Starting...**")
    final_path = None
    thumb_path = None
    
    try:
        user_data = await get_user(user_id)
        suffix = user_data.get("suffix", DEFAULT_SUFFIX)
        mode = user_data.get("mode", DEFAULT_MODE)
        custom_thumb_id = user_data.get("thumb")
        removal_list = user_data.get("removal_words", [])

        media = message.document or message.video or message.audio
        original_filename = media.file_name or "Unknown_File"
        
        is_junk = original_filename.startswith(("out_", "VID_", "TMP_"))
        
        if (mode == "caption" and message.caption):
            base_name = message.caption
        elif (is_junk and message.caption):
            base_name = message.caption
        else:
            base_name = os.path.splitext(original_filename)[0]
        
        base_name = clean_filename_text(base_name, removal_list)
        extension = os.path.splitext(original_filename)[1] or (".mp4" if message.video else ".mkv")
        new_filename = f"{base_name}{suffix}{extension}"
        final_path = os.path.join("downloads", new_filename)
        
        await status_msg.edit(f"⬇️ **Downloading:** `{new_filename}`")
        
        start_time = time.time()
        await message.download(
            file_name=final_path,
            progress=progress_bar,
            progress_args=(status_msg, "⬇️ **Downloading...**", start_time)
        )

        if custom_thumb_id:
            thumb_path = await client.download_media(custom_thumb_id)
        elif message.video and message.video.thumbs:
            thumb_path = await client.download_media(message.video.thumbs[0].file_id)

        duration, width, height = 0, 0, 0
        if extension.lower() in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            duration, width, height = get_metadata(final_path)

        start_time = time.time()
        
        await upload_file(client, message, final_path, thumb_path, new_filename, duration, width, height, status_msg, start_time)
        
        await status_msg.delete()

    except asyncio.CancelledError:
        await status_msg.edit("❌ **Skipped/Cancelled.**")
        raise asyncio.CancelledError 
    except Exception as e:
        await status_msg.edit(f"⚠️ Error: {e}")
    finally:
        if final_path and os.path.exists(final_path): os.remove(final_path)
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def incoming_file(client, message):
    # --- AUTH CHECK ---
    if not await is_authorized(message.from_user.id):
        return await message.reply_text(f"⛔ **Access Denied.**\nYour ID: `{message.from_user.id}`\nContact the owner to get access.")
    # ------------------
    await queue_handler(client, message)

if __name__ == "__main__":
    print("🤖 Bot Starting with Owner-Only Auth...")
    app.run()

