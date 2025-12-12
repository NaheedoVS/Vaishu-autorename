import os
import time
import math
import asyncio
import re
import aiofiles
from io import BytesIO
from pyrogram import Client, filters, idle, raw, utils
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified
from motor.motor_asyncio import AsyncIOMotorClient
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "0")) 
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")

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

# --- 1. PROGRESS BAR (1 Minute Timer) ---

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
        
        # --- STRICT 1 MINUTE TIMER ---
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

# --- 2. SINGLE-THREADED DOWNLOAD ENGINE ---

async def fast_download(client: Client, message: Message, file_path: str, status_msg, start_time):
    await message.download(
        file_name=file_path,
        progress=progress_bar,
        progress_args=(status_msg, "⬇️ **Downloading...**", start_time)
    )
    return file_path

# --- 3. ROBUST UPLOAD ENGINE (FIXED) ---

async def fast_upload(client: Client, file_path: str, status_msg, start_time):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    file_id = client.rnd_id()
    
    # CHANGED: Reduced Chunk Size to 512KB for Stability on Heroku
    CHUNK_SIZE = 512 * 1024 
    
    part_count = math.ceil(file_size / CHUNK_SIZE)
    uploaded_bytes = 0
    
    for part_index in range(part_count):
        # Retry loop for each chunk (max 3 retries)
        retries = 3
        while retries > 0:
            try:
                async with aiofiles.open(file_path, "rb") as f:
                    await f.seek(part_index * CHUNK_SIZE)
                    chunk = await f.read(CHUNK_SIZE)
                
                # Try uploading
                await client.invoke(
                    raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=part_index,
                        file_total_parts=part_count,
                        bytes=chunk
                    )
                )
                
                uploaded_bytes += len(chunk)
                await progress_bar(uploaded_bytes, file_size, status_msg, "⬆️ **Uploading...**", start_time)
                
                # Success - break retry loop and move to next part
                break 
                
            except FloodWait as e:
                # If Telegram says wait, we wait
                print(f"FloodWait: Sleeping {e.value}s")
                await asyncio.sleep(e.value)
                # Don't decrease retries for FloodWait, just retry
                continue
                
            except Exception as e:
                retries -= 1
                print(f"Upload Error (Part {part_index}): {e}. Retrying... ({retries} left)")
                await asyncio.sleep(2) # Wait 2s before retry
                
                if retries == 0:
                    # If all retries failed, raise the error to cancel the process
                    raise e

    return raw.types.InputFileBig(id=file_id, parts=part_count, name=file_name)

# --- HELPERS ---

async def get_user(user_id):
    user = await users_col.find_one({"_id": user_id})
    if not user:
        user = {
            "_id": user_id, 
            "suffix": DEFAULT_SUFFIX, 
            "mode": DEFAULT_MODE, 
            "thumb": None, 
            "watermark_text": "Protected",
            "removal_words": [] 
        }
        await users_col.insert_one(user)
    return user

async def update_user(user_id, key, value):
    await users_col.update_one({"_id": user_id}, {"$set": {key: value}}, upsert=True)

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

def add_watermark_sync(input_path, output_path, text):
    try:
        packet = BytesIO()
        can = canvas.Canvas(packet, pagesize=letter)
        can.setFont("Helvetica-Bold", 36)
        can.setFillColorRGB(0.5, 0.5, 0.5, 0.5)
        can.saveState()
        can.translate(300, 400)
        can.rotate(45)
        can.drawCentredString(0, 0, text)
        can.restoreState()
        can.save()
        packet.seek(0)
        watermark_pdf = PdfReader(packet)
        watermark_page = watermark_pdf.pages[0]
        reader = PdfReader(input_path)
        writer = PdfWriter()
        for page in reader.pages:
            page.merge_page(watermark_page)
            writer.add_page(page)
        with open(output_path, "wb") as f:
            writer.write(f)
        return True
    except: return False

# --- TEXT CLEANER ---

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

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text("Hey! I rename files.\n\nUse /setremove to filter out words!\nHindi & Emojis are removed automatically.")

@app.on_message(filters.command("autoname") & filters.private)
async def set_autoname(client, message):
    await update_user(message.from_user.id, "mode", "filename")
    await message.reply_text("✅ **Mode Set:** Filename + Suffix")

@app.on_message(filters.command("autocaption") & filters.private)
async def set_autocaption(client, message):
    await update_user(message.from_user.id, "mode", "caption")
    await message.reply_text("✅ **Mode Set:** Caption as Filename")

@app.on_message(filters.command("suffix") & filters.private)
async def set_suffix(client, message):
    if len(message.command) < 2: return
    new_suffix = " " + message.text.split(None, 1)[1]
    await update_user(message.from_user.id, "suffix", new_suffix)
    await message.reply_text(f"✅ Suffix set to: `{new_suffix}`")

@app.on_message(filters.command("setremove") & filters.private)
async def set_remove_words(client, message):
    if len(message.command) < 2:
        await message.reply_text("ℹ️ Usage: `/setremove word1, word2`")
        return
    words_raw = message.text.split(None, 1)[1]
    words_list = [w.strip() for w in words_raw.split(",")]
    await update_user(message.from_user.id, "removal_words", words_list)
    await message.reply_text(f"✅ **Removal List Updated:**\n`{words_list}`")

@app.on_message(filters.command("resetremove") & filters.private)
async def reset_remove_words(client, message):
    await update_user(message.from_user.id, "removal_words", [])
    await message.reply_text("🗑️ **Removal List Cleared.**")

@app.on_message(filters.command("pdfmark") & filters.private)
async def set_pdf_mark(client, message):
    text = message.text.split(None, 1)[1] if len(message.command) > 1 else None
    if not text: return
    await update_user(message.from_user.id, "watermark_text", text)
    await message.reply_text(f"✅ PDF Watermark set: `{text}`")

@app.on_message(filters.command("delthumb") & filters.private)
async def delete_thumbnail(client, message):
    await update_user(message.from_user.id, "thumb", None)
    await message.reply_text("🗑️ **Thumbnail Deleted.**")

@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client, message):
    await update_user(message.from_user.id, "thumb", message.photo.file_id)
    await message.reply_text("✅ **Thumbnail Saved!**")

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_process(client, message):
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
        await fast_download(client, message, final_path, status_msg, start_time)

        if extension.lower() == ".pdf" and user_data.get("watermark_text"):
            await status_msg.edit("📝 **Applying Watermark...**")
            wm_path = os.path.join("downloads", f"WM_{new_filename}")
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(None, add_watermark_sync, final_path, wm_path, user_data.get("watermark_text"))
            if success:
                os.remove(final_path)
                final_path = wm_path

        if custom_thumb_id:
            thumb_path = await client.download_media(custom_thumb_id)
        elif message.video and message.video.thumbs:
            thumb_path = await client.download_media(message.video.thumbs[0].file_id)

        duration, width, height = 0, 0, 0
        if extension.lower() in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            duration, width, height = get_metadata(final_path)

        start_time = time.time()
        uploaded_file = await fast_upload(client, final_path, status_msg, start_time)
        
        await status_msg.edit("🔄 **Processing Final File...**")
        
        if extension.lower() in [".mp4", ".mkv"]:
             await client.send_video(
                chat_id=message.chat.id,
                video=uploaded_file,
                caption=new_filename,
                thumb=thumb_path,
                duration=duration,
                width=width,
                height=height,
                supports_streaming=True
            )
        else:
            await client.send_document(
                chat_id=message.chat.id,
                document=uploaded_file,
                thumb=thumb_path,
                caption=new_filename,
                force_document=True
            )

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
    await queue_handler(client, message)

if __name__ == "__main__":
    print("🤖 Bot Starting with Robust Engine...")
    app.run()
