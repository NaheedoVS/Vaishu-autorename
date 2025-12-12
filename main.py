import os
import time
import math
import asyncio
from io import BytesIO
from pyrogram import Client, filters, idle
from pyrogram.types import Message, BotCommand
from motor.motor_asyncio import AsyncIOMotorClient
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# --- CONFIGURATION ---
# Load variables. If missing, it will crash safely instead of using fake URLs.
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
ongoing_tasks = {} 

if not os.path.isdir("downloads"):
    os.makedirs("downloads")

# --- PROGRESS BAR FUNCTIONS ---

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
        # Update every 5 seconds
        if (now - progress_bar.last_update_time) < 5 and current != total:
            return

        percentage = current * 100 / total
        speed = current / (now - start_time) if (now - start_time) > 0 else 1
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
    except Exception:
        pass

progress_bar.last_update_time = 0

# --- HELPERS ---

async def get_user(user_id):
    user = await users_col.find_one({"_id": user_id})
    if not user:
        user = {
            "_id": user_id,
            "suffix": DEFAULT_SUFFIX,
            "mode": DEFAULT_MODE,
            "thumb": None,
            "watermark_text": "Protected"
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

# Sync function for Threading
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
    except Exception as e:
        print(f"PDF Error: {e}")
        return False

# --- COMMANDS ---

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text("Hey! I am ready. Send me a file.")

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
    if len(message.command) < 2:
        await message.reply_text(f"Current suffix: `{DEFAULT_SUFFIX}`\nUsage: `/suffix <text>`")
        return
    new_suffix = " " + message.text.split(None, 1)[1]
    await update_user(message.from_user.id, "suffix", new_suffix)
    await message.reply_text(f"✅ Suffix set to: `{new_suffix}`")

@app.on_message(filters.command("pdfmark") & filters.private)
async def set_pdf_mark(client, message):
    text = message.text.split(None, 1)[1] if len(message.command) > 1 else None
    if not text:
        await message.reply_text("Usage: `/pdfmark <text>`")
        return
    await update_user(message.from_user.id, "watermark_text", text)
    await message.reply_text(f"✅ PDF Watermark set: `{text}`")

@app.on_message(filters.command("delthumb") & filters.private)
async def delete_thumbnail(client, message):
    await update_user(message.from_user.id, "thumb", None)
    await message.reply_text("🗑️ **Thumbnail Deleted.**\nI will now use the original video's thumbnail.")

@app.on_message(filters.command("viewthumb") & filters.private)
async def view_thumbnail(client, message):
    user_data = await get_user(message.from_user.id)
    thumb_id = user_data.get("thumb")
    if thumb_id:
        await client.send_photo(chat_id=message.chat.id, photo=thumb_id, caption="🖼️ This is your custom thumbnail.")
    else:
        await message.reply_text("❌ You have no custom thumbnail set.\n(I am using the original file's thumbnail).")

# --- SAVE THUMBNAIL (Photo Handler) ---
@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client, message):
    # This captures PHOTOS sent to the bot
    await update_user(message.from_user.id, "thumb", message.photo.file_id)
    await message.reply_text(
        "✅ **Thumbnail Saved!**\n\n"
        "I will use this photo for all future uploads.\n"
        "To delete it, use /delthumb"
    )

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_process(client, message):
    if message.from_user.id in ongoing_tasks:
        try:
            ongoing_tasks[message.from_user.id].cancel()
        except: pass
        del ongoing_tasks[message.from_user.id]
        await message.reply_text("❌ Task Cancelled.")
    else:
        await message.reply_text("No active task.")

# --- CORE LOGIC ---

async def process_file(client, message):
    user_id = message.from_user.id
    status_msg = await message.reply_text("⬇️ **Initiating...**")
    final_path = None
    thumb_path = None
    
    try:
        user_data = await get_user(user_id)
        suffix = user_data.get("suffix", DEFAULT_SUFFIX)
        mode = user_data.get("mode", DEFAULT_MODE)
        custom_thumb_id = user_data.get("thumb")

        media = message.document or message.video or message.audio
        original_filename = media.file_name or "Unknown_File"
        
        # --- SMART RENAMING ---
        # Use caption if mode is 'caption' OR if filename is junk (starts with 'out_'/'VID_')
        is_junk = original_filename.startswith(("out_", "VID_", "TMP_"))
        
        if (mode == "caption" and message.caption) or (is_junk and message.caption):
            base_name = message.caption
        else:
            base_name = os.path.splitext(original_filename)[0]
            
        extension = os.path.splitext(original_filename)[1]
        if not extension:
            extension = ".mp4" if message.video else ".mkv"
            
        new_filename = f"{base_name}{suffix}{extension}"
        final_path = os.path.join("downloads", new_filename)
        
        # --- DOWNLOAD ---
        start_time = time.time()
        path = await message.download(
            file_name=final_path,
            progress=progress_bar,
            progress_args=(status_msg, "⬇️ **Downloading...**", start_time)
        )

        # PDF Watermark (Threaded to prevent freeze)
        if extension.lower() == ".pdf" and user_data.get("watermark_text"):
            await status_msg.edit("📝 **Applying Watermark...**")
            wm_path = os.path.join("downloads", f"WM_{new_filename}")
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(None, add_watermark_sync, final_path, wm_path, user_data.get("watermark_text"))
            if success:
                os.remove(final_path)
                final_path = wm_path

        # --- THUMBNAIL LOGIC ---
        thumb_source = "None"
        if custom_thumb_id:
            thumb_path = await client.download_media(custom_thumb_id)
            thumb_source = "Custom"
        elif message.video and message.video.thumbs:
            thumb_path = await client.download_media(message.video.thumbs[0].file_id)
            thumb_source = "Original"

        # Metadata
        duration, width, height = 0, 0, 0
        if extension.lower() in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            duration, width, height = get_metadata(final_path)

        # --- UPLOAD ---
        start_time = time.time()
        upload_text = f"⬆️ **Uploading...**\n🖼️ Thumbnail: {thumb_source}"
        
        if extension.lower() in [".mp4", ".mkv"]:
             await client.send_video(
                chat_id=message.chat.id,
                video=final_path,
                caption=new_filename,
                thumb=thumb_path,
                duration=duration,
                width=width,
                height=height,
                supports_streaming=True,
                progress=progress_bar,
                progress_args=(status_msg, upload_text, start_time)
            )
        else:
            await client.send_document(
                chat_id=message.chat.id,
                document=final_path,
                thumb=thumb_path,
                caption=new_filename,
                force_document=True,
                progress=progress_bar,
                progress_args=(status_msg, upload_text, start_time)
            )

        await status_msg.delete()

    except asyncio.CancelledError:
        await status_msg.edit("❌ Process Cancelled.")
    except Exception as e:
        await status_msg.edit(f"⚠️ Error: {e}")
    finally:
        if final_path and os.path.exists(final_path): os.remove(final_path)
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)
        if user_id in ongoing_tasks: del ongoing_tasks[user_id]

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def incoming_file(client, message):
    if message.from_user.id in ongoing_tasks:
        await message.reply_text("⏳ **Wait!** One process is already running.")
        return

    task = asyncio.create_task(process_file(client, message))
    ongoing_tasks[message.from_user.id] = task
    try: await task
    except asyncio.CancelledError: pass 

# --- RUN ---
if __name__ == "__main__":
    print("🤖 Bot Starting...")
    app.run()
