import os
import asyncio
import time
from io import BytesIO
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from motor.motor_asyncio import AsyncIOMotorClient
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "123456")) 
API_HASH = os.environ.get("API_HASH", "your_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_token")
MONGO_URI = os.environ.get("MONGO_URI", "your_mongo_url")

# --- DATABASE SETUP ---
mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo["RenameBot"]
users_col = db["users"]

# --- BOT SETUP ---
app = Client("rename_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- GLOBAL VARS ---
DEFAULT_SUFFIX = " 🦋Vaiᡣ𐭩Su×@pglinsan2"
DEFAULT_MODE = "filename"
ongoing_tasks = {} 

# --- CRITICAL FIX: CREATE FOLDER ---
if not os.path.isdir("downloads"):
    os.makedirs("downloads")

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

def create_watermark(text):
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
    return PdfReader(packet)

async def add_watermark(input_path, output_path, text):
    try:
        watermark_pdf = create_watermark(text)
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
    await message.reply_text(f"Hey VaiSu! Welcome back.\n\nSend me files to rename.\nUse /cancel to stop a process.")

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_process(client, message):
    user_id = message.from_user.id
    if user_id in ongoing_tasks:
        try:
            task = ongoing_tasks[user_id]
            task.cancel()
            del ongoing_tasks[user_id]
            await message.reply_text("❌ Process Cancelled!")
        except Exception as e:
            await message.reply_text(f"Failed to cancel: {e}")
    else:
        await message.reply_text("No active process to cancel.")

@app.on_message(filters.command("mode") & filters.private)
async def set_mode(client, message):
    user = await get_user(message.from_user.id)
    current = user.get("mode", DEFAULT_MODE)
    new_mode = "caption" if current == "filename" else "filename"
    await update_user(message.from_user.id, "mode", new_mode)
    await message.reply_text(f"Rename mode changed to: **{new_mode}**")

@app.on_message(filters.command("suffix") & filters.private)
async def set_suffix(client, message):
    if len(message.command) < 2:
        await message.reply_text(f"Current suffix: `{DEFAULT_SUFFIX}`\nTo change: `/suffix <new text>`")
        return
    new_suffix = " " + message.text.split(None, 1)[1]
    await update_user(message.from_user.id, "suffix", new_suffix)
    await message.reply_text(f"Suffix updated to: `{new_suffix}`")

@app.on_message(filters.command("pdfmark") & filters.private)
async def set_pdf_mark(client, message):
    if len(message.command) < 2:
        await message.reply_text("Send `/pdfmark <text>` to set the watermark text for PDFs.")
        return
    text = message.text.split(None, 1)[1]
    await update_user(message.from_user.id, "watermark_text", text)
    await message.reply_text(f"PDF Watermark text set to: `{text}`")

@app.on_message(filters.photo & filters.private)
async def save_thumbnail(client, message):
    await update_user(message.from_user.id, "thumb", message.photo.file_id)
    await message.reply_text("Thumbnail saved!")

@app.on_message(filters.command("delthumb") & filters.private)
async def delete_thumbnail(client, message):
    await update_user(message.from_user.id, "thumb", None)
    await message.reply_text("Thumbnail deleted.")

# --- CORE LOGIC ---

async def process_file(client, message):
    user_id = message.from_user.id
    status_msg = await message.reply_text("⬇️ Downloading...")
    final_path = None
    thumb_path = None
    
    try:
        user_data = await get_user(user_id)
        suffix = user_data.get("suffix", DEFAULT_SUFFIX)
        mode = user_data.get("mode", DEFAULT_MODE)
        thumb_id = user_data.get("thumb")

        media = message.document or message.video or message.audio
        original_filename = media.file_name or "Unknown_File"
        
        # New Name Logic
        if mode == "caption" and message.caption:
            base_name = message.caption
        else:
            base_name = os.path.splitext(original_filename)[0]
            
        extension = os.path.splitext(original_filename)[1]
        if not extension:
            extension = ".mp4" if message.video else ".mkv"
            
        new_filename = f"{base_name}{suffix}{extension}"
        final_path = os.path.join("downloads", new_filename)
        
        # Download
        path = await message.download(file_name=final_path)

        # PDF Logic
        if extension.lower() == ".pdf":
            wm_text = user_data.get("watermark_text")
            if wm_text:
                await status_msg.edit("📝 Applying PDF Watermark...")
                wm_path = os.path.join("downloads", f"WM_{new_filename}")
                success = await add_watermark(final_path, wm_path, wm_text)
                if success:
                    os.remove(final_path)
                    final_path = wm_path

        await status_msg.edit("⬆️ Uploading...")

        # Metadata
        duration, width, height = 0, 0, 0
        if thumb_id:
            thumb_path = await client.download_media(thumb_id)
        
        if extension.lower() in [".mp4", ".mkv", ".avi", ".mov", ".webm"]:
            duration, width, height = get_metadata(final_path)

        # Send
        if extension.lower() in [".mp4", ".mkv"]:
             await client.send_video(
                chat_id=message.chat.id,
                video=final_path,
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
                document=final_path,
                thumb=thumb_path,
                caption=new_filename,
                force_document=True
            )

        await status_msg.delete()

    except asyncio.CancelledError:
        await status_msg.edit("❌ Cancelled.")
    except Exception as e:
        await status_msg.edit(f"Error: {e}")
    finally:
        if final_path and os.path.exists(final_path): os.remove(final_path)
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)
        if user_id in ongoing_tasks: del ongoing_tasks[user_id]

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def incoming_file(client, message):
    if message.from_user.id in ongoing_tasks:
        await message.reply_text("Wait for current process or /cancel.")
        return

    task = asyncio.create_task(process_file(client, message))
    ongoing_tasks[message.from_user.id] = task
    try: await task
    except asyncio.CancelledError: pass 

print("Bot Started...")
app.run()
