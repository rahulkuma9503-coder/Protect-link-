import os
import logging
import secrets
import string
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Dict, List

from fastapi import FastAPI, Request, Response
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler
)
from telegram.constants import ParseMode
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")

# MongoDB setup
client = MongoClient(MONGO_URL)
db = client.telegram_bot_db
links_collection = db.links
captcha_collection = db.captcha
users_collection = db.users
broadcasts_collection = db.broadcasts

# Create indexes
links_collection.create_index("encoded", unique=True)
links_collection.create_index("created_at", expireAfterSeconds=2592000)
captcha_collection.create_index([("user_id", 1), ("encoded", 1)], unique=True)
captcha_collection.create_index("created_at", expireAfterSeconds=300)
users_collection.create_index("user_id", unique=True)
broadcasts_collection.create_index("broadcast_id", unique=True)

# Initialize PTB Application
ptb_app = Application.builder().token(BOT_TOKEN).updater(None).build()

# Helper functions
def generate_encoded_string(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def generate_captcha_code() -> str:
    return ''.join(secrets.choice(string.digits) for _ in range(5))

async def ensure_user_in_db(user_id: int, username: str = None, first_name: str = None) -> None:
    try:
        users_collection.update_one(
            {"user_id": user_id},
            {
                "$setOnInsert": {
                    "username": username,
                    "first_name": first_name,
                    "joined_at": datetime.utcnow()
                },
                "$set": {
                    "last_active": datetime.utcnow(),
                    "username": username,
                    "first_name": first_name
                },
                "$inc": {"message_count": 1}
            },
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error ensuring user in DB: {e}")

# ================== COMMAND HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    args = context.args
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if not args:
        welcome_msg = "👋 Welcome! Use /protect <group_link> to create a protected link."
        if str(user.id) == ADMIN_USER_ID:
            welcome_msg += "\n\n👑 Admin commands available: /broadcast, /stats, /users, /health"
        await update.message.reply_text(welcome_msg)
        return
    
    if args[0].startswith("verify_"):
        encoded = args[0][7:]
        link_data = links_collection.find_one({"encoded": encoded})
        
        if not link_data:
            await update.message.reply_text("❌ Invalid or expired verification link.")
            return
        
        # Check existing CAPTCHA
        existing = captcha_collection.find_one({"user_id": user.id, "encoded": encoded})
        if existing:
            await update.message.reply_text(
                f"Your pending CAPTCHA: `{existing['captcha_code']}`\nSend it back within 5 minutes.",
                parse_mode="Markdown"
            )
            return
        
        captcha_code = generate_captcha_code()
        captcha_data = {
            "user_id": user.id,
            "encoded": encoded,
            "captcha_code": captcha_code,
            "created_at": datetime.utcnow()
        }
        
        try:
            captcha_collection.insert_one(captcha_data)
            await update.message.reply_text(
                f"🔒 CAPTCHA Code: `{captcha_code}`\nSend it back within 5 minutes.",
                parse_mode="Markdown"
            )
        except DuplicateKeyError:
            await update.message.reply_text("You already have a pending verification.")

async def protect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if update.effective_chat.type != "private":
        await update.message.reply_text("⚠️ Please use this in private chat.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /protect https://t.me/joinchat/ABCD1234")
        return
    
    group_link = context.args[0]
    if not (group_link.startswith("https://t.me/") or group_link.startswith("https://telegram.me/")):
        await update.message.reply_text("❌ Please provide a valid Telegram invite link.")
        return
    
    encoded = generate_encoded_string()
    link_data = {
        "encoded": encoded,
        "group_link": group_link,
        "created_by": user.id,
        "created_at": datetime.utcnow(),
        "verification_count": 0
    }
    
    try:
        links_collection.insert_one(link_data)
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        
        keyboard = [[
            InlineKeyboardButton("🔗 Share Link", url=f"https://t.me/share/url?url={protected_link}"),
            InlineKeyboardButton("📋 Copy", callback_data=f"copy_{encoded}")
        ]]
        
        await update.message.reply_text(
            f"✅ Link protected!\n\n**Protected Link:** {protected_link}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error in /protect: {e}")
        await update.message.reply_text("❌ An error occurred.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if update.effective_chat.type != "private" or not update.message.text:
        return
    
    message_text = update.message.text.strip()
    if len(message_text) == 5 and message_text.isdigit():
        captcha_data = captcha_collection.find_one({"user_id": user.id})
        
        if captcha_data:
            if message_text == captcha_data["captcha_code"]:
                link_data = links_collection.find_one({"encoded": captcha_data["encoded"]})
                if link_data:
                    await update.message.reply_text(f"✅ Verified! Group link: {link_data['group_link']}")
                    captcha_collection.delete_one({"user_id": user.id})
                    links_collection.update_one(
                        {"encoded": captcha_data["encoded"]},
                        {"$inc": {"verification_count": 1}}
                    )
                else:
                    await update.message.reply_text("❌ Link expired.")
                    captcha_collection.delete_one({"user_id": user.id})
            else:
                await update.message.reply_text("❌ Incorrect code.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ Admin only.")
        return
    
    if update.message.reply_to_message:
        await broadcast_replied(update, context)
    elif context.args:
        await broadcast_text(update, context)
    else:
        await update.message.reply_text("Usage: /broadcast <text> OR reply to a message with /broadcast")

async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message_text = ' '.join(context.args)
    users = list(users_collection.find({}, {"user_id": 1}))
    
    status_msg = await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
    
    success = 0
    failed = 0
    
    for user_data in users:
        try:
            await context.bot.send_message(user_data["user_id"], message_text, parse_mode=ParseMode.MARKDOWN)
            success += 1
            if (success + failed) % 10 == 0:
                await status_msg.edit_text(f"🔄 Sent: {success + failed}/{len(users)}")
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_data["user_id"]})
    
    await status_msg.edit_text(f"✅ Done! Success: {success}, Failed: {failed}")

async def broadcast_replied(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    replied = update.message.reply_to_message
    users = list(users_collection.find({}, {"user_id": 1}))
    
    status_msg = await update.message.reply_text(f"📢 Broadcasting media to {len(users)} users...")
    
    success = 0
    failed = 0
    
    for user_data in users:
        try:
            if replied.text:
                await context.bot.send_message(user_data["user_id"], replied.text)
            elif replied.photo:
                await context.bot.send_photo(user_data["user_id"], replied.photo[-1].file_id, caption=replied.caption)
            elif replied.video:
                await context.bot.send_video(user_data["user_id"], replied.video.file_id, caption=replied.caption)
            elif replied.document:
                await context.bot.send_document(user_data["user_id"], replied.document.file_id, caption=replied.caption)
            elif replied.sticker:
                await context.bot.send_sticker(user_data["user_id"], replied.sticker.file_id)
            else:
                await context.bot.send_message(user_data["user_id"], "📨 You received a broadcast message")
            
            success += 1
            if (success + failed) % 10 == 0:
                await status_msg.edit_text(f"🔄 Sent: {success + failed}/{len(users)}")
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_data["user_id"]})
    
    await status_msg.edit_text(f"✅ Done! Success: {success}, Failed: {failed}")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ Admin only.")
        return
    
    total_users = users_collection.count_documents({})
    total_links = links_collection.count_documents({})
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_users = users_collection.count_documents({"last_active": {"$gte": today}})
    
    await update.message.reply_text(
        f"📊 **Stats**\n"
        f"• Users: {total_users}\n"
        f"• Active Today: {today_users}\n"
        f"• Links: {total_links}",
        parse_mode="Markdown"
    )

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ Admin only.")
        return
    
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    page_size = 10
    skip = (page - 1) * page_size
    
    users_list = list(users_collection.find().sort("last_active", -1).skip(skip).limit(page_size))
    total = users_collection.count_documents({})
    
    text = "👥 **Users**\n\n"
    for u in users_list:
        text += f"• {u.get('first_name', 'User')} (@{u.get('username', 'N/A')})\n"
    
    text += f"\nPage {page}/{(total + page_size - 1) // page_size}"
    await update.message.reply_text(text, parse_mode="Markdown")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        client.admin.command('ping')
        mongo_status = "✅"
    except:
        mongo_status = "❌"
    
    await update.message.reply_text(
        f"🤖 **Health Check**\n"
        f"• MongoDB: {mongo_status}\n"
        f"• Users: {users_collection.count_documents({})}\n"
        f"• Links: {links_collection.count_documents({})}",
        parse_mode="Markdown"
    )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith("copy_"):
        encoded = query.data[5:]
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        await query.edit_message_text(f"✅ Link: `{protected_link}`", parse_mode="Markdown")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error: {context.error}")

# ================== FASTAPI SETUP ==================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup PTB application"""
    logger.info("Starting PTB application...")
    await ptb_app.initialize()
    await ptb_app.start()
    
    # Set webhook
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
    await ptb_app.bot.set_webhook(webhook_url, allowed_updates=["message", "callback_query"])
    logger.info(f"Webhook set to: {webhook_url}")
    
    yield
    
    logger.info("Shutting down PTB application...")
    await ptb_app.stop()
    await ptb_app.shutdown()

# Create FastAPI app
app = FastAPI(lifespan=lifespan)

# Add handlers to PTB app
ptb_app.add_handler(CommandHandler("start", start))
ptb_app.add_handler(CommandHandler("protect", protect))
ptb_app.add_handler(CommandHandler("broadcast", broadcast))
ptb_app.add_handler(CommandHandler("stats", stats))
ptb_app.add_handler(CommandHandler("users", users))
ptb_app.add_handler(CommandHandler("health", health))
ptb_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
ptb_app.add_handler(CallbackQueryHandler(callback_handler))
ptb_app.add_error_handler(error_handler)

@app.post("/webhook")
async def process_update(request: Request):
    """Handle incoming Telegram updates"""
    json_data = await request.json()
    update = Update.de_json(json_data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=HTTPStatus.OK)

@app.get("/")
async def root():
    return {"status": "Telegram Bot is running", "timestamp": datetime.utcnow().isoformat()}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8443))
    uvicorn.run(app, host="0.0.0.0", port=port)