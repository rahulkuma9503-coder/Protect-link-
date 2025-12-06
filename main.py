import os
import logging
import secrets
import string
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Dict, List, Optional

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
SUPPORT_CHANNEL = os.environ.get("SUPPORT_CHANNEL", "@YourSupportChannel")  # Add this in Render

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

async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of support channel"""
    try:
        if not SUPPORT_CHANNEL:
            return True  # Allow all if channel not set
        
        # Remove @ if present
        channel = SUPPORT_CHANNEL.replace("@", "")
        
        # Try to get chat member
        member = await context.bot.get_chat_member(f"@{channel}", user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Error checking channel membership: {e}")
        return False

async def send_channel_verification(update: Update, context: ContextTypes.DEFAULT_TYPE, encoded: str = None) -> None:
    """Send message asking user to join channel"""
    keyboard = [
        [InlineKeyboardButton("✅ Join Support Channel", url=f"https://t.me/{SUPPORT_CHANNEL.replace('@', '')}")],
        [InlineKeyboardButton("🔁 Check Membership", callback_data=f"check_membership_{encoded}")]
    ]
    
    await update.message.reply_text(
        f"📢 **Channel Verification Required**\n\n"
        f"To use this bot, you must join our support channel:\n"
        f"👉 {SUPPORT_CHANNEL}\n\n"
        f"1. Click the button below to join\n"
        f"2. Then click 'Check Membership'",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# ================== COMMAND HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    args = context.args
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    # Check channel membership for all commands except in private welcome
    if not args:
        welcome_msg = "👋 Welcome! Use /protect <group_link> to create a protected link."
        if str(user.id) == ADMIN_USER_ID:
            welcome_msg += "\n\n👑 Admin commands available: /broadcast, /stats, /users, /health"
        await update.message.reply_text(welcome_msg)
        return
    
    if args[0].startswith("verify_"):
        encoded = args[0][7:]
        
        # Check if user is in support channel
        is_member = await is_user_in_channel(user.id, context)
        
        if not is_member:
            await send_channel_verification(update, context, encoded)
            return
        
        # User is in channel, proceed with verification
        await handle_verification_start(update, context, user.id, encoded)

async def handle_verification_start(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, encoded: str) -> None:
    """Start verification process for user"""
    link_data = links_collection.find_one({"encoded": encoded})
    
    if not link_data:
        await update.message.reply_text("❌ Invalid or expired verification link.")
        return
    
    # Check existing CAPTCHA
    existing = captcha_collection.find_one({"user_id": user_id, "encoded": encoded})
    if existing:
        # Show verify button
        keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
        await update.message.reply_text(
            "✅ You have a pending verification.\n\n"
            "Click the button below to start verification:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
    
    # Generate new CAPTCHA
    captcha_code = generate_captcha_code()
    captcha_data = {
        "user_id": user_id,
        "encoded": encoded,
        "captcha_code": captcha_code,
        "created_at": datetime.utcnow()
    }
    
    try:
        captcha_collection.insert_one(captcha_data)
        
        # Show verify button
        keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
        await update.message.reply_text(
            "🔒 **Verification Required**\n\n"
            "Click the button below to start the verification process:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except DuplicateKeyError:
        keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
        await update.message.reply_text(
            "✅ Verification session found.\n\n"
            "Click the button below to continue:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    
    if query.data.startswith("check_membership_"):
        encoded = query.data[17:]
        is_member = await is_user_in_channel(user.id, context)
        
        if is_member:
            await handle_verification_start_from_callback(query, context, user.id, encoded)
        else:
            await query.edit_message_text(
                "❌ You're still not a member of the support channel.\n\n"
                f"Please join {SUPPORT_CHANNEL} and try again.",
                parse_mode="Markdown"
            )
    
    elif query.data.startswith("verify_"):
        encoded = query.data[7:]
        await handle_captcha_verification(query, context, user.id, encoded)
    
    elif query.data.startswith("copy_"):
        encoded = query.data[5:]
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        await query.edit_message_text(
            f"✅ **Protected Link Generated**\n\n"
            f"Share this link with others:\n"
            f"`{protected_link}`",
            parse_mode="Markdown"
        )
    
    elif query.data.startswith("share_link_"):
        encoded = query.data[11:]
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        
        share_text = (
            f"🔗 **Join via Protected Link**\n\n"
            f"Click below to join through verification:\n"
            f"{protected_link}"
        )
        
        keyboard = [[
            InlineKeyboardButton("📤 Share Link", url=f"https://t.me/share/url?url={protected_link}&text=Join%20via%20protected%20link"),
            InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{encoded}")
        ]]
        
        await query.edit_message_text(
            share_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )

async def handle_verification_start_from_callback(query, context, user_id: int, encoded: str) -> None:
    """Start verification from callback"""
    keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
    
    await query.edit_message_text(
        "✅ **Channel Verified!**\n\n"
        "Now click the button below to start the CAPTCHA verification:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_captcha_verification(query, context, user_id: int, encoded: str) -> None:
    """Handle CAPTCHA verification via inline button"""
    captcha_data = captcha_collection.find_one({"user_id": user_id, "encoded": encoded})
    
    if not captcha_data:
        await query.edit_message_text("❌ No pending verification found.")
        return
    
    # Show CAPTCHA code and ask user to enter it
    captcha_code = captcha_data["captcha_code"]
    
    await query.edit_message_text(
        f"🔢 **Enter CAPTCHA Code**\n\n"
        f"Your verification code is: `{captcha_code}`\n\n"
        f"Please send this 5-digit code back to me within 5 minutes.",
        parse_mode="Markdown"
    )

async def protect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    
    # Check channel membership
    is_member = await is_user_in_channel(user.id, context)
    if not is_member:
        await send_channel_verification(update, context)
        return
    
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
            InlineKeyboardButton("🔗 Share Protected Link", url=f"https://t.me/share/url?url={protected_link}&text=Join%20via%20protected%20link"),
            InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{encoded}")
        ]]
        
        await update.message.reply_text(
            f"✅ **Link Protected Successfully!**\n\n"
            f"**Original Link:** {group_link}\n\n"
            f"**Protected Link:**\n`{protected_link}`\n\n"
            f"Share the protected link with others. They'll need to:\n"
            f"1. Join our support channel\n"
            f"2. Complete verification\n"
            f"3. Get the group link via button",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error in /protect: {e}")
        await update.message.reply_text("❌ An error occurred.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    
    # Check channel membership for all messages
    is_member = await is_user_in_channel(user.id, context)
    if not is_member:
        await send_channel_verification(update, context)
        return
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if update.effective_chat.type != "private" or not update.message.text:
        return
    
    message_text = update.message.text.strip()
    
    # Handle CAPTCHA code entry
    if len(message_text) == 5 and message_text.isdigit():
        captcha_data = captcha_collection.find_one({"user_id": user.id})
        
        if captcha_data:
            if message_text == captcha_data["captcha_code"]:
                link_data = links_collection.find_one({"encoded": captcha_data["encoded"]})
                if link_data:
                    # Send group link as button (NOT directly)
                    keyboard = [[
                        InlineKeyboardButton("🚀 Join Group", url=link_data["group_link"]),
                        InlineKeyboardButton("📤 Share Protected Link", callback_data=f"share_link_{captcha_data['encoded']}")
                    ]]
                    
                    await update.message.reply_text(
                        f"✅ **Verification Successful!**\n\n"
                        f"Click the button below to join the group:\n\n"
                        f"After joining, you can share the protected link with others.",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="Markdown"
                    )
                    
                    # Cleanup and update stats
                    captcha_collection.delete_one({"user_id": user.id})
                    links_collection.update_one(
                        {"encoded": captcha_data["encoded"]},
                        {"$inc": {"verification_count": 1}}
                    )
                else:
                    await update.message.reply_text("❌ Link has expired.")
                    captcha_collection.delete_one({"user_id": user.id})
            else:
                await update.message.reply_text("❌ Incorrect code. Please try again.")

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
        f"• Links: {total_links}\n"
        f"• Support Channel: {SUPPORT_CHANNEL}",
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
        f"• Links: {links_collection.count_documents({})}\n"
        f"• Support Channel: {SUPPORT_CHANNEL}",
        parse_mode="Markdown"
    )

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
    await ptb_app.bot.set_webhook(
        webhook_url, 
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True
    )
    logger.info(f"Webhook set to: {webhook_url}")
    logger.info(f"Support Channel: {SUPPORT_CHANNEL}")
    
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
    return {"status": "ok", "support_channel": SUPPORT_CHANNEL}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8443))
    uvicorn.run(app, host="0.0.0.0", port=port)