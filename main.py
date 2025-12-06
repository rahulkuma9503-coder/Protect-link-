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
from telegram.error import BadRequest, Forbidden
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
SUPPORT_CHANNEL = os.environ.get("SUPPORT_CHANNEL", "@YourSupportChannel")

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
    """Check if user is member of support channel with proper error handling"""
    try:
        if not SUPPORT_CHANNEL or SUPPORT_CHANNEL == "@YourSupportChannel":
            logger.warning("SUPPORT_CHANNEL not configured, skipping check")
            return True
        
        # Clean channel username
        channel = SUPPORT_CHANNEL.replace("@", "").strip()
        if not channel:
            return True
        
        logger.info(f"Checking membership for user {user_id} in channel {channel}")
        
        # Get chat member
        try:
            member = await context.bot.get_chat_member(f"@{channel}", user_id)
            status = member.status
            
            # User is considered member if they have one of these statuses
            if status in ["member", "administrator", "creator"]:
                logger.info(f"User {user_id} is a member (status: {status})")
                return True
            else:
                logger.info(f"User {user_id} is not a member (status: {status})")
                return False
                
        except BadRequest as e:
            if "user not found" in str(e).lower() or "chat not found" in str(e).lower():
                logger.info(f"User {user_id} is not a member (not found in channel)")
                return False
            elif "not enough rights" in str(e).lower():
                logger.error(f"Bot doesn't have admin rights in channel @{channel}")
                await send_admin_alert(context, f"⚠️ Bot needs admin rights in @{channel} to check membership")
                return False
            else:
                logger.error(f"BadRequest checking membership: {e}")
                return False
        except Forbidden:
            logger.info(f"User {user_id} has blocked the bot or bot was removed from channel")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error checking channel membership: {e}")
        # In case of error, allow the user to proceed
        return True

async def send_admin_alert(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send alert to admin"""
    try:
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=message)
    except Exception as e:
        logger.error(f"Failed to send admin alert: {e}")

async def require_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = "use_bot") -> bool:
    """Check and enforce channel membership. Returns True if user is member."""
    user = update.effective_user
    
    # Always allow admin
    if str(user.id) == ADMIN_USER_ID:
        return True
    
    is_member = await is_user_in_channel(user.id, context)
    
    if not is_member:
        await send_channel_verification(update, context, action)
        return False
    
    return True

async def send_channel_verification(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None) -> None:
    """Send message asking user to join channel with proper URL"""
    if not SUPPORT_CHANNEL or SUPPORT_CHANNEL == "@YourSupportChannel":
        # If channel not configured, just allow
        return
    
    # Clean channel name
    channel = SUPPORT_CHANNEL.replace("@", "").strip()
    
    # Create callback data
    callback_data = f"check_{action}" if action else "check_membership"
    
    keyboard = [
        [InlineKeyboardButton("✅ Join Support Channel", url=f"https://t.me/{channel}")],
        [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=callback_data)]
    ]
    
    message_text = (
        f"📢 **Channel Verification Required**\n\n"
        f"To use this bot, you must join our support channel first:\n"
        f"👉 @{channel}\n\n"
        f"**Instructions:**\n"
        f"1. Click 'Join Support Channel' button above\n"
        f"2. Join the channel\n"
        f"3. Come back and click 'I've Joined - Check Now'\n\n"
        f"⚠️ *You must join to proceed*"
    )
    
    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

# ================== COMMAND HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    args = context.args
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    # Check channel membership for all non-admin users
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "start")
            return
    
    if not args:
        welcome_msg = "👋 Welcome! Use /protect <group_link> to create a protected link."
        if str(user.id) == ADMIN_USER_ID:
            welcome_msg += "\n\n👑 Admin commands available: /broadcast, /stats, /users, /health"
        await update.message.reply_text(welcome_msg)
        return
    
    if args[0].startswith("verify_"):
        encoded = args[0][7:]
        
        # Re-check membership for verification links
        if str(user.id) != ADMIN_USER_ID:
            is_member = await is_user_in_channel(user.id, context)
            if not is_member:
                await send_channel_verification(update, context, f"verify_{encoded}")
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
    
    if query.data.startswith("check_"):
        action = query.data[6:]
        
        # Check membership
        is_member = await is_user_in_channel(user.id, context)
        
        if is_member:
            if action.startswith("verify_"):
                encoded = action[7:]
                await handle_verification_start_from_callback(query, context, user.id, encoded)
            elif action == "protect":
                await query.edit_message_text(
                    "✅ **Channel Verified!**\n\n"
                    "You can now use /protect command.\n\n"
                    "Type: `/protect <group_link>`",
                    parse_mode="Markdown"
                )
            elif action == "start":
                await query.edit_message_text(
                    "✅ **Channel Verified!**\n\n"
                    "You can now use the bot.\n\n"
                    "Commands:\n"
                    "• /protect - Protect a group link\n"
                    "• /start - Show this message",
                    parse_mode="Markdown"
                )
            else:
                await query.edit_message_text(
                    "✅ **Channel Verified!**\n\n"
                    "You can now use the bot.",
                    parse_mode="Markdown"
                )
        else:
            # Still not a member
            channel = SUPPORT_CHANNEL.replace("@", "").strip()
            keyboard = [
                [InlineKeyboardButton("✅ Join Support Channel", url=f"https://t.me/{channel}")],
                [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=query.data)]
            ]
            
            await query.edit_message_text(
                "❌ **You're still not a member!**\n\n"
                f"Please join @{channel} first, then click 'I've Joined - Check Now'.\n\n"
                f"Make sure you've actually joined the channel.",
                reply_markup=InlineKeyboardMarkup(keyboard),
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
    
    # Check channel membership (except for admin)
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "protect")
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
    
    # Check channel membership for all messages (except admin)
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "message")
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

# [Rest of the code remains same - broadcast, stats, users, health functions]

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
    
    # Log channel info
    if SUPPORT_CHANNEL and SUPPORT_CHANNEL != "@YourSupportChannel":
        logger.info(f"Support Channel configured: {SUPPORT_CHANNEL}")
        # Send admin alert about channel verification
        try:
            await ptb_app.bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=f"🤖 Bot started with channel verification enabled.\n\nSupport Channel: {SUPPORT_CHANNEL}"
            )
        except:
            pass
    else:
        logger.warning("SUPPORT_CHANNEL not configured properly!")
    
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
    channel_status = "Not configured" if not SUPPORT_CHANNEL or SUPPORT_CHANNEL == "@YourSupportChannel" else SUPPORT_CHANNEL
    return {
        "status": "Telegram Bot is running",
        "timestamp": datetime.utcnow().isoformat(),
        "support_channel": channel_status
    }

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "support_channel": SUPPORT_CHANNEL if SUPPORT_CHANNEL else "Not configured"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8443))
    uvicorn.run(app, host="0.0.0.0", port=port)