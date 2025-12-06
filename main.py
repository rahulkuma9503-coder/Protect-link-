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
SUPPORT_CHANNEL_ID = os.environ.get("SUPPORT_CHANNEL_ID")  # Channel ID (e.g., -1001234567890)

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

# Channel cache
channel_info = {
    "id": None,
    "title": "Support Channel",
    "username": None,
    "invite_link": None,
    "type": "channel"
}

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

async def get_channel_info(context: ContextTypes.DEFAULT_TYPE) -> Dict:
    """Get channel information and generate invite link"""
    global channel_info
    
    try:
        if SUPPORT_CHANNEL_ID:
            channel_id = int(SUPPORT_CHANNEL_ID)
            
            # Try to get channel info
            chat = await context.bot.get_chat(channel_id)
            
            # Update channel info
            channel_info.update({
                "id": chat.id,
                "title": chat.title,
                "username": chat.username,
                "type": chat.type
            })
            
            # Try to generate invite link
            try:
                # First try to create an invite link
                invite_link = await context.bot.create_chat_invite_link(
                    chat_id=channel_id,
                    member_limit=1,
                    creates_join_request=False
                )
                channel_info["invite_link"] = invite_link.invite_link
            except Exception as e:
                logger.warning(f"Could not create invite link: {e}")
                
                # Fallback: Use public link if channel has username
                if chat.username:
                    channel_info["invite_link"] = f"https://t.me/{chat.username}"
                else:
                    # Last resort: Use t.me/c/ format for private channels
                    # Channel IDs are negative, remove the -100 prefix
                    channel_id_clean = str(abs(chat.id))[3:]  # Remove first 3 digits (100)
                    channel_info["invite_link"] = f"https://t.me/c/{channel_id_clean}"
            
            logger.info(f"Channel info loaded: {channel_info['title']} (ID: {channel_info['id']})")
            
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        # Set default fallback link
        channel_info["invite_link"] = f"https://t.me/c/{SUPPORT_CHANNEL_ID[4:]}" if SUPPORT_CHANNEL_ID and len(SUPPORT_CHANNEL_ID) > 4 else None
    
    return channel_info

async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of support channel using channel ID"""
    try:
        if not SUPPORT_CHANNEL_ID:
            logger.warning("SUPPORT_CHANNEL_ID not configured, skipping check")
            return True
        
        channel_id = int(SUPPORT_CHANNEL_ID)
        
        logger.info(f"Checking membership for user {user_id} in channel ID: {channel_id}")
        
        # Get chat member using channel ID
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            status = member.status
            
            # User is considered member if they have one of these statuses
            if status in ["member", "administrator", "creator"]:
                logger.info(f"User {user_id} is a member (status: {status})")
                return True
            else:
                logger.info(f"User {user_id} is not a member (status: {status})")
                return False
                
        except BadRequest as e:
            if "user not found" in str(e).lower():
                logger.info(f"User {user_id} is not a member (not found in channel)")
                return False
            elif "chat not found" in str(e).lower():
                logger.error(f"Channel with ID {channel_id} not found or bot not in channel")
                await send_admin_alert(context, f"⚠️ Bot cannot access channel ID: {channel_id}. Make sure bot is added as admin.")
                return False
            elif "not enough rights" in str(e).lower():
                logger.error(f"Bot doesn't have admin rights in channel ID: {channel_id}")
                await send_admin_alert(context, f"⚠️ Bot needs admin rights in channel ID: {channel_id} to check membership")
                return False
            else:
                logger.error(f"BadRequest checking membership: {e}")
                return False
        except Forbidden:
            logger.info(f"User {user_id} has blocked the bot")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error checking channel membership: {e}")
        return False

async def send_admin_alert(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send alert to admin"""
    try:
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=message)
    except Exception as e:
        logger.error(f"Failed to send admin alert: {e}")

async def send_channel_verification(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None) -> None:
    """Send message asking user to join channel with generated invite link"""
    if not SUPPORT_CHANNEL_ID:
        # If channel not configured, just allow
        return
    
    # Get channel info and generate invite link
    channel = await get_channel_info(context)
    
    if not channel["invite_link"]:
        await update.message.reply_text(
            "⚠️ **Channel Verification Required**\n\n"
            "Please contact the admin to get the channel invite link.",
            parse_mode="Markdown"
        )
        return
    
    # Create callback data
    callback_data = f"check_{action}" if action else "check_membership"
    
    keyboard = [
        [InlineKeyboardButton("✅ Join Support Channel", url=channel["invite_link"])],
        [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=callback_data)]
    ]
    
    message_text = (
        f"📢 **Channel Verification Required**\n\n"
        f"To use this bot, you must join our support channel first:\n"
        f"👉 **{channel['title']}**\n\n"
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
            channel = await get_channel_info(context)
            keyboard = [
                [InlineKeyboardButton("✅ Join Support Channel", url=channel["invite_link"])],
                [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=query.data)]
            ]
            
            await query.edit_message_text(
                "❌ **You're still not a member!**\n\n"
                f"Please join **{channel['title']}** first, then click 'I've Joined - Check Now'.\n\n"
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
    
    elif query.data.startswith("users_"):
        # Handle users pagination
        page = int(query.data[6:])
        await handle_users_pagination(query, context, page)

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

# ================== ADMIN COMMANDS ==================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin broadcast command"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.")
        return
    
    if update.message.reply_to_message:
        await broadcast_replied(update, context)
    elif context.args:
        await broadcast_text(update, context)
    else:
        await update.message.reply_text(
            "📢 **Broadcast Usage:**\n\n"
            "1. **Text Broadcast:** `/broadcast Your message here`\n"
            "2. **Media Broadcast:** Reply to any message with `/broadcast`\n\n"
            "**Supported Media Types:**\n"
            "• Photos\n• Videos\n• Documents\n• Audio\n• Voice\n• Stickers\n• GIFs\n• Polls",
            parse_mode="Markdown"
        )

async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast text message to all users"""
    user = update.effective_user
    message_text = ' '.join(context.args)
    
    # Get all users
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return
    
    # Send initial status
    status_msg = await update.message.reply_text(f"📢 Broadcasting to {total_users} users...\n🔄 Sent: 0/{total_users}")
    
    success_count = 0
    failed_count = 0
    
    for user_data in all_users:
        user_id = user_data["user_id"]
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN
            )
            success_count += 1
            
            # Update status every 10 messages
            if (success_count + failed_count) % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"📢 Broadcasting to {total_users} users...\n"
                        f"🔄 Sent: {success_count + failed_count}/{total_users}\n"
                        f"✅ Success: {success_count}\n"
                        f"❌ Failed: {failed_count}"
                    )
                except:
                    pass
            
            # Small delay to avoid rate limits
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send to user {user_id}: {e}")
            
            # Remove user if they blocked the bot
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    # Final status update
    final_text = (
        f"✅ **Broadcast Completed!**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%"
    )
    
    await status_msg.edit_text(final_text, parse_mode="Markdown")

async def broadcast_replied(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast a replied message (supports all media types)"""
    user = update.effective_user
    replied_message = update.message.reply_to_message
    
    # Get all users
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return
    
    # Send initial status
    status_msg = await update.message.reply_text(f"📢 Broadcasting media to {total_users} users...\n🔄 Sent: 0/{total_users}")
    
    success_count = 0
    failed_count = 0
    
    for user_data in all_users:
        user_id = user_data["user_id"]
        
        try:
            # Forward the message based on its type
            if replied_message.text:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=replied_message.text,
                    parse_mode=ParseMode.MARKDOWN if replied_message.parse_mode == "Markdown" else None
                )
            elif replied_message.photo:
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=replied_message.photo[-1].file_id,
                    caption=replied_message.caption,
                    parse_mode=ParseMode.MARKDOWN if replied_message.caption_entities else None
                )
            elif replied_message.video:
                await context.bot.send_video(
                    chat_id=user_id,
                    video=replied_message.video.file_id,
                    caption=replied_message.caption,
                    parse_mode=ParseMode.MARKDOWN if replied_message.caption_entities else None
                )
            elif replied_message.document:
                await context.bot.send_document(
                    chat_id=user_id,
                    document=replied_message.document.file_id,
                    caption=replied_message.caption,
                    parse_mode=ParseMode.MARKDOWN if replied_message.caption_entities else None
                )
            elif replied_message.audio:
                await context.bot.send_audio(
                    chat_id=user_id,
                    audio=replied_message.audio.file_id,
                    caption=replied_message.caption,
                    parse_mode=ParseMode.MARKDOWN if replied_message.caption_entities else None
                )
            elif replied_message.voice:
                await context.bot.send_voice(
                    chat_id=user_id,
                    voice=replied_message.voice.file_id
                )
            elif replied_message.sticker:
                await context.bot.send_sticker(
                    chat_id=user_id,
                    sticker=replied_message.sticker.file_id
                )
            elif replied_message.animation:  # GIFs
                await context.bot.send_animation(
                    chat_id=user_id,
                    animation=replied_message.animation.file_id,
                    caption=replied_message.caption,
                    parse_mode=ParseMode.MARKDOWN if replied_message.caption_entities else None
                )
            else:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="📨 You received a broadcast message"
                )
            
            success_count += 1
            
            # Update status every 10 messages
            if (success_count + failed_count) % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"📢 Broadcasting to {total_users} users...\n"
                        f"🔄 Sent: {success_count + failed_count}/{total_users}\n"
                        f"✅ Success: {success_count}\n"
                        f"❌ Failed: {failed_count}"
                    )
                except:
                    pass
            
            # Small delay to avoid rate limits
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send to user {user_id}: {e}")
            
            # Remove user if they blocked the bot
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    # Final status update
    final_text = (
        f"✅ **Broadcast Completed!**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%"
    )
    
    await status_msg.edit_text(final_text, parse_mode="Markdown")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot statistics - Admin only"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.")
        return
    
    # Get channel info
    channel = await get_channel_info(context)
    
    # Get statistics
    total_users = users_collection.count_documents({})
    total_links = links_collection.count_documents({})
    total_captchas = captcha_collection.count_documents({})
    
    # Get today's stats
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_users = users_collection.count_documents({"last_active": {"$gte": today}})
    
    stats_text = (
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Users:**\n"
        f"• Total Users: {total_users}\n"
        f"• Active Today: {today_users}\n\n"
        f"🔗 **Links:**\n"
        f"• Total Protected Links: {total_links}\n\n"
        f"🔐 **CAPTCHAs:**\n"
        f"• Pending CAPTCHAs: {total_captchas}\n\n"
        f"📢 **Channel:**\n"
        f"• Title: {channel['title']}\n"
        f"• ID: `{channel['id'] or SUPPORT_CHANNEL_ID}`\n"
        f"• Invite Link: {channel['invite_link'] or 'Not available'}"
    )
    
    await update.message.reply_text(stats_text, parse_mode="Markdown")

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all users with pagination - Admin only"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.")
        return
    
    # Parse page number from args
    page = 1
    if context.args:
        try:
            page = int(context.args[0])
        except:
            pass
    
    await handle_users_pagination(update, context, page)

async def handle_users_pagination(update, context, page: int = 1):
    """Handle users pagination for both command and callback"""
    page_size = 10
    skip = (page - 1) * page_size
    
    # Get users for this page
    users_list = list(users_collection.find(
        {},
        {"user_id": 1, "username": 1, "first_name": 1, "last_active": 1}
    ).sort("last_active", -1).skip(skip).limit(page_size))
    
    total_users = users_collection.count_documents({})
    total_pages = (total_users + page_size - 1) // page_size
    
    if not users_list:
        if hasattr(update, 'message'):
            await update.message.reply_text("No users found.")
        else:
            await update.edit_message_text("No users found.")
        return
    
    users_text = "👥 **Users List**\n\n"
    for i, u in enumerate(users_list):
        username = f"@{u.get('username')}" if u.get('username') else "No username"
        last_active = u.get('last_active', datetime.utcnow())
        days_ago = (datetime.utcnow() - last_active).days
        
        users_text += (
            f"{skip + i + 1}. {u.get('first_name', 'User')}\n"
            f"   👤 {username}\n"
            f"   🆔 ID: `{u.get('user_id')}`\n"
            f"   ⏰ Active: {days_ago} days ago\n\n"
        )
    
    users_text += f"📄 Page {page}/{total_pages} • Total Users: {total_users}"
    
    # Create pagination buttons
    keyboard = []
    if page > 1:
        keyboard.append([InlineKeyboardButton("⬅️ Previous", callback_data=f"users_{page-1}")])
    if page < total_pages:
        if keyboard:
            keyboard[0].append(InlineKeyboardButton("Next ➡️", callback_data=f"users_{page+1}"))
        else:
            keyboard.append([InlineKeyboardButton("Next ➡️", callback_data=f"users_{page+1}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    if hasattr(update, 'message'):
        await update.message.reply_text(
            users_text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
    else:
        await update.edit_message_text(
            users_text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check command"""
    try:
        # Check MongoDB connection
        client.admin.command('ping')
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = f"❌ Error: {e}"
    
    # Get bot info
    bot_info = await context.bot.get_me()
    
    # Get channel info
    channel = await get_channel_info(context)
    
    status_text = (
        f"🤖 **Bot Status**\n\n"
        f"• Bot: @{bot_info.username}\n"
        f"• MongoDB: {mongo_status}\n"
        f"• Users: {users_collection.count_documents({})}\n"
        f"• Links: {links_collection.count_documents({})}\n"
        f"• Pending CAPTCHAs: {captcha_collection.count_documents({})}\n"
        f"• Support Channel: {channel['title']} (ID: {channel['id'] or SUPPORT_CHANNEL_ID})\n\n"
        f"🕒 Server Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    
    await update.message.reply_text(status_text, parse_mode="Markdown")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors"""
    logger.error(f"Update {update} caused error: {context.error}")
    
    if update and update.effective_chat:
        try:
            await update.effective_chat.send_message(
                "❌ An error occurred. Please try again later."
            )
        except Exception:
            pass

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
    
    # Initialize channel info
    try:
        await get_channel_info(ContextTypes.DEFAULT_TYPE())
        logger.info(f"Channel info initialized: {channel_info['title']}")
        
        # Send admin alert about channel verification
        await ptb_app.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"🤖 Bot started with channel verification enabled.\n\n"
                 f"Channel: {channel_info['title']}\n"
                 f"ID: {channel_info['id']}\n"
                 f"Invite Link: {channel_info.get('invite_link', 'Not available')}"
        )
    except Exception as e:
        logger.error(f"Failed to initialize channel info: {e}")
        if SUPPORT_CHANNEL_ID:
            await ptb_app.bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=f"⚠️ Bot started but failed to initialize channel info.\n\n"
                     f"Channel ID: {SUPPORT_CHANNEL_ID}\n"
                     f"Error: {str(e)[:100]}..."
            )
    
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
    channel_status = f"ID: {SUPPORT_CHANNEL_ID}" if SUPPORT_CHANNEL_ID else "Not configured"
    return {
        "status": "Telegram Bot is running",
        "timestamp": datetime.utcnow().isoformat(),
        "support_channel": channel_status
    }

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "support_channel_id": SUPPORT_CHANNEL_ID if SUPPORT_CHANNEL_ID else "Not configured"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8443))
    uvicorn.run(app, host="0.0.0.0", port=port)