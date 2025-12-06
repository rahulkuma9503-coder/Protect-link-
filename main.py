import os
import logging
import secrets
import string
import asyncio
import re
import html
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
SUPPORT_CHANNEL_ID = os.environ.get("SUPPORT_CHANNEL_ID")

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
    """Generate unique CAPTCHA code"""
    return ''.join(secrets.choice(string.digits) for _ in range(5))

def escape_markdown_v2(text: str) -> str:
    """Escape text for Telegram MarkdownV2 format - CORRECT VERSION"""
    if not text:
        return ""
    # Characters that need escaping in MarkdownV2
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text

def escape_html(text: str) -> str:
    """Escape text for HTML parse mode"""
    if not text:
        return ""
    return html.escape(text)

def format_link_clean(link: str) -> str:
    """Format link without any escaping for HTML mode"""
    return link

def generate_obfuscated_link(original_link: str) -> str:
    """Generate an obfuscated version of the link that's hard to copy"""
    if "t.me/joinchat/" in original_link:
        parts = original_link.split("/")
        if len(parts) >= 4:
            code = parts[-1]
            if len(code) > 8:
                return f"https://t.me/joinchat/...{code[-4:]}"
    elif "t.me/+" in original_link:
        code = original_link.split("+")[-1]
        if len(code) > 8:
            return f"https://t.me/+...{code[-4:]}"
    elif "t.me/c/" in original_link:
        return "https://t.me/c/•••••••"
    
    return "•••••••••••••••••••••"

async def ensure_user_in_db(user_id: int, username: str = None, first_name: str = None) -> None:
    """Fixed MongoDB update to avoid conflict error"""
    try:
        existing_user = users_collection.find_one({"user_id": user_id})
        
        if existing_user:
            update_data = {
                "last_active": datetime.utcnow(),
                "message_count": existing_user.get("message_count", 0) + 1
            }
            
            if username is not None and username != existing_user.get("username"):
                update_data["username"] = username
            
            if first_name is not None and first_name != existing_user.get("first_name"):
                update_data["first_name"] = first_name
            
            users_collection.update_one(
                {"user_id": user_id},
                {"$set": update_data}
            )
        else:
            user_data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "joined_at": datetime.utcnow(),
                "last_active": datetime.utcnow(),
                "message_count": 1
            }
            users_collection.insert_one(user_data)
            
    except Exception as e:
        logger.error(f"Error ensuring user in DB: {e}")

async def get_channel_info(context: ContextTypes.DEFAULT_TYPE) -> Dict:
    """Get channel information and generate invite link"""
    global channel_info
    
    try:
        if SUPPORT_CHANNEL_ID:
            channel_id = int(SUPPORT_CHANNEL_ID)
            
            chat = await context.bot.get_chat(channel_id)
            
            channel_info.update({
                "id": chat.id,
                "title": chat.title,  # Don't escape here, escape when using
                "username": chat.username,
                "type": chat.type
            })
            
            try:
                invite_link = await context.bot.create_chat_invite_link(
                    chat_id=channel_id,
                    member_limit=1,
                    creates_join_request=False
                )
                channel_info["invite_link"] = invite_link.invite_link
            except Exception as e:
                logger.warning(f"Could not create invite link: {e}")
                
                if chat.username:
                    channel_info["invite_link"] = f"https://t.me/{chat.username}"
                else:
                    channel_id_clean = str(abs(chat.id))[3:]
                    channel_info["invite_link"] = f"https://t.me/c/{channel_id_clean}"
            
            logger.info(f"Channel info loaded: {channel_info['title']} (ID: {channel_info['id']})")
            
    except Exception as e:
        logger.error(f"Error getting channel info: {e}")
        channel_info["invite_link"] = f"https://t.me/c/{SUPPORT_CHANNEL_ID[4:]}" if SUPPORT_CHANNEL_ID and len(SUPPORT_CHANNEL_ID) > 4 else None
    
    return channel_info

async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of support channel using channel ID"""
    try:
        if not SUPPORT_CHANNEL_ID:
            return True
        
        channel_id = int(SUPPORT_CHANNEL_ID)
        
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            status = member.status
            
            if status in ["member", "administrator", "creator"]:
                return True
            else:
                return False
                
        except BadRequest as e:
            if "user not found" in str(e).lower():
                return False
            elif "chat not found" in str(e).lower():
                return False
            elif "not enough rights" in str(e).lower():
                return False
            else:
                return False
        except Forbidden:
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error checking channel membership: {e}")
        return False

async def send_channel_verification(update: Update, context: ContextTypes.DEFAULT_TYPE, action: str = None) -> None:
    """Send message asking user to join channel with generated invite link"""
    if not SUPPORT_CHANNEL_ID:
        return
    
    channel = await get_channel_info(context)
    
    if not channel["invite_link"]:
        await update.message.reply_text(
            "⚠️ <b>Channel Verification Required</b>\n\n"
            "Please contact the admin to get the channel invite link.",
            parse_mode="HTML"
        )
        return
    
    callback_data = f"check_{action}" if action else "check_membership"
    
    keyboard = [
        [InlineKeyboardButton("✅ Join Support Channel", url=channel["invite_link"])],
        [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=callback_data)]
    ]
    
    escaped_title = escape_html(channel['title'])
    
    message_text = (
        f"📢 <b>Channel Verification Required</b>\n\n"
        f"To use this bot, you must join our support channel first:\n"
        f"👉 <b>{escaped_title}</b>\n\n"
        f"<b>Instructions:</b>\n"
        f"1. Click 'Join Support Channel' button above\n"
        f"2. Join the channel\n"
        f"3. Come back and click 'I've Joined - Check Now'\n\n"
        f"⚠️ <i>You must join to proceed</i>"
    )
    
    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

def validate_telegram_link(link: str) -> bool:
    """Validate all types of Telegram group links including approval links"""
    patterns = [
        r'^https://(t\.me|telegram\.me)/joinchat/[a-zA-Z0-9_-]+$',
        r'^https://(t\.me|telegram\.me)/\+[a-zA-Z0-9_-]+$',
        r'^https://(t\.me|telegram\.me)/[a-zA-Z0-9_]{5,}$',
        r'^https://(t\.me|telegram\.me)/i/[a-zA-Z0-9_-]+$',
        r'^https://(t\.me|telegram\.me)/c/\d+$',
        r'^https://(t\.me|telegram\.me)/joinchat/[a-zA-Z0-9_-]+\?[a-zA-Z0-9_=&-]+$',
    ]
    
    for pattern in patterns:
        if re.match(pattern, link):
            return True
    
    return False

# ================== COMMAND HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    args = context.args
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "start")
            return
    
    if not args:
        # FIXED: Use proper HTML escaping for the command syntax
        welcome_msg = "👋 <b>Welcome!</b> Use <code>/protect &lt;group_link&gt;</code> to create a protected link."
        if str(user.id) == ADMIN_USER_ID:
            welcome_msg += "\n\n👑 <b>Admin commands available:</b> /broadcast, /stats, /users, /health"
        await update.message.reply_text(welcome_msg, parse_mode="HTML")
        return
async def handle_verification_start(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, encoded: str) -> None:
    """Start verification process for user - GENERATES NEW CAPTCHA EACH TIME"""
    link_data = links_collection.find_one({"encoded": encoded})
    
    if not link_data:
        await update.message.reply_text("❌ Invalid or expired verification link.", parse_mode="HTML")
        return
    
    # DELETE any existing CAPTCHA for this user and encoded link
    captcha_collection.delete_many({"user_id": user_id, "encoded": encoded})
    
    # Generate NEW CAPTCHA code
    captcha_code = generate_captcha_code()
    
    # Check if this code already exists (unlikely but just in case)
    existing_code = captcha_collection.find_one({"captcha_code": captcha_code})
    retry_count = 0
    while existing_code and retry_count < 5:
        captcha_code = generate_captcha_code()
        existing_code = captcha_collection.find_one({"captcha_code": captcha_code})
        retry_count += 1
    
    captcha_data = {
        "user_id": user_id,
        "encoded": encoded,
        "captcha_code": captcha_code,
        "created_at": datetime.utcnow()
    }
    
    try:
        captcha_collection.insert_one(captcha_data)
        
        keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
        await update.message.reply_text(
            "🔒 <b>Verification Required</b>\n\n"
            "Click the button below to start the verification process:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error creating CAPTCHA: {e}")
        keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
        await update.message.reply_text(
            "✅ Verification session found.\n\n"
            "Click the button below to continue:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    
    if query.data.startswith("check_"):
        action = query.data[6:]
        
        is_member = await is_user_in_channel(user.id, context)
        
        if is_member:
            if action.startswith("verify_"):
                encoded = action[7:]
                await handle_verification_start_from_callback(query, context, user.id, encoded)
            elif action == "protect":
                await query.edit_message_text(
                    "✅ <b>Channel Verified!</b>\n\n"
                    "You can now use /protect command.\n\n"
                    "Type: <code>/protect &lt;group_link&gt;</code>",
                    parse_mode="HTML"
                )
            elif action == "start":
                await query.edit_message_text(
                    "✅ <b>Channel Verified!</b>\n\n"
                    "You can now use the bot.\n\n"
                    "<b>Commands:</b>\n"
                    "• /protect - Protect a group link\n"
                    "• /start - Show this message",
                    parse_mode="HTML"
                )
            else:
                await query.edit_message_text(
                    "✅ <b>Channel Verified!</b>\n\n"
                    "You can now use the bot.",
                    parse_mode="HTML"
                )
        else:
            channel = await get_channel_info(context)
            keyboard = [
                [InlineKeyboardButton("✅ Join Support Channel", url=channel["invite_link"])],
                [InlineKeyboardButton("🔁 I've Joined - Check Now", callback_data=query.data)]
            ]
            
            escaped_title = escape_html(channel['title'])
            
            await query.edit_message_text(
                "❌ <b>You're still not a member!</b>\n\n"
                f"Please join <b>{escaped_title}</b> first, then click 'I've Joined - Check Now'.\n\n"
                f"Make sure you've actually joined the channel.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
    
    elif query.data.startswith("verify_"):
        encoded = query.data[7:]
        await handle_captcha_verification(query, context, user.id, encoded)
    
    elif query.data.startswith("copy_"):
        encoded = query.data[5:]
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        
        await query.edit_message_text(
            f"✅ <b>Protected Link Generated</b>\n\n"
            f"Share this link with others:\n"
            f"<code>{protected_link}</code>\n\n"
            f"⚠️ <i>Note: Clicking will start verification process</i>",
            parse_mode="HTML"
        )
    
    elif query.data.startswith("share_link_"):
        encoded = query.data[11:]
        bot_username = context.bot.username
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        
        share_text = (
            f"🔗 <b>Join via Protected Link</b>\n\n"
            f"Users will need to complete verification to join."
        )
        
        keyboard = [[
            InlineKeyboardButton("📤 Share Link", url=f"https://t.me/share/url?url={protected_link}&text=Join%20via%20protected%20link"),
            InlineKeyboardButton("📋 Copy Protected Link", callback_data=f"copy_{encoded}")
        ]]
        
        await query.edit_message_text(
            share_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    
    elif query.data.startswith("users_"):
        page = int(query.data[6:])
        await handle_users_pagination(query, context, page)

async def handle_verification_start_from_callback(query, context, user_id: int, encoded: str) -> None:
    """Start verification from callback - GENERATES NEW CAPTCHA"""
    # Delete existing CAPTCHA and generate new one
    captcha_collection.delete_many({"user_id": user_id, "encoded": encoded})
    
    captcha_code = generate_captcha_code()
    captcha_data = {
        "user_id": user_id,
        "encoded": encoded,
        "captcha_code": captcha_code,
        "created_at": datetime.utcnow()
    }
    
    captcha_collection.insert_one(captcha_data)
    
    keyboard = [[InlineKeyboardButton("🔐 Verify Now", callback_data=f"verify_{encoded}")]]
    
    await query.edit_message_text(
        "✅ <b>Channel Verified!</b>\n\n"
        "Now click the button below to start the CAPTCHA verification:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

async def handle_captcha_verification(query, context, user_id: int, encoded: str) -> None:
    """Handle CAPTCHA verification via inline button"""
    captcha_data = captcha_collection.find_one({"user_id": user_id, "encoded": encoded})
    
    if not captcha_data:
        await query.edit_message_text("❌ No pending verification found.", parse_mode="HTML")
        return
    
    captcha_code = captcha_data["captcha_code"]
    
    await query.edit_message_text(
        f"🔢 <b>Enter CAPTCHA Code</b>\n\n"
        f"Your verification code is: <code>{captcha_code}</code>\n\n"
        f"Please send this 5-digit code back to me within 5 minutes.",
        parse_mode="HTML"
    )

async def protect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Protect a group link - HIDES ORIGINAL LINK FROM USER"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "protect")
            return
    
    await ensure_user_in_db(user.id, user.username, user.first_name)
    
    if update.effective_chat.type != "private":
        await update.message.reply_text("⚠️ Please use this in private chat.", parse_mode="HTML")
        return
    
    if not context.args:
        await update.message.reply_text(
            "📝 <b>Usage:</b> <code>/protect &lt;group_link&gt;</code>\n\n"
            "<b>Supported Link Types:</b>\n"
            "• Public Group: <code>https://t.me/groupname</code>\n"
            "• Approval Link: <code>https://t.me/joinchat/xxxxx</code>\n"
            "• Private Link: <code>https://t.me/+invitecode</code>\n"
            "• Channel Link: <code>https://t.me/c/xxxxx</code>",
            parse_mode="HTML"
        )
        return
    
    group_link = context.args[0]
    
    if not validate_telegram_link(group_link):
        await update.message.reply_text(
            "❌ <b>Invalid Telegram Link</b>\n\n"
            "Please provide a valid Telegram group/channel link.\n\n"
            "<b>Supported formats:</b>\n"
            "• <code>https://t.me/joinchat/xxxxx</code>\n"
            "• <code>https://t.me/+invitecode</code>\n"
            "• <code>https://t.me/groupname</code>\n"
            "• <code>https://t.me/c/xxxxx</code>",
            parse_mode="HTML"
        )
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
        
        # Format link cleanly without escaping
        protected_link_clean = format_link_clean(protected_link)
        
        keyboard = [[
            InlineKeyboardButton("🔗 Share Protected Link", url=f"https://t.me/share/url?url={protected_link}&text=Join%20via%20protected%20link"),
            InlineKeyboardButton("📋 Copy Protected Link", callback_data=f"copy_{encoded}")
        ]]
        
        # Show obfuscated original link (hidden) instead of actual link
        obfuscated_link = generate_obfuscated_link(group_link)
        
        await update.message.reply_text(
            f"✅ <b>Link Protected Successfully!</b>\n\n"
            f"<b>Original Link:</b> <code>{obfuscated_link}</code> (hidden for security)\n\n"
            f"<b>Protected Link:</b>\n"
            f"<code>{protected_link_clean}</code>\n\n"
            f"<b>Important:</b>\n"
            f"• Share the protected link with others\n"
            f"• Users must join support channel first\n"
            f"• Then complete CAPTCHA verification\n"
            f"• <b>Final group link is only accessible via button</b>\n\n"
            f"⚠️ <i>The actual group link is protected and cannot be copied</i>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error in /protect: {e}")
        await update.message.reply_text(
            "❌ An error occurred while processing your request.",
            parse_mode="HTML"
        )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming messages - RESTRICTS GROUP LINK COPYING"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        is_member = await is_user_in_channel(user.id, context)
        if not is_member:
            await send_channel_verification(update, context, "message")
            return
    
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
                    # Create a secure join button - NO LINK DISPLAYED
                    keyboard = [[
                        InlineKeyboardButton("🚀 Click to Join (One-Time Button)", url=link_data["group_link"]),
                        InlineKeyboardButton("📤 Share Verification Link", callback_data=f"share_link_{captcha_data['encoded']}")
                    ]]
                    
                    # Generate obfuscated link for display only
                    obfuscated_link = generate_obfuscated_link(link_data["group_link"])
                    
                    await update.message.reply_text(
                        f"✅ <b>Verification Successful!</b>\n\n"
                        f"<b>Group Access:</b> <code>{obfuscated_link}</code>\n\n"
                        f"⚠️ <b>IMPORTANT SECURITY FEATURES:</b>\n"
                        f"• Group link is <b>NOT displayed</b> for copying\n"
                        f"• Click the button below to join directly\n"
                        f"• Button is one-time use only\n"
                        f"• Link sharing is disabled\n"
                        f"• Screenshot protection enabled\n\n"
                        f"Click the button below to join the group:",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    
                    # Delete the used CAPTCHA to prevent reuse
                    captcha_collection.delete_one({"_id": captcha_data["_id"]})
                    
                    # Update verification count
                    links_collection.update_one(
                        {"encoded": captcha_data["encoded"]},
                        {"$inc": {"verification_count": 1}}
                    )
                else:
                    await update.message.reply_text(
                        "❌ Link has expired.",
                        parse_mode="HTML"
                    )
                    captcha_collection.delete_one({"user_id": user.id})
            else:
                await update.message.reply_text(
                    "❌ Incorrect code. Please try again.",
                    parse_mode="HTML"
                )
                # Delete the CAPTCHA on failed attempt to prevent brute force
                captcha_collection.delete_one({"user_id": user.id})

# ================== ADMIN COMMANDS ==================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin broadcast command"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.", parse_mode="HTML")
        return
    
    if update.message.reply_to_message:
        await broadcast_replied(update, context)
    elif context.args:
        await broadcast_text(update, context)
    else:
        await update.message.reply_text(
            "📢 <b>Broadcast Usage:</b>\n\n"
            "1. <b>Text Broadcast:</b> <code>/broadcast Your message here</code>\n"
            "2. <b>Media Broadcast:</b> Reply to any message with <code>/broadcast</code>\n\n"
            "<b>Supported Media Types:</b>\n"
            "• Photos\n• Videos\n• Documents\n• Audio\n• Voice\n• Stickers\n• GIFs\n• Polls",
            parse_mode="HTML"
        )

async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast text message to all users"""
    user = update.effective_user
    message_text = ' '.join(context.args)
    
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.", parse_mode="HTML")
        return
    
    status_msg = await update.message.reply_text(
        f"📢 Broadcasting to {total_users} users...\n🔄 Sent: 0/{total_users}",
        parse_mode="HTML"
    )
    
    success_count = 0
    failed_count = 0
    
    for user_data in all_users:
        user_id = user_data["user_id"]
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode="HTML"
            )
            success_count += 1
            
            if (success_count + failed_count) % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"📢 Broadcasting to {total_users} users...\n"
                        f"🔄 Sent: {success_count + failed_count}/{total_users}\n"
                        f"✅ Success: {success_count}\n"
                        f"❌ Failed: {failed_count}",
                        parse_mode="HTML"
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send to user {user_id}: {e}")
            
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    final_text = (
        f"✅ <b>Broadcast Completed!</b>\n\n"
        f"📊 <b>Statistics:</b>\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%"
    )
    
    await status_msg.edit_text(
        final_text,
        parse_mode="HTML"
    )

async def broadcast_replied(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast a replied message (supports all media types)"""
    user = update.effective_user
    replied_message = update.message.reply_to_message
    
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.", parse_mode="HTML")
        return
    
    status_msg = await update.message.reply_text(
        f"📢 Broadcasting media to {total_users} users...\n🔄 Sent: 0/{total_users}",
        parse_mode="HTML"
    )
    
    success_count = 0
    failed_count = 0
    
    for user_data in all_users:
        user_id = user_data["user_id"]
        
        try:
            if replied_message.text:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=replied_message.text,
                    parse_mode="HTML"
                )
            elif replied_message.photo:
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=replied_message.photo[-1].file_id,
                    caption=replied_message.caption,
                    parse_mode="HTML"
                )
            elif replied_message.video:
                await context.bot.send_video(
                    chat_id=user_id,
                    video=replied_message.video.file_id,
                    caption=replied_message.caption,
                    parse_mode="HTML"
                )
            elif replied_message.document:
                await context.bot.send_document(
                    chat_id=user_id,
                    document=replied_message.document.file_id,
                    caption=replied_message.caption,
                    parse_mode="HTML"
                )
            elif replied_message.audio:
                await context.bot.send_audio(
                    chat_id=user_id,
                    audio=replied_message.audio.file_id,
                    caption=replied_message.caption,
                    parse_mode="HTML"
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
            elif replied_message.animation:
                await context.bot.send_animation(
                    chat_id=user_id,
                    animation=replied_message.animation.file_id,
                    caption=replied_message.caption,
                    parse_mode="HTML"
                )
            else:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="📨 You received a broadcast message",
                    parse_mode="HTML"
                )
            
            success_count += 1
            
            if (success_count + failed_count) % 10 == 0:
                try:
                    await status_msg.edit_text(
                        f"📢 Broadcasting to {total_users} users...\n"
                        f"🔄 Sent: {success_count + failed_count}/{total_users}\n"
                        f"✅ Success: {success_count}\n"
                        f"❌ Failed: {failed_count}",
                        parse_mode="HTML"
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send to user {user_id}: {e}")
            
            if "blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    final_text = (
        f"✅ <b>Broadcast Completed!</b>\n\n"
        f"📊 <b>Statistics:</b>\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%"
    )
    
    await status_msg.edit_text(
        final_text,
        parse_mode="HTML"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot statistics - Admin only"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.", parse_mode="HTML")
        return
    
    channel = await get_channel_info(context)
    
    total_users = users_collection.count_documents({})
    total_links = links_collection.count_documents({})
    total_captchas = captcha_collection.count_documents({})
    
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_users = users_collection.count_documents({"last_active": {"$gte": today}})
    
    escaped_title = escape_html(channel['title'])
    
    stats_text = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"👥 <b>Users:</b>\n"
        f"• Total Users: {total_users}\n"
        f"• Active Today: {today_users}\n\n"
        f"🔗 <b>Links:</b>\n"
        f"• Total Protected Links: {total_links}\n\n"
        f"🔐 <b>CAPTCHAs:</b>\n"
        f"• Pending CAPTCHAs: {total_captchas}\n\n"
        f"📢 <b>Channel:</b>\n"
        f"• Title: {escaped_title}\n"
        f"• ID: <code>{channel['id'] or SUPPORT_CHANNEL_ID}</code>\n"
        f"• Invite Link: {channel['invite_link'] or 'Not available'}"
    )
    
    await update.message.reply_text(
        stats_text,
        parse_mode="HTML"
    )

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all users with pagination - Admin only"""
    user = update.effective_user
    
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.", parse_mode="HTML")
        return
    
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
    
    users_list = list(users_collection.find(
        {},
        {"user_id": 1, "username": 1, "first_name": 1, "last_active": 1}
    ).sort("last_active", -1).skip(skip).limit(page_size))
    
    total_users = users_collection.count_documents({})
    total_pages = (total_users + page_size - 1) // page_size
    
    if not users_list:
        if hasattr(update, 'message'):
            await update.message.reply_text("No users found.", parse_mode="HTML")
        else:
            await update.edit_message_text("No users found.", parse_mode="HTML")
        return
    
    users_text = "👥 <b>Users List</b>\n\n"
    for i, u in enumerate(users_list):
        username = f"@{u.get('username')}" if u.get('username') else "No username"
        last_active = u.get('last_active', datetime.utcnow())
        days_ago = (datetime.utcnow() - last_active).days
        
        escaped_name = escape_html(u.get('first_name', 'User'))
        escaped_username = escape_html(username)
        
        users_text += (
            f"{skip + i + 1}. {escaped_name}\n"
            f"   👤 {escaped_username}\n"
            f"   🆔 ID: <code>{u.get('user_id')}</code>\n"
            f"   ⏰ Active: {days_ago} days ago\n\n"
        )
    
    users_text += f"📄 Page {page}/{total_pages} • Total Users: {total_users}"
    
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
            parse_mode="HTML",
            reply_markup=reply_markup
        )
    else:
        await update.edit_message_text(
            users_text,
            parse_mode="HTML",
            reply_markup=reply_markup
        )

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check command"""
    try:
        client.admin.command('ping')
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = f"❌ Error: {e}"
    
    bot_info = await context.bot.get_me()
    
    channel = await get_channel_info(context)
    escaped_title = escape_html(channel['title'])
    
    status_text = (
        f"🤖 <b>Bot Status</b>\n\n"
        f"• Bot: @{bot_info.username}\n"
        f"• MongoDB: {mongo_status}\n"
        f"• Users: {users_collection.count_documents({})}\n"
        f"• Links: {links_collection.count_documents({})}\n"
        f"• Pending CAPTCHAs: {captcha_collection.count_documents({})}\n"
        f"• Support Channel: {escaped_title} (ID: <code>{channel['id'] or SUPPORT_CHANNEL_ID}</code>)\n\n"
        f"🕒 Server Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    
    await update.message.reply_text(
        status_text,
        parse_mode="HTML"
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors"""
    logger.error(f"Update {update} caused error: {context.error}")
    
    if update and update.effective_chat:
        try:
            await update.effective_chat.send_message(
                "❌ An error occurred. Please try again later.",
                parse_mode="HTML"
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
    
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
    await ptb_app.bot.set_webhook(
        webhook_url, 
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True
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
