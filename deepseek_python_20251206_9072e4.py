import os
import logging
import secrets
import string
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Union
from urllib.parse import urlparse

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    Poll,
    Message,
    Bot,
    User as TelegramUser
)
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
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")  # Add your Telegram user ID here
PORT = int(os.environ.get("PORT", 8443))

# MongoDB setup
client = MongoClient(MONGO_URL)
db = client.telegram_bot_db
links_collection = db.links
captcha_collection = db.captcha
users_collection = db.users
broadcasts_collection = db.broadcasts

# Create indexes
links_collection.create_index("encoded", unique=True)
links_collection.create_index("created_at", expireAfterSeconds=2592000)  # 30 days TTL
captcha_collection.create_index("user_id", unique=True)
captcha_collection.create_index("created_at", expireAfterSeconds=300)  # 5 minutes TTL
users_collection.create_index("user_id", unique=True)
broadcasts_collection.create_index("broadcast_id", unique=True)
broadcasts_collection.create_index("created_at")

# Generate random encoded string
def generate_encoded_string(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# Generate 5-digit CAPTCHA code
def generate_captcha_code() -> str:
    return ''.join(secrets.choice(string.digits) for _ in range(5))

async def ensure_user_in_db(user: TelegramUser) -> None:
    """Ensure user exists in users collection."""
    user_data = {
        "user_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "last_active": datetime.utcnow(),
        "joined_at": datetime.utcnow(),
        "message_count": 0
    }
    
    users_collection.update_one(
        {"user_id": user.id},
        {
            "$setOnInsert": user_data,
            "$set": {"last_active": datetime.utcnow()},
            "$inc": {"message_count": 1}
        },
        upsert=True
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command with verify parameter."""
    user = update.effective_user
    args = context.args
    
    # Track user in database
    await ensure_user_in_db(user)
    
    if not args:
        if user.id == int(ADMIN_USER_ID or 0):
            await update.message.reply_text(
                "👑 Welcome Admin!\n\n"
                "Available commands:\n"
                "/protect <group_link> - Protect a group link\n"
                "/broadcast - Broadcast message to all users\n"
                "/stats - Show bot statistics\n"
                "/users - List all users\n"
                "/health - Check bot health\n\n"
                "For normal users:\n"
                "Use /protect <group_link> to create a protected link."
            )
        else:
            await update.message.reply_text(
                "👋 Welcome! Use /protect <group_link> to create a protected link."
            )
        return
    
    # Check if it's a verify link
    if args[0].startswith("verify_"):
        encoded = args[0][7:]  # Remove "verify_" prefix
        
        # Find the original link
        link_data = links_collection.find_one({"encoded": encoded})
        if not link_data:
            await update.message.reply_text("❌ Invalid or expired verification link.")
            return
        
        # Check if user already has a pending CAPTCHA
        existing_captcha = captcha_collection.find_one({
            "user_id": user.id,
            "encoded": encoded
        })
        
        if existing_captcha:
            await update.message.reply_text(
                f"You already have a pending CAPTCHA: `{existing_captcha['captcha_code']}`\n"
                "Please enter it here within 5 minutes.",
                parse_mode="Markdown"
            )
            return
        
        # Generate new CAPTCHA
        captcha_code = generate_captcha_code()
        
        # Save to database
        captcha_data = {
            "user_id": user.id,
            "encoded": encoded,
            "captcha_code": captcha_code,
            "created_at": datetime.utcnow()
        }
        
        try:
            captcha_collection.insert_one(captcha_data)
            
            await update.message.reply_text(
                f"🔒 Verification required!\n\n"
                f"Your CAPTCHA code is: `{captcha_code}`\n\n"
                f"Please send this code back to me within 5 minutes to get the group link.",
                parse_mode="Markdown"
            )
        except DuplicateKeyError:
            await update.message.reply_text(
                "You already have a pending verification. Please complete it first."
            )

async def protect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /protect command."""
    user = update.effective_user
    
    # Track user in database
    await ensure_user_in_db(user)
    
    # Check if it's a private chat
    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "⚠️ Please use this command in a private chat with me."
        )
        return
    
    # Check if link is provided
    if not context.args:
        await update.message.reply_text(
            "Usage: /protect <group_invite_link>\n\n"
            "Example: /protect https://t.me/joinchat/ABCD1234"
        )
        return
    
    group_link = context.args[0]
    
    # Basic validation of Telegram invite link
    if not (group_link.startswith("https://t.me/") or 
            group_link.startswith("https://telegram.me/")):
        await update.message.reply_text(
            "❌ Please provide a valid Telegram group invite link."
        )
        return
    
    # Generate unique encoded string
    encoded = generate_encoded_string()
    
    # Save to database
    link_data = {
        "encoded": encoded,
        "group_link": group_link,
        "created_by": user.id,
        "created_at": datetime.utcnow(),
        "verification_count": 0
    }
    
    try:
        links_collection.insert_one(link_data)
        
        # Get bot username
        bot_username = context.bot.username
        
        # Create protected link
        protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
        
        # Create keyboard for sharing
        keyboard = [
            [InlineKeyboardButton("🔗 Share Protected Link", url=f"https://t.me/share/url?url={protected_link}")],
            [InlineKeyboardButton("📋 Copy Link", callback_data=f"copy_{encoded}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"✅ Link protected successfully!\n\n"
            f"**Original Link:** {group_link}\n"
            f"**Protected Link:** {protected_link}\n\n"
            f"Share this protected link. Users will need to complete a CAPTCHA to get the actual group link.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error saving link: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle regular messages (for CAPTCHA verification)."""
    user = update.effective_user
    
    # Track user in database
    await ensure_user_in_db(user)
    
    # Only process in private chats
    if update.effective_chat.type != "private":
        return
    
    message_text = update.message.text
    
    # Check if message is text
    if message_text:
        message_text = message_text.strip()
        # Check if it's a 5-digit code
        if len(message_text) == 5 and message_text.isdigit():
            # Find pending CAPTCHA for this user
            captcha_data = captcha_collection.find_one({"user_id": user.id})
            
            if captcha_data:
                if message_text == captcha_data["captcha_code"]:
                    # Get the original group link
                    link_data = links_collection.find_one({
                        "encoded": captcha_data["encoded"]
                    })
                    
                    if link_data:
                        # Send the group link
                        await update.message.reply_text(
                            f"✅ Verification successful!\n\n"
                            f"Here's your group link: {link_data['group_link']}\n\n"
                            f"Click the link to join the group.",
                            parse_mode="Markdown"
                        )
                        
                        # Delete CAPTCHA data
                        captcha_collection.delete_one({"user_id": user.id})
                        
                        # Track successful verification
                        links_collection.update_one(
                            {"encoded": captcha_data["encoded"]},
                            {"$inc": {"verification_count": 1}}
                        )
                    else:
                        await update.message.reply_text("❌ The link has expired or been deleted.")
                        captcha_collection.delete_one({"user_id": user.id})
                else:
                    await update.message.reply_text("❌ Incorrect code. Please try again.")
            else:
                # No pending CAPTCHA found
                await update.message.reply_text(
                    "No pending verification found. Please use a valid verification link."
                )

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /broadcast command - Admin only."""
    user = update.effective_user
    
    # Check if user is admin
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.")
        return
    
    # Check if replying to a message or has text
    if update.message.reply_to_message:
        # Broadcast the replied message
        await broadcast_replied_message(update, context)
    elif context.args:
        # Broadcast text message
        await broadcast_text(update, context)
    else:
        await update.message.reply_text(
            "📢 **Broadcast Usage:**\n\n"
            "1. **Text Broadcast:** `/broadcast Your message here`\n"
            "2. **Media Broadcast:** Reply to any message (photo, video, sticker, document, etc.) with `/broadcast`\n\n"
            "**Supported Media Types:**\n"
            "• Photos\n• Videos\n• Documents\n• Audio\n• Voice\n• Stickers\n• GIFs\n• Polls",
            parse_mode="Markdown"
        )

async def broadcast_replied_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast a replied message (supports all media types)."""
    user = update.effective_user
    replied_message = update.message.reply_to_message
    
    # Get all users
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return
    
    # Generate broadcast ID
    broadcast_id = generate_encoded_string(8)
    
    # Save broadcast info
    broadcast_data = {
        "broadcast_id": broadcast_id,
        "admin_id": user.id,
        "message_type": "replied",
        "total_users": total_users,
        "sent_count": 0,
        "failed_count": 0,
        "created_at": datetime.utcnow(),
        "status": "sending"
    }
    
    broadcasts_collection.insert_one(broadcast_data)
    
    # Send initial status
    status_msg = await update.message.reply_text(
        f"📢 Starting broadcast to {total_users} users...\n"
        f"🔄 Sent: 0/{total_users}\n"
        f"✅ Success: 0\n"
        f"❌ Failed: 0"
    )
    
    success_count = 0
    failed_count = 0
    
    # Process based on message type
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
            elif replied_message.poll:
                # Create a new poll with same options
                await context.bot.send_poll(
                    chat_id=user_id,
                    question=replied_message.poll.question,
                    options=[option.text for option in replied_message.poll.options],
                    is_anonymous=replied_message.poll.is_anonymous,
                    allows_multiple_answers=replied_message.poll.allows_multiple_answers
                )
            else:
                # If unsupported type, send as text
                await context.bot.send_message(
                    chat_id=user_id,
                    text="📨 You received a broadcast message (unsupported format)"
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
            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    # Final status update
    final_text = (
        f"✅ **Broadcast Completed!**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%\n\n"
        f"🆔 Broadcast ID: `{broadcast_id}`"
    )
    
    await status_msg.edit_text(final_text, parse_mode="Markdown")
    
    # Update broadcast record
    broadcasts_collection.update_one(
        {"broadcast_id": broadcast_id},
        {
            "$set": {
                "sent_count": success_count,
                "failed_count": failed_count,
                "status": "completed",
                "completed_at": datetime.utcnow()
            }
        }
    )

async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast text message to all users."""
    user = update.effective_user
    message_text = ' '.join(context.args)
    
    # Get all users
    all_users = list(users_collection.find({}, {"user_id": 1}))
    total_users = len(all_users)
    
    if total_users == 0:
        await update.message.reply_text("❌ No users found to broadcast to.")
        return
    
    # Generate broadcast ID
    broadcast_id = generate_encoded_string(8)
    
    # Save broadcast info
    broadcast_data = {
        "broadcast_id": broadcast_id,
        "admin_id": user.id,
        "message_type": "text",
        "message_text": message_text,
        "total_users": total_users,
        "sent_count": 0,
        "failed_count": 0,
        "created_at": datetime.utcnow(),
        "status": "sending"
    }
    
    broadcasts_collection.insert_one(broadcast_data)
    
    # Send initial status
    status_msg = await update.message.reply_text(
        f"📢 Starting text broadcast to {total_users} users...\n"
        f"🔄 Sent: 0/{total_users}\n"
        f"✅ Success: 0\n"
        f"❌ Failed: 0"
    )
    
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
            if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                users_collection.delete_one({"user_id": user_id})
    
    # Final status update
    final_text = (
        f"✅ **Text Broadcast Completed!**\n\n"
        f"📊 **Statistics:**\n"
        f"• Total Users: {total_users}\n"
        f"• ✅ Success: {success_count}\n"
        f"• ❌ Failed: {failed_count}\n"
        f"• 📈 Success Rate: {(success_count/total_users*100):.1f}%\n\n"
        f"🆔 Broadcast ID: `{broadcast_id}`\n"
        f"📝 **Message:**\n{message_text[:200]}..."
    )
    
    await status_msg.edit_text(final_text, parse_mode="Markdown")
    
    # Update broadcast record
    broadcasts_collection.update_one(
        {"broadcast_id": broadcast_id},
        {
            "$set": {
                "sent_count": success_count,
                "failed_count": failed_count,
                "status": "completed",
                "completed_at": datetime.utcnow()
            }
        }
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot statistics - Admin only."""
    user = update.effective_user
    
    # Check if user is admin
    if str(user.id) != ADMIN_USER_ID:
        await update.message.reply_text("❌ This command is only for administrators.")
        return
    
    # Get statistics
    total_users = users_collection.count_documents({})
    total_links = links_collection.count_documents({})
    total_captchas = captcha_collection.count_documents({})
    total_broadcasts = broadcasts_collection.count_documents({})
    
    # Get today's stats
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_users = users_collection.count_documents({"last_active": {"$gte": today}})
    today_links = links_collection.count_documents({"created_at": {"$gte": today}})
    
    # Get top 5 active users
    active_users = list(users_collection.find(
        {}, 
        {"username": 1, "first_name": 1, "message_count": 1}
    ).sort("message_count", -1).limit(5))
    
    active_users_text = "\n".join([
        f"{i+1}. {u.get('first_name', 'User')} (@{u.get('username', 'N/A')}) - {u.get('message_count', 0)} msgs"
        for i, u in enumerate(active_users)
    ])
    
    stats_text = (
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Users:**\n"
        f"• Total Users: {total_users}\n"
        f"• Active Today: {today_users}\n\n"
        f"🔗 **Links:**\n"
        f"• Total Protected Links: {total_links}\n"
        f"• Created Today: {today_links}\n\n"
        f"🔐 **CAPTCHAs:**\n"
        f"• Pending CAPTCHAs: {total_captchas}\n\n"
        f"📢 **Broadcasts:**\n"
        f"• Total Broadcasts: {total_broadcasts}\n\n"
        f"🏆 **Top 5 Active Users:**\n{active_users_text}"
    )
    
    await update.message.reply_text(stats_text, parse_mode="Markdown")

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all users with pagination - Admin only."""
    user = update.effective_user
    
    # Check if user is admin
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
        await update.message.reply_text("No users found.")
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
    
    await update.message.reply_text(
        users_text,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("copy_"):
        encoded = data[5:]
        link_data = links_collection.find_one({"encoded": encoded})
        
        if link_data:
            bot_username = context.bot.username
            protected_link = f"https://t.me/{bot_username}?start=verify_{encoded}"
            
            await query.edit_message_text(
                f"✅ Link copied to clipboard!\n\n"
                f"Protected link: `{protected_link}`\n\n"
                f"Share this link with others.",
                parse_mode="Markdown"
            )
    
    elif data.startswith("users_"):
        # Handle users pagination
        page = int(data[6:])
        
        page_size = 10
        skip = (page - 1) * page_size
        
        # Get users for this page
        users_list = list(users_collection.find(
            {},
            {"user_id": 1, "username": 1, "first_name": 1, "last_active": 1}
        ).sort("last_active", -1).skip(skip).limit(page_size))
        
        total_users = users_collection.count_documents({})
        total_pages = (total_users + page_size - 1) // page_size
        
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
        
        await query.edit_message_text(
            users_text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Health check command."""
    try:
        # Check MongoDB connection
        client.admin.command('ping')
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = f"❌ Error: {e}"
    
    # Get bot info
    bot_info = await context.bot.get_me()
    
    status_text = (
        f"🤖 **Bot Status**\n\n"
        f"• Bot: @{bot_info.username}\n"
        f"• MongoDB: {mongo_status}\n"
        f"• Users: {users_collection.count_documents({})}\n"
        f"• Links: {links_collection.count_documents({})}\n"
        f"• Pending CAPTCHAs: {captcha_collection.count_documents({})}\n"
        f"• Broadcasts: {broadcasts_collection.count_documents({})}\n\n"
        f"🕒 Server Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    
    await update.message.reply_text(status_text, parse_mode="Markdown")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors."""
    logger.error(f"Update {update} caused error: {context.error}")
    
    if update and update.effective_chat:
        try:
            await update.effective_chat.send_message(
                "❌ An error occurred. Please try again later."
            )
        except Exception:
            pass

def main() -> None:
    """Start the bot."""
    # Check environment variables
    required_vars = ["BOT_TOKEN", "MONGO_URL", "RENDER_EXTERNAL_URL", "ADMIN_USER_ID"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Missing environment variables: {missing_vars}")
        raise ValueError(f"Please set {', '.join(missing_vars)} environment variables")
    
    # Create Application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("protect", protect))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("health", health_check))
    
    # Add message handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Start webhook on Render
    logger.info("Starting bot in webhook mode...")
    
    # Set webhook
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=webhook_url,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == "__main__":
    main()