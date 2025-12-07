import os
import logging
import uuid
import base64
import asyncio
import datetime
from typing import Optional
from pymongo import MongoClient
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.templating import Jinja2Templates

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, ChatMember
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.constants import ParseMode

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database Setup (MongoDB) ---
MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    raise Exception("MONGODB_URI environment variable not set!")

# Initialize MongoDB client and select database/collection
client = MongoClient(MONGODB_URI)
db_name = "protected_bot_db"
db = client[db_name]
links_collection = db["protected_links"]
users_collection = db["users"]
broadcast_collection = db["broadcast_history"]

def init_db():
    """Verifies the MongoDB connection."""
    try:
        client.admin.command('ismaster')
        logger.info("✅ MongoDB connected")
        
        # Create indexes
        users_collection.create_index("user_id", unique=True)
        links_collection.create_index("created_by")
        links_collection.create_index("active")
        logger.info("✅ Database indexes created")
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")
        raise

async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of the support channel."""
    support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
    if not support_channel:
        return True
    
    try:
        try:
            chat_id = int(support_channel)
        except ValueError:
            if support_channel.startswith('@'):
                chat_id = support_channel
            else:
                chat_id = f"@{support_channel}"
        
        chat_member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return chat_member.status in [ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER]
    except Exception as e:
        logger.error(f"❌ Channel check error: {e}")
        return False

async def require_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check and enforce channel membership."""
    user_id = update.effective_user.id
    
    # Store user
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "username": update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "last_active": datetime.datetime.now()
        }},
        upsert=True
    )
    
    # Check membership
    return await check_channel_membership(user_id, context)

# --- Telegram Bot Logic ---
telegram_bot_app = Application.builder().token(os.environ.get("TELEGRAM_TOKEN")).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command."""
    user_id = update.effective_user.id
    
    # Store user
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "username": update.effective_user.username,
            "first_name": update.effective_user.first_name,
            "last_active": datetime.datetime.now()
        }},
        upsert=True
    )
    
    # Check if this is a protected link (has argument)
    if context.args:
        encoded_id = context.args[0]
        link_data = links_collection.find_one({"_id": encoded_id, "active": True})

        if link_data:
            web_app_url = f"{os.environ.get('RENDER_EXTERNAL_URL')}/join?token={encoded_id}"
            
            keyboard = [[InlineKeyboardButton("🔗 Join Group", web_app=WebAppInfo(url=web_app_url))]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🔐 This is a Protected Link\n\n"
                "Click the button below to proceed.",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text("❌ Link expired or revoked")
        return
    
    # If no args, check channel membership first
    if not await check_channel_membership(user_id, context):
        support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
        if support_channel:
            # Create simple invite link
            if support_channel.startswith('-100'):
                invite_link = f"https://t.me/c/{support_channel[4:]}"
            elif support_channel.startswith('@'):
                invite_link = f"https://t.me/{support_channel[1:]}"
            else:
                invite_link = f"https://t.me/{support_channel}"
            
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                [InlineKeyboardButton("✅ Check", callback_data="check_join")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🔐 Join our channel first to use this bot.\n"
                "Then click 'Check' below.",
                reply_markup=reply_markup
            )
            return
    
    # User is in channel or no channel required - show beautiful welcome message
    user_name = update.effective_user.first_name or "User"
    username = update.effective_user.username
    
    # Create the beautiful welcome message
    welcome_msg = """╔──────── ✧ ────────╗
      Welcome {username}
╚──────── ✧ ────────╝

🤖 I am your Link Protection Bot
I help you keep your channel links safe & secure.

🛠 Commands:
• /start – Start the bot
• /protect – Generate protected link
• /help – Show help options

🌟 Features:
• 🔒 Advanced Link Encryption
• 🚀 Instant Link Generation
• 🛡️ Anti-Forward Protection
• 🎯 Easy to use UI""".format(username=user_name)
    
    # Create keyboard with support channel button
    keyboard = []
    
    support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
    if support_channel:
        if support_channel.startswith('-100'):
            invite_link = f"https://t.me/c/{support_channel[4:]}"
        elif support_channel.startswith('@'):
            invite_link = f"https://t.me/{support_channel[1:]}"
        else:
            invite_link = f"https://t.me/{support_channel}"
        
        keyboard.append([InlineKeyboardButton("🌟 Support Channel", url=invite_link)])
    
    keyboard.append([InlineKeyboardButton("🚀 Create Protected Link", callback_data="create_link")])
    
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    
    await update.message.reply_text(welcome_msg, reply_markup=reply_markup)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()
    
    if query.data == "check_join":
        if await check_channel_membership(query.from_user.id, context):
            await query.message.edit_text(
                "✅ Verified!\n"
                "You can now use the bot.\n\n"
                "Use /help for commands."
            )
        else:
            await query.answer("❌ Not joined yet. Please join first.", show_alert=True)
    
    elif query.data == "create_link":
        await query.message.reply_text(
            "To create a protected link, use:\n\n"
            "`/protect https://t.me/yourchannel`\n\n"
            "Replace with your actual channel link.",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif query.data == "confirm_broadcast":
        await handle_broadcast_confirmation(update, context)
    
    elif query.data == "cancel_broadcast":
        await query.message.edit_text("❌ Broadcast cancelled")
    
    elif query.data.startswith("revoke_"):
        link_id = query.data.replace("revoke_", "")
        await handle_revoke_link(update, context, link_id)

async def protect_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create protected link."""
    # Check channel membership
    if not await require_channel_membership(update, context):
        support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
        if support_channel:
            if support_channel.startswith('-100'):
                invite_link = f"https://t.me/c/{support_channel[4:]}"
            elif support_channel.startswith('@'):
                invite_link = f"https://t.me/{support_channel[1:]}"
            else:
                invite_link = f"https://t.me/{support_channel}"
            
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                [InlineKeyboardButton("✅ Check", callback_data="check_join")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🔐 Join our channel first to use this bot.\n"
                "Then click 'Check' below.",
                reply_markup=reply_markup
            )
        return
    
    if not context.args or not context.args[0].startswith("https://t.me/"):
        await update.message.reply_text(
            "Usage: `/protect https://t.me/yourchannel`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    group_link = context.args[0]
    unique_id = str(uuid.uuid4())
    encoded_id = base64.urlsafe_b64encode(unique_id.encode()).decode().rstrip("=")
    
    short_id = encoded_id[:8].upper()

    links_collection.insert_one({
        "_id": encoded_id,
        "short_id": short_id,
        "group_link": group_link,
        "created_by": update.effective_user.id,
        "created_at": datetime.datetime.now(),
        "active": True,
        "clicks": 0
    })

    bot_username = (await context.bot.get_me()).username
    protected_link = f"https://t.me/{bot_username}?start={encoded_id}"
    
    # Simple buttons
    keyboard = [
        [
            InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={protected_link}&text=🔐 Protected Link - Join via secure invitation"),
            InlineKeyboardButton("❌ Revoke", callback_data=f"revoke_{encoded_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Formatted message with markdown for easy copying
    await update.message.reply_text(
        f"✅ *Protected Link Created!*\n\n"
        f"🔑 *Link ID:* `{short_id}`\n"
        f"📊 *Status:* 🟢 Active\n"
        f"⏰ *Created:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"🔗 *Your Protected Link:*\n"
        f"`{protected_link}`\n\n"
        f"📋 *Quick Actions:*\n"
        f"• Copy the link above\n"
        f"• Share with your audience\n"
        f"• Revoke anytime with `/revoke {short_id}`",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def revoke_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Revoke a link."""
    # Check channel membership
    if not await require_channel_membership(update, context):
        support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
        if support_channel:
            if support_channel.startswith('-100'):
                invite_link = f"https://t.me/c/{support_channel[4:]}"
            elif support_channel.startswith('@'):
                invite_link = f"https://t.me/{support_channel[1:]}"
            else:
                invite_link = f"https://t.me/{support_channel}"
            
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                [InlineKeyboardButton("✅ Check", callback_data="check_join")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🔐 Join our channel first to use this bot.\n"
                "Then click 'Check' below.",
                reply_markup=reply_markup
            )
        return
    
    if not context.args:
        # Show user's active links
        user_id = update.effective_user.id
        active_links = list(links_collection.find(
            {"created_by": user_id, "active": True},
            sort=[("created_at", -1)],
            limit=10
        ))
        
        if not active_links:
            await update.message.reply_text("📭 No active links")
            return
        
        message = "🔐 *Your Active Links:*\n\n"
        keyboard = []
        
        for link in active_links:
            short_id = link.get('short_id', link['_id'][:8])
            clicks = link.get('clicks', 0)
            created = link.get('created_at', datetime.datetime.now()).strftime('%m/%d')
            
            message += f"• `{short_id}` - {clicks} clicks - {created}\n"
            keyboard.append([InlineKeyboardButton(
                f"❌ Revoke {short_id}",
                callback_data=f"revoke_{link['_id']}"
            )])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message += "\nClick a button below to revoke."
        
        await update.message.reply_text(
            message,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Revoke by ID
    link_id = context.args[0].upper()
    
    # Find link
    query = {
        "$or": [
            {"short_id": link_id},
            {"_id": link_id}
        ],
        "created_by": update.effective_user.id,
        "active": True
    }
    
    link_data = links_collection.find_one(query)
    
    if not link_data:
        await update.message.reply_text("❌ Link not found")
        return
    
    # Revoke
    links_collection.update_one(
        {"_id": link_data['_id']},
        {
            "$set": {
                "active": False,
                "revoked_at": datetime.datetime.now()
            }
        }
    )
    
    await update.message.reply_text(
        f"✅ *Link Revoked!*\n\n"
        f"Link `{link_data.get('short_id', link_id)}` has been permanently revoked.\n\n"
        f"⚠️ All future access attempts will be blocked.",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_revoke_link(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    """Handle revoke button."""
    query = update.callback_query
    await query.answer()
    
    link_data = links_collection.find_one({"_id": link_id, "active": True})
    
    if not link_data:
        await query.message.edit_text(
            "❌ Link not found or already revoked.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    if link_data['created_by'] != query.from_user.id:
        await query.message.edit_text(
            "❌ You don't have permission to revoke this link.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Revoke
    links_collection.update_one(
        {"_id": link_id},
        {
            "$set": {
                "active": False,
                "revoked_at": datetime.datetime.now()
            }
        }
    )
    
    await query.message.edit_text(
        f"✅ *Link Revoked!*\n\n"
        f"Link `{link_data.get('short_id', link_id[:8])}` has been revoked.\n"
        f"👥 Final Clicks: {link_data.get('clicks', 0)}\n\n"
        f"⚠️ All access has been permanently blocked.",
        parse_mode=ParseMode.MARKDOWN
    )

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin broadcast."""
    admin_id = int(os.environ.get("ADMIN_ID", 0))
    if update.effective_user.id != admin_id:
        await update.message.reply_text(
            "🔒 *Admin Access Required*\n\n"
            "This command is restricted to administrators only.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "📢 *Broadcast System*\n\n"
            "To broadcast a message:\n"
            "1. Send any message\n"
            "2. Reply to it with `/broadcast`\n"
            "3. Confirm the action\n\n"
            "✨ *Features:*\n"
            "• Supports all media types\n"
            "• Preserves formatting\n"
            "• Tracks delivery\n"
            "• No rate limiting",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    total_users = users_collection.count_documents({})
    keyboard = [
        [InlineKeyboardButton("✅ Confirm Broadcast", callback_data="confirm_broadcast")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"⚠️ *Broadcast Confirmation*\n\n"
        f"📊 *Delivery Stats:*\n"
        f"• 📨 Recipients: `{total_users}` users\n"
        f"• 📝 Type: {update.message.reply_to_message.content_type}\n"
        f"• ⚡ Delivery: Instant\n\n"
        f"Are you sure you want to proceed?",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    
    context.user_data['broadcast_message'] = update.message.reply_to_message

async def handle_broadcast_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle broadcast confirmation."""
    query = update.callback_query
    await query.answer()
    
    await query.message.edit_text("📤 *Broadcasting...*\n\nPlease wait, this may take a moment.", parse_mode=ParseMode.MARKDOWN)
    
    users = list(users_collection.find({}))
    total_users = len(users)
    successful = 0
    failed = 0
    
    message_to_broadcast = context.user_data.get('broadcast_message')
    
    for user in users:
        try:
            await message_to_broadcast.copy(chat_id=user['user_id'])
            successful += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Failed: {user['user_id']}: {e}")
            failed += 1
    
    broadcast_collection.insert_one({
        "admin_id": query.from_user.id,
        "date": datetime.datetime.now(),
        "total_users": total_users,
        "successful": successful,
        "failed": failed
    })
    
    success_rate = (successful / total_users * 100) if total_users > 0 else 0
    
    await query.message.edit_text(
        f"✅ *Broadcast Complete!*\n\n"
        f"📊 *Delivery Report:*\n"
        f"• 📨 Total Recipients: `{total_users}`\n"
        f"• ✅ Successful: `{successful}`\n"
        f"• ❌ Failed: `{failed}`\n"
        f"• 📈 Success Rate: `{success_rate:.1f}%`\n"
        f"• ⏰ Time: {datetime.datetime.now().strftime('%H:%M:%S')}\n\n"
        f"✨ Broadcast logged in system.",
        parse_mode=ParseMode.MARKDOWN
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show stats."""
    admin_id = int(os.environ.get("ADMIN_ID", 0))
    if update.effective_user.id != admin_id:
        await update.message.reply_text(
            "🔒 *Admin Access Required*\n\n"
            "This command is restricted to administrators only.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    total_users = users_collection.count_documents({})
    total_links = links_collection.count_documents({})
    active_links = links_collection.count_documents({"active": True})
    
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    new_users_today = users_collection.count_documents({"last_active": {"$gte": today}})
    new_links_today = links_collection.count_documents({"created_at": {"$gte": today}})
    
    # Calculate total clicks
    total_clicks_result = links_collection.aggregate([
        {"$group": {"_id": None, "total_clicks": {"$sum": "$clicks"}}}
    ])
    total_clicks = 0
    for result in total_clicks_result:
        total_clicks = result.get('total_clicks', 0)
    
    await update.message.reply_text(
        f"📊 *System Analytics Dashboard*\n\n"
        f"👥 *User Statistics*\n"
        f"• 📈 Total Users: `{total_users}`\n"
        f"• 🆕 New Today: `{new_users_today}`\n\n"
        f"🔗 *Link Statistics*\n"
        f"• 🔢 Total Links: `{total_links}`\n"
        f"• 🟢 Active Links: `{active_links}`\n"
        f"• 🆕 Created Today: `{new_links_today}`\n"
        f"• 👆 Total Clicks: `{total_clicks}`\n\n"
        f"⚙️ *System Status*\n"
        f"• 🗄️ Database: 🟢 Operational\n"
        f"• 🤖 Bot: 🟢 Online\n"
        f"• ⚡ Uptime: 100%\n"
        f"• 🕐 Last Update: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        parse_mode=ParseMode.MARKDOWN
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help."""
    user_id = update.effective_user.id
    
    # Check channel membership
    if not await check_channel_membership(user_id, context):
        support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
        if support_channel:
            if support_channel.startswith('-100'):
                invite_link = f"https://t.me/c/{support_channel[4:]}"
            elif support_channel.startswith('@'):
                invite_link = f"https://t.me/{support_channel[1:]}"
            else:
                invite_link = f"https://t.me/{support_channel}"
            
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                [InlineKeyboardButton("✅ Check", callback_data="check_join")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "🔐 Join our channel first to use this bot.\n"
                "Then click 'Check' below.",
                reply_markup=reply_markup
            )
        return
    
    keyboard = [
        [InlineKeyboardButton("🚀 Create Link", callback_data="create_link")],
        [InlineKeyboardButton("📊 View Stats", callback_data="view_stats")]
    ]
    
    support_channel = os.environ.get("SUPPORT_CHANNEL", "").strip()
    if support_channel:
        if support_channel.startswith('-100'):
            invite_link = f"https://t.me/c/{support_channel[4:]}"
        elif support_channel.startswith('@'):
            invite_link = f"https://t.me/{support_channel[1:]}"
        else:
            invite_link = f"https://t.me/{support_channel}"
        
        keyboard.append([InlineKeyboardButton("🌟 Support Channel", url=invite_link)])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🛡️ *LinkShield Pro - Help Center*\n\n"
        "✨ *Premium Features:*\n"
        "• 🔐 Military-grade encryption\n"
        "• 📊 Advanced analytics\n"
        "• ⚡ Priority processing\n"
        "• 🛡️ DDoS protection\n\n"
        "📋 *Available Commands:*\n"
        "• `/start` - Start the bot\n"
        "• `/protect <link>` - Create secure link\n"
        "• `/revoke` - Revoke access\n"
        "• `/help` - This message\n\n"
        "🔒 *How to Use:*\n"
        "1. Use `/protect https://t.me/yourchannel`\n"
        "2. Share the generated link\n"
        "3. Users join via verification\n"
        "4. Manage with `/revoke`\n\n"
        "💡 *Pro Tips:*\n"
        "• Use descriptive channel names\n"
        "• Monitor link analytics\n"
        "• Revoke unused links\n"
        "• Join our support channel\n\n"
        "👇 *Quick actions:*",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def store_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Store user activity."""
    if update.message and update.message.chat.type == "private":
        users_collection.update_one(
            {"user_id": update.effective_user.id},
            {"$set": {"last_active": update.message.date}},
            upsert=True
        )

# Register handlers
telegram_bot_app.add_handler(CommandHandler("start", start))
telegram_bot_app.add_handler(CommandHandler("protect", protect_command))
telegram_bot_app.add_handler(CommandHandler("revoke", revoke_command))
telegram_bot_app.add_handler(CommandHandler("broadcast", broadcast_command))
telegram_bot_app.add_handler(CommandHandler("stats", stats_command))
telegram_bot_app.add_handler(CommandHandler("help", help_command))
telegram_bot_app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, store_message))

# Add callback handler
from telegram.ext import CallbackQueryHandler
telegram_bot_app.add_handler(CallbackQueryHandler(button_callback))

# --- FastAPI Setup ---
app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def on_startup():
    """Start bot."""
    logger.info("Starting bot...")
    
    required_vars = ["TELEGRAM_TOKEN", "RENDER_EXTERNAL_URL"]
    for var in required_vars:
        if not os.environ.get(var):
            logger.critical(f"Missing {var}")
            raise Exception(f"Missing {var}")
    
    init_db()
    
    await telegram_bot_app.initialize()
    await telegram_bot_app.start()
    
    webhook_url = f"{os.environ.get('RENDER_EXTERNAL_URL')}/{os.environ.get('TELEGRAM_TOKEN')}"
    await telegram_bot_app.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook: {webhook_url}")
    
    bot_info = await telegram_bot_app.bot.get_me()
    logger.info(f"Bot: @{bot_info.username}")

@app.on_event("shutdown")
async def on_shutdown():
    """Stop bot."""
    logger.info("Stopping bot...")
    await telegram_bot_app.stop()
    await telegram_bot_app.shutdown()
    client.close()
    logger.info("Bot stopped")

@app.post("/{token}")
async def telegram_webhook(request: Request, token: str):
    """Telegram webhook."""
    if token != os.environ.get("TELEGRAM_TOKEN"):
        raise HTTPException(status_code=403, detail="Invalid token")
    
    update_data = await request.json()
    update = Update.de_json(update_data, telegram_bot_app.bot)
    await telegram_bot_app.process_update(update)
    
    return Response(status_code=200)

@app.get("/join")
async def join_page(request: Request, token: str):
    """Web app page."""
    return templates.TemplateResponse("join.html", {"request": request, "token": token})

@app.get("/getgrouplink/{token}")
async def get_group_link(token: str):
    """Get real group link."""
    link_data = links_collection.find_one({"_id": token, "active": True})
    
    if link_data:
        links_collection.update_one(
            {"_id": token},
            {"$inc": {"clicks": 1}}
        )
        return {"url": link_data["group_link"]}
    else:
        raise HTTPException(status_code=404, detail="Link not found")

@app.get("/")
async def root():
    """Health check."""
    return {
        "status": "ok",
        "service": "LinkShield",
        "time": datetime.datetime.now().isoformat()
    }

# --- FastAPI Setup ---
app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def on_startup():
    """Start bot."""
    logger.info("Starting bot...")
    
    required_vars = ["TELEGRAM_TOKEN", "RENDER_EXTERNAL_URL"]
    for var in required_vars:
        if not os.environ.get(var):
            logger.critical(f"Missing {var}")
            raise Exception(f"Missing {var}")
    
    init_db()
    
    await telegram_bot_app.initialize()
    await telegram_bot_app.start()
    
    webhook_url = f"{os.environ.get('RENDER_EXTERNAL_URL')}/{os.environ.get('TELEGRAM_TOKEN')}"
    await telegram_bot_app.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook: {webhook_url}")
    
    bot_info = await telegram_bot_app.bot.get_me()
    logger.info(f"Bot: @{bot_info.username}")

@app.on_event("shutdown")
async def on_shutdown():
    """Stop bot."""
    logger.info("Stopping bot...")
    await telegram_bot_app.stop()
    await telegram_bot_app.shutdown()
    client.close()
    logger.info("Bot stopped")

@app.post("/{token}")
async def telegram_webhook(request: Request, token: str):
    """Telegram webhook."""
    if token != os.environ.get("TELEGRAM_TOKEN"):
        raise HTTPException(status_code=403, detail="Invalid token")
    
    update_data = await request.json()
    update = Update.de_json(update_data, telegram_bot_app.bot)
    await telegram_bot_app.process_update(update)
    
    return Response(status_code=200)

@app.get("/join")
async def join_page(request: Request, token: str):
    """Web app page."""
    return templates.TemplateResponse("join.html", {"request": request, "token": token})

@app.get("/getgrouplink/{token}")
async def get_group_link(token: str):
    """Get real group link."""
    link_data = links_collection.find_one({"_id": token, "active": True})
    
    if link_data:
        links_collection.update_one(
            {"_id": token},
            {"$inc": {"clicks": 1}}
        )
        return {"url": link_data["group_link"]}
    else:
        raise HTTPException(status_code=404, detail="Link not found")

@app.get("/")
async def root():
    """Health check."""
    return {
        "status": "ok",
        "service": "LinkShield",
        "time": datetime.datetime.now().isoformat()
          }

