import json # Keep for potential future use or config parsing
import telebot
from telebot import types
import threading
import time
import re
import html # For escaping HTML
import datetime # For timestamps
import logging
import os
from telebot.types import BotCommand

# --- Library Imports for MongoDB ---
import pymongo
from bson.objectid import ObjectId
from dotenv import load_dotenv # Optional: for environment variables

# --- Telegram Utilities ---
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, Message

# --- Environment Variable Setup (Optional but Recommended) ---
load_dotenv() # Loads variables from a .env file if it exists

# --- Constants ---
# *** UPDATE THIS WITH YOUR ANIME AUCTION BOT TOKEN ***
API_TOKEN = os.getenv('API_TOKEN', '7794179366:AAG9JZPxADB-vjoe_AQUeU7Zt4dtR8GcpvM')

# *** UPDATE WITH YOUR ADMIN/MOD IDs ***
admin_id = [6265981509, 6969086416, 1411248872, 6680499027, 5026099926 , 5785100406, 6799532757] # Example IDs
xmods = [6265981509, 6969086416, 1411248872, 6680499027, 5026099926 , 5785100406, 6799532757] # Admins are also mods
owner_id = [6265981509, 6969086416 , 5785100406, 6799532757] # Example IDs

CURRENT_BOT_VERSION = '1.0.1_anime_no_points' # Indicate new version

# *** UPDATE THIS to the Anime Bot's User ID that sends Pet/Core info ***
FORWARD_BOT_ID = 5364964725

# --- Define Categories ---
# *** NEW: Simpler categories ***
CATEGORY_PET = 'pet'
CATEGORY_CORE = 'core'
VALID_CATEGORIES = [CATEGORY_PET, CATEGORY_CORE]

# --- Sticker IDs (Optional: Replace with relevant Anime bot stickers or remove) ---
THINK_STICKER_ID = 'CAACAgIAAxkBAAMMZ87iQwTCaohIrXEPmuo737biU28AAvROAAJwfXFKVOLfiYCbwdQ2BA' # Example, replace or remove if not used
ANGRY_STICKER_ID = 'CAACAgIAAxkBAAMLZ87hgXAi9j4AAYh38qIXREOTPtxGAAJ9IwACxLexSlhnxsp8febCNgQ' # Example, replace or remove if not used
OK_STICKER_ID = 'CAACAgIAAxkBAAMWZ87jKxy7en5eagIoE5rTgLsYebgAAthNAAJh0cFLNNXdCJvy9Gc2BA' # Example, replace or remove if not used
WARNING_STICKER_ID = 'CAACAgIAAxkBAAMcZ87kE8hkAUM7_5SU9ooLpnaz1wEAAvJPAAL00MFLv3iVc316FkI2BA' # Example, replace or remove if not used
SOLD_STICKER_ID ='CAACAgIAAxkBAAMeZ87kMNn-RV1LpyhTTmXHLFazSs4AAtdFAAJ1U6FLxJBd2EJWgUo2BA' # Example, replace or remove if not used
# WELCOME_STICKER_ID = '...' # Example, replace or remove if not used

# --- Channel/Group IDs ---
# *** UPDATE THESE for the new bot's environment ***
AUCTION_GROUP_LINK = os.getenv('AUCTION_GROUP_LINK', 'https://t.me/YOUR_ANIME_AUCTION_LINK') # Public link
AUCTION_CHAT_ID = int(os.getenv('AUCTION_CHAT_ID', -1002136189599)) # Numerical ID for checking membership
TRADE_CHAT_ID = int(os.getenv('TRADE_CHAT_ID', -1002136189599)) # Numerical ID for checking membership (use a real link or ID for the group)
APPROVE_CHANNEL = int(os.getenv('APPROVE_CHANNEL', -1002569601260)) # Where admins approve/reject submissions
POST_CHANNEL = int(os.getenv('POST_CHANNEL', -1002194236485)) # Where approved items & bids are posted
REJECT_CHANNEL = int(os.getenv('REJECT_CHANNEL', APPROVE_CHANNEL)) # Where rejection notifications are logged (can be same as approve)
ADMIN_BID_LOG_CHANNEL = int(os.getenv('ADMIN_BID_LOG_CHANNEL', -1002569601260)) # For logging every bid attempt
ADMIN_GROUP_ID = int(os.getenv('ADMIN_GROUP_ID', APPROVE_CHANNEL)) # For high-level notifications
LOG_GROUP_ID = int(os.getenv('LOG_GROUP_ID', -1002569601260)) # ⚠️ Replace with your ACTUAL Log Group ID

# === Telegram Logging Handler ===
class TelegramLogHandler(logging.Handler):
    """A custom logging handler that sends log records to a Telegram chat."""
    def __init__(self, bot_instance, chat_id, level=logging.NOTSET):
        super().__init__(level=level)
        self.bot = bot_instance
        self.chat_id = chat_id
        self.formatter = logging.Formatter('%(levelname)s - %(message)s') # Simple default

    def emit(self, record):
        try:
            log_entry = self.format(record) # Format the record
            level_emoji = {
                logging.DEBUG: "⚙️ DEBUG", logging.INFO: "ℹ️ INFO",
                logging.WARNING: "⚠️ WARNING", logging.ERROR: "❌ ERROR",
                logging.CRITICAL: "🔥 CRITICAL",
            }.get(record.levelno, f"📊 LVL-{record.levelno}")
            safe_log_entry = html.escape(log_entry)
            telegram_message = f"<b>{level_emoji}</b>\n<pre>{safe_log_entry}</pre>" # Use <pre> for code-like formatting
            max_len = 4096
            if len(telegram_message) > max_len:
                telegram_message = telegram_message[:max_len - 20] + "\n... (truncated)"
            self.bot.send_message(
                self.chat_id, telegram_message, parse_mode='HTML',
                disable_web_page_preview=True
            )
        except Exception as e:
            print(f"!!! FAILED TO SEND LOG TO TELEGRAM ({self.chat_id}): {e}")
            try:
                original_log = self.format(record)
                print(f"--- Original Log Message: {original_log}")
            except Exception:
                print("--- Could not format original log message.")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.propagate = True # Set to False if you don't want logs appearing in console too

# --- MongoDB Setup ---
# *** UPDATE MONGODB URI ***
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://yesvashisht2005:yash2005@cluster0.jlfygps.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

if "user:PASSWORD" in MONGO_URI or "<PASSWORD>" in MONGO_URI: # Basic check
    logger.critical("\n" + "="*50 + "\n" +
                    "❌ CRITICAL WARNING: MongoDB URI seems to be using placeholders!\n" +
                    "   Please replace placeholders or set the MONGO_URI environment variable.\n" +
                    "="*50 + "\n")
    # Consider exiting: exit(1)

try:
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=20000)
    client.admin.command('ping')
    # *** USE A NEW DATABASE NAME FOR THE ANIME BOT ***
    db = client['anime_auction_db'] # Changed DB name
    users_col = db['users']                   # Stores user info, ban status, version
    bids_col = db['bids']                     # Stores active/past bid information
    approved_items_col = db['approved_items'] # Tracks items approved for auction
    pending_items_col = db['pending_items']   # For items awaiting admin approval
    config_col = db['config']                 # For counters, settings

    logger.info("Attempting to create MongoDB indexes...")
    users_col.create_index("user_id", unique=True)
    # users_col.create_index("points") # REMOVED
    users_col.create_index("is_banned")
    bids_col.create_index("bid_id", unique=True)
    bids_col.create_index("owner_id")
    bids_col.create_index("highest_bidder_id")
    bids_col.create_index("status")
    bids_col.create_index("item_type") # Index for pets/cores
    bids_col.create_index("item_name") # Maybe index item name
    approved_items_col.create_index([("user_id", 1), ("category", 1)])
    approved_items_col.create_index("link")
    pending_items_col.create_index("user_id")
    pending_items_col.create_index("submission_time")
    pending_items_col.create_index("status")
    config_col.create_index("key", unique=True)
    logger.info("✅ MongoDB Indexes checked/created.")
    logger.info("✅ Successfully connected to MongoDB.")

except pymongo.errors.ConfigurationError as e:
    logger.error(f"❌ MongoDB Configuration Error: {e}")
    exit(1)
except pymongo.errors.ServerSelectionTimeoutError as e:
    logger.error(f"❌ MongoDB Connection Timeout: {e}")
    exit(1)
except pymongo.errors.ConnectionFailure as e:
    logger.error(f"❌ MongoDB Connection Error: {e}")
    exit(1)
except Exception as e:
    logger.error(f"❌ An unexpected error occurred during MongoDB setup: {e}")
    exit(1)
# --- End MongoDB Setup ---

# --- Telegram Bot Initialization ---
bot = telebot.TeleBot(API_TOKEN)

# --- Add Telegram Logging Handler ---
if LOG_GROUP_ID and isinstance(LOG_GROUP_ID, int) and LOG_GROUP_ID < 0:
    try:
        telegram_handler = TelegramLogHandler(bot_instance=bot, chat_id=LOG_GROUP_ID)
        telegram_handler.setLevel(logging.INFO) # Set desired level
        telegram_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        telegram_handler.setFormatter(telegram_formatter)
        logger.addHandler(telegram_handler)
        logger.info(f"✅ Telegram logging handler initialized. Logs >= {logging.getLevelName(telegram_handler.level)} sent to Chat ID: {LOG_GROUP_ID}")
    except Exception as log_init_error:
        print(f"!!! FAILED TO INITIALIZE TELEGRAM LOG HANDLER: {log_init_error}")
        logger.error(f"Failed to initialize Telegram log handler: {log_init_error}", exc_info=False)
else:
    warn_msg = f"LOG_GROUP_ID ('{LOG_GROUP_ID}') is not set or is not a valid negative integer group ID. Telegram logging disabled."
    logger.warning(warn_msg)
    print(f"!!! WARNING: {warn_msg}")
# --- End Add Telegram Logging Handler ---

# --- Global State Variables ---
user_join_status = {}
user_states = {}      # Tracks user state for multi-step commands (like /add)
user_cache = {}       # Temporarily stores submission data during /add steps
pending_bids = {}     # Temporarily stores bids awaiting confirmation {confirmation_key: details}
sub_process = True    # Submission status flag
bid_ji = True         # Bidding status flag

# === Helper Functions ===

def escape(text):
    """Basic HTML escape for user-provided text."""
    if not text: return ""
    return html.escape(str(text))

def is_admin(user_id):
    return user_id in admin_id

def is_mod(user_id):
    return user_id in xmods # Includes admins

def is_banned(user_id):
    try:
        user_doc = users_col.find_one({"user_id": str(user_id)}, {"is_banned": 1})
        return user_doc and user_doc.get("is_banned", False)
    except Exception as e:
        logger.error(f"Error checking ban status for {user_id}: {e}")
        return False

def has_started_bot(user_id):
    try:
        return users_col.count_documents({"user_id": str(user_id)}) > 0
    except Exception as e:
        logger.error(f"Error checking if user {user_id} started: {e}")
        return False

def get_user_doc(user_id):
     try:
         return users_col.find_one({"user_id": str(user_id)})
     except Exception as e:
         logger.error(f"Error fetching user doc for {user_id}: {e}")
         return None

def is_user_updated(user_doc):
    """Check if the user's bot version matches the current bot version."""
    if not user_doc: return False
    return user_doc.get("version") == CURRENT_BOT_VERSION

def format_username_html(user_doc):
    if not user_doc: return "N/A"
    user_id = user_doc.get('user_id')
    name = escape(user_doc.get("name", f"User {user_id}"))
    return f'<a href="tg://user?id={user_id}">{name}</a>'

def parse_bid_amount(amount_str):
    """Parses bid strings like '1k', '500', '2.5k' into a float."""
    if not isinstance(amount_str, str): amount_str = str(amount_str)
    original_str = amount_str
    amount_str = amount_str.lower().replace('pd','').replace('s','').strip() # Keep replacements generic
    multiplier = 1.0
    if 'k' in amount_str:
        multiplier = 1000.0
        amount_str = amount_str.replace('k', '')
    try:
        return float(amount_str) * multiplier
    except ValueError:
        logger.warning(f"Could not parse bid amount: '{original_str}'")
        return 0.0

def get_next_bid_id():
    """Atomically increments and returns the next bid counter value."""
    try:
        counter_doc = config_col.find_one_and_update(
           {"_id": "bid_counter"}, {"$inc": {"value": 1}},
           upsert=True, return_document=pymongo.ReturnDocument.AFTER
        )
        if counter_doc and 'value' in counter_doc:
             current_value = counter_doc['value']
             if current_value == 0: # Handle first upsert case
                 counter_doc = config_col.find_one_and_update(
                     {"_id": "bid_counter"}, {"$set": {"value": 1}},
                     return_document=pymongo.ReturnDocument.AFTER
                 )
                 current_value = 1
             return f"A{current_value}" # Use 'A' for Anime Auction prefix
        else:
             logger.error("Failed to get/update bid counter, attempting recovery.")
             config_col.update_one({"_id": "bid_counter"}, {"$set": {"value": 1}}, upsert=True)
             return f"A1"
    except Exception as counter_err:
        logger.error(f"Error getting/updating bid counter: {counter_err}")
        fallback_doc = config_col.find_one({"_id": "bid_counter"})
        fallback_val = fallback_doc['value'] if fallback_doc else int(time.time()) % 10000
        return f"ERR{fallback_val}"

def create_bid_message(bid_id, highest_bidder_mention, current_bid, base_price):
    """Creates the standard text for the bid message."""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M UTC')
    current_bid_display = f"{current_bid:,.0f}" if isinstance(current_bid, (int, float)) else str(current_bid)
    base_price_display = f"{base_price:,.0f}" if isinstance(base_price, (int, float)) else str(base_price)

    if highest_bidder_mention:
        text = (f"╔═══ Bid Update ═══╗\n"
                f"║ 🏷️ Item ID: `{bid_id}`\n"
                f"║ 💰 Current Bid: `{current_bid_display}`\n"
                f"║ 👤 By: {highest_bidder_mention}\n" # Mention is already HTML/Markdown
                f"║ 🕒 {timestamp}\n╚════════════════╝")
    else:
        text = (f"╔═══ Auction Start ═══╗\n"
                f"║ 🏷️ Item ID: `{bid_id}`\n"
                f"║ 💰 Starting Bid: `{base_price_display}`\n"
                f"║ 👤 No bids yet!\n"
                f"║ 🕒 {timestamp}\n╚═════════════════╝")
    return text

def update_bid_message_in_channel(bid_id):
    """Fetches bid data and updates the message in POST_CHANNEL."""
    try:
        bid_data = bids_col.find_one({"bid_id": bid_id})
        if not bid_data or bid_data.get("status") != "active":
             if bid_data: logger.debug(f"Bid {bid_id} not active, skipping update.")
             return

        chat_id = bid_data.get("chat_id")
        message_id = bid_data.get("message_id")
        highest_bidder = bid_data.get("highest_bidder_mention")
        current_bid = bid_data.get("current_bid")
        base_price = bid_data.get("base_price")

        if not chat_id or not message_id:
            return logger.error(f"Missing chat_id or message_id for bid {bid_id}.")

        updated_text = create_bid_message(bid_id, highest_bidder, current_bid, base_price)
        markup = InlineKeyboardMarkup()
        bot_username = bot.get_me().username
        markup.row(
            InlineKeyboardButton("🔄 Refresh", callback_data=f"ref_{bid_id}"),
            InlineKeyboardButton("🔗 Place Bid", url=f"https://t.me/{bot_username}?start=bid-{bid_id}")
        )
        bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=updated_text,
            parse_mode="Markdown", disable_web_page_preview=True, reply_markup=markup
        )
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e):
              logger.debug(f"Bid message {bid_id} was not modified.")
         else:
              logger.error(f"API Error updating bid message for {bid_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error updating bid message for {bid_id}: {e}")

def get_min_bid_increment(current_bid):
    """Determines the minimum required bid increment based on new rules."""
    current_bid_numeric = parse_bid_amount(current_bid)
    if current_bid_numeric <= 50: return 1.0
    elif current_bid_numeric <= 100: return 2.0
    else: return 5.0

def is_valid_forwarded_message(message):
    """Checks if the forwarded message is from the specified Anime bot."""
    return message.forward_from and message.forward_from.id == FORWARD_BOT_ID

# === Telegram Bot Command Handlers ===

@bot.message_handler(commands=['start'])
def handle_start(message):
    user_id = message.from_user.id
    user_id_str = str(user_id)

    if is_banned(user_id):
        bot.reply_to(message, "You Are Banned By an Administrator")
        return

    username = f"@{message.from_user.username}" if message.from_user.username else ""
    full_name = message.from_user.full_name
    first_name = message.from_user.first_name

    user_data_for_db = {
        "name": full_name, "username_tg": username,
        "first_name": first_name, "last_updated": datetime.datetime.utcnow()
    }
    display_username = f'<a href="tg://user?id={user_id}">{escape(full_name)}</a>'

    if message.chat.type == 'private':
        args = message.text.split()
        command_param = args[1] if len(args) > 1 else None

        try:
             update_result = users_col.update_one(
                 {"user_id": user_id_str},
                 {"$set": user_data_for_db,
                  "$setOnInsert": {
                     "is_banned": False, "version": CURRENT_BOT_VERSION,
                     "join_date": datetime.datetime.utcnow(),
                 }},
                 upsert=True
             )
             if update_result.upserted_id:
                 logger.info(f"New user started: {user_id_str} ({full_name})")
                 send_welcome_message(message.chat.id, display_username)
                 return
             else:
                  logger.info(f"User {user_id_str} started again.")
                  # Optional: Check version update
                  # existing_doc = get_user_doc(user_id)
                  # if not is_user_updated(existing_doc):
                  #     update_prompt(message) # Or your update mechanism
                  #     return

        except Exception as e:
             logger.error(f"Database error during /start for user {user_id}: {e}")
             bot.reply_to(message, "Database error. Please try again later.")
             return

        if command_param:
            if command_param == 'add':
                sell(message)
                return
            # elif command_param == 'profile': # REMOVED
            #      view_profile(message)
            #      return
            elif command_param == 'cancel' or command_param == 'refresh':
                handle_refresh(message)
                return
            elif command_param.startswith('bid-'):
                if not bid_ji:
                     bot.reply_to(message, "Bidding is currently closed.")
                     return
                handle_bid_link(message, command_param)
                return

        if not command_param:
            bot.send_message(message.chat.id, f"Welcome back, {display_username}! Use /add to submit or /elements to browse.", parse_mode="html")

    else: # Message in group
        markup=InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Start Bot', url=f'https://t.me/{bot.get_me().username}?start=start'))
        bot.reply_to(message, "Please use bot commands in a private message with me.", reply_markup=markup, disable_web_page_preview=True)

def send_welcome_message(chat_id, display_username):
    """Sends the initial welcome message with group join links."""
    markup = types.InlineKeyboardMarkup(row_width=2)
    join_auction_btn = types.InlineKeyboardButton("Join Auction", url=AUCTION_GROUP_LINK)
    # Ensure TRADE_CHAT_ID is a valid public link or use a different mechanism if it's a private group ID
    trade_group_link = f"https://t.me/joinchat/{str(TRADE_CHAT_ID)}" if isinstance(TRADE_CHAT_ID, int) and TRADE_CHAT_ID < 0 else str(TRADE_CHAT_ID)
    if not isinstance(TRADE_CHAT_ID, str) or not TRADE_CHAT_ID.startswith("https://t.me/"):
        # Fallback or a more specific error if TRADE_CHAT_ID isn't a direct link
        logger.warning(f"TRADE_CHAT_ID ('{TRADE_CHAT_ID}') might not be a direct join link. Using it as is.")
        trade_group_link = str(TRADE_CHAT_ID) # Or a default placeholder if necessary
    
    join_trade_btn = types.InlineKeyboardButton("Join Trade Group", url=trade_group_link)
    joined_btn = types.InlineKeyboardButton("✅ I Have Joined ✅", callback_data="confirm_joined")

    markup.add(join_auction_btn, join_trade_btn)
    markup.add(joined_btn)

    caption = (
        f"Welcome, {display_username} To The Anime Auction Bot!\n\n"
         "This Bot lets you auction your Pets and Cores.\n\n"
         "Please join our Trade Group and Auction Group below, then click 'I Have Joined'."
    )
    # Optional: Send with photo
    bot.send_message(chat_id, caption, reply_markup=markup, parse_mode='HTML')


@bot.callback_query_handler(func=lambda call: call.data == "confirm_joined")
def handle_joined(call):
    """Handles the 'Joined' button click, checks membership, and prompts for stats."""
    user_id = call.from_user.id
    chat_id = call.message.chat.id

    try:
        auction_member = bot.get_chat_member(chat_id=AUCTION_CHAT_ID, user_id=user_id)
        trade_member = bot.get_chat_member(chat_id=TRADE_CHAT_ID, user_id=user_id) # TRADE_CHAT_ID needs to be the numerical ID for this check
        has_joined_auction = auction_member.status in ['member', 'administrator', 'creator']
        has_joined_trade = trade_member.status in ['member', 'administrator', 'creator']
    except telebot.apihelper.ApiTelegramException as e:
         logger.warning(f"Could not verify group membership for {user_id}: {e}. Allowing bypass.")
         has_joined_auction = True
         has_joined_trade = True
    except Exception as e:
        logger.error(f"Unexpected error verifying group membership for {user_id}: {e}")
        bot.answer_callback_query(call.id, "Error checking membership.", show_alert=True)
        return

    if has_joined_auction and has_joined_trade:
        try:
            bot.edit_message_text( # Edit the existing message if it was text
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Thanks for joining! 😊\n\nNow, one last step for verification...",
                reply_markup=None, parse_mode='html'
            )
        except telebot.apihelper.ApiTelegramException as e:
            if "message to edit not found" in str(e) or "message can't be edited" in str(e): # Original might have been photo
                 try:
                     bot.edit_message_caption(
                          chat_id=chat_id, message_id=call.message.message_id,
                          caption="Thanks for joining! 😊\n\nNow, one last step for verification...",
                          reply_markup=None, parse_mode='html'
                     )
                 except Exception as edit_err_cap:
                     logger.warning(f"Could not edit joined confirmation message (caption): {edit_err_cap}")
            else:
                 logger.warning(f"Could not edit joined confirmation message (text): {e}")


        bot.send_message(
            chat_id,
            "<b>Please forward your stats page from the main Anime Bot here.</b>\n\n"
            "<i>This helps verify your account for the auction.</i>",
            parse_mode='html'
        )
        bot.register_next_step_handler(call.message, process_stats_forward)
    else:
        missing_groups = []
        if not has_joined_auction: missing_groups.append("Auction Group")
        if not has_joined_trade: missing_groups.append("Trade Group")
        bot.answer_callback_query(call.id, f"Please join the {' and '.join(missing_groups)} first!", show_alert=True)

def process_stats_forward(message):
    """Processes the forwarded stats message for initial verification."""
    user_id = message.from_user.id
    chat_id = message.chat.id

    if not message.forward_date:
        bot.reply_to(message, "❌ Please *forward* the stats message from the Anime Bot.")
        bot.register_next_step_handler(message, process_stats_forward)
        return

    if not is_valid_forwarded_message(message):
        bot.reply_to(message, f"❌ This message was not forwarded from the required bot. Please forward the correct stats message.")
        bot.register_next_step_handler(message, process_stats_forward)
        return

    content_to_check = message.text or message.caption or ""
    if "#ID" not in content_to_check:
         bot.reply_to(message, "❌ The forwarded message doesn't seem to be a valid stats page (missing #ID).")
         bot.register_next_step_handler(message, process_stats_forward)
         return

    try:
        full_name = message.from_user.full_name
        display_username = f'<a href="tg://user?id={user_id}">{escape(full_name)}</a>'

        markup = InlineKeyboardMarkup().row(
            InlineKeyboardButton('Approve', callback_data=f'verify_approve_{user_id}'),
            InlineKeyboardButton('Ban (Alt)', callback_data=f'verify_ban_{user_id}')
        )

        tex = (f'❓ User Verification Request:\n'
               f' Name: {escape(full_name)}\n'
               f' User: {display_username}\n'
               f' ID: <code>{user_id}</code>\n\n'
               f'👇 Forwarded Stats below:')

        bot.send_message(APPROVE_CHANNEL, tex, reply_markup=markup, parse_mode='html')
        bot.forward_message(APPROVE_CHANNEL, chat_id, message.message_id)

        bot.reply_to(message, "✅ Your stats have been sent for verification. Please wait for approval.")
        logger.info(f"Stats verification request for {user_id} sent to admins.")

    except Exception as e:
        logger.error(f"Error processing stats forward for user {user_id}: {e}")
        bot.reply_to(message, "❌ An error occurred. Please contact an admin.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("verify_"))
def handle_initial_verification(call):
    admin_user_id = call.from_user.id
    if not is_mod(admin_user_id):
        bot.answer_callback_query(call.id, "Unauthorized.", show_alert=True)
        return

    try:
        parts = call.data.split('_')
        action = parts[1]
        user_id_to_verify = parts[2]
        user_id_int = int(user_id_to_verify)

        try:
            user_info = bot.get_chat(user_id_int)
            full_name = user_info.full_name
            username_tg = f"@{user_info.username}" if user_info.username else ""
            display_username_html = f'<a href="tg://user?id={user_id_int}">{escape(full_name)}</a>'
        except Exception as e:
            logger.error(f"Could not get chat details for {user_id_int} during verification: {e}")
            user_doc = get_user_doc(user_id_to_verify)
            if user_doc:
                full_name = user_doc.get("name", f"User {user_id_to_verify}")
                username_tg = user_doc.get("username_tg", "")
                display_username_html = format_username_html(user_doc)
            else:
                full_name = f"User {user_id_to_verify}"; username_tg = ""; display_username_html = f"User <code>{user_id_to_verify}</code>"

        try:
            action_text = "Approved" if action == "approve" else "Banned (Alt)"
            admin_mention = f"@{call.from_user.username}" if call.from_user.username else f"Admin {admin_user_id}"
            bot.edit_message_text(
                 f"Action: {action_text} for user {display_username_html} (<code>{user_id_to_verify}</code>)\nBy: {admin_mention}",
                 chat_id=call.message.chat.id, message_id=call.message.message_id,
                 reply_markup=None, parse_mode='html'
            )
        except Exception as edit_err:
            logger.warning(f"Could not edit verification message {call.message.message_id}: {edit_err}")

        if action == 'approve':
            users_col.update_one(
                {"user_id": user_id_to_verify},
                {"$set": {
                    "is_banned": False, "name": full_name, "username_tg": username_tg,
                    "version": CURRENT_BOT_VERSION,
                    "last_verified_by": str(admin_user_id),
                    "last_verified_time": datetime.datetime.utcnow()
                    },
                 "$setOnInsert": {
                     "join_date": datetime.datetime.utcnow(),
                 }
                },
                upsert=True
            )
            bot.send_message(user_id_int, "✅ Congratulations! Your account has been verified. You can now use the bot (e.g., `/add` to submit items).")
            bot.answer_callback_query(call.id, f"User {user_id_to_verify} approved.")
            logger.info(f"User {user_id_to_verify} approved by {admin_user_id}")

        elif action == 'ban':
            users_col.update_one(
                {"user_id": user_id_to_verify},
                {"$set": {"is_banned": True, "ban_reason": "Alt Account Verification Failed", "name": full_name, "username_tg": username_tg }},
                upsert=True
            )
            bot.send_message(user_id_int, "❌ Your account verification failed. You have been banned. Reason: Suspected Alt Account.")
            bot.answer_callback_query(call.id, f"User {user_id_to_verify} banned.")
            logger.info(f"User {user_id_to_verify} banned (alt) by {admin_user_id}")

    except (IndexError, ValueError):
        bot.answer_callback_query(call.id, "Error: Invalid callback data.", show_alert=True)
        logger.error(f"Invalid verify callback data: {call.data}")
    except Exception as e:
        logger.error(f"Error handling initial verification callback ({call.data}): {e}")
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

# === User List Commands ===
def get_page_html(page, per_page):
    start = (page - 1) * per_page
    try:
        user_cursor = users_col.find(
            {}, {"user_id": 1, "name": 1, "username_tg": 1}
        ).sort("join_date", pymongo.DESCENDING).skip(start).limit(per_page)
        user_lines = [f"{i}. {format_username_html(user_doc)}" for i, user_doc in enumerate(user_cursor, start=start + 1)]
        return "\n".join(user_lines) if user_lines else "No users found for this page."
    except Exception as e:
        logger.error(f"Error fetching user page {page}: {e}")
        return "Error retrieving user list."

@bot.message_handler(commands=['users'])
def users_list(message):
    chat_id = message.chat.id
    if not is_mod(message.from_user.id): return bot.reply_to(message, "❌ Unauthorized.")
    try: total_users = users_col.count_documents({})
    except Exception as e: return bot.reply_to(message, f"Error counting users: {e}")
    if not total_users: return bot.send_message(chat_id, "⚠️ No users found.")

    per_page = 20
    total_pages = (total_users + per_page - 1) // per_page
    markup = types.InlineKeyboardMarkup()
    if total_pages > 1: markup.add(types.InlineKeyboardButton("Next ➡️", callback_data=f"userspage:2"))
    markup.add(types.InlineKeyboardButton("❌ Close", callback_data=f"close_{message.from_user.id}"))
    page_content = get_page_html(1, per_page)
    header = f"<b>📄 Users List (Page 1/{total_pages}) Total: {total_users}</b>\n\n"
    bot.send_message(chat_id, header + page_content, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith('userspage:'))
def users_pagination(call):
    try:
        page = int(call.data.split(':')[1])
        total_users = users_col.count_documents({})
        per_page = 20
        total_pages = (total_users + per_page - 1) // per_page
        markup = types.InlineKeyboardMarkup()
        button_row = []
        if page > 1: button_row.append(types.InlineKeyboardButton("⬅️ Prev", callback_data=f"userspage:{page-1}"))
        if page < total_pages: button_row.append(types.InlineKeyboardButton("Next ➡️", callback_data=f"userspage:{page+1}"))
        if button_row: markup.row(*button_row)
        markup.add(types.InlineKeyboardButton("❌ Close", callback_data=f"close_{call.from_user.id}"))
        page_content = get_page_html(page, per_page)
        header = f"<b>📄 Users List (Page {page}/{total_pages}) Total: {total_users}</b>\n\n"
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                              text=header + page_content, reply_markup=markup, parse_mode="HTML")
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error handling users pagination (page {call.data}): {e}")
        bot.answer_callback_query(call.id, "Error updating list.")

# === Admin Commands (Ban, Unban, Msg, Verify) ===
@bot.message_handler(commands=['msg'])
def handle_msg(message):
    if is_banned(message.from_user.id): return
    if not is_admin(message.from_user.id): return bot.reply_to(message, "Unauthorized.")
    try:
        parts = message.text.split(maxsplit=2)
        target_user_id = int(parts[1]); user_message = parts[2]
    except (ValueError, IndexError): return bot.reply_to(message, "Usage: /msg <user_id> <message>")
    try:
        bot.send_message(target_user_id, f"ℹ️ **Message from Admin:**\n\n{user_message}", parse_mode="Markdown")
        bot.reply_to(message, f"Message sent to `{target_user_id}`.", parse_mode="Markdown")
        logger.info(f"Admin {message.from_user.id} sent msg to {target_user_id}")
    except Exception as e:
        bot.reply_to(message, f"Failed sending message to `{target_user_id}`: {e}", parse_mode="Markdown")
        logger.error(f"Failed sending message from {message.from_user.id} to {target_user_id}: {e}")

def get_user_id_from_arg(arg):
    try: return str(int(arg))
    except ValueError:
        try:
            clean_username = arg.lstrip('@')
            user_doc = users_col.find_one({"username_tg": f"@{clean_username}"}, {"user_id": 1})
            if user_doc: return user_doc['user_id']
            logger.warning(f"Could not find user in DB by username: @{clean_username}")
            return None
        except Exception as e: logger.error(f"Error resolving username {arg}: {e}"); return None

@bot.message_handler(commands=['unban'])
def unban_user(message):
    if not is_mod(message.from_user.id): return bot.reply_to(message, "Unauthorized.")
    args = message.text.split()[1:]; user_id_to_unban = None
    if message.reply_to_message: user_id_to_unban = str(message.reply_to_message.from_user.id)
    elif args: user_id_to_unban = get_user_id_from_arg(args[0])
    if not user_id_to_unban: return bot.reply_to(message, "Reply or provide User ID / @Username.")
    try:
        result = users_col.update_one({"user_id": user_id_to_unban},{"$set": {"is_banned": False, "ban_reason": None}})
        if result.matched_count == 0: bot.reply_to(message, "🚫 User not found.")
        elif result.modified_count == 1:
            bot.reply_to(message, f"✅ User `{user_id_to_unban}` unbanned.", parse_mode="Markdown")
            logger.info(f"User {user_id_to_unban} unbanned by {message.from_user.id}")
            try: bot.send_message(int(user_id_to_unban), "✅ You have been unbanned.")
            except Exception as e: logger.warning(f"Could not notify {user_id_to_unban} about unban: {e}")
        else:
            user_doc = get_user_doc(user_id_to_unban)
            if user_doc and not user_doc.get("is_banned", False): bot.reply_to(message, "🚫 User not currently banned.")
            else: logger.warning(f"Unban matched {user_id_to_unban} but modified_count 0."); bot.reply_to(message, "Inconsistency.")
    except Exception as e: logger.error(f"Error unbanning {user_id_to_unban}: {e}"); bot.reply_to(message, "Error unbanning.")

@bot.message_handler(commands=['ban'])
def ban_user(message):
    if not is_mod(message.from_user.id): return bot.reply_to(message, "Unauthorized.")
    args = message.text.split(maxsplit=2); target_arg = args[1] if len(args) > 1 else None
    reason = args[2] if len(args) > 2 else "No reason provided."; user_id_to_ban = None
    if message.reply_to_message:
        user_id_to_ban = str(message.reply_to_message.from_user.id)
        if target_arg and not target_arg.isdigit() and not target_arg.startswith('@'): reason = target_arg
        elif len(args) > 2: reason = args[2]
    elif target_arg: user_id_to_ban = get_user_id_from_arg(target_arg)
    if not user_id_to_ban: return bot.reply_to(message, "Reply or provide User ID / @Username.")
    if int(user_id_to_ban) in xmods or int(user_id_to_ban) == message.from_user.id: return bot.reply_to(message, "⚠️ Cannot ban mods/self.")
    try:
        result = users_col.update_one({"user_id": user_id_to_ban},{"$set": {"is_banned": True, "ban_reason": reason}}, upsert=True)
        current_status = users_col.find_one({"user_id": user_id_to_ban}, {"is_banned": 1})
        if current_status and current_status.get("is_banned"):
             if result.modified_count > 0 or result.upserted_id:
                 bot.reply_to(message, f"🚫 User `{user_id_to_ban}` banned. Reason: {escape(reason)}", parse_mode="Markdown")
                 logger.info(f"User {user_id_to_ban} banned by {message.from_user.id}. Reason: {reason}")
                 try: bot.send_message(int(user_id_to_ban), f"🚫 Banned. Reason: {escape(reason)}")
                 except Exception as e: logger.warning(f"Could not notify {user_id_to_ban} about ban: {e}")
             else: bot.reply_to(message, f"⚠️ User `{user_id_to_ban}` already banned.", parse_mode="Markdown")
        else: logger.error(f"Ban inconsistency for {user_id_to_ban}."); bot.reply_to(message, "Inconsistency.")
    except Exception as e: logger.error(f"Error banning {user_id_to_ban}: {e}"); bot.reply_to(message, "Error banning.")

@bot.message_handler(commands=['phg', 'verify'])
def handle_manual_verify(message):
    if not is_admin(message.from_user.id): return bot.reply_to(message, "🚫 Unauthorized.")
    args = message.text.split()[1:]; user_id_to_verify = None; user_info = None
    if message.reply_to_message:
        user_id_to_verify = str(message.reply_to_message.from_user.id)
        user_info = message.reply_to_message.from_user
    elif args:
        user_id_to_verify = get_user_id_from_arg(args[0])
        if user_id_to_verify:
            try: user_info = bot.get_chat(int(user_id_to_verify))
            except Exception as e:
                logger.warning(f"Could not fetch chat for {user_id_to_verify} in /verify: {e}")
                user_doc = get_user_doc(user_id_to_verify)
                if user_doc:
                    user_info = types.User(id=int(user_id_to_verify), first_name=user_doc.get("first_name", ""),
                                           last_name="", username=user_doc.get("username_tg", "").lstrip('@'), is_bot=False)
                    user_info.full_name = user_doc.get("name", f"User {user_id_to_verify}")
                else: return bot.reply_to(message, f"Cannot find user {user_id_to_verify}.")
        else: return bot.reply_to(message, f"❌ Invalid identifier '{args[0]}'.")
    else: return bot.reply_to(message, "❌ Reply or provide User ID / @Username.")
    if not user_info: return bot.reply_to(message, "❌ Could not get user info.")

    user_id_str = str(user_info.id)
    full_name = user_info.full_name
    username_tg = f"@{user_info.username}" if user_info.username else ""
    display_username_html = f'<a href="tg://user?id={user_id_str}">{escape(full_name)}</a>'
    if is_banned(user_id_str): return bot.reply_to(message, f"🚫 {display_username_html} is banned.", parse_mode='html')

    try:
        update_result = users_col.update_one(
            {"user_id": user_id_str},
            {"$set": {
                "name": full_name, "username_tg": username_tg, "first_name": user_info.first_name,
                "is_banned": False, "version": CURRENT_BOT_VERSION,
                "last_verified_by": str(message.from_user.id),
                "last_verified_time": datetime.datetime.utcnow(), "last_updated": datetime.datetime.utcnow(),
                },
             "$setOnInsert": {
                 "join_date": datetime.datetime.utcnow(),
                 }
            },
            upsert=True
        )
        if update_result.matched_count > 0 and not update_result.modified_count and not update_result.upserted_id:
             user_doc = get_user_doc(user_id_str)
             if user_doc and user_doc.get("version") == CURRENT_BOT_VERSION:
                 bot.reply_to(message, f"✅ {display_username_html} already verified/updated!", parse_mode='html')
             else:
                  bot.reply_to(message, f"✅ {display_username_html} updated/verified!", parse_mode='html')
                  logger.info(f"User {user_id_str} manually verified/updated by {message.from_user.id}")
                  try: bot.send_message(int(user_id_str), "✅ Your bot access updated/verified by admin!")
                  except Exception as e: logger.warning(f"Could not notify {user_id_str} about manual verify: {e}")
        else:
             bot.reply_to(message, f"✅ {display_username_html} successfully verified!", parse_mode='html')
             logger.info(f"User {user_id_str} manually verified by {message.from_user.id}")
             try: bot.send_message(int(user_id_str), "✅ Verified by admin!")
             except Exception as e: logger.warning(f"Could not notify {user_id_str} about manual verify: {e}")
    except Exception as e:
        logger.error(f"Error verifying {user_id_str} with /verify: {e}")
        bot.reply_to(message, "Error during verification.")

# === General Commands ===
@bot.message_handler(commands=['cancel'])
def handle_cancel(message):
    handle_refresh(message)

# --- Broadcast ---
pending_broadcasts = {}
@bot.message_handler(commands=["abroad"])
def broadcast_request(message: Message):
    if not is_mod(message.from_user.id): return bot.reply_to(message, "❌ Unauthorized.")
    admin_user_id = message.from_user.id
    broadcast_type = None; content = None; fwd_chat_id = None
    if message.reply_to_message:
        content = message.reply_to_message.message_id; fwd_chat_id = message.chat.id; broadcast_type = "forward"
    elif len(message.text.split()) > 1:
        content = message.text.replace("/abroad ", "", 1); broadcast_type = "text"
    else: return bot.reply_to(message, "❌ Reply or type message after /abroad.")
    try: target_count = users_col.count_documents({"is_banned": {"$ne": True}})
    except Exception as e: return bot.reply_to(message, f"Error counting users: {e}")
    if target_count == 0: return bot.reply_to(message, "⚠️ No target users found.")

    confirmation_key = f"bc_{admin_user_id}_{int(time.time())}"
    pending_broadcasts[confirmation_key] = {'type': broadcast_type, 'content': content, 'fwd_chat_id': fwd_chat_id,
                                            'target_count': target_count, 'requester_id': admin_user_id}
    schedule_pending_broadcast_cleanup(confirmation_key, 600)
    markup = InlineKeyboardMarkup().row(
        InlineKeyboardButton(f"✅ Yes, Send to {target_count}", callback_data=f"confirm_bc_{confirmation_key}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_bc_{confirmation_key}")
    )
    preview = f"Message:\n```\n{escape(content[:100])}...\n```" if broadcast_type == "text" else f"Forwarded message ID: {content}"
    bot.reply_to(message, f"❓ **Confirm Broadcast**\nType: {broadcast_type.capitalize()}\nTarget: {target_count}\n\n{preview}\n\nConfirm?", parse_mode="Markdown", reply_markup=markup)

def schedule_pending_broadcast_cleanup(key, timeout):
    def cleanup():
        if key in pending_broadcasts: logger.info(f"Cleaning expired broadcast: {key}"); del pending_broadcasts[key]
    threading.Timer(timeout, cleanup).start()

@bot.callback_query_handler(func=lambda call: call.data.startswith(("confirm_bc_", "cancel_bc_")))
def handle_broadcast_confirmation(call):
    admin_user_id = call.from_user.id; message = call.message
    try: action_part, confirmation_key = call.data.split("_", 2)[1:]; action = call.data.split("_")[0]
    except ValueError: logger.error(f"Invalid bc conf data: {call.data}"); return bot.answer_callback_query(call.id, "Error")
    pending_data = pending_broadcasts.get(confirmation_key)
    if not pending_data:
        try: bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=None)
        except Exception: pass
        return bot.answer_callback_query(call.id, "⚠️ Expired/Invalid.", show_alert=True)
    if admin_user_id != pending_data['requester_id']: return bot.answer_callback_query(call.id, "Not for you.")
    if action == "cancel":
        del pending_broadcasts[confirmation_key]
        try: bot.edit_message_text("❌ Broadcast cancelled.", chat_id=message.chat.id, message_id=message.message_id, reply_markup=None)
        except Exception: pass
        return bot.answer_callback_query(call.id, "❌ Cancelled.")
    if action == "confirm":
        if confirmation_key in pending_broadcasts: del pending_broadcasts[confirmation_key]
        else: return bot.answer_callback_query(call.id, "⚠️ Already processed/expired.", show_alert=True)
        try: bot.edit_message_text(f"⏳ Initializing broadcast to {pending_data['target_count']} users...", chat_id=message.chat.id, message_id=message.message_id, reply_markup=None)
        except Exception as edit_err: logger.warning(f"Could not edit bc conf msg: {edit_err}")
        execute_broadcast(admin_user_id, pending_data['type'], pending_data['content'], pending_data['fwd_chat_id'], pending_data['target_count'], message)
        bot.answer_callback_query(call.id, "Broadcast initiated!")

def execute_broadcast(admin_user_id, broadcast_type, content, fwd_chat_id, total_users, status_message):
    sent_count = 0; blocked_users = []; failed_users = []; start_time = time.time()
    logger.info(f"Executing broadcast by {admin_user_id}. Type: {broadcast_type}, Target: {total_users}")
    try:
        user_cursor = users_col.find({"is_banned": {"$ne": True}}, {"user_id": 1, "name": 1, "username_tg": 1})
        update_interval = max(5, total_users // 20); last_update_time = start_time; i = 0
        for user_doc in user_cursor:
            user_id_str = user_doc.get('user_id')
            if not user_id_str: logger.error("User doc missing user_id in broadcast."); failed_users.append({'id': 'UNK', 'doc': user_doc, 'error': 'Missing ID'}); continue
            try:
                user_id_int = int(user_id_str)
                if broadcast_type == "forward": bot.forward_message(user_id_int, fwd_chat_id, content)
                elif broadcast_type == "text": bot.send_message(user_id_int, content, parse_mode="Markdown")
                sent_count += 1; time.sleep(0.05)
            except telebot.apihelper.ApiTelegramException as e:
                error_str = f"{e.error_code} - {e.description}"
                logger.warning(f"Broadcast API Ex for {user_id_str}: {error_str}")
                if e.error_code == 403: blocked_users.append({'id': user_id_str, 'doc': user_doc})
                else: failed_users.append({'id': user_id_str, 'doc': user_doc, 'error': error_str})
            except Exception as e: logger.warning(f"Broadcast Gen Ex for {user_id_str}: {e}"); failed_users.append({'id': user_id_str, 'doc': user_doc, 'error': str(e)})

            i += 1; current_time = time.time()
            if (i % update_interval == 0) or (current_time - last_update_time > 15) or (i == total_users):
                 elapsed = current_time - start_time; blocked = len(blocked_users); failed = len(failed_users)
                 try:
                     bot.edit_message_text(f"⏳ Broadcasting... {i}/{total_users}.\n✅ Sent: {sent_count}, 🚫 Blocked: {blocked}, ❌ Failed: {failed}\n⏱️ {elapsed:.1f}s",
                                           chat_id=status_message.chat.id, message_id=status_message.message_id)
                     last_update_time = current_time
                 except Exception as edit_e:
                     if "message is not modified" not in str(edit_e):
                          logger.warning(f"Could not edit broadcast status: {edit_e}")

        end_time = time.time(); duration = end_time - start_time; blocked = len(blocked_users); failed = len(failed_users)
        final_lines = [f"🏁 Broadcast Complete!\n", f"✅ Sent: {sent_count}", f"🚫 Blocked/Inactive: {blocked}", f"❌ Other Failed: {failed}", f"👥 Total Targeted: {total_users}", f"⏱️ Duration: {duration:.2f}s"]

        if blocked_users:
            final_lines.append("\n🚫 **Blocked/Inactive Users (Max 15):**")
            for u_idx, u in enumerate(blocked_users[:15]):
                 user_link = format_username_html(u.get('doc')) if u.get('doc') else f"<code>{u.get('id', 'UNKNOWN')}</code>"
                 final_lines.append(f" - {user_link}")
            if len(blocked_users) > 15:
                 final_lines.append(" - ... (and more)")

        if failed_users:
            final_lines.append("\n❌ **Other Failed Users (Max 15):**")
            for u_idx, u in enumerate(failed_users[:15]):
                user_link = format_username_html(u.get('doc')) if u.get('doc') else f"<code>{u.get('id', 'UNKNOWN')}</code>"
                error_msg = escape(u.get('error','Unknown Error'))
                final_lines.append(f" - {user_link} ({error_msg})")
            if len(failed_users) > 15:
                 final_lines.append(" - ... (and more)")

        final_status = "\n".join(final_lines); final_status = final_status[:4092]+"..." if len(final_status) > 4096 else final_status
        try: bot.edit_message_text(final_status, chat_id=status_message.chat.id, message_id=status_message.message_id, parse_mode='HTML', disable_web_page_preview=True)
        except Exception as e: logger.error(f"Failed final broadcast status update: {e}"); bot.send_message(status_message.chat.id, final_status, parse_mode='HTML', disable_web_page_preview=True)
        logger.info(f"Broadcast finished. Sent: {sent_count}, Blocked: {blocked}, Failed: {failed}, Duration: {duration:.2f}s")

    except Exception as loop_err:
         logger.error(f"Error during broadcast execution: {loop_err}", exc_info=True)
         try: bot.edit_message_text(f"❌ Broadcast error: {escape(str(loop_err))}", chat_id=status_message.chat.id, message_id=status_message.message_id)
         except Exception: pass

# === Item Submission Handlers (/add - Simplified) ===

@bot.message_handler(commands=['add'])
def sell(message):
    user_id = message.from_user.id
    user_id_str = str(user_id)

    if is_banned(user_id): return bot.reply_to(message, "You are banned.")
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start the bot first.", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Start Bot', url=f'https://t.me/{bot.get_me().username}?start=start')))

    if not sub_process: return bot.reply_to(message, 'Submissions currently disabled.')
    if message.chat.type != 'private': return bot.reply_to(message, 'Please use /add in my private chat.', reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Submit Item', url=f'https://t.me/{bot.get_me().username}?start=add')))

    if user_id in user_states: del user_states[user_id]
    if user_id in user_cache: del user_cache[user_id]

    markup = types.InlineKeyboardMarkup().row(types.InlineKeyboardButton('✅ Yes', callback_data='sell_yes'), types.InlineKeyboardButton('❌ No', callback_data='sell_no'))
    display_username = format_username_html(user_doc)
    bot.send_message(user_id, f"Hello {display_username}!\n\nSubmit an item (Pet or Core) for auction?", parse_mode="html", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data in ['sell_yes', 'sell_no'])
def handle_sell_confirmation(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id; message_id = call.message.message_id
    if call.data == 'sell_yes':
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add( types.InlineKeyboardButton('🐾 Pet', callback_data=f'sell_category_{CATEGORY_PET}'),
                    types.InlineKeyboardButton('💎 Core', callback_data=f'sell_category_{CATEGORY_CORE}'))
        markup.add(types.InlineKeyboardButton('Cancel Submission', callback_data='cancel_submission'))
        try: bot.edit_message_text('Okay! What category?', chat_id, message_id, reply_markup=markup)
        except Exception as e: logger.warning(f"Error editing sell conf: {e}"); bot.send_message(chat_id, 'Okay! What category?', reply_markup=markup)
        bot.answer_callback_query(call.id)
    elif call.data == 'sell_no':
        try: bot.edit_message_text('Alright! Use /add when ready. ✨', chat_id, message_id, reply_markup=None)
        except Exception: pass
        bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('sell_category_'))
def handle_category_selection(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id; message_id = call.message.message_id
    if user_id in user_states: del user_states[user_id]
    if user_id in user_cache: del user_cache[user_id]
    category = call.data.split('_')[-1]
    if category not in VALID_CATEGORIES: return bot.answer_callback_query(call.id, "Error: Unknown category.", show_alert=True)
    logger.info(f"User {user_id} selected category: {category}")
    user_states[user_id] = {'step': 'ask_forward', 'category': category}
    user_cache[user_id] = {'category': category}
    category_display = category.capitalize()
    next_instruction = f"Selected **{category_display}**.\n\nPlease **forward** the item's stats page from the main bot."
    try:
        bot.edit_message_text(next_instruction, chat_id, message_id, reply_markup=None, parse_mode="Markdown")
        bot.register_next_step_handler(call.message, process_item_forward)
    except Exception as e:
        logger.warning(f"Error editing cat select: {e}"); bot.send_message(chat_id, next_instruction, parse_mode="Markdown")
        bot.register_next_step_handler(call.message, process_item_forward)
    bot.answer_callback_query(call.id)

def process_item_forward(message):
    user_id = message.from_user.id; chat_id = message.chat.id
    if user_id not in user_states or user_states[user_id].get('step') != 'ask_forward': return logger.warning(f"Ignoring forward {user_id}, wrong state.")
    category = user_cache.get(user_id, {}).get('category')
    if not category: logger.error(f"Cat missing {user_id}"); bot.reply_to(message, "Error (cat missing). Try /add again."); return
    if not message.forward_date: bot.reply_to(message, "❌ Please *forward*."); bot.register_next_step_handler(message, process_item_forward); return
    if not is_valid_forwarded_message(message): bot.reply_to(message, f"❌ Not from correct bot."); bot.register_next_step_handler(message, process_item_forward); return

    forwarded_text = None; forwarded_caption = None; forwarded_photo_id = None; item_identifier = None
    if message.photo and message.caption: forwarded_caption = message.caption; forwarded_photo_id = message.photo[-1].file_id; content_to_check = forwarded_caption
    elif message.text: forwarded_text = message.text; content_to_check = forwarded_text
    else: bot.reply_to(message, "❌ Needs text or photo+caption."); bot.register_next_step_handler(message, process_item_forward); return

    id_match = re.search(r"#ID\s*(\d+)", content_to_check, re.IGNORECASE)
    if not id_match: bot.reply_to(message, "❌ `#ID XXXXX` format missing."); bot.register_next_step_handler(message, process_item_forward); return

    name_match = re.match(r"^(.*?)\s+#ID", content_to_check, re.IGNORECASE | re.DOTALL)
    item_identifier = name_match.group(1).strip() if name_match else f"{category.capitalize()} #{id_match.group(1)}"
    item_identifier = re.sub(r"^\W+\s*", "", item_identifier)
    logger.info(f"Extracted Item: {item_identifier} for {user_id}")

    user_cache[user_id]['forwarded_message_id'] = message.message_id
    user_cache[user_id]['forwarded_text'] = forwarded_text
    user_cache[user_id]['forwarded_caption'] = forwarded_caption
    user_cache[user_id]['forwarded_photo_id'] = forwarded_photo_id
    user_cache[user_id]['item_name'] = item_identifier

    user_states[user_id]['step'] = 'ask_base_price'
    bot.send_message(chat_id, f"✅ Stats for **{escape(item_identifier)}** received.\n\n➡️ Enter **Starting Bid (Base Price)** (e.g., `1k`, `5000`).", parse_mode="Markdown")
    bot.register_next_step_handler(message, process_base_price)

def process_base_price(message):
    user_id = message.from_user.id; chat_id = message.chat.id
    if user_id not in user_states or user_states[user_id]['step'] != 'ask_base_price': return
    category = user_cache.get(user_id, {}).get('category');
    if not category: logger.error(f"Cat missing {user_id}"); bot.reply_to(message, "Error (cat missing). Try /add."); return
    base_price_str = message.text.strip()
    numeric_base = parse_bid_amount(base_price_str)
    if numeric_base <= 0: bot.reply_to(message, "⚠️ Invalid base price (e.g., `1k`, `5000`)."); bot.register_next_step_handler(message, process_base_price); return
    min_price = 1 # MODIFIED: Minimum base price
    if numeric_base < min_price: bot.reply_to(message, f"⚠️ Minimum base price is {min_price}."); bot.register_next_step_handler(message, process_base_price); return
    user_cache[user_id]['base_price_str'] = base_price_str
    show_submission_preview(user_id, category)

def show_submission_preview(user_id, item_kind):
    if user_id not in user_cache: logger.warning(f"Cache miss {user_id}"); bot.send_message(user_id, "Error building preview. Try /add again."); return
    cache = user_cache[user_id]; category = cache.get('category', 'unknown')
    base_price_str = cache.get('base_price_str', 'N/A'); item_name = cache.get('item_name', 'N/A')
    photo_id = cache.get('forwarded_photo_id'); forwarded_text = cache.get('forwarded_text'); forwarded_caption = cache.get('forwarded_caption')
    user_doc = get_user_doc(user_id); display_username = format_username_html(user_doc) if user_doc else f"User {user_id}"

    preview_caption = (f"<b>Category:</b> #{category.capitalize()}\n"
                       f"<b>Submitted by:</b> {display_username}\n"
                       f"<b>Item:</b> {escape(item_name)}\n"
                       f"<b>Base Price:</b> {escape(base_price_str)}\n\n"
                       f"--- Forwarded Stats ---")
    if forwarded_caption: preview_caption += f"\n<pre>{escape(forwarded_caption[:300])}{'...' if len(forwarded_caption) > 300 else ''}</pre>"
    elif forwarded_text: preview_caption += f"\n<pre>{escape(forwarded_text[:300])}{'...' if len(forwarded_text) > 300 else ''}</pre>"
    cache['final_caption'] = preview_caption

    markup = InlineKeyboardMarkup().row(InlineKeyboardButton("✅ Submit", callback_data="final_submit"), InlineKeyboardButton("❌ Discard", callback_data="cancel_submission"))
    try:
        final_preview_text = cache['final_caption'] + "\n\n<i>Review carefully. Submit to send for approval.</i>"
        if photo_id:
            if len(final_preview_text) > 1024: final_preview_text = final_preview_text[:1020] + "\n..."
            bot.send_photo(user_id, photo_id, caption=final_preview_text, parse_mode='HTML', reply_markup=markup)
        else:
            if len(final_preview_text) > 4096: final_preview_text = final_preview_text[:4092] + "\n..."
            bot.send_message(user_id, final_preview_text, parse_mode='HTML', reply_markup=markup, disable_web_page_preview=True)
        if user_id in user_states: del user_states[user_id]
    except Exception as e: logger.error(f"Error sending preview {user_id}: {e}"); bot.send_message(user_id, "Error displaying preview. Try /add again.")

@bot.callback_query_handler(func=lambda call: call.data in ["final_submit", "cancel_submission"])
def handle_final_submission_action(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id; message_id = call.message.message_id
    if call.data == "cancel_submission":
        if user_id in user_cache: del user_cache[user_id]
        if user_id in user_states: del user_states[user_id]
        try: bot.edit_message_text("❌ Submission discarded.", chat_id, message_id, reply_markup=None)
        except Exception: bot.send_message(chat_id,"Submission discarded.")
        bot.answer_callback_query(call.id, "Submission cancelled.")
        return

    if call.data == "final_submit":
        if user_id not in user_cache or 'final_caption' not in user_cache[user_id]:
            logger.warning(f"Cache missing final submit {user_id}")
            try: bot.edit_message_text("❌ Error: Data lost. Try /add.", chat_id, message_id, reply_markup=None)
            except Exception: pass
            return bot.answer_callback_query(call.id, "Error: Data missing", show_alert=True)

        submission_data = user_cache.pop(user_id)
        category = submission_data.get('category', 'unknown')
        final_caption = submission_data.get('final_caption')
        photo_file_id = submission_data.get('forwarded_photo_id')
        forwarded_text = submission_data.get('forwarded_text')
        forwarded_caption = submission_data.get('forwarded_caption')
        base_price_str = submission_data.get('base_price_str', '0')
        item_name = submission_data.get('item_name', 'Unknown')

        pending_doc = {
            "user_id": str(user_id), "item_type": category,
            "submission_time": datetime.datetime.utcnow(),
            "details_text": final_caption, "forwarded_text": forwarded_text,
            "forwarded_caption": forwarded_caption, "photo_file_id": photo_file_id,
            "status": "pending", "item_name": item_name, "base_price_str": base_price_str,
        }

        try:
            insert_result = pending_items_col.insert_one(pending_doc)
            pending_id = insert_result.inserted_id
        except Exception as e:
            logger.error(f"Failed insert pending {user_id}: {e}"); bot.edit_message_text("❌ DB error submitting. Try later.", chat_id, message_id, reply_markup=None); return bot.answer_callback_query(call.id, "DB Error", show_alert=True)

        success_message = f"✅ Your **{escape(item_name)}** submitted for approval!\n\nYou'll be notified when reviewed."
        try: bot.edit_message_text(success_message, chat_id, message_id, reply_markup=None, parse_mode="Markdown")
        except Exception: bot.send_message(chat_id, success_message, parse_mode="Markdown")

        admin_markup = types.InlineKeyboardMarkup().row(types.InlineKeyboardButton('Approve', callback_data=f'approve_{pending_id}_{user_id}'), types.InlineKeyboardButton('Reject', callback_data=f'reject_{pending_id}_{user_id}'))
        user_doc = get_user_doc(user_id)
        display_username = format_username_html(user_doc) if user_doc else f"User {user_id}"
        admin_info_caption = (f"👇 Pending Submission | ID: `{pending_id}`\n"
                             f"Item: **{escape(item_name)}**\n"
                             f"Category: {category.capitalize()}\n"
                             f"Submitter: {display_username}\n"
                             f"Base Price: {escape(base_price_str)}\n\n"
                             f"--- Forwarded Content ---")
        try:
            if photo_file_id:
                full_admin_caption = admin_info_caption
                if forwarded_caption: full_admin_caption += f"\n<pre>{escape(forwarded_caption)}</pre>"
                if len(full_admin_caption) > 1024: full_admin_caption = full_admin_caption[:1020] + "\n..."
                bot.send_photo(APPROVE_CHANNEL, photo_file_id, caption=full_admin_caption, parse_mode='HTML', reply_markup=admin_markup)
            elif forwarded_text:
                full_admin_text = admin_info_caption + f"\n<pre>{escape(forwarded_text)}</pre>"
                if len(full_admin_text) > 4096: full_admin_text = full_admin_text[:4092] + "\n..."
                bot.send_message(APPROVE_CHANNEL, full_admin_text, parse_mode='HTML', reply_markup=admin_markup, disable_web_page_preview=True)
            else:
                 logger.error(f"No forwarded content found for pending item {pending_id}")
                 admin_info_caption += "\n[Error: Forwarded content missing]"
                 bot.send_message(APPROVE_CHANNEL, admin_info_caption, parse_mode='HTML', reply_markup=admin_markup)
            logger.info(f"Submission {pending_id} from {user_id} sent to admin channel.")
        except Exception as e:
            logger.error(f"Failed to send pending item {pending_id} to admin channel {APPROVE_CHANNEL}: {e}")
        bot.answer_callback_query(call.id, "Item Submitted for Approval!")


# === Admin Actions (Approve/Reject) ===
@bot.callback_query_handler(func=lambda call: call.data.startswith(("approve_", "reject_")))
def handle_admin_actions(call):
    admin_user_id = call.from_user.id
    if not is_mod(admin_user_id): return bot.answer_callback_query(call.id, "Unauthorized.")

    try:
        parts = call.data.split("_"); action = parts[0]; pending_id_str = parts[1]; original_user_id = parts[2]
        pending_id = ObjectId(pending_id_str)
        pending_item = pending_items_col.find_one({"_id": pending_id, "status": "pending"})
        if not pending_item:
            try: bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception: pass
            return bot.answer_callback_query(call.id, "⚠️ Already processed/not found.", show_alert=True)

        userd = int(original_user_id)
        item_type = pending_item.get('item_type', 'unknown')
        item_photo_id = pending_item.get('photo_file_id')
        item_forwarded_text = pending_item.get('forwarded_text')
        item_forwarded_caption = pending_item.get('forwarded_caption')
        item_name = pending_item.get('item_name', 'Unknown Item')
        base_price_str = pending_item.get('base_price_str', '0')
        admin_mention_html = f'<a href="tg://user?id={admin_user_id}">{escape(call.from_user.full_name)}</a>'
        owner_doc = get_user_doc(userd); owner_mention = format_username_html(owner_doc) if owner_doc else f"User {userd}"

        if action == 'approve':
            posted_msg = None
            try:
                 main_post_caption_info = (f"✨ **{escape(item_name)}** ✨\n"
                                           f"Category: #{item_type.capitalize()}\n"
                                           f"Seller: {owner_mention}\n"
                                           f"Base Price: `{escape(base_price_str)}`\n\n"
                                           f"--- Details ---")
                 if item_photo_id:
                     full_caption = main_post_caption_info
                     if item_forwarded_caption: full_caption += f"\n<pre>{escape(item_forwarded_caption)}</pre>"
                     if len(full_caption) > 1024: full_caption = full_caption[:1020] + "\n..."
                     posted_msg = bot.send_photo(POST_CHANNEL, item_photo_id, caption=full_caption, parse_mode='HTML')
                 elif item_forwarded_text:
                     full_text = main_post_caption_info + f"\n<pre>{escape(item_forwarded_text)}</pre>"
                     if len(full_text) > 4096: full_text = full_text[:4092] + "\n..."
                     posted_msg = bot.send_message(POST_CHANNEL, full_text, parse_mode='HTML', disable_web_page_preview=True)
                 else:
                     posted_msg = bot.send_message(POST_CHANNEL, main_post_caption_info + "\n[Error: Content missing]", parse_mode='HTML')
                 auction_post_link = f"https://t.me/c/{str(POST_CHANNEL)[4:]}/{posted_msg.message_id}"
                 logger.info(f"Posted item {pending_id} to auction channel. Link: {auction_post_link}")
            except Exception as post_err:
                 logger.error(f"Failed post approved {pending_id} to {POST_CHANNEL}: {post_err}")
                 bot.send_message(userd, f"❌ Error posting your approved item '{escape(item_name)}'. Contact admin.")
                 return bot.answer_callback_query(call.id, "Error posting to auction.", show_alert=True)

            try:
                 approved_items_col.insert_one({
                     "user_id": str(userd), "category": item_type, "name": item_name,
                     "link": auction_post_link, "approval_time": datetime.datetime.utcnow(),
                     "approved_by": str(admin_user_id), "pending_item_id": pending_id
                 })
            except Exception as db_err: logger.error(f"Failed insert approved {pending_id}: {db_err}")

            bid_id = get_next_bid_id()
            base_price_numeric = parse_bid_amount(base_price_str)
            bot_username = bot.get_me().username
            bid_markup = InlineKeyboardMarkup().row(InlineKeyboardButton("🔄 Refresh", callback_data=f"ref_{bid_id}"), InlineKeyboardButton("🔗 Place Bid", url=f"https://t.me/{bot_username}?start=bid-{bid_id}"))
            bid_text = create_bid_message(bid_id, None, base_price_numeric, base_price_numeric)
            try:
                 bid_msg = bot.send_message(POST_CHANNEL, bid_text, reply_markup=bid_markup, parse_mode="Markdown")
            except Exception as bid_msg_err:
                 logger.error(f"Failed send bid msg {bid_id}: {bid_msg_err}")
                 bot.send_message(userd, f"❌ Error setting up bid for '{escape(item_name)}'. Contact admin.")
                 try: bot.delete_message(posted_msg.chat.id, posted_msg.message_id)
                 except: pass
                 approved_items_col.delete_one({"link": auction_post_link})
                 return bot.answer_callback_query(call.id, "Error creating bid message.", show_alert=True)

            bid_doc = {
                "bid_id": bid_id, "owner_id": str(userd), "owner_mention": owner_mention,
                "base_price": base_price_numeric, "current_bid": base_price_numeric,
                "highest_bidder_id": None, "highest_bidder_mention": None,
                "message_id": bid_msg.message_id, "chat_id": bid_msg.chat.id,
                "auction_post_link": auction_post_link, "item_type": item_type,
                "item_name": item_name, "status": "active", "history": {},
                "creation_time": datetime.datetime.utcnow(), "approved_by": str(admin_user_id)
            }
            try: bids_col.insert_one(bid_doc)
            except Exception as bids_db_err:
                 logger.error(f"CRITICAL: Failed insert bid doc {bid_id}: {bids_db_err}")
                 bot.send_message(APPROVE_CHANNEL, f"🚨 CRITICAL DB ERROR: Failed save bid {bid_id}. Item {item_name} live but DB missing! Use /remo {bid_id} & re-approve. @admin")
                 try: bot.delete_message(bid_msg.chat.id, bid_msg.message_id)
                 except: pass
                 return bot.answer_callback_query(call.id, "CRITICAL DB ERROR creating bid record.", show_alert=True)

            pending_items_col.delete_one({"_id": pending_id})

            try:
                 # MODIFIED Notification Message
                 user_notification_text = f"🎉 Your **{escape(item_name)}** has been approved and is now up for auction!"
                 user_notification_markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton('View Auction Item', url=auction_post_link)) # Link to specific item
                 bot.send_message(userd, user_notification_text, parse_mode='markdown', reply_markup=user_notification_markup, disable_web_page_preview=True)
            except Exception as notify_err: logger.warning(f"Could not notify {userd} about approval: {notify_err}")
            
            # MODIFIED: Edit Admin Message
            try:
                original_content_html = ""
                if call.message.photo: # If the message being edited is a photo (likely with caption)
                    original_content_html = call.message.caption_html or ""
                elif call.message.text: # If the message being edited is text
                    original_content_html = call.message.html_text or ""
                
                # Append approval info
                edit_text_html = original_content_html + f"\n\n✅ Approved by {admin_mention_html}. Bid ID: <code>{bid_id}</code>"

                if call.message.photo:
                    bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=edit_text_html, parse_mode='HTML', reply_markup=None)
                else:
                    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=edit_text_html, parse_mode='HTML', reply_markup=None, disable_web_page_preview=True)
            except Exception as edit_err:
                 logger.warning(f"Could not edit approval msg {call.message.message_id}: {edit_err}")
                 # Fallback message if edit fails
                 bot.send_message(APPROVE_CHANNEL, f"✅ Item '{escape(item_name)}' (`{pending_id_str}`) approved by {admin_mention_html}. Bid ID: `{bid_id}`", parse_mode='HTML')


            bot.answer_callback_query(call.id, f"Approved. Bid ID: {bid_id}")

        elif action == 'reject':
            markup = InlineKeyboardMarkup(row_width=2)
            safe_item_name = item_name[:25].replace("_", "-")
            reasons = [('Invalid Stats', 's'), ('Base Price High', 'b'), ('Duplicate/Spam', 'd'), ('Other', 'o')]
            buttons = [InlineKeyboardButton(text, callback_data=f'rejreason_{code}_{pending_id_str}_{userd}_{safe_item_name}') for text, code in reasons]
            for i in range(0, len(buttons), 2): markup.row(*buttons[i:i+2])

            try:
                original_content_html = ""
                if call.message.photo: original_content_html = call.message.caption_html or ""
                elif call.message.text: original_content_html = call.message.html_text or ""
                
                edit_text_html = original_content_html + f"\n\n✍️ Select rejection reason (by {admin_mention_html}):"

                if call.message.photo: bot.edit_message_caption(chat_id=call.message.chat.id, message_id=call.message.message_id, caption=edit_text_html, parse_mode='HTML', reply_markup=markup)
                else: bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=edit_text_html, parse_mode='HTML', reply_markup=markup, disable_web_page_preview=True)
            except Exception as e: logger.warning(f"Could not edit msg for reject reason: {e}"); bot.send_message(call.message.chat.id, f"Select reason for rejecting '{escape(item_name)}':", reply_markup=markup)

            bot.answer_callback_query(call.id, "Select rejection reason.")

    except ObjectId.InvalidId: bot.answer_callback_query(call.id, "❌ Invalid item ID.", show_alert=True)
    except Exception as e: logger.error(f"Error in handle_admin_actions {call.data}: {e}", exc_info=True); bot.answer_callback_query(call.id, "Error.", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("rejreason_"))
def handle_rejection_reason(call):
    admin_user_id = call.from_user.id
    if not is_mod(admin_user_id): return bot.answer_callback_query(call.id, "Unauthorized.")

    try:
        parts = call.data.split("_"); reason_code = parts[1]; pending_id_str = parts[2]; userd = int(parts[3]); item_name_safe = parts[4]
        pending_id = ObjectId(pending_id_str)
        admin_mention_html = f'<a href="tg://user?id={admin_user_id}">{escape(call.from_user.full_name)}</a>'


        pending_item = pending_items_col.find_one({"_id": pending_id})
        full_item_name = pending_item.get('item_name', item_name_safe) if pending_item else item_name_safe
        delete_result = pending_items_col.delete_one({"_id": pending_id})
        if delete_result.deleted_count == 0:
            try: bot.delete_message(call.message.chat.id, call.message.message_id) # Try to delete the message with reason buttons
            except Exception: pass
            return bot.answer_callback_query(call.id, "⚠️ Already processed.", show_alert=True)

        reason_text_map = {'s': 'Invalid Stats / Format.', 'b': 'Base Price Too High.', 'd': 'Duplicate / Spam.', 'o': 'Other/Admin Discretion.'}
        reason_full_text = reason_text_map.get(reason_code, 'Reason not specified.')

        notification_text = f"🔴 Submission **{escape(full_item_name)}** rejected.\n**Reason:** {reason_full_text}"
        try: bot.send_message(userd, notification_text, parse_mode='Markdown')
        except Exception as notify_err: logger.warning(f"Could not notify {userd} about rejection: {notify_err}")

        user_doc_rejected = get_user_doc(userd); rejected_user_mention = format_username_html(user_doc_rejected) if user_doc_rejected else f"User {userd}"
        log_text = (f"🚫 Item Rejected: '{escape(full_item_name)}' (`{pending_id_str}`)\n👤 Submitter: {rejected_user_mention}\n"
                    f"🛡️ Rejected By: {admin_mention_html}\n💬 Reason: {reason_full_text}")
        try: bot.send_message(REJECT_CHANNEL, log_text, parse_mode='HTML', disable_web_page_preview=True)
        except Exception as log_err: logger.error(f"Failed log rejection to {REJECT_CHANNEL}: {log_err}")

        try:
            # Edit the original admin message (which had the item preview and Approve/Reject buttons)
            # to show the final action.
            # We need to reconstruct the original item preview part if it's not available in call.message directly
            # or simply state the action. For simplicity, just state the action on the message that had the reason buttons.
            final_edit_text = f"Action Taken: Rejected Item <code>{pending_id_str}</code>\nReason: {reason_full_text}\nBy: {admin_mention_html}"
            bot.edit_message_text(final_edit_text, call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode='HTML')
        except Exception as edit_err: logger.warning(f"Could not edit rejection reason msg: {edit_err}")

        bot.answer_callback_query(call.id, f"Rejection ({reason_code}) processed.")
        logger.info(f"Item {pending_id_str} rejected by {admin_user_id}. Reason: {reason_code}")

    except ObjectId.InvalidId: bot.answer_callback_query(call.id, "❌ Invalid item ID.", show_alert=True)
    except (IndexError, ValueError): bot.answer_callback_query(call.id, "❌ Invalid callback data.", show_alert=True); logger.error(f"Invalid rejreason cb: {call.data}")
    except Exception as e: logger.error(f"Error processing rejection {call.data}: {e}", exc_info=True); bot.answer_callback_query(call.id, "Error.", show_alert=True)

# === Bidding System ===
@bot.message_handler(commands=['bid'])
def biddy(message):
    global bid_ji
    if not is_mod(message.from_user.id): return bot.reply_to(message, "Unauthorized.")
    args = message.text.split()
    if len(args) < 2 or args[1].lower() not in ['on', 'off']: return bot.reply_to(message, f"Usage: /bid <on|off>\nCurrent: {'ON' if bid_ji else 'OFF'}")
    action = args[1].lower()
    if action == 'on':
        if bid_ji: return bot.reply_to(message, 'ℹ️ Bidding already ENABLED.')
        bid_ji = True; bot.reply_to(message, '✅ Bidding ENABLED.'); logger.info(f"Bidding ENABLED by {message.from_user.id}")
        try: bot.send_message(POST_CHANNEL, "--- Bidding is now OPEN ---")
        except: pass
    elif action == 'off':
        if not bid_ji: return bot.reply_to(message, 'ℹ️ Bidding already DISABLED.')
        bid_ji = False; bot.reply_to(message, '❌ Bidding DISABLED. Active auctions closing.'); logger.info(f"Bidding DISABLED by {message.from_user.id}")
        try:
             close_result = bids_col.update_many({"status": "active"},{"$set": {"status": "closed", "closed_time": datetime.datetime.utcnow()}})
             logger.info(f"Marked {close_result.modified_count} bids as closed.")
             try: bot.send_message(POST_CHANNEL, "--- Bidding is now CLOSED ---")
             except: pass
        except Exception as e: logger.error(f"Error closing bids: {e}")

def handle_bid_link(message, command_param):
    user_id = message.from_user.id; user_id_str = str(user_id)
    if is_banned(user_id): return bot.reply_to(message, "You are banned.")
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start the bot first.")

    if not bid_ji: return bot.reply_to(message, "Bidding currently closed.")
    try: bid_id = command_param.split('-')[1].upper()
    except IndexError: return bot.reply_to(message, "Invalid bid link.")

    bid_data = bids_col.find_one({"bid_id": bid_id})
    if not bid_data: return bot.reply_to(message, f"⚠️ Auction `{bid_id}` not found.")
    if bid_data.get("status") != "active": return bot.reply_to(message, f"⚠️ Auction `{bid_id}` ended.")
    if user_id_str == bid_data.get('owner_id'): return bot.reply_to(message, "❌ Cannot bid on own item!")

    item_name_display = escape(bid_data.get('item_name', bid_id))
    current_bid = bid_data.get('current_bid', 0.0); current_bid_display = f"{current_bid:,.0f}"
    min_increment = get_min_bid_increment(current_bid); required_bid_display = f"{current_bid + min_increment:,.0f}"

    msg = bot.send_message(message.chat.id, f"🛒 Bidding on: **{item_name_display}** (`{bid_id}`)\n"
                                           f"💰 Current: `{current_bid_display}` | Min Next: `{required_bid_display}`\n\n"
                                           f"➡️ Enter your bid amount (e.g., `1k`, `5500`). Rules: /brules", parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_bid_amount_input, bid_id, message.from_user.full_name, current_bid)

def process_bid_amount_input(message, bid_id, bidder_full_name, current_bid_at_prompt):
    user_id = message.from_user.id; user_id_str = str(user_id); bid_amount_str = message.text.strip()
    bid_data = bids_col.find_one({"bid_id": bid_id})
    if not bid_data or bid_data.get("status") != "active": return bot.reply_to(message, "❌ Auction ended/removed.")

    bid_amount_numeric = parse_bid_amount(bid_amount_str)
    if bid_amount_numeric <= 0: return bot.reply_to(message, "❌ Invalid bid amount.", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Try Again', url=f"https://t.me/{bot.get_me().username}?start=bid-{bid_id}")))

    current_bid_latest = bid_data.get('current_bid', 0.0); highest_bidder_id = bid_data.get('highest_bidder_id')
    if user_id_str == highest_bidder_id: return bot.reply_to(message, "⚠️ You are already highest bidder.")

    min_increment = get_min_bid_increment(current_bid_latest); required_bid = current_bid_latest + min_increment
    required_bid_display = f"{required_bid:,.0f}"; current_bid_latest_display = f"{current_bid_latest:,.0f}"; min_increment_display = f"{min_increment:,.0f}"

    if bid_amount_numeric < required_bid:
        return bot.reply_to(message, f"❌ Bid too low! Min `{required_bid_display}` (Current: `{current_bid_latest_display}` + Incr: `{min_increment_display}`).", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Try Again', url=f"https://t.me/{bot.get_me().username}?start=bid-{bid_id}")))

    bidder_mention_md = f"[{escape(bidder_full_name)}](tg://user?id={user_id})"
    confirmation_key = f"{user_id}_{bid_id}_{int(time.time())}"
    pending_bids[confirmation_key] = {'user_id': user_id, 'bidder_mention': bidder_mention_md, 'bid_id': bid_id,
                                      'bid_amount': bid_amount_numeric, 'previous_bidder_id': highest_bidder_id,
                                      'original_message_id': message.message_id}
    markup = InlineKeyboardMarkup(row_width=2).add(InlineKeyboardButton("✅ Confirm Bid", callback_data=f"confirmbid_{confirmation_key}"), InlineKeyboardButton("❌ Cancel", callback_data=f"cancelbid_{confirmation_key}"))
    item_name_display = escape(bid_data.get('item_name', bid_id)); bid_amount_display = f"{bid_amount_numeric:,.0f}"
    bot.reply_to(message, f"🔔 **Bid Confirmation**\nItem: **{item_name_display}** (`{bid_id}`)\nYour Bid: `{bid_amount_display}`\nCurrent: `{current_bid_latest_display}`\n\nConfirm?", parse_mode="Markdown", reply_markup=markup)
    schedule_bid_expiration(confirmation_key, 120)

def schedule_bid_expiration(confirmation_key, timeout=120):
    def expire_bid():
        if confirmation_key in pending_bids:
             bid_details = pending_bids.pop(confirmation_key)
             logger.info(f"Pending bid {confirmation_key} expired.")
             try: bot.edit_message_text("⏰ Bid confirmation expired. Try again.", chat_id=bid_details['user_id'], message_id=bid_details['original_message_id'] + 1, reply_markup=None) # original_message_id + 1 for the bot's reply
             except Exception as e: logger.warning(f"Could not edit expired bid conf: {e}")
    threading.Timer(timeout, expire_bid).start()

@bot.callback_query_handler(func=lambda call: call.data.startswith(("confirmbid_", "cancelbid_")))
def handle_bid_confirmation(call):
    user_id = call.from_user.id; chat_id = call.message.chat.id; message_id = call.message.message_id
    try: action, confirmation_key = call.data.split("_", 1)
    except ValueError: logger.error(f"Invalid bid conf cb: {call.data}"); return bot.answer_callback_query(call.id, "Error")
    if confirmation_key not in pending_bids:
        try: bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
        except Exception: pass
        return bot.answer_callback_query(call.id, "⚠️ Expired/Invalid.", show_alert=True)
    bid_details = pending_bids.pop(confirmation_key)
    if user_id != bid_details['user_id']: pending_bids[confirmation_key] = bid_details; return bot.answer_callback_query(call.id, "Not for you.")

    if action == "cancelbid":
        try: bot.edit_message_text("❌ Bid cancelled.", chat_id=chat_id, message_id=message_id, reply_markup=None)
        except Exception: pass
        return bot.answer_callback_query(call.id, "❌ Bid cancelled.")

    if action == "confirmbid":
        try:
            bidder_id = bid_details['user_id']; bidder_id_str = str(bidder_id); bidder_mention = bid_details['bidder_mention']
            bid_id = bid_details['bid_id']; bid_amount = bid_details['bid_amount']; previous_bidder_id_str = bid_details['previous_bidder_id']

            update_result = bids_col.find_one_and_update(
                {"bid_id": bid_id, "status": "active"},
                [
                    {"$set": {
                        "current_bid_check": "$current_bid",
                        "current_bid": {"$cond": [{"$lt": ["$current_bid", bid_amount]}, bid_amount, "$current_bid"]},
                        "highest_bidder_id": {"$cond": [{"$lt": ["$current_bid", bid_amount]}, bidder_id_str, "$highest_bidder_id"]},
                        "highest_bidder_mention": {"$cond": [{"$lt": ["$current_bid", bid_amount]}, bidder_mention, "$highest_bidder_mention"]},
                        "last_bid_time": {"$cond": [{"$lt": ["$current_bid", bid_amount]}, datetime.datetime.utcnow(), "$last_bid_time"]},
                        f"history.{bidder_id_str}": {"$cond": [{"$lt": ["$current_bid", bid_amount]}, {'mention': bidder_mention, 'amount': bid_amount, 'time': datetime.datetime.utcnow()}, f"$history.{bidder_id_str}"]}
                    }}
                ],
                return_document=pymongo.ReturnDocument.AFTER
            )

            if not update_result or update_result.get('current_bid') != bid_amount:
                 logger.warning(f"Bid {bid_id} failed atomicity check or auction ended.")
                 current_bid_db = update_result.get('current_bid_check') if update_result else 'N/A'
                 current_bid_db_display = f"{current_bid_db:,.0f}" if isinstance(current_bid_db,(int, float)) else "N/A"
                 fail_text = f"❌ Auction ended or outbid! Current: `{current_bid_db_display}`."
                 try: bot.edit_message_text(fail_text, chat_id=chat_id, message_id=message_id, reply_markup=None, parse_mode="Markdown")
                 except Exception: pass
                 return bot.answer_callback_query(call.id, "❌ Outbid or auction ended!", show_alert=True)

            log_bid_to_admin_channel(bidder_id, bidder_mention, bid_id, bid_amount)
            # add_points removed
            update_bid_message_in_channel(bid_id)

            bid_link_url = "#"; msg_link_chat_id = update_result.get('chat_id'); msg_link_msg_id = update_result.get('message_id')
            if msg_link_chat_id and msg_link_msg_id: bid_link_url = f"https://t.me/c/{str(msg_link_chat_id)[4:]}/{msg_link_msg_id}"
            bid_amount_display = f"{bid_amount:,.0f}"
            confirmation_text = (f"✅ Bid `{bid_amount_display}` on `{bid_id}` placed!\n"
                                 f"📌 [View Bid]({bid_link_url})") # Points message removed
            try: bot.edit_message_text(confirmation_text, chat_id=chat_id, message_id=message_id, parse_mode="markdown", reply_markup=None, disable_web_page_preview=True)
            except Exception as edit_err: logger.warning(f"Could not edit bid conf success: {edit_err}"); bot.send_message(chat_id, confirmation_text, parse_mode="markdown", disable_web_page_preview=True)

            if previous_bidder_id_str and previous_bidder_id_str != bidder_id_str:
                notify_outbid_user(previous_bidder_id_str, bid_amount, bid_id)

            bot.answer_callback_query(call.id, "✅ Bid confirmed!")
            logger.info(f"User {bidder_id_str} bid {bid_amount} on {bid_id}")

        except Exception as e:
            logger.error(f"Error confirming bid {confirmation_key}: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Error confirming bid.", show_alert=True)
            try: bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
            except Exception: pass

def log_bid_to_admin_channel(user_id, user_mention, bid_id, bid_amount):
    try:
        bid_amount_display = f"{bid_amount:,.0f}"
        log_text = (f" Bidding Log \n👤 User: {user_mention} (`{user_id}`)\n"
                    f"🏷️ Item ID: `{bid_id}`\n💰 Bid: `{bid_amount_display}`\n🕒 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        bot.send_message(ADMIN_BID_LOG_CHANNEL, log_text, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e: logger.error(f"Failed log bid to {ADMIN_BID_LOG_CHANNEL}: {e}")

def notify_outbid_user(previous_bidder_id_str, new_bid_amount, bid_id):
    try:
        bid_data = bids_col.find_one({"bid_id": bid_id}, {"item_name": 1, "chat_id": 1, "message_id": 1})
        if not bid_data: return
        item_name = escape(bid_data.get("item_name", bid_id)); msg_chat_id = bid_data.get("chat_id"); msg_message_id = bid_data.get("message_id")
        view_link = f"https://t.me/c/{str(msg_chat_id)[4:]}/{msg_message_id}" if msg_chat_id and msg_message_id else "#"
        bot_username = bot.get_me().username; new_bid_amount_display = f"{new_bid_amount:,.0f}"
        markup = InlineKeyboardMarkup().add(InlineKeyboardButton("🔗 Place New Bid", url=f'https://t.me/{bot_username}?start=bid-{bid_id}'))
        bot.send_message(int(previous_bidder_id_str), f"⚠️ Outbid on **{item_name}** (`{bid_id}`)!\n🔹 New Bid: `{new_bid_amount_display}`\n🔗 [View Item]({view_link})",
                         parse_mode="Markdown", disable_web_page_preview=True, reply_markup=markup)
        logger.info(f"Notified {previous_bidder_id_str} of outbid on {bid_id}")
    except Exception as e: logger.error(f"Error notifying prev bidder {previous_bidder_id_str}: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("ref_"))
def refresh_bid(call):
    try: bid_id = call.data.split("_")[1]; update_bid_message_in_channel(bid_id); bot.answer_callback_query(call.id, f"✅ {bid_id} refreshed!")
    except IndexError: bot.answer_callback_query(call.id, "Error: Invalid data.")
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): bot.answer_callback_query(call.id, "ℹ️ Already up-to-date.")
         else: logger.error(f"Error refreshing bid {call.data}: {e}"); bot.answer_callback_query(call.id, "Error refreshing.")
    except Exception as e: logger.error(f"Error refreshing bid {call.data}: {e}"); bot.answer_callback_query(call.id, "Error refreshing.")

@bot.message_handler(commands=['removebid'])
def remove_last_bid(message):
    if not is_mod(message.from_user.id):
        return bot.reply_to(message, "❌ Unauthorized.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return bot.reply_to(message, "⚠️ Usage: `/removebid <bid_id>`")
    bid_id_to_modify = args[1].strip().upper()
    try:
        bid_data = bids_col.find_one({"bid_id": bid_id_to_modify})
        if not bid_data:
            return bot.reply_to(message, f"❌ No auction found with Bid ID: `{bid_id_to_modify}`")
        if bid_data.get("status") != "active":
             return bot.reply_to(message, f"⚠️ Auction `{bid_id_to_modify}` is not active.")
        current_bidder_id = bid_data.get('highest_bidder_id')
        history = bid_data.get('history', {})
        if not current_bidder_id:
            return bot.reply_to(message, f"⚠️ There are no bids to remove on `{bid_id_to_modify}`.")
        last_bid_info = history.pop(current_bidder_id, None)
        removed_bid_amount = last_bid_info.get('amount', 0.0) if last_bid_info else 0.0
        removed_bid_amount_display = f"{removed_bid_amount:,.0f}"
        new_highest_bidder_id = None
        new_bid_amount = bid_data.get('base_price', 0.0)
        new_highest_bidder_mention = None
        if history:
            try:
                new_highest_bidder_id = max(history, key=lambda k: history[k].get('amount', 0.0))
                new_bid_amount = history[new_highest_bidder_id].get('amount', new_bid_amount)
                new_highest_bidder_mention = history[new_highest_bidder_id].get('mention')
            except (ValueError, KeyError) as e:
                 logger.info(f"History became empty or key error for {bid_id_to_modify} after removing last bid: {e}")
                 pass
        new_bid_amount_display = f"{new_bid_amount:,.0f}"
        base_price_display = f"{bid_data.get('base_price', 0.0):,.0f}"
        update_result = bids_col.update_one(
            {"bid_id": bid_id_to_modify},
            {"$set": {
                "current_bid": new_bid_amount, "highest_bidder_id": new_highest_bidder_id,
                "highest_bidder_mention": new_highest_bidder_mention, "history": history,
                "last_bid_time": datetime.datetime.utcnow()
               }
            }
        )
        if update_result.modified_count > 0:
            update_bid_message_in_channel(bid_id_to_modify)
            admin_notification = f"✅ Last bid (`{removed_bid_amount_display}`) removed from `{bid_id_to_modify}` by @{message.from_user.username}.\n"
            if new_highest_bidder_mention:
                admin_notification += f" Reinstated bidder: {new_highest_bidder_mention} at `{new_bid_amount_display}`."
            else:
                admin_notification += f" No previous bidders. Bid reset to base price `{base_price_display}`."
            bot.reply_to(message, admin_notification, parse_mode="Markdown", disable_web_page_preview=True)
            logger.info(f"Last bid removed from {bid_id_to_modify} by {message.from_user.id}.")
            if last_bid_info:
                try: bot.send_message(int(current_bidder_id), f"ℹ️ Your bid `{removed_bid_amount_display}` on `{bid_id_to_modify}` removed by admin.", parse_mode="Markdown")
                except Exception as e: logger.warning(f"Could not notify removed bidder {current_bidder_id}: {e}")
            if new_highest_bidder_id:
                try: bot.send_message(int(new_highest_bidder_id), f"🎉 You are now highest bidder on `{bid_id_to_modify}` with `{new_bid_amount_display}`!", parse_mode="Markdown")
                except Exception as e: logger.warning(f"Could not notify new highest bidder {new_highest_bidder_id}: {e}")
        else:
             bot.reply_to(message, f"⚠️ Could not remove bid from `{bid_id_to_modify}`. Already removed or error occurred.")
             if current_bidder_id and last_bid_info: history[current_bidder_id] = last_bid_info
    except Exception as e:
        logger.error(f"Error removing last bid for {bid_id_to_modify}: {e}", exc_info=True)
        bot.reply_to(message, "An error occurred while removing the bid.")

# === User Information Commands ===
@bot.message_handler(commands=['myitems'])
def my_items(message):
    user_id = message.from_user.id; user_id_str = str(user_id)
    if is_banned(user_id): return bot.reply_to(message, "You are banned.")
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start the bot first.", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Start', url=f'https://t.me/{bot.get_me().username}?start=start')))
    pending_items = []; approved_items = []
    try:
        pending_cursor = pending_items_col.find(
            {"user_id": user_id_str, "status": "pending"},
            {"item_name": 1, "item_type": 1, "submission_time": 1, "_id": 1}
        ).sort("submission_time", pymongo.DESCENDING)
        pending_items = list(pending_cursor)
        approved_cursor = approved_items_col.find(
            {"user_id": user_id_str},
            {"name": 1, "category": 1, "link": 1, "approval_time": 1}
        ).sort("approval_time", pymongo.DESCENDING)
        approved_items = list(approved_cursor)
    except Exception as e: logger.error(f"Error /myitems {user_id}: {e}"); return bot.reply_to(message, "Error fetching items.")
    text_lines = ["📦 **Your Submitted Items**\n"]; markup = InlineKeyboardMarkup(); has_items = False
    if pending_items:
        has_items = True; text_lines.append("⏳ **Pending Approval:**")
        for item in pending_items:
            name = escape(item.get('item_name', f"ID {item.get('_id')}")); type = escape(item.get('item_type', '?').capitalize())
            time_str = item.get('submission_time').strftime('%y-%m-%d %H:%M') if item.get('submission_time') else 'N/A'
            text_lines.append(f"  - {name} ({type}) | Submitted: {time_str}")
        text_lines.append("")
    if approved_items:
        has_items = True; text_lines.append("✅ **Approved & Auctioned:**")
        for item in approved_items:
            name = escape(item.get('name', '?')); category = escape(item.get('category', '?').capitalize()); link = item.get('link', '#')
            text_lines.append(f"  - [{name} ({category})]({link})")
            markup.add(InlineKeyboardButton(f"View: {name[:25]}", url=link))
        text_lines.append("")
    if not has_items: text_lines.append("No items submitted yet. Use /add!")
    markup.add(InlineKeyboardButton("❌ Close", callback_data=f"close_{user_id}"))
    full_text = "\n".join(text_lines); full_text = full_text[:4092] + "\n..." if len(full_text) > 4096 else full_text
    bot.reply_to(message, full_text, reply_markup=markup, parse_mode="Markdown", disable_web_page_preview=True)

@bot.message_handler(commands=['mybids'])
def my_bids(message):
    user_id = message.from_user.id; user_id_str = str(user_id)
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start bot.")
    try:
        bids_cursor = bids_col.find(
            {"status": "active", "$or": [{"highest_bidder_id": user_id_str}, {f"history.{user_id_str}": {"$exists": True}}]},
            {"bid_id": 1, "item_name": 1, "current_bid": 1, "highest_bidder_id": 1, "chat_id": 1, "message_id": 1, f"history.{user_id_str}.amount": 1}
        ).sort("last_bid_time", pymongo.DESCENDING)
        user_bids_info = []
        for bid in bids_cursor:
            bid_id = bid['bid_id']; item_name = escape(bid.get('item_name', bid_id)); current_bid = bid.get('current_bid', 0.0)
            is_highest = (bid.get('highest_bidder_id') == user_id_str)
            user_bid = bid.get('history', {}).get(user_id_str, {}).get('amount', current_bid) if not is_highest else current_bid
            user_bid_display = f"{user_bid:,.0f}"
            msg_chat_id = bid.get('chat_id'); msg_message_id = bid.get('message_id')
            link = f"https://t.me/c/{str(msg_chat_id)[4:]}/{msg_message_id}" if msg_chat_id and msg_message_id else "#"
            user_bids_info.append({"bid_id": bid_id, "name": item_name, "amount_display": user_bid_display, "amount_numeric": user_bid, "is_highest": is_highest, "link": link})
        pending_count = 0
        for key, pb in list(pending_bids.items()):
            if str(pb['user_id']) == user_id_str:
                 pending_count += 1
                 pdb = bids_col.find_one({"bid_id": pb['bid_id']}, {"chat_id": 1, "message_id": 1, "item_name": 1})
                 link_p = "#"; item_name_p = f"Item {pb['bid_id']}"
                 if pdb: msg_chat_id_p=pdb.get('chat_id'); msg_message_id_p=pdb.get('message_id'); link_p = f"https://t.me/c/{str(msg_chat_id_p)[4:]}/{msg_message_id_p}" if msg_chat_id_p and msg_message_id_p else "#"; item_name_p = escape(pdb.get('item_name', item_name_p))
                 p_amount = pb['bid_amount']; p_amount_display = f"{p_amount:,.0f}"
                 user_bids_info.append({"bid_id": pb['bid_id'], "name": item_name_p, "amount_display": p_amount_display, "amount_numeric": p_amount, "is_highest": False, "link": link_p, "is_pending": True})
        if not user_bids_info: return bot.reply_to(message, "❌ No active bids.")
        markup = InlineKeyboardMarkup(); text_lines = ["📜 **Your Active Bids:**\n"]; text_lines[0] += f" ({pending_count} Pending)" if pending_count else ""
        user_bids_info.sort(key=lambda x: (not x.get('is_pending', False), not x['is_highest'], -x['amount_numeric']))
        for bi in user_bids_info:
            status = "⏳ Pending" if bi.get('is_pending') else ("⭐ Highest" if bi['is_highest'] else "📉 Outbid")
            text_lines.append(f"- `{bi['bid_id']}` ({bi['name']}): `{bi['amount_display']}` {status}")
            btn_text = f"{bi['name'][:15]} ({bi['bid_id']}) - {'Confirm?' if bi.get('is_pending') else 'View'}"
            markup.add(InlineKeyboardButton(btn_text, url=bi['link']))
        markup.add(InlineKeyboardButton("❌ Close", callback_data=f"close_{user_id}"))
        full_text = "\n".join(text_lines); full_text = full_text[:4092]+"..." if len(full_text)>4096 else full_text
        bot.reply_to(message, full_text, reply_markup=markup, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e: logger.error(f"Error /mybids {user_id}: {e}"); bot.reply_to(message, "Error fetching bids.")

@bot.message_handler(commands=['mywins', 'myphg'])
def my_wins_command(message):
    user_id = message.from_user.id; user_id_str = str(user_id)
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start bot.")
    if bid_ji: return bot.reply_to(message, "⚠️ Auction ongoing. Shows results after bidding closes. Use /mybids.")
    try:
        won_bids_cursor = bids_col.find(
            {"highest_bidder_id": user_id_str}, # No status check, assume ended if bid_ji is false
            {"bid_id": 1, "item_name": 1, "current_bid": 1, "owner_mention": 1, "chat_id": 1, "message_id": 1}
        ).sort("current_bid", pymongo.DESCENDING)
        won_items = list(won_bids_cursor)
        if not won_items: return bot.reply_to(message, "❌ No items won yet.")
        text_lines = ["🏆 **Items Won (Auction Concluded):**\n"]; markup = InlineKeyboardMarkup(); total_cost = 0
        for item in won_items:
            bid_id = item['bid_id']; item_name = escape(item.get('item_name', bid_id)); bid_amount = item.get('current_bid', 0.0)
            total_cost += bid_amount; bid_amount_display = f"{bid_amount:,.0f}"; owner_display = item.get('owner_mention', '? Seller')
            msg_chat_id = item.get('chat_id'); msg_message_id = item.get('message_id')
            link = f"https://t.me/c/{str(msg_chat_id)[4:]}/{msg_message_id}" if msg_chat_id and msg_message_id else "#"
            text_lines.append(f"• `{bid_id}`: **{item_name}**\n  Bid: `{bid_amount_display}` | Seller: {owner_display}\n  [View Bid]({link})")
        total_cost_display = f"{total_cost:,.0f}"; text_lines.append(f"\n---\n💰 **Total Cost:** `{total_cost_display}`")
        markup.add(InlineKeyboardButton("❌ Close", callback_data=f"close_{user_id}"))
        full_text = "\n\n".join(text_lines); full_text = full_text[:4092]+"..." if len(full_text)>4096 else full_text
        bot.reply_to(message, full_text, parse_mode='Markdown', reply_markup=markup, disable_web_page_preview=True)
    except Exception as e: logger.error(f"Error /mywins {user_id}: {e}"); bot.reply_to(message, "Error fetching wins.")

@bot.message_handler(commands=['mysold'])
def handle_mysold(message):
    user_id = message.from_user.id; user_id_str = str(user_id)
    user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start bot.")
    if bid_ji: return bot.reply_to(message, "⚠️ Auction ongoing. Shows results after bidding closes.")
    try:
        sold_bids_cursor = bids_col.find(
            {"owner_id": user_id_str, "highest_bidder_id": {"$ne": None}}, # No status check
            {"bid_id": 1, "item_name": 1, "current_bid": 1, "highest_bidder_mention": 1, "chat_id": 1, "message_id": 1}
        ).sort("current_bid", pymongo.DESCENDING)
        sold_items = list(sold_bids_cursor)
        if not sold_items: return bot.reply_to(message, "❌ No items sold yet.")
        text_lines = ["🏷️ **Your Sold Items (Auction Concluded):**\n"]; markup = InlineKeyboardMarkup(); total_earned = 0
        for item in sold_items:
            bid_id = item['bid_id']; item_name = escape(item.get('item_name', bid_id)); bid_amount = item.get('current_bid', 0.0)
            total_earned += bid_amount; bid_amount_display = f"{bid_amount:,.0f}"; buyer_mention = item.get('highest_bidder_mention', '? Buyer')
            msg_chat_id = item.get('chat_id'); msg_message_id = item.get('message_id')
            link = f"https://t.me/c/{str(msg_chat_id)[4:]}/{msg_message_id}" if msg_chat_id and msg_message_id else "#"
            text_lines.append(f"• `{bid_id}`: **{item_name}**\n  Sold For: `{bid_amount_display}` | Buyer: {buyer_mention}\n  [View Bid]({link})")
        total_earned_display = f"{total_earned:,.0f}"; text_lines.append(f"\n---\n📈 **Total Earned:** `{total_earned_display}`")
        markup.add(InlineKeyboardButton("❌ Close", callback_data=f"close_{user_id}"))
        full_text = "\n\n".join(text_lines); full_text = full_text[:4092]+"..." if len(full_text)>4096 else full_text
        bot.reply_to(message, full_text, parse_mode='Markdown', reply_markup=markup, disable_web_page_preview=True)
    except Exception as e: logger.error(f"Error /mysold {user_id}: {e}"); bot.reply_to(message, "Error fetching sold items.")

# === Bot Control Commands ===
@bot.message_handler(commands=['sub'])
def subon(message):
    global sub_process
    if not is_mod(message.from_user.id):
        return bot.reply_to(message, "Unauthorized.")
    args = message.text.split()
    current_status = "ON" if sub_process else "OFF"
    if len(args) < 2 or args[1].lower() not in ['on', 'off']:
        return bot.reply_to(message, f"Usage: /sub <on|off>\nCurrent status: {current_status}")
    action = args[1].lower()
    if action == 'on':
        if sub_process:
            return bot.reply_to(message, 'ℹ️ Submissions are already ENABLED.')
        sub_process = True
        bot.reply_to(message, '✅ Submissions are now ENABLED.')
        logger.info(f"Submissions ENABLED by {message.from_user.id}")
    elif action == 'off':
        if not sub_process:
            return bot.reply_to(message, 'ℹ️ Submissions are already DISABLED.')
        sub_process = False
        bot.reply_to(message, '❌ Submissions are now DISABLED.')
        logger.info(f"Submissions DISABLED by {message.from_user.id}")
        
# === Utility & Info Commands ===
@bot.message_handler(commands=['getid'])
def get_file_id(message):
    if not message.reply_to_message: return bot.reply_to(message, "Reply to media.")
    replied = message.reply_to_message; file_id = None; file_type = None; file_unique_id = None
    if replied.sticker: file_id = replied.sticker.file_id; file_unique_id = replied.sticker.file_unique_id; file_type = "Sticker"
    elif replied.photo: file_id = replied.photo[-1].file_id; file_unique_id = replied.photo[-1].file_unique_id; file_type = "Photo"
    elif replied.animation: file_id = replied.animation.file_id; file_unique_id = replied.animation.file_unique_id; file_type = "Animation"
    elif replied.video: file_id = replied.video.file_id; file_unique_id = replied.video.file_unique_id; file_type = "Video"
    elif replied.document: file_id = replied.document.file_id; file_unique_id = replied.document.file_unique_id; file_type = f"Document ({replied.document.mime_type})"
    if file_id: bot.reply_to(message, f"<b>{file_type} ID:</b>\n<code>{escape(file_id)}</code>\n\n<b>Unique ID:</b>\n<code>{escape(file_unique_id)}</code>", parse_mode='HTML')
    else: bot.reply_to(message, "No file ID found.")

@bot.message_handler(commands=['brules'])
def prules(msg):
    # MODIFIED Bidding Rules Text
    text = """<b>📜 Bidding Rules</b>

➡️ **Minimum Bid Increments:**
   • Bid ≤ 50: min +1
   • Bid 51 to 100: min +2
   • Bid > 100: min +5
   *(Subject to change based on item value)*

➡️ **Bid Removal:** Removing a confirmed bid requires admin approval. Use /report or contact admin.

➡️ **Group Membership:** You **MUST** be a member of both the <a href="{auction_link}">Auction Channel</a> and the <a href="{trade_link}">Trade Group</a>.

➡️ **Issues:** Use /report or contact an admin in the trade group.
""".format(auction_link=AUCTION_GROUP_LINK, trade_link=TRADE_CHAT_ID if isinstance(TRADE_CHAT_ID, str) and TRADE_CHAT_ID.startswith("https") else f"https://t.me/joinchat/{TRADE_CHAT_ID}" if isinstance(TRADE_CHAT_ID, int) else "#")
    markup = InlineKeyboardMarkup().add(InlineKeyboardButton('❌ Close', callback_data=f'close_{msg.from_user.id}'))
    bot.reply_to(msg, text, reply_markup=markup, parse_mode='html', disable_web_page_preview=True)

@bot.message_handler(commands=['subrules'])
def subrule(msg):
    text = """<b>📜 Submission Rules</b>

➡️ **Cancellation (During Auction):** Cancelling after approval may incur penalties. Contact an admin.

➡️ **Buyer Failure:** If the highest bidder fails to pay, they may face penalties/bans.

➡️ **Cancellation (Pending):** Items can usually be cancelled for free while pending approval. Use /report replying to your submission preview.

➡️ **Group Membership:** Ensure you are in the <a href="{auction_link}">Auction Channel</a> and <a href="{trade_link}">Trade Group</a>.
""".format(auction_link=AUCTION_GROUP_LINK, trade_link=TRADE_CHAT_ID if isinstance(TRADE_CHAT_ID, str) and TRADE_CHAT_ID.startswith("https") else f"https://t.me/joinchat/{TRADE_CHAT_ID}" if isinstance(TRADE_CHAT_ID, int) else "#")
    markup = InlineKeyboardMarkup().add(InlineKeyboardButton('❌ Close', callback_data=f'close_{msg.from_user.id}'))
    bot.reply_to(msg, text, reply_markup=markup, parse_mode='html', disable_web_page_preview=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("close_"))
def closed(call):
    try:
        target_user_id_str = call.data.split("_")[1]
        # Check if target_user_id_str is a valid integer. If it's for closing a message in a group by anyone, it might be 0 or some other convention.
        # For user-specific close buttons, it should be the user's ID.
        is_allowed_to_close = False
        if target_user_id_str == str(call.from_user.id):
            is_allowed_to_close = True
        elif is_mod(call.from_user.id): # Mods can close any message with such a button
            is_allowed_to_close = True
        # Special case: if target_user_id_str was '0' or similar for public close buttons
        elif target_user_id_str == '0': # Convention for public close buttons
             is_allowed_to_close = True


        if is_allowed_to_close:
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id)
        else: bot.answer_callback_query(call.id, "Not for you.")
    except ValueError: # If target_user_id_str is not an int (e.g. from an old format)
        logger.warning(f"Could not parse target_user_id from close_ callback: {call.data}")
        # Fallback: allow closing if it's the user's own message or if user is mod
        if call.message.reply_to_message and call.message.reply_to_message.from_user.id == call.from_user.id or is_mod(call.from_user.id):
             bot.delete_message(call.message.chat.id, call.message.message_id)
             bot.answer_callback_query(call.id)
        else:
            # Default to allowing any user to close if the target ID part is malformed or "0"
            # This is a common pattern for "Close" buttons on public/bot messages.
            # If a message has a close_0, anyone can click it.
            # If it has close_USERID, only that user or a mod can.
            # For simplicity here, if parsing fails but button exists, assume it can be closed.
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
                bot.answer_callback_query(call.id)
            except Exception as e_del:
                logger.debug(f"Minor error closing msg (fallback): {e_del}"); bot.answer_callback_query(call.id)

    except Exception as e: logger.debug(f"Minor error closing msg: {e}"); bot.answer_callback_query(call.id)


@bot.message_handler(commands=['report'])
def report_command(msg):
    if not msg.reply_to_message: return bot.reply_to(msg, "⚠️ Reply to the message to report.")
    replied_msg = msg.reply_to_message; chat_id = msg.chat.id
    replied_msg_link = f"https://t.me/c/{str(chat_id)[4:]}/{replied_msg.message_id}" if chat_id < 0 else "#" # Link to replied message
    preview_text = replied_msg.text[:50] if replied_msg.text else replied_msg.caption[:50] if replied_msg.caption else "[Media]"
    
    # Link to the /report command message itself
    report_command_message_link = f"https://t.me/{msg.chat.username}/{msg.message_id}" if msg.chat.username else f"https://t.me/c/{str(chat_id)[4:]}/{msg.message_id}" if chat_id < 0 else "#"


    markup = InlineKeyboardMarkup().row(
        InlineKeyboardButton("✅ Confirm Report", callback_data=f"confir_report:{replied_msg.message_id}:{chat_id}"), # Added chat_id of reported msg
        InlineKeyboardButton("❌ Cancel", callback_data="cance_report")
    )
    bot.reply_to(msg, f"📝 Report this message?\nPreview: \"<i>{escape(preview_text)}...</i>\"\n<a href='{replied_msg_link}'>[Link to Reported Message]</a>", reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("confir_report:") or call.data == "cance_report")
def handle_report_confirmation(call):
    current_chat_id = call.message.chat.id # Chat where the confirm/cancel buttons are
    message_id_with_buttons = call.message.message_id # Message ID of the confirm/cancel buttons
    user_id = call.from_user.id

    if call.data == "cance_report":
        try: bot.delete_message(current_chat_id, message_id_with_buttons)
        except Exception: pass
        return bot.answer_callback_query(call.id, "❌ Report canceled.")
    try:
        parts = call.data.split(":")
        reported_message_id = int(parts[1])
        original_chat_id_of_reported_msg = int(parts[2]) # Chat ID where the reported message exists

        reporter_mention = f"[{escape(call.from_user.full_name)}](tg://user?id={user_id})"
        report_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Link to the message that contained the /report command (the user's message)
        # This is call.message.reply_to_message if the bot replied to the /report command
        report_initiator_message_link = "#"
        if call.message.reply_to_message:
            initiator_chat_id = call.message.reply_to_message.chat.id
            initiator_msg_id = call.message.reply_to_message.message_id
            if initiator_chat_id < 0 : # Group/Channel
                report_initiator_message_link = f"https://t.me/c/{str(initiator_chat_id)[4:]}/{initiator_msg_id}"
            # else: # Private chat, direct link might not be standard or useful for admins this way

        # Link to the actual message being reported
        reported_message_actual_link = f"https://t.me/c/{str(original_chat_id_of_reported_msg)[4:]}/{reported_message_id}" if original_chat_id_of_reported_msg < 0 else "#"


        report_text = (f"🚨 **New Report** 🚨\n👤 By: {reporter_mention} (`{user_id}`)\n🕒 {report_time}\n"
                       f"🔗 Context (/report cmd): [Link]({report_initiator_message_link})\n"
                       f"🔗 Actual Reported Msg: [Link]({reported_message_actual_link})\n👇 Forwarded below:")
        
        bot.send_message(APPROVE_CHANNEL, report_text, parse_mode="Markdown", disable_web_page_preview=True)
        bot.forward_message(APPROVE_CHANNEL, original_chat_id_of_reported_msg, reported_message_id) # Forward from original chat
        
        bot.answer_callback_query(call.id, "✅ Report sent.", show_alert=True)
        logger.info(f"Report by {user_id} for msg {reported_message_id} in chat {original_chat_id_of_reported_msg}")
    except Exception as e: bot.answer_callback_query(call.id, "⚠️ Failed to send report."); logger.error(f"Error reporting: {e}", exc_info=True)
    
    try: bot.delete_message(current_chat_id, message_id_with_buttons) # Delete the "Confirm Report?" message
    except Exception: pass


# === Item List Command (/elements - Simplified) ===
@bot.message_handler(commands=['elements'])
def send_elements_menu(message):
    user_id = message.from_user.id; user_doc = get_user_doc(user_id)
    if not user_doc: return bot.reply_to(message, "Please /start bot.")
    elements_items_list_menu(message.chat.id, msg_id_to_reply=message.message_id)

def elements_items_list_menu(chat_id, msg_id_to_reply=None, edit_message_id=None):
    photo = "https://via.placeholder.com/400x200.png?text=Browse+Items" # MODIFIED: Uncommented, ensure this is a valid URL or File ID
    text = "📦 Browse Auction Items by Category:"
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(types.InlineKeyboardButton('🐾 Pets', callback_data=f'listcat_{CATEGORY_PET}'),
               types.InlineKeyboardButton('💎 Cores', callback_data=f'listcat_{CATEGORY_CORE}'))
    
    # Determine the user_id for the close button. If chat_id is positive, it's a private chat, so use chat_id.
    # Otherwise, it's a group, and a generic close button might be desired (target_user_id=0 or similar)
    # or it should be specific to the user who triggered the command if `msg_id_to_reply` is available.
    # For simplicity, if editing, we assume the original interaction was with call.from_user.id
    # For new message, if msg_id_to_reply, we can try to get from_user.id from the original message if needed,
    # but here we'll use chat_id if private, or a generic '0' for group for the close button.
    close_button_user_id_target = chat_id if chat_id > 0 else 0
    if edit_message_id: # If editing, the callback query (call) would have from_user.id
        # This function doesn't get `call` directly. If this is called from a callback,
        # the caller should pass the correct user_id for the close button.
        # For now, using chat_id or 0 is a safe default.
        pass


    markup.add(types.InlineKeyboardButton('❌ Close', callback_data=f'close_{close_button_user_id_target}'))

    try:
        if edit_message_id:
            # If the message to be edited was originally text, edit_message_media will fail.
            # We need to know the original message type.
            # For simplicity, let's try to edit as media, and if it fails, try to delete and resend.
            # Or, better, if this function is called for an edit, it should expect the original to be media.
            try:
                bot.edit_message_media(media=InputMediaPhoto(photo, caption=text), chat_id=chat_id, message_id=edit_message_id, reply_markup=markup)
            except telebot.apihelper.ApiTelegramException as e:
                if "message can't be edited" in str(e) or "message to edit not found" in str(e) or "ానికి వచన శీర్షిక లేదు" in str(e).lower() or "there is no caption in the message to edit" in str(e).lower(): # Hindi for "message has no caption"
                    logger.warning(f"Failed to edit /elements as media (likely original was text or no caption): {e}. Sending new message.")
                    bot.delete_message(chat_id, edit_message_id) # Delete old
                    bot.send_photo(chat_id=chat_id, photo=photo, caption=text, reply_markup=markup) # Send new
                else:
                    raise # Re-raise other API errors
        else:
            send_params = {"chat_id": chat_id, "photo": photo, "caption": text, "reply_markup": markup}
            if msg_id_to_reply: send_params["reply_to_message_id"] = msg_id_to_reply
            bot.send_photo(**send_params)
    except Exception as e:
        logger.error(f"Error sending/editing elements menu (photo attempt): {e}")
        # Fallback to text if photo fails
        try:
            if edit_message_id:
                bot.edit_message_text(text, chat_id=chat_id, message_id=edit_message_id, reply_markup=markup, parse_mode='Markdown') # Assuming text is Markdown
            else:
                send_params_text = {"chat_id": chat_id, "text": text, "reply_markup": markup, "parse_mode": "Markdown"}
                if msg_id_to_reply: send_params_text["reply_to_message_id"] = msg_id_to_reply
                bot.send_message(**send_params_text)
        except Exception as te:
            logger.error(f"Failed text fallback for elements menu: {te}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('listcat_'))
def handle_list_category(call):
    category_map = { CATEGORY_PET: 'Pets', CATEGORY_CORE: 'Cores' }
    category_key = call.data.split('_', 1)[1]
    if category_key not in category_map: return bot.answer_callback_query(call.id, "Unknown category.")
    category_display = category_map[category_key]
    chat_id = call.message.chat.id; message_id = call.message.message_id

    try:
        projection = {"item_name": 1, "bid_id": 1, "chat_id": 1, "message_id": 1}
        items_cursor = bids_col.find({"status": "active", "item_type": category_key}, projection).limit(50)
        items_list = list(items_cursor)

        if not items_list: text = f"😕 No active **{category_display}** found."
        else:
            text = f"🔎 Active **{category_display}** items:\n\n"
            item_lines = []
            for item in items_list:
                name = escape(item.get('item_name', item.get('bid_id', 'N/A'))); bid_id = item.get('bid_id', 'N/A')
                msg_chat_id = item.get('chat_id'); msg_message_id = item.get('message_id')
                link = f"https://t.me/c/{str(msg_chat_id)[4:]}/{msg_message_id}" if msg_chat_id and msg_message_id else "#"
                item_lines.append(f"• [{name} (`{bid_id}`)]({link})")
            text += "\n".join(item_lines)

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton('🔙 Back to Categories', callback_data='back_to_elements_menu'))
        markup.add(types.InlineKeyboardButton('❌ Close', callback_data=f'close_{call.from_user.id}'))

        max_len = 1024 
        if len(text) > max_len: text = text[:max_len - 4] + "\n..."
        
        # Determine if original message had photo or was text
        if call.message.photo:
            bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text, reply_markup=markup, parse_mode='Markdown')
        else: # Original was text
            bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, reply_markup=markup, parse_mode='Markdown', disable_web_page_preview=True)

        bot.answer_callback_query(call.id)

    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e): bot.answer_callback_query(call.id)
        elif "ానికి వచన శీర్షిక లేదు" in str(e).lower() or "there is no caption in the message to edit" in str(e).lower(): # Handle if trying to edit caption of text message
            logger.warning(f"Attempted to edit caption on a text message for listcat {category_key}. Trying edit_message_text. Error: {e}")
            try:
                bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id, reply_markup=markup, parse_mode='Markdown', disable_web_page_preview=True)
                bot.answer_callback_query(call.id)
            except Exception as e2:
                logger.error(f"Failed fallback edit_message_text for listcat {category_key}: {e2}")
                bot.answer_callback_query(call.id, "Error updating list.")
        else: 
            logger.error(f"Failed edit listcat {category_key}: {e}"); 
            bot.answer_callback_query(call.id, "Error updating list.")
    except Exception as e: 
        logger.error(f"Error listcat {category_key}: {e}"); 
        bot.answer_callback_query(call.id, "Error fetching items.")


@bot.callback_query_handler(func=lambda call: call.data == 'back_to_elements_menu')
def handle_back_to_elements_menu(call):
     elements_items_list_menu(call.message.chat.id, edit_message_id=call.message.message_id) # Pass user_id for close button
     bot.answer_callback_query(call.id)

# === Reset Commands (Admin Only) ===
@bot.message_handler(commands=['resetd'])
def reset_bid_data(message):
    if not is_admin(message.from_user.id): return bot.reply_to(message, "❌ Unauthorized.")
    markup = InlineKeyboardMarkup().row(InlineKeyboardButton("⚠️ YES, Clear ALL Bids ⚠️", callback_data="confirm_reset_bids"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_reset_bids"))
    bot.reply_to(message, "🚨 **DANGER!** Delete ALL bid info & reset counter?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data in ["confirm_reset_bids", "cancel_reset_bids"])
def handle_reset_bids_confirmation(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    if call.data == "cancel_reset_bids": bot.edit_message_text("Bid reset cancelled.", call.message.chat.id, call.message.message_id, reply_markup=None); return bot.answer_callback_query(call.id, "Cancelled.")
    if call.data == "confirm_reset_bids":
         try:
             delete_result = bids_col.delete_many({})
             config_col.update_one({"_id": "bid_counter"}, {"$set": {"value": 0}}, upsert=True)
             bot.edit_message_text(f"✅ All bid data ({delete_result.deleted_count}) cleared & counter reset.", call.message.chat.id, call.message.message_id, reply_markup=None)
             bot.answer_callback_query(call.id, "Bids cleared."); logger.warning(f"All bid data cleared by {call.from_user.id}")
         except Exception as e: logger.error(f"Error resetd: {e}"); bot.edit_message_text("❌ Error clearing bids.", call.message.chat.id, call.message.message_id, reply_markup=None); bot.answer_callback_query(call.id, "Error.")

@bot.message_handler(commands=['reseti'])
def reset_item_lists(message):
    if not is_admin(message.from_user.id): return bot.reply_to(message, "❌ Unauthorized.")
    markup = InlineKeyboardMarkup().row(InlineKeyboardButton("⚠️ YES, Clear Items ⚠️", callback_data="confirm_reset_items"), InlineKeyboardButton("❌ Cancel", callback_data="cancel_reset_items"))
    bot.reply_to(message, "🚨 **DANGER!** Delete ALL approved/pending item records?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data in ["confirm_reset_items", "cancel_reset_items"])
def handle_reset_items_confirmation(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    if call.data == "cancel_reset_items": bot.edit_message_text("Item reset cancelled.", call.message.chat.id, call.message.message_id, reply_markup=None); return bot.answer_callback_query(call.id, "Cancelled.")
    if call.data == "confirm_reset_items":
        try:
            approved_deleted = approved_items_col.delete_many({})
            pending_deleted = pending_items_col.delete_many({})
            bot.edit_message_text(f"✅ Cleared {approved_deleted.deleted_count} approved & {pending_deleted.deleted_count} pending items.", call.message.chat.id, call.message.message_id, reply_markup=None)
            bot.answer_callback_query(call.id, "Item lists cleared."); logger.warning(f"Item lists cleared by {call.from_user.id}")
        except Exception as e: logger.error(f"Error reseti: {e}"); bot.edit_message_text("❌ Error clearing items.", call.message.chat.id, call.message.message_id, reply_markup=None); bot.answer_callback_query(call.id, "Error.")

@bot.message_handler(commands=["remo"])
def remove_auction_item(message):
    if not is_mod(message.from_user.id):
        return bot.reply_to(message, "❌ Unauthorized.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message, "⚠️ Usage: `/remo <bid_id>` (e.g., `/remo A123`)")
    bid_id_to_remove = args[1].strip().upper()
    try:
        bid_doc = bids_col.find_one({"bid_id": bid_id_to_remove})
        if not bid_doc:
            return bot.reply_to(message, f"❌ No auction found with Bid ID: `{bid_id_to_remove}`")
        item_name = escape(bid_doc.get("item_name", bid_id_to_remove))
        item_status = bid_doc.get("status", "unknown")
        main_post_link = bid_doc.get('auction_post_link')
        deleted_bid_msg = False; deleted_post_msg = False
        bid_msg_chat_id = bid_doc.get('chat_id'); bid_msg_id = bid_doc.get('message_id')
        if bid_msg_chat_id and bid_msg_id:
            try: bot.delete_message(bid_msg_chat_id, bid_msg_id); logger.info(f"Deleted bid message {bid_msg_id} for {bid_id_to_remove}"); deleted_bid_msg = True
            except Exception as e: logger.warning(f"Could not delete bid message {bid_msg_id} for {bid_id_to_remove}: {e}")
        else: logger.warning(f"Missing chat/message ID for bid message of {bid_id_to_remove}")
        if main_post_link and main_post_link != '#':
            try:
                link_parts = main_post_link.split('/')
                post_msg_id = int(link_parts[-1])
                post_chat_id_str = link_parts[-2]
                post_chat_id = int(f"-100{post_chat_id_str}")
                bot.delete_message(post_chat_id, post_msg_id); logger.info(f"Deleted main post message {post_msg_id} for {bid_id_to_remove}"); deleted_post_msg = True
            except (IndexError, ValueError) as e : logger.error(f"Could not parse main post link for {bid_id_to_remove}: {main_post_link} - {e}")
            except Exception as e: logger.warning(f"Could not delete main post message for {bid_id_to_remove} from link {main_post_link}: {e}")
        else: logger.warning(f"Missing or invalid auction_post_link for {bid_id_to_remove}")
        bids_delete_result = bids_col.delete_one({"bid_id": bid_id_to_remove})
        approved_delete_result = None
        if main_post_link: approved_delete_result = approved_items_col.delete_one({"link": main_post_link})
        admin_msg_parts = [f"✅ Attempted removal of auction **{item_name}** (`{bid_id_to_remove}`) by @{message.from_user.username}."]
        admin_msg_parts.append(f"- Bid message deleted: {'✅' if deleted_bid_msg else '❌'}")
        admin_msg_parts.append(f"- Main post deleted: {'✅' if deleted_post_msg else '❌'}")
        admin_msg_parts.append(f"- Bid DB record deleted: {'✅' if bids_delete_result.deleted_count > 0 else '❌ (Status was '+item_status+')'}")
        if main_post_link: admin_msg_parts.append(f"- Approved item DB record deleted: {'✅' if approved_delete_result and approved_delete_result.deleted_count > 0 else '❌ (Not found or no link)'}")
        else: admin_msg_parts.append("- Approved item DB record: No link to check.")
        admin_msg = "\n".join(admin_msg_parts)
        bot.reply_to(message, admin_msg, parse_mode="Markdown")
        logger.info(f"Auction {bid_id_to_remove} removal process executed by {message.from_user.id}. Success flags - BidMsg: {deleted_bid_msg}, PostMsg: {deleted_post_msg}, BidDB: {bids_delete_result.deleted_count>0}, ApprovedDB: {approved_delete_result.deleted_count>0 if approved_delete_result else False}")
    except Exception as e:
        logger.error(f"Error removing item {bid_id_to_remove}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ An error occurred while removing item `{bid_id_to_remove}`.")

# === Refresh/Cancel Command ===
@bot.message_handler(commands=['refresh'])
def handle_refresh(message):
    if is_banned(message.from_user.id): return
    if message.chat.type == 'private':
        user_id = message.from_user.id; action_taken = False
        if user_id in user_states: del user_states[user_id]; action_taken = True; logger.info(f"State cancelled {user_id} via /refresh")
        if user_id in user_cache: del user_cache[user_id]; action_taken = True; logger.info(f"Cache cleared {user_id} via /refresh.")
        keys_to_remove = [k for k, v in list(pending_bids.items()) if v.get('user_id') == user_id]
        if keys_to_remove:
            action_taken = True
            for key in keys_to_remove:
                try:
                    bid_details = pending_bids[key]; pending_msg_id = bid_details.get('original_message_id', 0) + 1
                    if pending_msg_id > 0: bot.edit_message_text("❌ Cancelled by /refresh.", chat_id=user_id, message_id=pending_msg_id, reply_markup=None)
                except Exception: pass
                del pending_bids[key]
            logger.info(f"Cleared {len(keys_to_remove)} pending bids {user_id} via /refresh")
        if action_taken: bot.send_message(message.chat.id, "✅ Bot refreshed. Active command cancelled.");
        else: bot.send_message(message.chat.id, "✅ Bot refreshed. No active process found.");
    else:
        markup=InlineKeyboardMarkup().add(InlineKeyboardButton('Refresh Bot State', url=f'https://t.me/{bot.get_me().username}?start=refresh'))
        bot.reply_to(message, "Please use /refresh in private chat.", reply_markup=markup, disable_web_page_preview=True)

# === Polling Start ===
if __name__ == '__main__':
    logger.info(f"Anime Auction Bot starting... Version: {CURRENT_BOT_VERSION}")
    try:
        # MODIFIED Bot Commands
        bot.set_my_commands([
            BotCommand("start", "Start/Restart the bot"),
            BotCommand("add", "Submit a Pet or Core for auction"),
            BotCommand("elements", "Browse active auction items"),
            BotCommand("mybids", "View your current bids"),
            BotCommand("mywins", "View items you won (after auction)"),
            BotCommand("mysold", "View items you sold (after auction)"),
            BotCommand("myitems", "View your pending/approved items"),
            # BotCommand("profile", "View your auction profile"), # REMOVED
            # BotCommand("leaderboard", "Show top points earners"), # REMOVED
            BotCommand("brules", "View bidding rules"),
            BotCommand("subrules", "View submission rules"),
            # BotCommand("info", "Get info on an active auction item"), # REMOVED
            BotCommand("report", "Report an issue/message to admins"),
            BotCommand("refresh", "Refresh bot state / Cancel command"),
            BotCommand("cancel", "Cancel current operation (same as /refresh)"),
        ])
        logger.info("Bot commands updated.")
    except Exception as cmd_err:
        logger.error(f"Could not set bot commands: {cmd_err}")

    logger.info("Starting polling...")
    bot.infinity_polling(logger_level=logging.INFO, timeout=20, long_polling_timeout=30)
    logger.info("Bot stopped.")
