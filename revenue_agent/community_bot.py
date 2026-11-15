import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_ONBOARDING_BOT_TOKEN") # Get this from @BotFather on Telegram

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fires when a user sends /start to the bot."""
    welcome_message = (
        "⚡️ Welcome to the Black Blood Support Portal.\n\n"
        "I am the automated assistant. How can I help you today?"
    )
    
    # Create interactive buttons for the user to click
    keyboard = [
        [InlineKeyboardButton("💎 View VIP Plans", callback_data='view_plans')],
        [InlineKeyboardButton("📊 Verify Past Performance", callback_data='view_results')],
        [InlineKeyboardButton("❓ Contact Human Support", callback_data='contact_support')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(welcome_message, reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the button clicks from the user."""
    query = update.callback_query
    await query.answer() # Acknowledge the click to Telegram

    if query.data == 'view_plans':
        plans_text = (
            "🔥 **BLACK BLOOD VIP ACCESS** 🔥\n\n"
            "Institutional-grade Nifty & BankNifty alerts driven by live volatility models.\n\n"
            "• **1 Month:** ₹4,999\n"
            "• **3 Months:** ₹12,999 (Save 13%)\n\n"
            "Tap below to instantly activate via Cosmofeed:"
        )
        # Link buttons directly to your checkout
        pay_keyboard = [
            [InlineKeyboardButton("💳 Subscribe Monthly", url='https://cosmofeed.com/your_monthly_link')],
            [InlineKeyboardButton("💳 Subscribe Quarterly", url='https://cosmofeed.com/your_quarterly_link')],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data='main_menu')]
        ]
        await query.edit_message_text(text=plans_text, reply_markup=InlineKeyboardMarkup(pay_keyboard), parse_mode='Markdown')

    elif query.data == 'view_results':
        results_text = (
            "📊 **PERFORMANCE METRICS**\n\n"
            "Our engine dynamically adjusts to the India VIX. When the market chops, we protect capital. "
            "When volatility spikes, we execute.\n\n"
            "Check our free channel history for transparent, time-stamped proof of execution."
        )
        back_keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        await query.edit_message_text(text=results_text, reply_markup=InlineKeyboardMarkup(back_keyboard))

    elif query.data == 'contact_support':
        support_text = "An administrator will be with you shortly. Please leave your message here."
        back_keyboard = [[InlineKeyboardButton("🔙 Cancel", callback_data='main_menu')]]
        await query.edit_message_text(text=support_text, reply_markup=InlineKeyboardMarkup(back_keyboard))

    elif query.data == 'main_menu':
        # Re-draw the main menu
        keyboard = [
            [InlineKeyboardButton("💎 View VIP Plans", callback_data='view_plans')],
            [InlineKeyboardButton("📊 Verify Past Performance", callback_data='view_results')],
            [InlineKeyboardButton("❓ Contact Human Support", callback_data='contact_support')]
        ]
        await query.edit_message_text(
            text="Main Menu. How can I help you?", 
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

if __name__ == '__main__':
    if not BOT_TOKEN:
        print("❌ ERROR: TELEGRAM_ONBOARDING_BOT_TOKEN is missing from .env file.")
        exit(1)

    print("🤖 Launching Black Blood Community Manager Bot...")
    
    # Build the application
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(CallbackQueryHandler(button_handler))

    # Start polling Telegram for messages
    print("✅ Bot is online and listening for /start commands.")
    app.run_polling()