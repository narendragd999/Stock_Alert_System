import sqlite3
import pandas as pd
import yfinance as yf
import asyncio
import telegram
import time
import schedule
import threading
import uuid
from datetime import datetime
import pytz
import logging
import json

# Configure logging
logging.basicConfig(
    filename='stock_alerts.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set timezone to IST
ist = pytz.timezone('Asia/Kolkata')

# Database setup
def init_db():
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
                 (id TEXT PRIMARY KEY, 
                  symbol TEXT, 
                  initial_price REAL, 
                  alert_price REAL, 
                  target_price REAL, 
                  strategy TEXT, 
                  enabled INTEGER, 
                  created_at INTEGER, 
                  alert_triggered INTEGER DEFAULT 0, 
                  last_notified_alert INTEGER DEFAULT 0, 
                  last_notified_target INTEGER DEFAULT 0,
                  notification_cooldown INTEGER DEFAULT 600)''')
    
    c.execute("PRAGMA table_info(stocks)")
    columns = [info[1] for info in c.fetchall()]
    
    for column, column_type, default in [
        ('initial_price', 'REAL', '0'),
        ('created_at', 'INTEGER', '0'),
        ('alert_triggered', 'INTEGER', '0'),
        ('last_notified_alert', 'INTEGER', '0'),
        ('last_notified_target', 'INTEGER', '0'),
        ('notification_cooldown', 'INTEGER', '600')
    ]:
        if column not in columns:
            c.execute(f"ALTER TABLE stocks ADD COLUMN {column} {column_type} DEFAULT {default}")
    
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_strategy ON stocks (symbol, strategy)''')
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
TELEGRAM_TOKEN = "YOUR_TELEGRAM_TOKEN"  # Replace with your actual Telegram bot token
CHAT_ID = "YOUR_CHAT_ID"  # Replace with your actual Telegram chat ID
bot = telegram.Bot(token=TELEGRAM_TOKEN)

# Price checking and notification logic
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Sent Telegram message: {message}")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")

def get_stock_data(symbol):
    try:
        ticker = symbol.upper() if symbol.endswith('.NS') else f"{symbol.upper()}.NS"
        stock = yf.Ticker(ticker)
        
        info = stock.info
        current_price = info.get('regularMarketPrice', None)
        if current_price is None:
            logging.warning(f"No valid current price for {symbol}. Response: {info}")
            return None, None
        
        hist = stock.history(period="1y2mo", interval="1d")
        if hist.empty:
            logging.warning(f"No historical data for {symbol}")
            return current_price, None
        
        v20_range = None
        for i in range(len(hist) - 1, -1, -1):
            current_candle = hist.iloc[i]
            if current_candle['Close'] > current_candle['Open']:
                for j in range(i - 1, -1, -1):
                    prev_candle = hist.iloc[j]
                    gain_percent = ((current_candle['High'] - prev_candle['Low']) / prev_candle['Low']) * 100
                    if gain_percent >= 20:
                        momentum_broken = False
                        for k in range(j + 1, i + 1):
                            if hist.iloc[k]['Close'] < hist.iloc[k]['Open']:
                                momentum_broken = True
                                break
                        if not momentum_broken:
                            v20_range = (prev_candle['Low'], current_candle['High'])
                            break
                if v20_range:
                    break
        
        if not v20_range:
            logging.warning(f"No V20 range found for {symbol}")
            return current_price, None
        
        logging.info(f"Fetched data for {symbol}: Current price ₹{current_price:.2f}, V20 range low ₹{v20_range[0]:.2f}, high ₹{v20_range[1]:.2f}")
        return current_price, v20_range
    
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for {symbol}: {e}")
        return None, None
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None, None

def add_stock(symbol, strategy="V20"):
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
    existing_stock = c.fetchone()
    
    if existing_stock:
        logging.warning(f"Attempted to add duplicate stock {symbol.upper()} with strategy {strategy}")
        conn.close()
        return
    
    current_price, v20_range = get_stock_data(symbol)
    if current_price is not None and v20_range:
        alert_price, target_price = v20_range
        created_at = int(time.time())
        c.execute("""INSERT INTO stocks 
                     (id, symbol, initial_price, alert_price, target_price, strategy, enabled, created_at, 
                     alert_triggered, last_notified_alert, last_notified_target, notification_cooldown) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                 (str(uuid.uuid4()), symbol.upper(), current_price, alert_price, target_price, strategy, 1, 
                  created_at, 0, 0, 0, 600))
        conn.commit()
        logging.info(f"Added stock {symbol.upper()} with initial price ₹{current_price:.2f}, alert price ₹{alert_price:.2f}, target price ₹{target_price:.2f}, strategy {strategy}")
    else:
        logging.error(f"Failed to add stock {symbol.upper()}: No V20 range data")
    conn.close()

def check_prices():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)
    conn.close()

    current_time = time.time()

    for _, row in df.iterrows():
        try:
            current_price, _ = get_stock_data(row['symbol'])
            if current_price is None:
                continue

            if current_time - row['created_at'] < 60 and row['alert_triggered'] == 0:
                if abs(current_price - row['alert_price']) / row['alert_price'] <= 0.01:
                    logging.info(f"Skipping initial alert notification for {row['symbol']} as price is already at/beyond V20 alert price")
                    continue

            if row['alert_price'] > 0 and row['alert_triggered'] == 0:
                last_alert_time = row['last_notified_alert'] if row['last_notified_alert'] > 0 else row['created_at']
                if current_time - last_alert_time >= row['notification_cooldown'] and current_price <= row['alert_price'] + 0.01:
                    message = f"🚨 V20 Buy Alert: {row['symbol']} hit buy price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET alert_triggered = 1, last_notified_alert = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Buy alert triggered for {row['symbol']} at ₹{current_price:.2f}")

            if row['target_price'] > 0 and row['alert_triggered'] == 1:
                last_target_time = row['last_notified_target'] if row['last_notified_target'] > 0 else row['created_at']
                if current_time - last_target_time >= row['notification_cooldown'] and current_price >= row['target_price'] - 0.01:
                    message = f"🎯 V20 Sell Alert: {row['symbol']} hit target price ₹{row['target_price']:.2f}! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_target = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Sell alert triggered for {row['symbol']} at ₹{current_price:.2f}")

        except Exception as e:
            logging.error(f"Error checking {row['symbol']}: {e}")

# Schedule price checks
def run_scheduler():
    schedule.every(5).minutes.do(check_prices)  # Check every 5 minutes (adjust as needed)
    while True:
        schedule.run_pending()
        time.sleep(60)

# Start scheduler in background thread
if __name__ == "__main__":
    # Example: Add stocks programmatically (optional)
    # add_stock("BAJAJHFL.NS")
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logging.info("Stock alert system started in background")
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stock alert system stopped by user")
        scheduler_thread.join()