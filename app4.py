import streamlit as st
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
import io
import csv
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
    
    # Create stocks table
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
                  notification_cooldown INTEGER DEFAULT 3600)''')
    
    # Check for missing columns and add them
    c.execute("PRAGMA table_info(stocks)")
    columns = [info[1] for info in c.fetchall()]
    
    for column, column_type, default in [
        ('initial_price', 'REAL', '0'),
        ('created_at', 'INTEGER', '0'),
        ('alert_triggered', 'INTEGER', '0'),
        ('last_notified_alert', 'INTEGER', '0'),
        ('last_notified_target', 'INTEGER', '0'),
        ('notification_cooldown', 'INTEGER', '3600')
    ]:
        if column not in columns:
            c.execute(f"ALTER TABLE stocks ADD COLUMN {column} {column_type} DEFAULT {default}")
    
    # Create strategies table
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    
    # Create unique index to prevent duplicate stock-symbol-strategy combinations
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_strategy ON stocks (symbol, strategy)''')
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
CHAT_ID = st.secrets["CHAT_ID"]
bot = telegram.Bot(token=TELEGRAM_TOKEN)

# Function to fetch current price and candlestick data using yfinance
def get_stock_data(symbol):
    try:
        # Ensure correct ticker format for Indian stocks
        ticker = symbol.upper() if symbol.endswith('.NS') else f"{symbol.upper()}.NS"
        stock = yf.Ticker(ticker)
        
        # Fetch current price
        info = stock.info
        current_price = info.get('regularMarketPrice', None)
        if current_price is None:
            logging.warning(f"No valid current price for {symbol}. Response: {info}")
            return None, None
        
        # Fetch 20 days of daily candlestick data
        hist = stock.history(period="20d", interval="1d")
        if hist.empty:
            logging.warning(f"No historical data for {symbol}")
            return current_price, None
        
        # Find the most recent green candle (close > open)
        green_candles = hist[hist['Close'] > hist['Open']]
        if green_candles.empty:
            logging.warning(f"No green candles found for {symbol}")
            return current_price, None
        
        latest_green_candle = green_candles.iloc[-1]
        green_low = latest_green_candle['Low']
        green_high = latest_green_candle['High']
        
        logging.info(f"Fetched data for {symbol}: Current price ₹{current_price:.2f}, Green candle low ₹{green_low:.2f}, High ₹{green_high:.2f}")
        return current_price, (green_low, green_high)
    
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for {symbol}: {e}. Response may be empty or malformed.")
        st.warning(f"Failed to fetch data for {symbol}: Invalid or empty response from Yahoo Finance.")
        return None, None
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        st.warning(f"Error fetching data for {symbol}: {e}")
        return None, None

# Streamlit app
st.title("V20 Stock Alert System")

# Sidebar for configuration
st.sidebar.header("Configuration")
check_interval = st.sidebar.slider("Price Check Interval (minutes)", 1, 60, 5)
default_cooldown = st.sidebar.number_input("Notification Cooldown (seconds)", min_value=300, max_value=86400, value=3600)

# Strategy management
st.sidebar.subheader("Manage Strategies")
new_strategy = st.sidebar.text_input("Add New Strategy")
if st.sidebar.button("Add Strategy"):
    if new_strategy:
        conn = sqlite3.connect('stock_alerts.db')
        c = conn.cursor()
        c.execute("INSERT INTO strategies (id, name) VALUES (?, ?)", (str(uuid.uuid4()), new_strategy))
        conn.commit()
        conn.close()
        st.sidebar.success(f"Strategy '{new_strategy}' added!")
        logging.info(f"Added strategy: {new_strategy}")

# Load strategies
conn = sqlite3.connect('stock_alerts.db')
c = conn.cursor()
c.execute("SELECT name FROM strategies")
strategies = [row[0] for row in c.fetchall()]
conn.close()
if not strategies:
    strategies = ["V20"]

# Export stocks to CSV
def export_stocks_to_csv():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks", conn)
    conn.close()
    
    # Convert timestamps to human-readable format
    df['created_at'] = df['created_at'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    df['last_notified_alert'] = df['last_notified_alert'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    df['last_notified_target'] = df['last_notified_target'].apply(lambda x: datetime.fromtimestamp(x, tz=ist).strftime('%Y-%m-%d %H:%M:%S') if x > 0 else '')
    
    # Convert DataFrame to CSV
    output = io.StringIO()
    df.to_csv(output, index=False)
    return output.getvalue()

st.sidebar.subheader("Export Stocks")
if st.sidebar.button("Export All Stocks to CSV"):
    csv_data = export_stocks_to_csv()
    st.sidebar.download_button(
        label="Download Stocks CSV",
        data=csv_data,
        file_name=f"stock_alerts_{datetime.now(ist).strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    logging.info("Exported all stocks to CSV")

# Stock input form
st.subheader("Add New Stock Alert (V20 Strategy)")
with st.form(key="add_stock_form"):
    col1, col2, col3 = st.columns(3)
    with col1:
        symbol = st.text_input("Stock Symbol", placeholder="BAJAJHFL.NS")
    with col2:
        target_price = st.number_input("Target Price (V20 High)", min_value=0.0, step=0.01)
    with col3:
        strategy = st.selectbox("Strategy", strategies, index=strategies.index("V20") if "V20" in strategies else 0)
    submit_button = st.form_submit_button("Add Stock")

    if submit_button and symbol:
        # Check for duplicate stock-symbol-strategy combination
        conn = sqlite3.connect('stock_alerts.db')
        c = conn.cursor()
        c.execute("SELECT id FROM stocks WHERE symbol = ? AND strategy = ?", (symbol.upper(), strategy))
        existing_stock = c.fetchone()
        
        if existing_stock:
            st.error(f"Stock {symbol.upper()} with strategy '{strategy}' already exists!")
            logging.warning(f"Attempted to add duplicate stock {symbol.upper()} with strategy {strategy}")
            conn.close()
        else:
            current_price, green_candle_data = get_stock_data(symbol)
            if current_price is not None:
                # Calculate V20 range low price (20% gain from current price)
                alert_price = current_price * 1.20  # 20% gain
                if green_candle_data:
                    green_low, _ = green_candle_data
                    # Align alert price with green candle low if within 1%
                    if abs(alert_price - green_low) / alert_price <= 0.01:
                        alert_price = green_low
                
                created_at = int(time.time())
                try:
                    c.execute("""INSERT INTO stocks 
                                 (id, symbol, initial_price, alert_price, target_price, strategy, enabled, created_at, 
                                 alert_triggered, last_notified_alert, last_notified_target, notification_cooldown) 
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (str(uuid.uuid4()), symbol.upper(), current_price, alert_price, target_price, strategy, 1, 
                              created_at, 0, 0, 0, default_cooldown))
                    conn.commit()
                    st.success(f"Added {symbol.upper()} with V20 alert price ₹{alert_price:.2f} (20% gain) and target price ₹{target_price:.2f}")
                    logging.info(f"Added stock {symbol.upper()} with initial price ₹{current_price:.2f}, alert price ₹{alert_price:.2f}, target price ₹{target_price:.2f}, strategy {strategy}")
                except sqlite3.IntegrityError:
                    st.error(f"Stock {symbol.upper()} with strategy '{strategy}' already exists!")
                    logging.warning(f"IntegrityError: Attempted to add duplicate stock {symbol.upper()} with strategy {strategy}")
                conn.close()
            else:
                st.error(f"Invalid symbol {symbol.upper()}: No price data available. Ensure the symbol is correct (e.g., BAJAJHFL.NS).")
                logging.error(f"Failed to add stock {symbol.upper()}: No price data")
                conn.close()

# Display and manage stocks
st.subheader("Current Stock Alerts (V20 Strategy)")
conn = sqlite3.connect('stock_alerts.db')
df = pd.read_sql_query("SELECT * FROM stocks", conn)
conn.close()

if not df.empty:
    for index, row in df.iterrows():
        with st.expander(f"{row['symbol']} - {row['strategy']}"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                if st.button("Delete", key=f"delete_{row['id']}"):
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("DELETE FROM stocks WHERE id = ?", (row['id'],))
                    conn.commit()
                    conn.close()
                    st.experimental_rerun()
                    logging.info(f"Deleted stock {row['symbol']}")
            with col2:
                if st.button("Edit", key=f"edit_{row['id']}"):
                    st.session_state[f"edit_mode_{row['id']}"] = True
            with col3:
                if st.button("Disable/Enable", key=f"toggle_{row['id']}"):
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    new_status = 0 if row['enabled'] else 1
                    c.execute("UPDATE stocks SET enabled = ? WHERE id = ?", (new_status, row['id']))
                    conn.commit()
                    conn.close()
                    st.experimental_rerun()
                    logging.info(f"{'Enabled' if new_status else 'Disabled'} stock {row['symbol']}")
            with col4:
                st.write(f"Enabled: {'Yes' if row['enabled'] else 'No'}")

            if st.session_state.get(f"edit_mode_{row['id']}", False):
                with st.form(key=f"edit_form_{row['id']}"):
                    ecol1, ecol2, ecol3 = st.columns(3)
                    with ecol1:
                        new_alert_price = st.number_input("New Alert Price (V20 Low)", value=float(row['alert_price']), key=f"alert_{row['id']}")
                    with ecol2:
                        new_target_price = st.number_input("New Target Price (V20 High)", value=float(row['target_price']), key=f"target_{row['id']}")
                    with ecol3:
                        new_strategy = st.selectbox("New Strategy", strategies, index=strategies.index(row['strategy']), key=f"strat_{row['id']}")
                    if st.form_submit_button("Save Changes"):
                        conn = sqlite3.connect('stock_alerts.db')
                        c = conn.cursor()
                        try:
                            c.execute("UPDATE stocks SET alert_price = ?, target_price = ?, strategy = ? WHERE id = ?",
                                     (new_alert_price, new_target_price, new_strategy, row['id']))
                            conn.commit()
                            st.session_state[f"edit_mode_{row['id']}"] = False
                            st.experimental_rerun()
                            logging.info(f"Updated stock {row['symbol']} with new alert price ₹{new_alert_price:.2f}, target price ₹{new_target_price:.2f}, strategy {new_strategy}")
                        except sqlite3.IntegrityError:
                            st.error(f"Cannot update: Stock {row['symbol']} with strategy '{new_strategy}' already exists!")
                            logging.warning(f"IntegrityError: Attempted to update stock {row['symbol']} to duplicate strategy {new_strategy}")
                        conn.close()

            st.write(f"Initial Price: ₹{row['initial_price']:.2f}")
            st.write(f"V20 Alert Price (20% Gain): ₹{row['alert_price']:.2f}")
            st.write(f"Target Price (V20 High): ₹{row['target_price']:.2f}")
            st.write(f"Alert Triggered: {'Yes' if row['alert_triggered'] else 'No'}")
            created_at = datetime.fromtimestamp(row['created_at'], tz=ist).strftime('%Y-%m-%d %H:%M:%S') if row['created_at'] > 0 else 'N/A'
            st.write(f"Created At: {created_at}")
            current_price, green_candle_data = get_stock_data(row['symbol'])
            if current_price:
                st.write(f"Current Price: ₹{current_price:.2f}")
                if green_candle_data:
                    green_low, green_high = green_candle_data
                    st.write(f"Latest Green Candle Low: ₹{green_low:.2f}")
                    st.write(f"Latest Green Candle High: ₹{green_high:.2f}")
            else:
                st.write("Current Price: Unavailable")

# Price checking and notification logic
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Sent Telegram message: {message}")
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")
        st.error(f"Failed to send Telegram message: {e}")

def check_prices():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)
    conn.close()

    current_time = time.time()

    for _, row in df.iterrows():
        try:
            current_price, green_candle_data = get_stock_data(row['symbol'])
            if current_price is None or green_candle_data is None:
                continue
            green_low, green_high = green_candle_data

            # Skip notifications if stock was just added and price is already at/beyond alert price
            if current_time - row['created_at'] < 60 and row['alert_triggered'] == 0:
                if abs(current_price - row['alert_price']) / row['alert_price'] <= 0.01:
                    logging.info(f"Skipping initial alert notification for {row['symbol']} as price is already at/beyond V20 alert price")
                    continue

            # Check V20 alert price (20% gain aligned with green candle low)
            if row['alert_price'] > 0 and row['alert_triggered'] == 0:
                last_alert_time = row['last_notified_alert'] if row['last_notified_alert'] > 0 else row['created_at']
                if (current_time - last_alert_time >= row['notification_cooldown'] and
                    current_price >= row['alert_price'] and
                    abs(current_price - green_low) / green_low <= 0.01):  # Within 1% of green candle low
                    message = f"🚨 V20 Alert: {row['symbol']} hit alert price ₹{row['alert_price']:.2f} (20% gain, green candle low)! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET alert_triggered = 1, last_notified_alert = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Alert triggered for {row['symbol']} at ₹{current_price:.2f}")

            # Check target price (V20 high aligned with green candle high)
            if row['target_price'] > 0 and row['alert_triggered'] == 1:
                last_target_time = row['last_notified_target'] if row['last_notified_target'] > 0 else row['created_at']
                if (current_time - last_target_time >= row['notification_cooldown'] and
                    current_price >= row['target_price'] and
                    abs(current_price - green_high) / green_high <= 0.01):  # Within 1% of green candle high
                    message = f"🎯 V20 Target: {row['symbol']} hit target price ₹{row['target_price']:.2f} (green candle high)! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_target = ? WHERE id = ?", (current_time, row['id']))
                    conn.commit()
                    conn.close()
                    logging.info(f"Target triggered for {row['symbol']} at ₹{current_price:.2f}")

        except Exception as e:
            logging.error(f"Error checking {row['symbol']}: {e}")
            st.error(f"Error checking {row['symbol']}: {e}")

# Schedule price checks
def run_scheduler():
    schedule.every(check_interval).minutes.do(check_prices)
    while True:
        schedule.run_pending()
        time.sleep(60)

# Start scheduler in background thread
if 'scheduler_thread' not in st.session_state:
    st.session_state.scheduler_thread = True
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()