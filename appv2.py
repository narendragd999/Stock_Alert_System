import streamlit as st
import sqlite3
import requests
import pandas as pd
import asyncio
import telegram
import time
import schedule
import threading
import uuid
from datetime import datetime
import pytz

# Set timezone to IST
ist = pytz.timezone('Asia/Kolkata')

# Database setup
def init_db():
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
                 (id TEXT PRIMARY KEY, symbol TEXT, alert_price REAL, target_price REAL, 
                  strategy TEXT, enabled INTEGER, last_notified_alert REAL, last_notified_target REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
# TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "your-telegram-bot-token")
# CHAT_ID = st.secrets.get("CHAT_ID", "your-chat-id")
# bot = telegram.Bot(token=TELEGRAM_TOKEN)
TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
CHAT_ID = st.secrets["CHAT_ID"]
bot = telegram.Bot(token=TELEGRAM_TOKEN)

# NSE headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/equity-derivatives-watch",
}

# Initialize NSE session
nse_session = None
def initialize_nse_session():
    global nse_session
    if nse_session is None:
        nse_session = requests.Session()
        try:
            response = nse_session.get("https://www.nseindia.com/", headers=headers)
            if response.status_code != 200:
                st.warning(f"Failed to load NSE homepage: {response.status_code}")
                return False
            time.sleep(2)
            response = nse_session.get("https://www.nseindia.com/market-data/equity-derivatives-watch", headers=headers)
            time.sleep(2)
            if response.status_code != 200:
                st.warning(f"Failed to load NSE derivatives page: {response.status_code}")
                return False
        except Exception as e:
            st.error(f"Error initializing NSE session: {e}")
            return False
    return True

# Function to fetch current price from NSE
def get_current_price_nse(ticker):
    global nse_session
    try:
        # Strip .NS suffix for NSE API
        ticker = ticker.upper().replace(".NS", "")
        if nse_session is None and not initialize_nse_session():
            return None
        quote_url = f"https://www.nseindia.com/api/quote-equity?symbol={ticker}"
        response = nse_session.get(quote_url, headers=headers)
        if response.status_code == 200:
            quote_data = response.json()
            last_price = quote_data.get('priceInfo', {}).get('lastPrice', 0)
            if last_price > 0:
                return last_price
            else:
                st.warning(f"No valid price data for {ticker}")
                return None
        else:
            st.warning(f"Failed to fetch price for {ticker}: HTTP {response.status_code}")
            return None
    except Exception as e:
        st.warning(f"Error fetching NSE price for {ticker}: {e}")
        return None

# Streamlit app
st.title("Stock Alert System")

# Sidebar for configuration
st.sidebar.header("Configuration")
check_interval = st.sidebar.slider("Price Check Interval (minutes)", 1, 60, 5)

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

# Load strategies
conn = sqlite3.connect('stock_alerts.db')
c = conn.cursor()
c.execute("SELECT name FROM strategies")
strategies = [row[0] for row in c.fetchall()]
conn.close()
if not strategies:
    strategies = ["Buy", "Sell", "Hold"]

# Stock input form
st.subheader("Add New Stock Alert")
with st.form(key="add_stock_form"):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        symbol = st.text_input("Stock Symbol", placeholder="SRF.NS")
    with col2:
        alert_price = st.number_input("Alert Price", min_value=0.0, step=0.01)
    with col3:
        target_price = st.number_input("Target Price", min_value=0.0, step=0.01)
    with col4:
        strategy = st.selectbox("Strategy", strategies)
    submit_button = st.form_submit_button("Add Stock")

    if submit_button and symbol:
        # Validate symbol by checking if price data is available
        price = get_current_price_nse(symbol)
        if price is not None:
            conn = sqlite3.connect('stock_alerts.db')
            c = conn.cursor()
            c.execute("INSERT INTO stocks (id, symbol, alert_price, target_price, strategy, enabled, last_notified_alert, last_notified_target) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                     (str(uuid.uuid4()), symbol.upper(), alert_price, target_price, strategy, 1, 0, 0))
            conn.commit()
            conn.close()
            st.success(f"Added {symbol.upper()} to alerts!")
        else:
            st.error(f"Invalid symbol {symbol.upper()}: No price data available")

# Display and manage stocks
st.subheader("Current Stock Alerts")
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
            with col4:
                st.write(f"Enabled: {'Yes' if row['enabled'] else 'No'}")

            if st.session_state.get(f"edit_mode_{row['id']}", False):
                with st.form(key=f"edit_form_{row['id']}"):
                    ecol1, ecol2, ecol3 = st.columns(3)
                    with ecol1:
                        new_alert_price = st.number_input("New Alert Price", value=float(row['alert_price']), key=f"alert_{row['id']}")
                    with ecol2:
                        new_target_price = st.number_input("New Target Price", value=float(row['target_price']), key=f"target_{row['id']}")
                    with ecol3:
                        new_strategy = st.selectbox("New Strategy", strategies, index=strategies.index(row['strategy']), key=f"strat_{row['id']}")
                    if st.form_submit_button("Save Changes"):
                        conn = sqlite3.connect('stock_alerts.db')
                        c = conn.cursor()
                        c.execute("UPDATE stocks SET alert_price = ?, target_price = ?, strategy = ? WHERE id = ?",
                                 (new_alert_price, new_target_price, new_strategy, row['id']))
                        conn.commit()
                        conn.close()
                        st.session_state[f"edit_mode_{row['id']}"] = False
                        st.experimental_rerun()

            st.write(f"Alert Price: ₹{row['alert_price']:.2f}")
            st.write(f"Target Price: ₹{row['target_price']:.2f}")
            current_price = get_current_price_nse(row['symbol'])
            if current_price:
                st.write(f"Current Price: ₹{current_price:.2f}")
            else:
                st.write("Current Price: Unavailable")

# Price checking and notification logic
async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        st.error(f"Failed to send Telegram message: {e}")

def check_prices():
    conn = sqlite3.connect('stock_alerts.db')
    df = pd.read_sql_query("SELECT * FROM stocks WHERE enabled = 1", conn)
    conn.close()

    for _, row in df.iterrows():
        try:
            current_price = get_current_price_nse(row['symbol'])
            if current_price is None:
                continue
            current_time = time.time()

            # Check alert price
            if (row['alert_price'] > 0 and 
                ((current_price <= row['alert_price'] and current_price < row['last_notified_alert']) or 
                 (current_price >= row['alert_price'] and current_price > row['last_notified_alert']))):
                message = f"🚨 Alert: {row['symbol']} hit alert price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                asyncio.run(send_telegram_message(message))
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("UPDATE stocks SET last_notified_alert = ? WHERE id = ?", (current_price, row['id']))
                conn.commit()
                conn.close()

            # Check target price
            if (row['target_price'] > 0 and 
                ((current_price <= row['target_price'] and current_price < row['last_notified_target']) or 
                 (current_price >= row['target_price'] and current_price > row['last_notified_target']))):
                message = f"🎯 Target: {row['symbol']} hit target price ₹{row['target_price']:.2f}! Current: ₹{current_price:.2f}"
                asyncio.run(send_telegram_message(message))
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("UPDATE stocks SET last_notified_target = ? WHERE id = ?", (current_price, row['id']))
                conn.commit()
                conn.close()

        except Exception as e:
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