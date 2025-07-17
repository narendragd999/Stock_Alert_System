import streamlit as st
import sqlite3
import yfinance as yf
import pandas as pd
import asyncio
import telegram
import time
import schedule
import threading
import uuid

# Database setup
def init_db():
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
                 (id TEXT PRIMARY KEY, symbol TEXT, alert_price REAL, target_price REAL, 
                  strategy TEXT, enabled INTEGER, last_notified_alert REAL, last_notified_target REAL,
                  last_notified_alert_range INTEGER, last_notified_target_range INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
TELEGRAM_TOKEN = st.secrets["TELEGRAM_TOKEN"]
CHAT_ID = st.secrets["CHAT_ID"]
bot = telegram.Bot(token=TELEGRAM_TOKEN)

# Streamlit app
st.title("Stock Alert System")

# Sidebar for configuration
st.sidebar.header("Configuration")
check_interval = st.sidebar.slider("Price Check Interval (minutes)", 1, 60, 5)
if "alert_range_percent" not in st.session_state:
    st.session_state.alert_range_percent = 3.0
alert_range_percent = st.sidebar.slider(
    "Alert Range Percentage (%)", 
    min_value=0.1, 
    max_value=10.0, 
    value=st.session_state.alert_range_percent, 
    step=0.1,
    help="Set the percentage range for alert and target price notifications"
)
st.session_state.alert_range_percent = alert_range_percent

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

# Stock input form with ticker validation
st.subheader("Add New Stock Alert")
with st.form(key="add_stock_form"):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        symbol = st.text_input("Stock Symbol", placeholder="AAPL or AAPL.NS")
    with col2:
        alert_price = st.number_input("Alert Price", min_value=0.0, step=0.01)
    with col3:
        target_price = st.number_input("Target Price", min_value=0.0, step=0.01)
    with col4:
        strategy = st.selectbox("Strategy", strategies)
    submit_button = st.form_submit_button("Add Stock")

    if submit_button and symbol:
        try:
            stock = yf.Ticker(symbol.upper())
            info = stock.info
            if not info or 'symbol' not in info:
                st.error(f"Invalid ticker symbol: {symbol.upper()}. Please enter a valid ticker (e.g., AAPL, UPL.NS).")
            else:
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("INSERT INTO stocks (id, symbol, alert_price, target_price, strategy, enabled, last_notified_alert, last_notified_target, last_notified_alert_range, last_notified_target_range) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (str(uuid.uuid4()), symbol.upper(), alert_price, target_price, strategy, 1, 0, 0, 0, 0))
                conn.commit()
                conn.close()
                st.success(f"Added {symbol.upper()} to alerts!")
        except Exception as e:
            st.error(f"Error validating ticker {symbol.upper()}: {e}")

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
                    st.rerun()
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
                    st.rerun()
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
                        st.rerun()

            st.write(f"Alert Price: ${row['alert_price']:.2f}")
            st.write(f"Target Price: ${row['target_price']:.2f}")
            try:
                stock = yf.Ticker(row['symbol'])
                info = stock.info
                if not info or 'symbol' not in info:
                    st.write(f"Current Price: Unavailable (Invalid ticker: {row['symbol']})")
                else:
                    current_price = stock.history(period="1d")['Close'].iloc[-1]
                    st.write(f"Current Price: ${current_price:.2f}")
            except:
                st.write(f"Current Price: Unavailable (Error fetching data for {row['symbol']})")

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
            stock = yf.Ticker(row['symbol'])
            info = stock.info
            if not info or 'symbol' not in info:
                st.error(f"Invalid or delisted ticker: {row['symbol']}")
                continue

            current_price = stock.history(period="1d")['Close'].iloc[-1]
            current_time = time.time()

            # Check alert price
            if (row['alert_price'] > 0 and 
                ((current_price <= row['alert_price'] and current_price < row['last_notified_alert']) or 
                 (current_price >= row['alert_price'] and current_price > row['last_notified_alert']))):
                message = f"🚨 Alert: {row['symbol']} hit alert price ${row['alert_price']:.2f}! Current: ${current_price:.2f}"
                asyncio.run(send_telegram_message(message))
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("UPDATE stocks SET last_notified_alert = ?, last_notified_alert_range = 0 WHERE id = ?", 
                         (current_price, row['id']))
                conn.commit()
                conn.close()

            # Check target price
            if (row['target_price'] > 0 and 
                ((current_price <= row['target_price'] and current_price < row['last_notified_target']) or 
                 (current_price >= row['target_price'] and current_price > row['last_notified_target']))):
                message = f"🎯 Target: {row['symbol']} hit target price ${row['target_price']:.2f}! Current: ${current_price:.2f}"
                asyncio.run(send_telegram_message(message))
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("UPDATE stocks SET last_notified_target = ?, last_notified_target_range = 0 WHERE id = ?", 
                         (current_price, row['id']))
                conn.commit()
                conn.close()

            # Check configurable range for alert price
            if row['alert_price'] > 0 and row['last_notified_alert_range'] == 0:
                range_percent = st.session_state.alert_range_percent / 100
                alert_lower = row['alert_price'] * (1 - range_percent)
                alert_upper = row['alert_price'] * (1 + range_percent)
                if alert_lower <= current_price <= alert_upper:
                    message = f"⚠️ Be Alert: {row['symbol']} is within {st.session_state.alert_range_percent}% of alert price ${row['alert_price']:.2f}! Current: ${current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_alert_range = 1 WHERE id = ?", (row['id'],))
                    conn.commit()
                    conn.close()

            # Check configurable range for target price
            if row['target_price'] > 0 and row['last_notified_target_range'] == 0:
                range_percent = st.session_state.alert_range_percent / 100
                target_lower = row['target_price'] * (1 - range_percent)
                target_upper = row['target_price'] * (1 + range_percent)
                if target_lower <= current_price <= target_upper:
                    message = f"⚠️ Be Alert: {row['symbol']} is within {st.session_state.alert_range_percent}% of target price ${row['target_price']:.2f}! Current: ${current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_target_range = 1 WHERE id = ?", (row['id'],))
                    conn.commit()
                    conn.close()

        except Exception as e:
            st.error(f"Error checking {row['symbol']}: {e}")
            continue

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