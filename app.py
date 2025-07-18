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
from dateutil.parser import parse
import math

# Set timezone to IST
ist = pytz.timezone('Asia/Kolkata')

# Database setup
def init_db():
    conn = sqlite3.connect('stock_alerts.db')
    c = conn.cursor()
    
    # Create stocks table with unique constraint on id and added_time column
    c.execute('''CREATE TABLE IF NOT EXISTS stocks
                 (id TEXT PRIMARY KEY, 
                  symbol TEXT, 
                  alert_price REAL, 
                  target_price REAL, 
                  strategy TEXT, 
                  enabled INTEGER, 
                  last_notified_alert REAL, 
                  last_notified_target REAL,
                  last_notified_pre_alert REAL, 
                  last_notified_pre_target REAL,
                  alert_trigger_time TEXT,
                  target_trigger_time TEXT,
                  status TEXT,
                  added_time TEXT)''')
    
    # Check if the new columns exist and add them if they don't
    c.execute("PRAGMA table_info(stocks)")
    columns = [info[1] for info in c.fetchall()]
    
    if 'alert_trigger_time' not in columns:
        c.execute("ALTER TABLE stocks ADD COLUMN alert_trigger_time TEXT")
    
    if 'target_trigger_time' not in columns:
        c.execute("ALTER TABLE stocks ADD COLUMN target_trigger_time TEXT")
    
    if 'status' not in columns:
        c.execute("ALTER TABLE stocks ADD COLUMN status TEXT DEFAULT 'Open'")
    
    if 'added_time' not in columns:
        c.execute("ALTER TABLE stocks ADD COLUMN added_time TEXT")
    
    # Create strategies table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS strategies
                 (id TEXT PRIMARY KEY, name TEXT)''')
    
    # Ensure no duplicate IDs in stocks table
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_stocks_id ON stocks(id)")
    
    conn.commit()
    conn.close()

# Initialize database
init_db()

# Telegram bot setup
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
    if not isinstance(ticker, str) or pd.isna(ticker):
        return None
    try:
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
st.set_page_config(page_title="Stock Alert System", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; padding: 20px; }
    .stButton>button { background-color: #4CAF50; color: white; border-radius: 5px; }
    .stButton>button:hover { background-color: #45a049; }
    .stExpander { background-color: white; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .css-1d391kg { background-color: #ffffff; }
    .search-bar { padding: 10px; margin-bottom: 20px; }
    .status-open { background-color: #e6f3ff; }
    .status-closed { background-color: #ffe6e6; }
    </style>
""", unsafe_allow_html=True)

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
        if not isinstance(symbol, str) or not symbol.strip():
            st.error("Please enter a valid stock symbol")
        else:
            price = get_current_price_nse(symbol)
            if price is not None:
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                added_time = datetime.now(ist).isoformat()
                c.execute("INSERT INTO stocks (id, symbol, alert_price, target_price, strategy, enabled, last_notified_alert, last_notified_target, last_notified_pre_alert, last_notified_pre_target, alert_trigger_time, target_trigger_time, status, added_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (str(uuid.uuid4()), symbol.upper(), alert_price, target_price, strategy, 1, 0, 0, 0, 0, None, None, 'Open', added_time))
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

# Check if DataFrame is empty
if df.empty:
    st.info("No stock alerts found. Add a stock to start tracking.")
else:
    # Remove any rows with invalid symbols
    df = df[df['symbol'].notna() & df['symbol'].str.strip().astype(bool)]

    # Calculate additional metrics for display
    def calculate_metrics(row):
        if not isinstance(row['symbol'], str) or pd.isna(row['symbol']) or not row['symbol'].strip():
            return pd.Series({
                'Current Price': None,
                'Target %': None,
                'Remaining Target %': None,
                'Duration (Days)': None,
                'Duration (Date)': None
            })
        current_price = get_current_price_nse(row['symbol'])
        target_percentage = abs(row['target_price'] - row['alert_price']) / row['alert_price'] * 100 if row['alert_price'] > 0 else 0
        remaining_target_percentage = abs(row['target_price'] - current_price) / row['alert_price'] * 100 if row['alert_price'] > 0 and row['status'] == 'Open' and current_price is not None else 0
        duration_days = duration_date = None
        if row['alert_trigger_time'] and row['target_trigger_time']:
            try:
                alert_time = parse(row['alert_trigger_time'])
                target_time = parse(row['target_trigger_time'])
                duration = target_time - alert_time
                duration_days = duration.days + duration.seconds / (24 * 3600)
                duration_date = f"{duration.days} days, {int(duration.seconds / 3600)} hours"
            except Exception:
                duration_days = duration_date = None
        return pd.Series({
            'Current Price': current_price,
            'Target %': target_percentage,
            'Remaining Target %': remaining_target_percentage,
            'Duration (Days)': duration_days,
            'Duration (Date)': duration_date
        })

    # Prepare data for display
    display_df = df.copy().reset_index(drop=True)  # Ensure unique index
    metrics = display_df.apply(calculate_metrics, axis=1)
    display_df = pd.concat([display_df, metrics], axis=1)

    # Filter and search
    st.markdown('<div class="search-bar">', unsafe_allow_html=True)
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("Search Stocks", placeholder="Enter symbol or strategy...")
    with col2:
        status_filter = st.selectbox("Filter by Status", ["All", "Open", "Closed"])
    st.markdown('</div>', unsafe_allow_html=True)

    # Apply filters
    filtered_df = display_df.copy().reset_index(drop=True)  # Ensure unique index
    if search_term:
        filtered_df = filtered_df[
            filtered_df['symbol'].str.contains(search_term, case=False, na=False) |
            filtered_df['strategy'].str.contains(search_term, case=False, na=False)
        ]
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df['status'] == status_filter]

    # Ensure all required columns exist
    required_columns = [
        'symbol', 'alert_price', 'target_price', 'strategy', 'Current Price',
        'Target %', 'Remaining Target %', 'alert_trigger_time', 'target_trigger_time',
        'Duration (Days)', 'Duration (Date)', 'status'
    ]
    for col in required_columns:
        if col not in filtered_df.columns:
            filtered_df[col] = None

    # Display table with sorting
    st.dataframe(
        filtered_df[required_columns].style.apply(
            lambda x: ['background-color: #e6f3ff' if x['status'] == 'Open' else 'background-color: #ffe6e6' for _ in x],
            axis=1
        ).format(
            {
                'alert_price': '₹{:.2f}',
                'target_price': '₹{:.2f}',
                'Current Price': lambda x: f'₹{x:.2f}' if pd.notna(x) else 'N/A',
                'Target %': '{:.2f}%',
                'Remaining Target %': '{:.2f}%',
                'Duration (Days)': lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A',
                'Duration (Date)': lambda x: x if pd.notna(x) else 'N/A',
                'alert_trigger_time': lambda x: x if pd.notna(x) else 'Not triggered',
                'target_trigger_time': lambda x: x if pd.notna(x) else 'Not triggered',
                'status': '{}'
            },
            na_rep='N/A'
        ),
        use_container_width=True,
        height=400
    )

    # Detailed view and management
    for index, row in filtered_df.iterrows():
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
                        new_strategy = st.selectbox("New Strategy", strategies, index=strategies.index(row['strategy']) if row['strategy'] in strategies else 0, key=f"strat_{row['id']}")
                    if st.form_submit_button("Save Changes"):
                        conn = sqlite3.connect('stock_alerts.db')
                        c = conn.cursor()
                        c.execute("UPDATE stocks SET alert_price = ?, target_price = ?, strategy = ? WHERE id = ?",
                                 (new_alert_price, new_target_price, new_strategy, row['id']))
                        conn.commit()
                        conn.close()
                        st.session_state[f"edit_mode_{row['id']}"] = False
                        st.rerun()

            st.write(f"Alert Price: ₹{row['alert_price']:.2f}")
            st.write(f"Target Price: ₹{row['target_price']:.2f}")
            st.write(f"Current Price: ₹{row['Current Price']:.2f}" if pd.notna(row['Current Price']) else "Current Price: Unavailable")
            st.write(f"Target %: {row['Target %']:.2f}%")
            if row['status'] == 'Open':
                st.write(f"Remaining Target %: {row['Remaining Target %']:.2f}%")
            st.write(f"Alert Trigger Time: {row['alert_trigger_time'] or 'Not triggered'}")
            st.write(f"Target Trigger Time: {row['target_trigger_time'] or 'Not triggered'}")
            if pd.notna(row['Duration (Date)']):
                st.write(f"Target Duration: {row['Duration (Date)']}")
            if pd.notna(row['Duration (Days)']):
                st.write(f"Target Duration (Days): {row['Duration (Days)']:.2f}")
            st.write(f"Added Time: {row['added_time'] or 'N/A'}")

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
        if not isinstance(row['symbol'], str) or pd.isna(row['symbol']) or not row['symbol'].strip():
            continue
        try:
            current_price = get_current_price_nse(row['symbol'])
            if current_price is None:
                continue
            current_time = datetime.now(ist)
            added_time = parse(row['added_time']) if row['added_time'] else current_time

            # Only trigger alerts if current time is after added_time
            if current_time <= added_time:
                continue

            # Check alert price
            if (row['alert_price'] > 0 and row['alert_trigger_time'] is None and
                ((current_price <= row['alert_price'] and current_price < row['last_notified_alert']) or 
                 (current_price >= row['alert_price'] and current_price > row['last_notified_alert']))):
                message = f"🚨 Alert: {row['symbol']} hit alert price ₹{row['alert_price']:.2f}! Current: ₹{current_price:.2f}"
                asyncio.run(send_telegram_message(message))
                conn = sqlite3.connect('stock_alerts.db')
                c = conn.cursor()
                c.execute("UPDATE stocks SET last_notified_alert = ?, alert_trigger_time = ? WHERE id = ?",
                         (current_price, current_time.isoformat(), row['id']))
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
                c.execute("UPDATE stocks SET last_notified_target = ?, target_trigger_time = ?, status = ? WHERE id = ?",
                         (current_price, current_time.isoformat(), 'Closed', row['id']))
                conn.commit()
                conn.close()

            # Check for pre-alert (within 3% of alert price)
            if row['alert_price'] > 0:
                alert_threshold = row['alert_price'] * 0.03  # 3% range
                pre_alert_lower = row['alert_price'] - alert_threshold
                pre_alert_upper = row['alert_price'] + alert_threshold
                if (pre_alert_lower <= current_price <= pre_alert_upper and 
                    current_price != row['last_notified_pre_alert']):
                    message = f"⚠️ Pre-Alert: {row['symbol']} is near alert price ₹{row['alert_price']:.2f} (within 3%)! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_pre_alert = ? WHERE id = ?", (current_price, row['id']))
                    conn.commit()
                    conn.close()

            # Check for pre-target (within 3% of target price)
            if row['target_price'] > 0:
                target_threshold = row['target_price'] * 0.03  # 3% range
                pre_target_lower = row['target_price'] - target_threshold
                pre_target_upper = row['target_price'] + target_threshold
                if (pre_target_lower <= current_price <= pre_target_upper and 
                    current_price != row['last_notified_pre_target']):
                    message = f"⚠️ Pre-Target: {row['symbol']} is near target price ₹{row['target_price']:.2f} (within 3%)! Current: ₹{current_price:.2f}"
                    asyncio.run(send_telegram_message(message))
                    conn = sqlite3.connect('stock_alerts.db')
                    c = conn.cursor()
                    c.execute("UPDATE stocks SET last_notified_pre_target = ? WHERE id = ?", (current_price, row['id']))
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