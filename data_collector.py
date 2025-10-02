"""
Background Data Collector - Runs 24/7 to collect and store klines + liquidations
Run this separately: python data_collector.py
"""

import sqlite3
import json
import time
from datetime import datetime, timezone
from websocket import WebSocketApp
import requests
import threading

DB_PATH = "btc_data.db"

def init_database():
    """Initialize database with proper schema"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Klines table - stores every 1-minute candle
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS klines (
            timestamp INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Liquidations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS liquidations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            amount REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp, side, amount)
        )
    """)
    
    # Collector state table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collector_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_kline_timestamp INTEGER,
            last_liquidation_timestamp INTEGER,
            total_klines_collected INTEGER DEFAULT 0,
            total_liquidations_collected INTEGER DEFAULT 0,
            is_running INTEGER DEFAULT 0,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("INSERT OR IGNORE INTO collector_state (id, is_running) VALUES (1, 0)")
    conn.commit()
    conn.close()

def save_kline(timestamp, symbol, open_price, high, low, close, volume):
    """Save single kline to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO klines (timestamp, symbol, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (timestamp, symbol, open_price, high, low, close, volume))
        
        cursor.execute("""
            UPDATE collector_state 
            SET last_kline_timestamp = ?,
                total_klines_collected = total_klines_collected + 1,
                last_update = CURRENT_TIMESTAMP
            WHERE id = 1
        """, (timestamp,))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving kline: {e}")
        return False

def save_liquidation(symbol, side, price, quantity, amount, timestamp):
    """Save liquidation to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO liquidations 
            (symbol, side, price, quantity, amount, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (symbol, side, price, quantity, amount, timestamp))
        
        if cursor.rowcount > 0:
            cursor.execute("""
                UPDATE collector_state 
                SET last_liquidation_timestamp = ?,
                    total_liquidations_collected = total_liquidations_collected + 1,
                    last_update = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (timestamp,))
        
        conn.commit()
        conn.close()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Error saving liquidation: {e}")
        return False

def update_collector_status(is_running):
    """Update collector running status"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE collector_state 
            SET is_running = ?, last_update = CURRENT_TIMESTAMP
            WHERE id = 1
        """, (1 if is_running else 0,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error updating status: {e}")

def fetch_historical_klines(symbol="BTCUSDT", interval="1m", limit=1500):
    """Fetch and store historical klines from API"""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    
    try:
        print(f"Fetching {limit} historical candles...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        saved_count = 0
        for candle in data:
            timestamp = candle[0]
            if save_kline(
                timestamp=timestamp,
                symbol=symbol,
                open_price=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=float(candle[5])
            ):
                saved_count += 1
        
        print(f"✅ Saved {saved_count}/{len(data)} historical candles to database")
        return True
    except Exception as e:
        print(f"❌ Error fetching historical klines: {e}")
        return False

# WebSocket handlers for klines
def on_kline_message(ws, message):
    """Handle kline WebSocket messages"""
    try:
        data = json.loads(message)
        if 'k' in data:
            kline = data['k']
            # Only save completed candles
            if kline['x']:  # x = is candle closed
                timestamp = kline['t']
                symbol = kline['s']
                
                if save_kline(
                    timestamp=timestamp,
                    symbol=symbol,
                    open_price=float(kline['o']),
                    high=float(kline['h']),
                    low=float(kline['l']),
                    close=float(kline['c']),
                    volume=float(kline['v'])
                ):
                    dt = datetime.fromtimestamp(timestamp/1000)
                    print(f"📊 Saved candle: {symbol} @ {dt.strftime('%H:%M:%S')} - Close: ${float(kline['c']):,.2f}")
    except Exception as e:
        print(f"Error processing kline: {e}")

def on_kline_error(ws, error):
    print(f"❌ Kline WebSocket error: {error}")
    update_collector_status(False)

def on_kline_close(ws, close_status_code, close_msg):
    print(f"⚠️ Kline WebSocket closed: {close_status_code} - {close_msg}")
    update_collector_status(False)

def on_kline_open(ws):
    print(f"✅ Kline WebSocket connected at {datetime.now().strftime('%H:%M:%S')}")
    update_collector_status(True)

# WebSocket handlers for liquidations
def on_liq_message(ws, message):
    """Handle liquidation WebSocket messages"""
    try:
        data = json.loads(message)
        if 'o' in data:
            symbol = data['o']['s']
            side = data['o']['S']
            price = float(data['o']['p'])
            quantity = float(data['o']['q'])
            amount = price * quantity
            timestamp = data['o']['T']
            
            if symbol == 'BTCUSDT' and save_liquidation(symbol, side, price, quantity, amount, timestamp):
                dt = datetime.fromtimestamp(timestamp/1000)
                print(f"💥 Saved liquidation: {side} ${amount:,.0f} @ ${price:,.2f} - {dt.strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"Error processing liquidation: {e}")

def on_liq_error(ws, error):
    print(f"❌ Liquidation WebSocket error: {error}")

def on_liq_close(ws, close_status_code, close_msg):
    print(f"⚠️ Liquidation WebSocket closed: {close_status_code} - {close_msg}")

def on_liq_open(ws):
    print(f"✅ Liquidation WebSocket connected at {datetime.now().strftime('%H:%M:%S')}")

def run_kline_websocket():
    """Run kline WebSocket in loop"""
    while True:
        try:
            ws = WebSocketApp(
                "wss://fstream.binance.com/ws/btcusdt@kline_1m",
                on_message=on_kline_message,
                on_error=on_kline_error,
                on_close=on_kline_close,
                on_open=on_kline_open
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(5)
        except Exception as e:
            print(f"Kline WebSocket crashed: {e}, restarting...")
            time.sleep(5)

def run_liquidation_websocket():
    """Run liquidation WebSocket in loop"""
    while True:
        try:
            ws = WebSocketApp(
                "wss://fstream.binance.com/ws/btcusdt@forceOrder",
                on_message=on_liq_message,
                on_error=on_liq_error,
                on_close=on_liq_close,
                on_open=on_liq_open
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(5)
        except Exception as e:
            print(f"Liquidation WebSocket crashed: {e}, restarting...")
            time.sleep(5)

def main():
    print("=" * 60)
    print("BTC/USDT Data Collector - Background Service")
    print("=" * 60)
    
    # Initialize database
    print("Initializing database...")
    init_database()
    
    # Fetch historical data first
    print("\nFetching historical klines from Binance API...")
    fetch_historical_klines(limit=1500)
    
    print("\n" + "=" * 60)
    print("Starting WebSocket streams...")
    print("=" * 60)
    
    # Start WebSocket threads
    kline_thread = threading.Thread(target=run_kline_websocket, daemon=True, name="KlineCollector")
    liq_thread = threading.Thread(target=run_liquidation_websocket, daemon=True, name="LiquidationCollector")
    
    kline_thread.start()
    liq_thread.start()
    
    print("\n✅ Data collector is running!")
    print("📊 Collecting klines (1-minute candles)")
    print("💥 Collecting liquidations")
    print("\nPress Ctrl+C to stop...\n")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(60)
            # Print status every minute
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT total_klines_collected, total_liquidations_collected FROM collector_state WHERE id = 1")
            stats = cursor.fetchone()
            conn.close()
            if stats:
                print(f"📈 Status: {stats[0]} candles, {stats[1]} liquidations collected")
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping data collector...")
        update_collector_status(False)
        print("✅ Data collector stopped")

if __name__ == "__main__":
    main()