"""
Database Checker - Verify data is being stored correctly
Run this to check if klines and liquidations are being saved properly
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = "btc_data.db"

def check_database():
    """Check database contents and status"""
    print("=" * 60)
    print("🔍 BTC/USDT Database Checker")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Check if tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📊 Tables found: {[table[0] for table in tables]}")
        
        # Check klines data
        print("\n" + "=" * 40)
        print("📈 KLINES DATA")
        print("=" * 40)
        
        cursor.execute("SELECT COUNT(*) FROM klines")
        kline_count = cursor.fetchone()[0]
        print(f"Total klines: {kline_count:,}")
        
        if kline_count > 0:
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM klines")
            min_ts, max_ts = cursor.fetchone()
            min_time = datetime.fromtimestamp(min_ts/1000)
            max_time = datetime.fromtimestamp(max_ts/1000)
            print(f"Date range: {min_time} to {max_time}")
            
            # Recent klines
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume 
                FROM klines 
                ORDER BY timestamp DESC 
                LIMIT 5
            """)
            recent_klines = cursor.fetchall()
            print("\n📊 Recent 5 klines:")
            for kline in recent_klines:
                ts, open_price, high, low, close, volume = kline
                dt = datetime.fromtimestamp(ts/1000)
                print(f"  {dt.strftime('%H:%M:%S')} | O:{open_price:.2f} H:{high:.2f} L:{low:.2f} C:{close:.2f} V:{volume:.0f}")
        
        # Check liquidations data
        print("\n" + "=" * 40)
        print("💥 LIQUIDATIONS DATA")
        print("=" * 40)
        
        cursor.execute("SELECT COUNT(*) FROM liquidations")
        liq_count = cursor.fetchone()[0]
        print(f"Total liquidations: {liq_count:,}")
        
        if liq_count > 0:
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM liquidations")
            min_ts, max_ts = cursor.fetchone()
            min_time = datetime.fromtimestamp(min_ts/1000)
            max_time = datetime.fromtimestamp(max_ts/1000)
            print(f"Date range: {min_time} to {max_time}")
            
            # Recent liquidations
            cursor.execute("""
                SELECT timestamp, side, price, amount 
                FROM liquidations 
                ORDER BY timestamp DESC 
                LIMIT 10
            """)
            recent_liqs = cursor.fetchall()
            print("\n💥 Recent 10 liquidations:")
            for liq in recent_liqs:
                ts, side, price, amount = liq
                dt = datetime.fromtimestamp(ts/1000)
                print(f"  {dt.strftime('%H:%M:%S')} | {side} | ${price:.2f} | ${amount:,.0f}")
            
            # Liquidation stats
            cursor.execute("SELECT side, COUNT(*), SUM(amount) FROM liquidations GROUP BY side")
            liq_stats = cursor.fetchall()
            print("\n📊 Liquidation stats:")
            for side, count, total_amount in liq_stats:
                print(f"  {side}: {count:,} liquidations, ${total_amount:,.0f} total")
        
        # Check collector status
        print("\n" + "=" * 40)
        print("⚙️ COLLECTOR STATUS")
        print("=" * 40)
        
        cursor.execute("""
            SELECT is_running, total_klines_collected, total_liquidations_collected, 
                   last_update, last_kline_timestamp
            FROM collector_state WHERE id = 1
        """)
        status = cursor.fetchone()
        
        if status:
            is_running, total_klines, total_liqs, last_update, last_kline_ts = status
            print(f"Collector running: {'✅ Yes' if is_running else '❌ No'}")
            print(f"Total klines collected: {total_klines:,}")
            print(f"Total liquidations collected: {total_liqs:,}")
            print(f"Last update: {last_update}")
            if last_kline_ts:
                last_kline_time = datetime.fromtimestamp(last_kline_ts/1000)
                print(f"Last kline time: {last_kline_time}")
        else:
            print("❌ No collector status found")
        
        # Check data freshness
        print("\n" + "=" * 40)
        print("🕐 DATA FRESHNESS")
        print("=" * 40)
        
        now = datetime.now()
        
        # Check last kline
        if kline_count > 0:
            cursor.execute("SELECT MAX(timestamp) FROM klines")
            last_kline_ts = cursor.fetchone()[0]
            last_kline_time = datetime.fromtimestamp(last_kline_ts/1000)
            kline_age = now - last_kline_time
            print(f"Last kline: {last_kline_time} ({kline_age.total_seconds():.0f} seconds ago)")
            
            if kline_age.total_seconds() > 120:  # More than 2 minutes
                print("⚠️ WARNING: Klines are not fresh! Check if data_collector.py is running.")
            else:
                print("✅ Klines are fresh")
        
        # Check last liquidation
        if liq_count > 0:
            cursor.execute("SELECT MAX(timestamp) FROM liquidations")
            last_liq_ts = cursor.fetchone()[0]
            last_liq_time = datetime.fromtimestamp(last_liq_ts/1000)
            liq_age = now - last_liq_time
            print(f"Last liquidation: {last_liq_time} ({liq_age.total_seconds():.0f} seconds ago)")
            
            if liq_age.total_seconds() > 300:  # More than 5 minutes
                print("⚠️ WARNING: No recent liquidations. This might be normal if no liquidations occurred.")
            else:
                print("✅ Recent liquidations found")
        
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ Database check completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")

if __name__ == "__main__":
    check_database()
