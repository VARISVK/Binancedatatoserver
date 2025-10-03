import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, timedelta, timezone
import subprocess
import threading
import time
import os

DB_PATH = "btc_data.db"

# Global variable to track if data collector is running
_data_collector_started = False

def start_data_collector():
    """Start data collector in background if not already running"""
    global _data_collector_started
    
    if _data_collector_started:
        return
    
    try:
        # Check if data collector is already running
        status = get_collector_status()
        if status and status['is_running']:
            _data_collector_started = True
            return
        
        # Start data collector in background using Popen
        try:
            process = subprocess.Popen(
                ["python", "data_collector.py"], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE
            )
            _data_collector_started = True
            print("✅ Data collector started in background")
        except Exception as e:
            print(f"❌ Failed to start data collector: {e}")
            st.error(f"Failed to start data collector: {e}")
        
    except Exception as e:
        st.error(f"Failed to start data collector: {e}")

st.set_page_config(
    page_title="BTC/USDT Live Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .stApp { background-color: #0e1117; }
    .main .block-container { padding-top: 2rem; padding-bottom: 0rem; }
    div[data-testid="metric-container"] {
        background-color: #262730;
        border: 1px solid #464646;
        padding: 15px;
        border-radius: 10px;
        color: #FAFAFA;
        box-shadow: 0 2px 4px 0 rgba(0,0,0,0.2);
    }
    div[data-testid="metric-container"] > label[data-testid="metric-label"] > div {
        color: #FAFAFA;
        font-size: 16px !important;
        font-weight: 600;
    }
    div[data-testid="metric-container"] > div[data-testid="metric-value"] > div {
        color: #09AB3B;
        font-size: 28px !important;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=5)
def get_klines_from_db(hours=24):
    """Get klines from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Convert hours to milliseconds properly
        cutoff_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
        
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM klines
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_timestamp,))
        conn.close()
        
        if not df.empty:
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.rename(columns={
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading klines: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=5)
def get_liquidations_from_db(hours=24):
    """Get recent liquidations from database for the same timeframe as candles"""
    try:
        conn = sqlite3.connect(DB_PATH)
        # Convert hours to milliseconds to match the klines timeframe
        cutoff_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
        
        query = """
            SELECT symbol, side, price, quantity, amount, timestamp
            FROM liquidations
            WHERE timestamp >= ? AND symbol = 'BTCUSDT'
            ORDER BY timestamp ASC
        """
        
        df = pd.read_sql_query(query, conn, params=(cutoff_timestamp,))
        conn.close()
        
        if not df.empty:
            df['time'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        st.error(f"Error loading liquidations: {e}")
        return pd.DataFrame()

def get_collector_status():
    """Get data collector status"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT is_running, total_klines_collected, total_liquidations_collected, 
                   last_update, last_kline_timestamp
            FROM collector_state WHERE id = 1
        """)
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'is_running': bool(row[0]),
                'total_klines': row[1],
                'total_liquidations': row[2],
                'last_update': row[3],
                'last_candle_time': datetime.fromtimestamp(row[4]/1000) if row[4] else None
            }
    except:
        pass
    return None

def get_db_stats():
    """Get database statistics"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM klines")
        kline_stats = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM liquidations")
        liq_count = cursor.fetchone()[0]
        
        conn.close()
        
        if kline_stats and kline_stats[0] > 0:
            oldest = datetime.fromtimestamp(kline_stats[1]/1000)
            newest = datetime.fromtimestamp(kline_stats[2]/1000)
            duration = newest - oldest
            
            return {
                'total_candles': kline_stats[0],
                'total_liquidations': liq_count,
                'oldest_candle': oldest,
                'newest_candle': newest,
                'duration_hours': duration.total_seconds() / 3600
            }
    except:
        pass
    return None

def create_candlestick_chart(df, liquidations_df, timeframe_name="1 Minute"):
    """Create candlestick chart with liquidation bubbles"""
    if df.empty:
        return None
    
    # Only show liquidations on 1-minute charts
    show_liquidations = timeframe_name == "1 Minute"
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.7, 0.3],
        subplot_titles=('', 'Volume')
    )
    
    fig.add_trace(
        go.Candlestick(
            x=df['Date'],
            open=df['Open'],
            high=df['High'],
            low=df['Low'],
            close=df['Close'],
            name="BTCUSDT",
            increasing_line_color='#00D4AA',
            decreasing_line_color='#FF6B6B',
            increasing_fillcolor='#00D4AA',
            decreasing_fillcolor='#FF6B6B'
        ),
        row=1, col=1
    )
    
    # Add liquidation bubbles - only on 1-minute charts
    if show_liquidations and not liquidations_df.empty:
        chart_start = df['Date'].min()
        chart_end = df['Date'].max()
        # Filter liquidations to match the exact chart timeframe
        liq_df = liquidations_df[(liquidations_df['time'] >= chart_start) & 
                                  (liquidations_df['time'] <= chart_end)]
        
        if not liq_df.empty:
            # Show individual liquidation bubbles
            long_liqs = liq_df[liq_df['side'] == 'SELL']
            if not long_liqs.empty:
                fig.add_trace(
                    go.Scatter(
                        x=long_liqs['time'],
                        y=long_liqs['price'],
                        mode='markers',
                        marker=dict(
                            size=long_liqs['amount'].apply(lambda x: min(max(x/1000, 5), 30)),
                            color='red',
                            opacity=0.7,
                            line=dict(width=1, color='darkred')
                        ),
                        name='Long Liquidations',
                        hovertemplate='<b>Long Liq</b><br>Price: $%{y:,.2f}<br>Amount: $%{customdata:,.0f}<extra></extra>',
                        customdata=long_liqs['amount']
                    ),
                    row=1, col=1
                )
            
            short_liqs = liq_df[liq_df['side'] == 'BUY']
            if not short_liqs.empty:
                fig.add_trace(
                    go.Scatter(
                        x=short_liqs['time'],
                        y=short_liqs['price'],
                        mode='markers',
                        marker=dict(
                            size=short_liqs['amount'].apply(lambda x: min(max(x/1000, 5), 30)),
                            color='lime',
                            opacity=0.7,
                            line=dict(width=1, color='green')
                        ),
                        name='Short Liquidations',
                        hovertemplate='<b>Short Liq</b><br>Price: $%{y:,.2f}<br>Amount: $%{customdata:,.0f}<extra></extra>',
                        customdata=short_liqs['amount']
                    ),
                    row=1, col=1
                )
    
    colors = ['#00D4AA' if df.iloc[i]['Close'] >= df.iloc[i]['Open'] 
              else '#FF6B6B' for i in range(len(df))]
    
    fig.add_trace(
        go.Bar(x=df['Date'], y=df['Volume'], marker_color=colors, 
               name="Volume", opacity=0.7, showlegend=True),
        row=2, col=1
    )
    
    fig.update_layout(
        title='BTC/USDT Perpetual Futures + Liquidations (Database + Live)',
        title_font=dict(size=24, color='#FAFAFA'),
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='#FAFAFA'),
        xaxis_rangeslider_visible=False,
        height=700,
        showlegend=True,
        legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#FAFAFA')),
        margin=dict(l=0, r=0, t=50, b=0)
    )
    
    fig.update_xaxes(gridcolor='#2F3349', linecolor='#2F3349')
    fig.update_yaxes(gridcolor='#2F3349', linecolor='#2F3349')
    
    if len(df) > 0:
        latest_price = df.iloc[-1]['Close']
        fig.add_annotation(
            x=df.iloc[-1]['Date'], y=latest_price,
            text=f"${latest_price:,.2f}",
            showarrow=True, arrowhead=2,
            bgcolor="#00D4AA", font=dict(color="white", size=12)
        )
    
    return fig

def main():
    # Auto-start data collector
    start_data_collector()
    
    # Header
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        st.title("₿ BTC/USDT Live Dashboard")
        st.caption("Real-time data from database + WebSocket streams")
    
    with col2:
        # Only 1-minute timeframe for liquidation display
        timeframe = "1 Minute"
        st.info("📊 **1-Minute Chart** - Shows individual liquidations")
    
    with col3:
        auto_refresh = st.checkbox("🔄 Auto Refresh", value=True)
    
    # Fixed to 1-minute timeframe
    hours = 100/60  # Show 100 candles minimum
    display_name = "Last 100 candles"
    
    # Collector status
    status = get_collector_status()
    db_stats = get_db_stats()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if status and status['is_running']:
            st.success("🟢 Data Collector Running")
        else:
            st.error("🔴 Data Collector Offline")
            st.warning("⚠️ Run: `python data_collector.py` to start collecting data")
    
    with col2:
        if db_stats:
            st.info(f"📊 Database: {db_stats['total_candles']:,} candles stored")
    
    with col3:
        if db_stats:
            st.info(f"💥 Total Liquidations: {db_stats['total_liquidations']:,}")
    
    # Database info
    if db_stats:
        with st.expander("📈 Database Statistics"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Candles", f"{db_stats['total_candles']:,}")
            with col2:
                st.metric("Data Duration", f"{db_stats['duration_hours']:.1f} hours")
            with col3:
                st.metric("Total Liquidations", f"{db_stats['total_liquidations']:,}")
            
            st.caption(f"Oldest candle: {db_stats['oldest_candle'].strftime('%Y-%m-%d %H:%M:%S')}")
            st.caption(f"Newest candle: {db_stats['newest_candle'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    with st.spinner(f"📡 Loading {display_name} data from database..."):
        df = get_klines_from_db(hours=hours)
        # Get liquidations for the same timeframe as candles
        liquidations_df = get_liquidations_from_db(hours=hours)
    
    if not df.empty:
        current_price = df.iloc[-1]['Close']
        prev_price = df.iloc[-2]['Close'] if len(df) >= 2 else current_price
        price_change_pct = ((current_price - prev_price) / prev_price) * 100
        
        high_price = df['High'].max()
        low_price = df['Low'].min()
        total_volume = df['Volume'].sum()
        
        # Recent liquidations stats (last 5 minutes)
        if not liquidations_df.empty and 'time' in liquidations_df.columns:
            # Convert cutoff time to timezone-naive to match the database datetime
            cutoff_time = datetime.now() - timedelta(minutes=5)
            # Ensure time column is timezone-naive
            if liquidations_df['time'].dt.tz is not None:
                liquidations_df['time'] = liquidations_df['time'].dt.tz_localize(None)
            recent_liqs = liquidations_df[liquidations_df['time'] > cutoff_time]
            total_liq_amount = recent_liqs['amount'].sum() if not recent_liqs.empty else 0
            long_liqs = len(recent_liqs[recent_liqs['side'] == 'SELL']) if not recent_liqs.empty else 0
            short_liqs = len(recent_liqs[recent_liqs['side'] == 'BUY']) if not recent_liqs.empty else 0
        else:
            recent_liqs = pd.DataFrame()
            total_liq_amount = 0
            long_liqs = 0
            short_liqs = 0
        
        # Metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("Current Price", f"${current_price:,.2f}", f"{price_change_pct:+.2f}%")
        with col2:
            st.metric(f"{hours}h High", f"${high_price:,.2f}")
        with col3:
            st.metric(f"{hours}h Low", f"${low_price:,.2f}")
        with col4:
            st.metric(f"{hours}h Volume", f"{total_volume:,.0f}")
        with col5:
            st.metric("Liquidations (5min)", f"{len(recent_liqs)}", f"${total_liq_amount:,.0f}")
        with col6:
            st.metric("Long/Short", f"{long_liqs}/{short_liqs}")
        
        # Chart
        fig = create_candlestick_chart(df, liquidations_df, timeframe)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        
        
        # Info
        st.markdown("### 🎯 Liquidation Legend")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("🔴 **Red Bubbles** = Long Liquidations")
        with col2:
            st.markdown("🟢 **Green Bubbles** = Short Liquidations")
        with col3:
            st.markdown("📏 **Bubble Size** = Liquidation Amount")
        
        # Recent liquidations
        if not recent_liqs.empty:
            with st.expander(f"⚡ Recent Liquidations (Last 5 min) - {len(recent_liqs)} events"):
                display_df = recent_liqs[['time', 'side', 'price', 'amount']].copy()
                display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.2f}")
                display_df['amount'] = display_df['amount'].apply(lambda x: f"${x:,.0f}")
                st.dataframe(display_df.sort_values('time', ascending=False), use_container_width=True)
        
        # Debug info for liquidations
        with st.expander("🔍 Debug: Liquidation Data"):
            st.write(f"**Chart data range:** {df['Date'].min()} to {df['Date'].max()}")
            st.write(f"**Total liquidations in timeframe:** {len(liquidations_df)}")
            if not liquidations_df.empty:
                st.write(f"**Liquidation time range:** {liquidations_df['time'].min()} to {liquidations_df['time'].max()}")
                # Show liquidations that match the chart timeframe
                chart_start = df['Date'].min()
                chart_end = df['Date'].max()
                matching_liqs = liquidations_df[(liquidations_df['time'] >= chart_start) & 
                                                (liquidations_df['time'] <= chart_end)]
                st.write(f"**Liquidations matching chart timeframe:** {len(matching_liqs)}")
                
                # Show liquidation display info
                st.write("**📍 Liquidation Mode:** Individual liquidation bubbles will be shown")
                long_count = len(matching_liqs[matching_liqs['side'] == 'SELL'])
                short_count = len(matching_liqs[matching_liqs['side'] == 'BUY'])
                st.write(f"**Long liquidations to display:** {long_count}")
                st.write(f"**Short liquidations to display:** {short_count}")
                
                if not matching_liqs.empty:
                    st.dataframe(matching_liqs[['time', 'side', 'price', 'amount']].head(10))
    else:
        st.warning("⚠️ No data in database. Make sure data_collector.py is running!")
    
    # Auto-refresh
    if auto_refresh:
        import time
        time.sleep(5)  # Refresh every 5 seconds
        st.rerun()

if __name__ == "__main__":
    main()