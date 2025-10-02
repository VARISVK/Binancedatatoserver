# BTC/USDT Live Dashboard with Liquidations

A real-time cryptocurrency dashboard that displays BTC/USDT price data and liquidation events.

## Features

- 📊 **Real-time Price Charts**: 1-minute candlestick charts
- 💥 **Liquidation Tracking**: Individual liquidation bubbles on charts
- 🔄 **Auto-refresh**: Updates every 5 seconds
- 📈 **Database Storage**: SQLite database for historical data
- 🎯 **Clean Interface**: Focused on liquidation analysis

## Setup

### Local Development

1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start data collector** (in one terminal):
   ```bash
   python data_collector.py
   ```

4. **Start dashboard** (in another terminal):
   ```bash
   streamlit run main.py
   ```

### Database Check

Run the database checker to verify data collection:
```bash
python db_checker.py
```

## Files

- `main.py` - Streamlit dashboard
- `data_collector.py` - Background data collector
- `db_checker.py` - Database verification tool
- `btc_data.db` - SQLite database
- `requirements.txt` - Python dependencies

## Data Sources

- **Price Data**: Binance Futures WebSocket (1-minute candles)
- **Liquidations**: Binance Futures WebSocket (real-time)

## Deployment

This project can be deployed to Render.com or similar cloud platforms.
