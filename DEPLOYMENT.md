# BTC/USDT Dashboard - Render.com Deployment

## Problem Solved
The original issue was that after suspending and resuming your Render.com deployment, the `data_collector.py` wasn't running, causing the dashboard to show no data.

## Solution
I've created multiple approaches to ensure the data collector starts automatically:

### Option 1: Automatic Startup (Recommended)
The `main.py` has been updated to automatically start the data collector when the Streamlit app loads. This is the simplest solution.

### Option 2: Startup Script
Use `startup.py` as your start command in Render.com. This script runs both the data collector and Streamlit app.

### Option 3: Render Configuration
Use the `render.yaml` file for automatic deployment configuration.

## Files Added/Modified

### New Files:
- `startup.py` - Runs both data collector and Streamlit
- `render.yaml` - Render.com configuration
- `test_deployment.py` - Test script
- `DEPLOYMENT.md` - This file

### Modified Files:
- `main.py` - Added automatic data collector startup

## Deployment Instructions

### Method 1: Using startup.py (Recommended)
1. In your Render.com service settings:
   - **Start Command**: `python startup.py`
   - **Port**: 10000

### Method 2: Using render.yaml
1. Push your code with `render.yaml` to GitHub
2. Connect to Render.com - it will auto-detect the configuration

### Method 3: Modified main.py (Automatic)
1. Just use `streamlit run main.py` as your start command
2. The data collector will start automatically

## Testing Locally
Run the test script to verify everything works:
```bash
python test_deployment.py
```

## How It Works
1. When the app starts, it checks if the data collector is running
2. If not running, it starts the data collector in a background thread
3. The dashboard then displays data from the database
4. Both services run in the same process on Render.com

## Troubleshooting
- If data collector still doesn't start, check the Render.com logs
- Make sure all dependencies are in `requirements.txt`
- Verify the database file permissions on Render.com

## Environment Variables
Set these in Render.com if needed:
- `PORT=10000`
- Any other environment variables your app needs
