#!/usr/bin/env python3
"""
Startup script for Render.com deployment
Runs both data collector and Streamlit app
"""

import subprocess
import threading
import time
import sys
import os
from pathlib import Path

def run_data_collector():
    """Run data collector in background thread"""
    print("🚀 Starting data collector...")
    try:
        # Run data_collector.py with Popen to avoid blocking
        process = subprocess.Popen([sys.executable, "data_collector.py"], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)
        # Keep the process running
        process.wait()
    except Exception as e:
        print(f"❌ Data collector error: {e}")

def run_streamlit():
    """Run Streamlit app"""
    print("🚀 Starting Streamlit app...")
    try:
        # Run streamlit with proper configuration for Render
        cmd = [
            sys.executable, "-m", "streamlit", "run", "main.py",
            "--server.port=10000",
            "--server.address=0.0.0.0",
            "--server.headless=true",
            "--server.enableCORS=false",
            "--server.enableXsrfProtection=false"
        ]
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Streamlit failed: {e}")
    except Exception as e:
        print(f"❌ Streamlit error: {e}")

def main():
    print("=" * 60)
    print("🚀 BTC/USDT Dashboard - Starting Services")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("data_collector.py").exists():
        print("❌ data_collector.py not found!")
        sys.exit(1)
    
    if not Path("main.py").exists():
        print("❌ main.py not found!")
        sys.exit(1)
    
    # Start data collector in background thread
    collector_thread = threading.Thread(target=run_data_collector, daemon=True)
    collector_thread.start()
    
    # Give data collector time to initialize
    print("⏳ Waiting for data collector to initialize...")
    time.sleep(10)
    
    # Start Streamlit (this will block)
    run_streamlit()

if __name__ == "__main__":
    main()
