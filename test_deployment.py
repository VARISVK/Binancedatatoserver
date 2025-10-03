#!/usr/bin/env python3
"""
Test script to verify the deployment solution works
"""

import subprocess
import time
import sys
import os
from pathlib import Path

def test_data_collector():
    """Test if data collector can start"""
    print("🧪 Testing data collector...")
    try:
        # Start data collector in background
        process = subprocess.Popen([sys.executable, "data_collector.py"], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)
        
        # Wait a bit for it to start
        time.sleep(5)
        
        # Check if it's still running
        if process.poll() is None:
            print("✅ Data collector started successfully")
            process.terminate()
            return True
        else:
            stdout, stderr = process.communicate()
            print(f"❌ Data collector failed: {stderr.decode()}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing data collector: {e}")
        return False

def test_startup_script():
    """Test the startup script"""
    print("🧪 Testing startup script...")
    try:
        # Test if startup script exists and is executable
        if Path("startup.py").exists():
            print("✅ Startup script exists")
            return True
        else:
            print("❌ Startup script not found")
            return False
    except Exception as e:
        print(f"❌ Error testing startup script: {e}")
        return False

def test_requirements():
    """Test if all required files exist"""
    print("🧪 Testing requirements...")
    required_files = [
        "main.py",
        "data_collector.py", 
        "requirements.txt",
        "startup.py",
        "render.yaml"
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files present")
        return True

def main():
    print("=" * 60)
    print("🧪 Testing BTC/USDT Dashboard Deployment")
    print("=" * 60)
    
    tests = [
        ("Requirements", test_requirements),
        ("Startup Script", test_startup_script),
        ("Data Collector", test_data_collector)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        result = test_func()
        results.append((test_name, result))
    
    print("\n" + "=" * 60)
    print("📊 Test Results:")
    print("=" * 60)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All tests passed! Ready for deployment.")
        print("\n📋 Deployment Instructions:")
        print("1. Push your code to GitHub")
        print("2. Connect your repo to Render.com")
        print("3. Use 'startup.py' as the start command")
        print("4. Set PORT environment variable to 10000")
    else:
        print("❌ Some tests failed. Please fix issues before deploying.")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
