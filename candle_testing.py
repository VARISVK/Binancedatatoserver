import requests

# Test different limits
url = "https://fapi.binance.com/fapi/v1/klines"

for limit in [100, 500, 1000, 1500]:
    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": limit
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        print(f"Requested: {limit}, Got: {len(data)} candles")
    else:
        print(f"Requested: {limit}, Failed: {response.status_code}")