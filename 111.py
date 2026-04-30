from pathlib import Path
import os
import time
import hmac
import hashlib

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"

load_dotenv(ENV_PATH, override=True)

API_KEY = os.getenv("BINANCE_FUTURES_TESTNET_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_FUTURES_TESTNET_API_SECRET", "").strip()

BASE_URL = "https://demo-fapi.binance.com"
PROXY_URL = "http://127.0.0.1:7890"


def check_public(use_proxy: bool) -> None:
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"

    proxies = None
    if use_proxy:
        proxies = {
            "http": PROXY_URL,
            "https": PROXY_URL,
        }

    print("\n=== public exchangeInfo ===")
    print("use_proxy =", use_proxy)
    print("url =", url)

    try:
        resp = requests.get(url, proxies=proxies, timeout=15)
        print("status_code =", resp.status_code)
        print("response =", resp.text[:500])
    except Exception as e:
        print("error_type =", type(e).__name__)
        print("error =", e)


def check_signed_account(use_proxy: bool) -> None:
    timestamp = int(time.time() * 1000)
    query = f"timestamp={timestamp}&recvWindow=5000"

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url = f"{BASE_URL}/fapi/v2/account?{query}&signature={signature}"

    proxies = None
    if use_proxy:
        proxies = {
            "http": PROXY_URL,
            "https": PROXY_URL,
        }

    print("\n=== signed account ===")
    print("use_proxy =", use_proxy)
    print("url =", url.split("?")[0])
    print("api_key len =", len(API_KEY), "head =", API_KEY[:6], "tail =", API_KEY[-6:])

    try:
        resp = requests.get(
            url,
            headers={"X-MBX-APIKEY": API_KEY},
            proxies=proxies,
            timeout=15,
        )
        print("status_code =", resp.status_code)
        print("response =", resp.text[:1000])
    except Exception as e:
        print("error_type =", type(e).__name__)
        print("error =", e)


print("env_path =", ENV_PATH)
print("env_exists =", ENV_PATH.exists())
print("base_url =", BASE_URL)

check_public(use_proxy=False)
check_public(use_proxy=True)

check_signed_account(use_proxy=False)
check_signed_account(use_proxy=True)