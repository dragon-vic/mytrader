"""Read Binance stock option quotes from its public web endpoint.

This web endpoint is undocumented and may change. The response has no bid/ask
timestamp; captured_at_utc records retrieval time, not market data time.
"""

import json
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen


URL = "https://www.binance.com/bapi/equity/v1/friendly/equity/option/get-option-quote"


def main() -> int:
    symbols = sys.argv[1:]
    if not symbols:
        print("Usage: python tools/binance_stock_option_quote.py OCC_SYMBOL [OCC_SYMBOL ...]", file=sys.stderr)
        return 2

    body = json.dumps({"occSymbols": symbols}).encode("utf-8")
    request = Request(URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=15) as response:
        payload = json.load(response)

    if payload.get("code") != "000000" or not payload.get("success"):
        raise RuntimeError(f"Binance quote request failed: {payload.get('code')} {payload.get('message')}")

    quotes = {quote["os"]: quote for quote in payload["data"]}
    missing = set(symbols) - quotes.keys()
    if missing:
        raise RuntimeError(f"Binance returned no quote for: {', '.join(sorted(missing))}")

    result = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": URL,
        "quotes": [
            {
                "symbol": symbol,
                "description": quotes[symbol]["n"],
                "bid": quotes[symbol]["bp"],
                "bid_size": int(quotes[symbol]["bs"]),
                "ask": quotes[symbol]["ap"],
                "ask_size": int(quotes[symbol]["as"]),
                "last": quotes[symbol]["l"],
                "multiplier": int(quotes[symbol]["m"]),
            }
            for symbol in symbols
        ],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
