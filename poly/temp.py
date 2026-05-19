import requests

base = "https://gamma-api.polymarket.com/events"

params_volume = {
    "active": "true",
    "closed": "false",
    "order": "volume_24hr",
    "ascending": "false",
    "limit": 20,
}

params_liquidity = {
    "active": "true",
    "closed": "false",
    "order": "liquidity",
    "ascending": "false",
    "limit": 20,
}

for name, params in [
    ("24h volume top events", params_volume),
    ("liquidity top events", params_liquidity),
]:
    data = requests.get(base, params=params, timeout=10).json()

    print("\n", name)
    for e in data[:10]:
        print(
            e.get("title"),
            "volume24hr=", e.get("volume24hr"),
            "volume=", e.get("volume"),
            "liquidity=", e.get("liquidity"),
            "liquidityClob=", e.get("liquidityClob"),
            "slug=", e.get("slug"),
        )