import json
import random
import socket
import time


while True:
    msg = {
        "instrument_id": "BTCUSDT-PERP.BINANCE",
        "side": random.choice(["BUY", "SELL"]),
        "sent_ns": time.time_ns(),
    }

    data = json.dumps(msg).encode("utf-8") + b"\n"

    with socket.create_connection(("127.0.0.1", 9001)) as s:
        s.sendall(data)

    print("sent:", msg)

    time.sleep(60)