import json
import random
import socket
import time

from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_HOST
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT
from utils.arguments import EXTERNAL_SIGNAL_DEFAULT_PORT
from utils.arguments import EXTERNAL_SIGNAL_SEND_INTERVAL_SECONDS


while True:
    msg = {
        "instrument_id": EXTERNAL_SIGNAL_DEFAULT_INSTRUMENT,
        "side": random.choice(["BUY", "SELL"]),
        "sent_ns": time.time_ns(),
    }

    data = json.dumps(msg).encode("utf-8") + b"\n"

    with socket.create_connection((EXTERNAL_SIGNAL_DEFAULT_HOST, EXTERNAL_SIGNAL_DEFAULT_PORT)) as s:
        s.sendall(data)

    print("sent:", msg)

    time.sleep(EXTERNAL_SIGNAL_SEND_INTERVAL_SECONDS)
