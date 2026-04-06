"""Generate a test payload for the serverless training endpoint.

Produces a JSON with synthetic daily OHLCV data for testing.
Usage: python gen_test_payload.py > test_payload.json
"""

import json
import random

random.seed(42)

STOCK_CODES = [f"{600000 + i:06d}.SH" for i in range(50)]
DATES = [f"2024-{m:02d}-{d:02d}" for m in range(1, 4) for d in range(2, 22)]  # ~60 dates


def random_bar(base_price: float = 10.0) -> dict:
    o = base_price * (1 + random.gauss(0, 0.02))
    c = o * (1 + random.gauss(0, 0.03))
    h = max(o, c) * (1 + abs(random.gauss(0, 0.01)))
    lo = min(o, c) * (1 - abs(random.gauss(0, 0.01)))
    vol = random.randint(100000, 5000000)
    return {
        "open": round(o, 2),
        "high": round(h, 2),
        "low": round(lo, 2),
        "close": round(c, 2),
        "volume": vol,
        "amount": round(c * vol, 2),
        "is_suspended": False,
    }


daily_data = {}
for d in DATES:
    daily_data[d] = {}
    for code in STOCK_CODES:
        if random.random() < 0.02:  # 2% chance suspended
            daily_data[d][code] = {
                "open": 0,
                "high": 0,
                "low": 0,
                "close": 0,
                "volume": 0,
                "amount": 0,
                "is_suspended": True,
            }
        else:
            daily_data[d][code] = random_bar(base_price=random.uniform(5, 50))

payload = {
    "mode": "full",
    "daily_data": daily_data,
    "init_model_b64": None,
    "s3_config": None,
}

print(json.dumps(payload))
