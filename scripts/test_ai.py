from __future__ import annotations

import argparse
import json
import requests


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--scope", default="all_india")
    ap.add_argument("--date", default="2024-01-01")
    ap.add_argument("--train", default="")
    args = ap.parse_args()

    base = f"http://{args.host}:{args.port}"

    # Ask
    q = {"scope": args.scope, "date": args.date, "query": "otp and top risks"}
    r = requests.post(f"{base}/ai/ask", json=q, timeout=5)
    print("ASK:", r.status_code)
    print(json.dumps(r.json(), indent=2))

    # Suggest
    s = {"scope": args.scope, "date": args.date, "train_id": (args.train or None), "max_hold_min": 3}
    r2 = requests.post(f"{base}/ai/suggest", json=s, timeout=8)
    print("SUGGEST:", r2.status_code)
    print(json.dumps(r2.json(), indent=2))


if __name__ == "__main__":
    main()

