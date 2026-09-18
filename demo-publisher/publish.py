"""Demo backend publisher (Section 77, 86).

Publishes to a channel through the HTTP publish API at a configurable
rate, using an Idempotency-Key so a retried POST cannot create a second
logical message.

  python demo-publisher/publish.py <api_key> <channel> [rate_per_sec] [count]
"""
import asyncio
import sys
import uuid

import httpx

GATEWAY = "http://127.0.0.1:8000"


async def main() -> None:
    api_key, channel = sys.argv[1], sys.argv[2]
    rate = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0
    count = int(sys.argv[4]) if len(sys.argv) > 4 else 100
    headers = {"Authorization": f"Bearer {api_key}"}

    async with httpx.AsyncClient(timeout=10) as http:
        for i in range(count):
            key = str(uuid.uuid4())
            for attempt in range(3):  # retry with the SAME idempotency key
                try:
                    r = await http.post(
                        f"{GATEWAY}/v1/publish",
                        headers={**headers, "Idempotency-Key": key},
                        json={"channel": channel, "payload": {"n": i, "text": f"message {i}"}},
                    )
                    if r.status_code == 200:
                        print(f"[{i}] seq={r.json()['sequence']}")
                        break
                    print(f"[{i}] HTTP {r.status_code} -> retrying with same key")
                except httpx.HTTPError as e:
                    print(f"[{i}] {type(e).__name__} -> retrying with same key")
                await asyncio.sleep(0.25 * (2 ** attempt))
            await asyncio.sleep(1 / rate)


asyncio.run(main())
