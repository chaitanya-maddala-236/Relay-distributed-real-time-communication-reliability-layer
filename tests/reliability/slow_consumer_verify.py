"""Slow-consumer and backpressure test (Section 38, 42, 111, Demo 4).

Flow control is ACK-based (see apps/gateway/connections/manager.py), so a
"slow consumer" here is a client that keeps receiving but stops
acknowledging — i.e. it cannot keep up with processing. Such a client
must:
  * fill its in-flight window, then its bounded queue
  * be disconnected with SLOW_CONSUMER
  * NOT block or slow delivery to a healthy, acknowledging client on the
    same channel

Run the gateway with small bounds so this completes quickly:
  RELAY_MAX_INFLIGHT_MESSAGES=10 \
  RELAY_CONNECTION_QUEUE_MAX_MESSAGES=20 \
  RELAY_CONNECTION_QUEUE_MAX_BYTES=200000 \
  uvicorn apps.gateway.main:app --port 8000
"""
from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx
import websockets

CONTROL = "http://127.0.0.1:8001"
GATEWAY = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000/ws"

PASS, FAIL = [], []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))


async def connect(token: str, **kwargs):
    ws = await websockets.connect(WS, max_size=4_000_000, **kwargs)
    await ws.send(json.dumps({"type": "CONNECT", "token": token}))
    return ws, json.loads(await ws.recv())


async def main() -> int:
    async with httpx.AsyncClient(timeout=20) as http:
        t = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "slowtest"})).json()
        key = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t["id"]})).json()["api_key"]
        auth = {"Authorization": f"Bearer {key}"}
        channel = f"t-{t['id']}:room-pressure"

        # Fast client drains AND acknowledges, keeping its window open.
        # Slow client drains but never acknowledges, so its window fills.
        fast_ws, _ = await connect(key)
        slow_ws, slow_info = await connect(key)

        for ws in (fast_ws, slow_ws):
            await ws.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
            await ws.recv()
        await asyncio.sleep(0.4)

        received_fast: list[int] = []
        fast_latencies: list[float] = []
        slow_received: list[int] = []

        async def drain_fast() -> None:
            """Healthy client: consumes and acknowledges every message."""
            try:
                while True:
                    raw = await asyncio.wait_for(fast_ws.recv(), timeout=20)
                    frame = json.loads(raw)
                    if frame.get("type") == "MESSAGE":
                        received_fast.append(frame["sequence"])
                        sent = frame["payload"].get("sent_at")
                        if sent:
                            fast_latencies.append(time.time() - sent)
                        await fast_ws.send(json.dumps(
                            {"type": "ACK", "channel": frame["channel"], "sequence": frame["sequence"]}
                        ))
                    elif frame.get("type") == "DISCONNECT":
                        return
            except (TimeoutError, websockets.ConnectionClosed):
                return

        slow_disconnect_reason: list[str] = []

        async def drain_slow_without_ack() -> None:
            """Unhealthy client: reads frames but never ACKs, simulating a
            consumer that cannot keep up with processing."""
            try:
                while True:
                    raw = await asyncio.wait_for(slow_ws.recv(), timeout=20)
                    frame = json.loads(raw)
                    if frame.get("type") == "MESSAGE":
                        slow_received.append(frame["sequence"])
                    elif frame.get("type") == "DISCONNECT":
                        slow_disconnect_reason.append(frame.get("reason", ""))
                        return
            except (TimeoutError, websockets.ConnectionClosed):
                return

        drainer = asyncio.create_task(drain_fast())
        slow_drainer = asyncio.create_task(drain_slow_without_ack())

        print("\n1. Publishing a burst while one client refuses to read")
        payload_blob = "x" * 8000  # ~8 KB per message
        total = 400
        for i in range(total):
            await http.post(
                f"{GATEWAY}/v1/publish",
                headers=auth,
                json={"channel": channel, "payload": {"i": i, "blob": payload_blob, "sent_at": time.time()}},
            )

        # Give delivery time to settle.
        await asyncio.sleep(3)

        print("\n2. Slow (non-acknowledging) client outcome (Section 38)")
        await asyncio.wait_for(asyncio.shield(slow_drainer), timeout=15)
        slow_reason = slow_disconnect_reason[0] if slow_disconnect_reason else None

        check("slow client was disconnected", slow_ws.close_code is not None or slow_reason is not None,
              f"close_code={slow_ws.close_code}")
        check("disconnect carried the SLOW_CONSUMER reason",
              slow_reason == "SLOW_CONSUMER" or slow_ws.close_code == 4409,
              f"close_code={slow_ws.close_code} reason={slow_reason}")
        check("slow client stopped receiving well before the full burst (window held it back)",
              0 < len(slow_received) < total, f"slow received {len(slow_received)} of {total}")

        print("\n3. Healthy client isolation (Section 42)")
        await asyncio.sleep(3)
        drainer.cancel()
        check("fast client stayed connected", fast_ws.close_code is None,
              f"close_code={fast_ws.close_code}")
        check(f"fast client received the full burst ({len(received_fast)}/{total})",
              len(received_fast) == total, f"got {len(received_fast)}")
        check("fast client received them in order",
              received_fast == sorted(received_fast))
        if fast_latencies:
            p95 = sorted(fast_latencies)[int(len(fast_latencies) * 0.95) - 1]
            print(f"       fast-client delivery latency: p50={sorted(fast_latencies)[len(fast_latencies)//2]*1000:.1f}ms "
                  f"p95={p95*1000:.1f}ms max={max(fast_latencies)*1000:.1f}ms")

        print("\n4. Metrics reflect what happened (Section 64)")
        metrics = (await http.get(f"{GATEWAY}/metrics")).text
        slow_total = 0.0
        for line in metrics.splitlines():
            if line.startswith("relay_slow_consumers_total "):
                slow_total = float(line.split()[1])
        check("relay_slow_consumers_total incremented", slow_total >= 1, f"value={slow_total}")

        with __import__("contextlib").suppress(Exception):
            await fast_ws.close()

    print(f"\n{'='*60}\nPASSED: {len(PASS)}   FAILED: {len(FAIL)}")
    if FAIL:
        print("Failures: " + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
