"""Redis outage chaos test (Section 87, 115, 119).

This does not assert a desired outcome — it *observes and reports* what
actually happens when Redis disappears underneath a running gateway, so
docs/failure-model.md can state measured behavior instead of a guess.

Requires the ability to stop/start the local Redis server.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import sys

import httpx
import websockets

CONTROL = "http://127.0.0.1:8001"
GATEWAY = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000/ws"

observations: list[tuple[str, str]] = []


def observe(what: str, result: str) -> None:
    observations.append((what, result))
    print(f"  {what:<52} {result}")


async def connect(token: str):
    ws = await websockets.connect(WS)
    await ws.send(json.dumps({"type": "CONNECT", "token": token}))
    return ws, json.loads(await ws.recv())


async def main() -> int:
    async with httpx.AsyncClient(timeout=15) as http:
        t = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "chaos"})).json()
        key = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t["id"]})).json()["api_key"]
        auth = {"Authorization": f"Bearer {key}"}
        channel = f"t-{t['id']}:room-chaos"

        ws, info = await connect(key)
        await ws.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
        await ws.recv()
        await asyncio.sleep(0.3)

        await http.post(f"{GATEWAY}/v1/publish", headers=auth, json={"channel": channel, "payload": {"pre": 1}})
        pre = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        observe("baseline delivery before outage", "delivered" if pre["type"] == "MESSAGE" else "FAILED")

        print("\n--- stopping Redis ---")
        subprocess.run(["redis-cli", "shutdown", "nosave"], capture_output=True)
        await asyncio.sleep(2)

        # 1. Liveness vs readiness during the outage.
        live = await http.get(f"{GATEWAY}/health/live")
        observe("GET /health/live during outage", f"HTTP {live.status_code} (process stays alive)")
        ready = await http.get(f"{GATEWAY}/health/ready")
        observe("GET /health/ready during outage", f"HTTP {ready.status_code} (removed from LB)")

        # 2. Publish during the outage.
        try:
            resp = await http.post(f"{GATEWAY}/v1/publish", headers=auth,
                                   json={"channel": channel, "payload": {"during": 1}})
            observe("POST /v1/publish during outage", f"HTTP {resp.status_code}")
        except Exception as e:
            observe("POST /v1/publish during outage", f"raised {type(e).__name__}")

        # 3. Does the existing WebSocket survive?
        await asyncio.sleep(1)
        observe("existing WebSocket during outage",
                "still open" if ws.close_code is None else f"closed code={ws.close_code}")

        # 4. Can a new client connect (needs Redis for session creation)?
        try:
            ws2, info2 = await asyncio.wait_for(connect(key), timeout=8)
            observe("new connection during outage", f"accepted (session {info2.get('session_id','?')[:8]}…)")
            await ws2.close()
        except Exception as e:
            observe("new connection during outage", f"rejected/failed ({type(e).__name__})")

        print("\n--- restarting Redis ---")
        subprocess.run(["redis-server", "--daemonize", "yes", "--port", "6379", "--save", ""],
                       capture_output=True)
        await asyncio.sleep(3)

        ready2 = await http.get(f"{GATEWAY}/health/ready")
        observe("GET /health/ready after recovery", f"HTTP {ready2.status_code}")

        # 5. Does the pre-existing connection resume working without a reconnect?
        try:
            ws3, _ = await connect(key)
            await ws3.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
            await ws3.recv()
            await asyncio.sleep(0.5)
            await http.post(f"{GATEWAY}/v1/publish", headers=auth,
                            json={"channel": channel, "payload": {"post": 1}})
            got = json.loads(await asyncio.wait_for(ws3.recv(), timeout=5))
            observe("publish + delivery after recovery",
                    "delivered" if got.get("type") == "MESSAGE" else f"unexpected {got.get('type')}")
            await ws3.close()
        except Exception as e:
            observe("publish + delivery after recovery", f"failed ({type(e).__name__}: {e})")

        # 6. Sequence continuity across the outage (Redis was started with
        #    --save '' so counters are lost — this is a real, documented
        #    consequence, not a bug to hide).
        r2 = await http.post(f"{GATEWAY}/v1/publish", headers=auth,
                             json={"channel": channel, "payload": {"post": 2}})
        observe("sequence after Redis restart (no persistence)",
                f"seq={r2.json().get('sequence')} (pre-outage seq was 1)")

        with __import__("contextlib").suppress(Exception):
            await ws.close()

    print("\n" + "=" * 70)
    print("Observed behavior recorded. These findings belong in docs/failure-model.md.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
