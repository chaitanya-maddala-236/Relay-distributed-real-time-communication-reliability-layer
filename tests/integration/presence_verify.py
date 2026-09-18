"""Presence verification (Section 47-49, Phase 10).

The interesting property is the race in Section 49: closing one of a
user's connections must not mark them offline while another is live.
"""
from __future__ import annotations

import asyncio
import json
import sys

import httpx
import websockets

CONTROL = "http://127.0.0.1:8001"
GATEWAY = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000/ws"

PASS, FAIL = [], []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))


async def connect(token: str, user_id: str | None = None):
    ws = await websockets.connect(WS)
    await ws.send(json.dumps({"type": "CONNECT", "token": token, "user_id": user_id}))
    return ws, json.loads(await ws.recv())


async def main() -> int:
    async with httpx.AsyncClient(timeout=10) as http:
        t = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "presence"})).json()
        key = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t["id"]})).json()["api_key"]
        auth = {"Authorization": f"Bearer {key}"}

        t2 = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "presence-other"})).json()
        key2 = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t2["id"]})).json()["api_key"]

        async def presence_of(user: str) -> dict:
            return (await http.get(f"{GATEWAY}/v1/presence/{user}", headers=auth)).json()

        print("\n1. Unknown user is OFFLINE")
        p = await presence_of("alice")
        check("unknown user reports OFFLINE", p["state"] == "OFFLINE" and p["connections"] == 0, str(p))

        print("\n2. Connecting brings a user ONLINE")
        ws1, _ = await connect(key, user_id="alice")
        await asyncio.sleep(0.3)
        p = await presence_of("alice")
        check("user is ONLINE after connecting", p["state"] == "ONLINE", str(p))
        check("one connection counted", p["connections"] == 1, str(p))

        print("\n3. Multiple connections for one user (Section 49)")
        ws2, _ = await connect(key, user_id="alice")
        await asyncio.sleep(0.3)
        p = await presence_of("alice")
        check("two connections counted", p["connections"] == 2, str(p))
        check("still ONLINE", p["state"] == "ONLINE", str(p))

        print("\n4. Closing ONE connection must NOT mark the user offline")
        await ws1.close()
        await asyncio.sleep(0.6)
        p = await presence_of("alice")
        check("user remains ONLINE with a sibling connection live",
              p["state"] == "ONLINE", str(p))
        check("connection count decremented to 1", p["connections"] == 1, str(p))

        print("\n5. IDLE is client-reportable, ONLINE/OFFLINE are derived")
        await ws2.send(json.dumps({"type": "PRESENCE", "state": "IDLE"}))
        await asyncio.sleep(0.3)
        p = await presence_of("alice")
        check("user reports IDLE", p["state"] == "IDLE", str(p))
        await ws2.send(json.dumps({"type": "PRESENCE", "state": "ONLINE"}))
        await asyncio.sleep(0.3)
        p = await presence_of("alice")
        check("user back to ONLINE", p["state"] == "ONLINE", str(p))

        print("\n6. Closing the LAST connection marks the user OFFLINE")
        await ws2.close()
        await asyncio.sleep(0.6)
        p = await presence_of("alice")
        check("user is OFFLINE after the last connection closes",
              p["state"] == "OFFLINE" and p["connections"] == 0, str(p))

        print("\n7. Presence is tenant-scoped (Section 14)")
        ws3, _ = await connect(key, user_id="bob")
        await asyncio.sleep(0.3)
        other = (await http.get(f"{GATEWAY}/v1/presence/bob",
                                headers={"Authorization": f"Bearer {key2}"})).json()
        check("another tenant cannot see this tenant's user",
              other["state"] == "OFFLINE", str(other))
        mine = await presence_of("bob")
        check("own tenant sees the user online", mine["state"] == "ONLINE", str(mine))

        listing = (await http.get(f"{GATEWAY}/v1/presence", headers=auth)).json()
        check("tenant presence listing includes the online user",
              any(u["user_id"] == "bob" for u in listing), str(listing))
        await ws3.close()

    print(f"\n{'='*60}\nPASSED: {len(PASS)}   FAILED: {len(FAIL)}")
    if FAIL:
        print("Failures: " + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
