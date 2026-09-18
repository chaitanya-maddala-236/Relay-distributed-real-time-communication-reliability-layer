"""Multi-node distributed test (Section 113).

Two gateway processes share one Redis. A client connected to node A must
receive messages published through node B, and a session created on one
node must be resumable on the other (Section 129: sticky sessions are not
the reliability mechanism).

Run with both gateways up on ports 8000 (relay-node-a) and 8010
(relay-node-b).
"""
from __future__ import annotations

import asyncio
import json
import sys

import httpx
import websockets

CONTROL = "http://127.0.0.1:8001"
NODE_A_HTTP = "http://127.0.0.1:8000"
NODE_B_HTTP = "http://127.0.0.1:8010"
NODE_A_WS = "ws://127.0.0.1:8000/ws"
NODE_B_WS = "ws://127.0.0.1:8010/ws"

PASS, FAIL = [], []


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))


async def connect(url: str, token: str, session_id: str | None = None):
    ws = await websockets.connect(url, max_size=2_000_000)
    await ws.send(json.dumps({"type": "CONNECT", "token": token, "session_id": session_id}))
    return ws, json.loads(await ws.recv())


async def subscribe(ws, channel: str):
    await ws.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
    return json.loads(await ws.recv())


async def recv_messages(ws, count: int, timeout: float = 5.0):
    out = []
    try:
        while len(out) < count:
            frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
            if frame.get("type") == "MESSAGE":
                out.append(frame)
    except TimeoutError:
        pass
    return out


async def main() -> int:
    async with httpx.AsyncClient(timeout=10) as http:
        t = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "multinode"})).json()
        key = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t["id"]})).json()["api_key"]
        auth = {"Authorization": f"Bearer {key}"}
        channel = f"t-{t['id']}:room-distributed"

        print("\n1. Cross-node fanout (Section 31, 113)")
        ws_a, c_a = await connect(NODE_A_WS, key)
        ws_b, c_b = await connect(NODE_B_WS, key)
        check("client A landed on node a", c_a["node_id"] == "relay-node-a", c_a["node_id"])
        check("client B landed on node b", c_b["node_id"] == "relay-node-b", c_b["node_id"])

        await subscribe(ws_a, channel)
        await subscribe(ws_b, channel)
        await asyncio.sleep(0.4)

        # Publish through node A only.
        r = await http.post(f"{NODE_A_HTTP}/v1/publish", headers=auth,
                            json={"channel": channel, "payload": {"from": "node-a"}})
        seq = r.json()["sequence"]
        got_a = await recv_messages(ws_a, 1)
        got_b = await recv_messages(ws_b, 1)
        check("local subscriber (node a) received it", len(got_a) == 1)
        check("remote subscriber (node b) received it via Redis Pub/Sub", len(got_b) == 1)
        if got_a and got_b:
            check("same message_id across nodes", got_a[0]["message_id"] == got_b[0]["message_id"])
            check("same sequence across nodes", got_a[0]["sequence"] == got_b[0]["sequence"] == seq)

        print("\n2. Shared sequence space across nodes (Section 80)")
        seqs = []
        for i in range(10):
            target = NODE_A_HTTP if i % 2 == 0 else NODE_B_HTTP
            resp = await http.post(f"{target}/v1/publish", headers=auth,
                                   json={"channel": channel, "payload": {"i": i}})
            seqs.append(resp.json()["sequence"])
        check("sequence is globally monotonic for the channel regardless of publishing node",
              seqs == sorted(seqs) and len(set(seqs)) == len(seqs), str(seqs))

        received_a = await recv_messages(ws_a, 10)
        received_b = await recv_messages(ws_b, 10)
        check("node a subscriber got all 10", len(received_a) == 10, f"got {len(received_a)}")
        check("node b subscriber got all 10", len(received_b) == 10, f"got {len(received_b)}")
        check("both nodes delivered the same ordering",
              [m["sequence"] for m in received_a] == [m["sequence"] for m in received_b])

        print("\n3. Session portability across nodes (Section 129)")
        last = received_a[-1]["sequence"] if received_a else 0
        await ws_a.send(json.dumps({"type": "ACK", "channel": channel, "sequence": last}))
        await asyncio.sleep(0.2)
        session_id = c_a["session_id"]
        await ws_a.close()

        for i in range(3):
            await http.post(f"{NODE_A_HTTP}/v1/publish", headers=auth,
                            json={"channel": channel, "payload": {"missed": i}})

        # Reconnect the SAME session to a DIFFERENT node.
        ws_moved, c_moved = await connect(NODE_B_WS, key, session_id=session_id)
        check("session resumed on a different node", c_moved["session_id"] == session_id)
        check("served by node b now", c_moved["node_id"] == "relay-node-b", c_moved["node_id"])

        await ws_moved.send(json.dumps({"type": "RESUME", "session_id": session_id,
                                        "last_ack_by_channel": {channel: last}}))
        replayed, resumed = [], None
        for _ in range(8):
            frame = json.loads(await asyncio.wait_for(ws_moved.recv(), timeout=5))
            if frame["type"] == "MESSAGE":
                replayed.append(frame)
            elif frame["type"] == "RESUMED":
                resumed = frame
                break
        check("RESUMED on the new node", resumed is not None)
        check("missed messages replayed after cross-node move", len(replayed) == 3, f"got {len(replayed)}")

        print("\n4. Both nodes visible in the registry (Section 54)")
        nodes = (await http.get(f"{CONTROL}/admin/nodes")).json()
        ids = {n["node_id"] for n in nodes}
        check("both nodes registered", {"relay-node-a", "relay-node-b"} <= ids, str(ids))

        for ws in (ws_b, ws_moved):
            await ws.close()

    print(f"\n{'='*60}\nPASSED: {len(PASS)}   FAILED: {len(FAIL)}")
    if FAIL:
        print("Failures: " + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
