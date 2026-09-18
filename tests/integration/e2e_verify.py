"""End-to-end verification of the Relay protocol against running services.

This is not a mock: it provisions a real tenant + API key through the
control plane, opens real WebSocket connections to the gateway, publishes
through the real HTTP publish API, and asserts on what actually arrives.

Scenarios covered (mapping to PRD demo sections):
  1. handshake + fanout to multiple subscribers      (Demo 1, Section 134)
  2. sequence monotonicity + channel ordering         (Section 21-22, 43)
  3. disconnect -> publish -> reconnect -> RESUME     (Demo 2, Section 135)
  4. idempotent publish                               (Section 79)
  5. cross-tenant subscribe is rejected               (Section 14, 34)
  6. invalid auth is rejected                         (Section 13)
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


def check(name: str, condition: bool, detail: str = "") -> None:
    (PASS if condition else FAIL).append(name)
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {name}" + (f" -- {detail}" if detail and not condition else ""))


async def provision(http: httpx.AsyncClient, name: str) -> tuple[str, str]:
    t = (await http.post(f"{CONTROL}/admin/tenants", json={"name": name})).json()
    k = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": t["id"]})).json()
    return t["id"], k["api_key"]


async def connect(token: str, session_id: str | None = None):
    ws = await websockets.connect(WS, max_size=2_000_000)
    await ws.send(json.dumps({"type": "CONNECT", "token": token, "session_id": session_id}))
    connected = json.loads(await ws.recv())
    return ws, connected


async def subscribe(ws, channel: str) -> dict:
    await ws.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
    return json.loads(await ws.recv())


async def recv_messages(ws, count: int, timeout: float = 5.0) -> list[dict]:
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
        tenant_a, key_a = await provision(http, "acme")
        tenant_b, key_b = await provision(http, "globex")
        auth_a = {"Authorization": f"Bearer {key_a}"}
        channel = f"t-{tenant_a}:room-engineering"

        print("\n1. Handshake + multi-subscriber fanout (Demo 1)")
        ws1, c1 = await connect(key_a)
        ws2, c2 = await connect(key_a)
        check("CONNECTED returns connection_id", bool(c1.get("connection_id")), str(c1))
        check("CONNECTED returns session_id", bool(c1.get("session_id")), str(c1))
        check("each connection gets a distinct connection_id",
              c1["connection_id"] != c2["connection_id"])
        check("both connections share no session", c1["session_id"] != c2["session_id"])

        s1 = await subscribe(ws1, channel)
        s2 = await subscribe(ws2, channel)
        check("SUBSCRIBED ack returned", s1.get("type") == "SUBSCRIBED" and s2.get("type") == "SUBSCRIBED")
        await asyncio.sleep(0.3)  # let pubsub listener attach

        r = await http.post(f"{GATEWAY}/v1/publish", headers=auth_a,
                            json={"channel": channel, "payload": {"text": "hello"}})
        check("publish accepted", r.status_code == 200, r.text)
        published = r.json()

        got1 = await recv_messages(ws1, 1)
        got2 = await recv_messages(ws2, 1)
        check("subscriber 1 received the message", len(got1) == 1)
        check("subscriber 2 received the message (fanout)", len(got2) == 1)
        if got1 and got2:
            check("both received identical message_id",
                  got1[0]["message_id"] == got2[0]["message_id"] == published["message_id"])
            check("envelope carries sequence + channel + payload",
                  got1[0]["sequence"] == published["sequence"]
                  and got1[0]["channel"] == channel
                  and got1[0]["payload"]["text"] == "hello")

        print("\n2. Sequence monotonicity and per-channel ordering")
        N = 25
        for i in range(N):
            await http.post(f"{GATEWAY}/v1/publish", headers=auth_a,
                            json={"channel": channel, "payload": {"n": i}})
        ordered = await recv_messages(ws1, N)
        seqs = [m["sequence"] for m in ordered]
        check(f"all {N} messages delivered", len(ordered) == N, f"got {len(ordered)}")
        check("sequences strictly increasing", seqs == sorted(seqs) and len(set(seqs)) == len(seqs))
        check("payload order preserved", [m["payload"]["n"] for m in ordered] == list(range(N)))

        other_channel = f"t-{tenant_a}:room-other"
        r_other = await http.post(f"{GATEWAY}/v1/publish", headers=auth_a,
                                  json={"channel": other_channel, "payload": {"x": 1}})
        check("separate channel has its own sequence space",
              r_other.json()["sequence"] == 1, r_other.text)

        print("\n3. Disconnect -> publish -> reconnect -> RESUME (Demo 2)")
        last_seq = seqs[-1] if seqs else 0
        await ws1.send(json.dumps({"type": "ACK", "channel": channel, "sequence": last_seq}))
        await asyncio.sleep(0.2)
        session_to_resume = c1["session_id"]
        await ws1.close()

        missed = 3
        for i in range(missed):
            await http.post(f"{GATEWAY}/v1/publish", headers=auth_a,
                            json={"channel": channel, "payload": {"missed": i}})

        ws3, c3 = await connect(key_a, session_id=session_to_resume)
        check("session survived the connection (same session_id)",
              c3["session_id"] == session_to_resume, f"{c3['session_id']} != {session_to_resume}")
        check("new connection_id issued on reconnect", c3["connection_id"] != c1["connection_id"])

        await ws3.send(json.dumps({"type": "RESUME", "session_id": session_to_resume,
                                   "last_ack_by_channel": {channel: last_seq}}))
        replayed = []
        resumed = None
        for _ in range(missed + 2):
            frame = json.loads(await asyncio.wait_for(ws3.recv(), timeout=5))
            if frame["type"] == "MESSAGE":
                replayed.append(frame)
            elif frame["type"] == "RESUMED":
                resumed = frame
                break
        check("RESUMED frame returned", resumed is not None)
        check(f"exactly the {missed} missed messages replayed", len(replayed) == missed,
              f"got {len(replayed)}")
        check("replayed messages are the ones after last ack",
              all(m["sequence"] > last_seq for m in replayed))
        check("replay preserves order",
              [m["sequence"] for m in replayed] == sorted(m["sequence"] for m in replayed))

        print("\n4. Idempotent publish (Section 79)")
        headers_idem = {**auth_a, "Idempotency-Key": "test-key-001"}
        first = (await http.post(f"{GATEWAY}/v1/publish", headers=headers_idem,
                                 json={"channel": channel, "payload": {"once": True}})).json()
        second = (await http.post(f"{GATEWAY}/v1/publish", headers=headers_idem,
                                  json={"channel": channel, "payload": {"once": True}})).json()
        check("retried publish returns the original message_id",
              first["message_id"] == second["message_id"], f"{first} vs {second}")
        check("retried publish does not allocate a new sequence",
              first["sequence"] == second["sequence"])

        print("\n5. Tenant isolation (Section 14, 34)")
        ws_b, _ = await connect(key_b)
        resp = await subscribe(ws_b, channel)  # tenant A's channel
        check("cross-tenant SUBSCRIBE rejected",
              resp.get("type") == "ERROR" and resp.get("code") == "CHANNEL_FORBIDDEN", str(resp))
        pub_cross = await http.post(f"{GATEWAY}/v1/publish",
                                    headers={"Authorization": f"Bearer {key_b}"},
                                    json={"channel": channel, "payload": {}})
        check("cross-tenant publish rejected with 403", pub_cross.status_code == 403,
              f"status={pub_cross.status_code}")
        await ws_b.close()

        print("\n6. Authentication (Section 13)")
        ws_bad = await websockets.connect(WS)
        await ws_bad.send(json.dumps({"type": "CONNECT", "token": "relay_not_a_real_key"}))
        bad = json.loads(await ws_bad.recv())
        check("invalid API key rejected with AUTH_FAILED",
              bad.get("type") == "ERROR" and bad.get("code") == "AUTH_FAILED", str(bad))
        await ws_bad.close()

        no_auth = await http.post(f"{GATEWAY}/v1/publish", json={"channel": channel, "payload": {}})
        check("publish without credentials rejected with 401", no_auth.status_code == 401)

        print("\n7. Protocol validation (Section 33, 106)")
        ws4, _ = await connect(key_a)
        await ws4.send(json.dumps({"type": "SUBSCRIBE", "channel": "bad channel\n*"}))
        invalid = json.loads(await ws4.recv())
        check("unsafe channel name rejected",
              invalid.get("type") == "ERROR" and invalid.get("code") == "INVALID_CHANNEL", str(invalid))
        await ws4.send(json.dumps({"type": "NONSENSE"}))
        unknown = json.loads(await ws4.recv())
        check("unknown frame type rejected", unknown.get("code") == "UNKNOWN_MESSAGE", str(unknown))
        await ws4.close()

        ws_first = await websockets.connect(WS)
        await ws_first.send(json.dumps({"type": "SUBSCRIBE", "channel": channel}))
        pre_connect = json.loads(await ws_first.recv())
        check("frames before CONNECT rejected", pre_connect.get("code") == "INVALID_FRAME", str(pre_connect))
        await ws_first.close()

        print("\n8. Node registration + metrics (Section 54, 64)")
        nodes = (await http.get(f"{CONTROL}/admin/nodes")).json()
        check("gateway node registered in Redis", any(n["node_id"] == "relay-node-a" for n in nodes), str(nodes))
        metrics_text = (await http.get(f"{GATEWAY}/metrics")).text
        check("metrics endpoint exposes Prometheus format",
              "relay_connections_active" in metrics_text and "# TYPE" in metrics_text)

        for ws in (ws2, ws3):
            await ws.close()

    print(f"\n{'='*60}\nPASSED: {len(PASS)}   FAILED: {len(FAIL)}")
    if FAIL:
        print("Failures: " + ", ".join(FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
