"""Create a demo tenant + API key against a running control plane."""
import asyncio, sys
import httpx

CONTROL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as http:
        tenant = (await http.post(f"{CONTROL}/admin/tenants", json={"name": "demo"})).json()
        key = (await http.post(f"{CONTROL}/admin/api-keys", json={"tenant_id": tenant["id"]})).json()
        print(f"tenant_id : {tenant['id']}")
        print(f"api_key   : {key['api_key']}")
        print(f"channel   : t-{tenant['id']}:room-engineering")
        print("\nThe API key is shown once and stored only as a hash.")


asyncio.run(main())
