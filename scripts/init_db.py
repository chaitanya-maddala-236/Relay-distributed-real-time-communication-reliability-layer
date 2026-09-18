import asyncio
from packages.database.engine import engine
from packages.database.models import Base

async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("schema created")

asyncio.run(main())
