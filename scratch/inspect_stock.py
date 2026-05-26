import asyncio
import sys
from piccolo.engine import engine_finder
from new_agri_bot_backend.tables import AvailableStock

sys.stdout.reconfigure(encoding='utf-8')

async def main():
    engine = engine_finder()
    await engine.start_connection_pool()
    try:
        columns = await AvailableStock.raw(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'available_stock'"
        ).run()
        print("Columns in available_stock:")
        for col in columns:
            print(f" - {col['column_name']}")
    finally:
        await engine.close_connection_pool()

if __name__ == '__main__':
    asyncio.run(main())
