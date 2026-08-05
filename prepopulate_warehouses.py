import asyncio
import os
import sys

# setup path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
os.environ["PICCOLO_CONF"] = "piccolo_conf"

from new_agri_bot_backend.tables import ValidWarehouseAdmin

async def main():
    warehouses = [
        'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич',
        'Полтавський підрозділ  ТОВ "Фірма Ерідон" с.Супрунівка (Харківський підрозділ)',
        'Харківський підрозділ  ТОВ "Фірма Ерідон" м.Балаклія',
        'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (Транзитний склад)',
    ]
    for w in warehouses:
        exists = await ValidWarehouseAdmin.exists().where(ValidWarehouseAdmin.name == w)
        if not exists:
            await ValidWarehouseAdmin.insert(ValidWarehouseAdmin(name=w))
            print(f"Added {w}")

if __name__ == "__main__":
    asyncio.run(main())
