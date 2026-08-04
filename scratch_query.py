import asyncio
import os
import json
os.environ["PICCOLO_CONF"] = "piccolo_conf"
from new_agri_bot_backend.tables import Submissions

async def main():
    rows = await Submissions.select(
        Submissions.client, 
        Submissions.contract_supplement, 
        Submissions.shipping_warehouse, 
        Submissions.division,
        Submissions.product.product.as_alias("product")
    ).where(Submissions.contract_supplement == 'ТЕ-00055595')
    
    with open("scratch_output5.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    
if __name__ == "__main__":
    asyncio.run(main())
