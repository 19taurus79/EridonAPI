"""
Диагностика: ищем товар во всех таблицах и смотрим как он там хранится.
Запуск: python diagnose_product.py
"""
import asyncio
import asyncpg
import re

# --- Прямое подключение к БД ---
DB_DSN = "postgresql://taurus:56LaWT7T@195.189.226.96:5432/taurus_db_test"
SEARCH = "ЛГ59580"


async def main():
    conn = await asyncpg.connect(DB_DSN)

    print(f"\n{'='*70}")
    print(f"ДІАГНОСТИКА ТОВАРУ: містить '{SEARCH}'")
    print(f"{'='*70}")

    # 1. ProductGuide — всі записи з SEARCH
    print(f"\n[1] ProductGuide — всі записи:")
    rows = await conn.fetch(
        "SELECT id, product FROM product_guide WHERE product ILIKE $1",
        f"%{SEARCH}%"
    )
    if not rows:
        print("   ❌ НЕ ЗНАЙДЕНО в ProductGuide!")
    pg_ids = []
    pg_names = []
    for r in rows:
        print(f"   id  : {r['id']}")
        print(f"   name: [{r['product']}]")
        print(f"   len : {len(r['product'])} символів")
        print()
        pg_ids.append(str(r['id']))
        pg_names.append(r['product'])

    # 2. Submissions — через FK на product_guide
    print(f"\n[2] Submissions — через JOIN на product_guide:")
    rows = await conn.fetch("""
        SELECT pg.product AS product_name, s.line_of_business, s.different, s.document_status
        FROM submissions s
        JOIN product_guide pg ON s.product = pg.id
        WHERE pg.product ILIKE $1
        LIMIT 5
    """, f"%{SEARCH}%")
    if not rows:
        print("   ❌ НЕ ЗНАЙДЕНО в Submissions!")
    sub_names = []
    for r in rows:
        print(f"   product  : [{r['product_name']}]")
        print(f"   different: {r['different']}")
        print(f"   status   : {r['document_status']}")
        print(f"   lob      : {r['line_of_business']}")
        print()
        sub_names.append(r['product_name'])

    # 3. Remains — через FK на product_guide
    print(f"\n[3] Remains — через JOIN на product_guide:")
    rows = await conn.fetch("""
        SELECT pg.product AS product_name, r.warehouse, r.buh, r.skl
        FROM remains r
        JOIN product_guide pg ON r.product = pg.id
        WHERE pg.product ILIKE $1
        LIMIT 5
    """, f"%{SEARCH}%")
    if not rows:
        print("   ❌ НЕ ЗНАЙДЕНО в Remains!")
    rem_names = []
    for r in rows:
        print(f"   product  : [{r['product_name']}]")
        print(f"   warehouse: {r['warehouse']}")
        print(f"   buh      : {r['buh']}")
        print()
        rem_names.append(r['product_name'])

    # 4. valid_free_stock — напряму по рядку product
    print(f"\n[4] valid_free_stock (VIEW) — рядок product напряму:")
    rows = await conn.fetch("""
        SELECT product, division, warehouse, free_qty, buh_qty
        FROM valid_free_stock
        WHERE product ILIKE $1
    """, f"%{SEARCH}%")
    if not rows:
        print("   ❌ НЕ ЗНАЙДЕНО в valid_free_stock!")
    vfs_names = []
    for r in rows:
        print(f"   product  : [{r['product']}]")
        print(f"   len      : {len(r['product'])} символів")
        print(f"   division : {r['division']}")
        print(f"   free_qty : {r['free_qty']}")
        print()
        vfs_names.append(r['product'])

    # 5. free_stock — через FK на product_guide
    print(f"\n[5] free_stock — через JOIN на product_guide:")
    rows = await conn.fetch("""
        SELECT pg.product AS product_name, fs.division, fs.warehouse, fs.free_qty
        FROM free_stock fs
        JOIN product_guide pg ON fs.product = pg.id
        WHERE pg.product ILIKE $1
        LIMIT 10
    """, f"%{SEARCH}%")
    if not rows:
        print("   ❌ НЕ ЗНАЙДЕНО в free_stock!")
    fs_names = []
    for r in rows:
        print(f"   product  : [{r['product_name']}]")
        print(f"   division : {r['division']}")
        print(f"   free_qty : {r['free_qty']}")
        print()
        fs_names.append(r['product_name'])

    # 6. ПОРІВНЯННЯ РЯДКІВ
    print(f"\n{'='*70}")
    print(f"[6] ПОРІВНЯННЯ РЯДКІВ product між джерелами:")
    print(f"{'='*70}")
    u_sub = list(set(sub_names))
    u_rem = list(set(rem_names))
    u_vfs = list(set(vfs_names))
    u_fs  = list(set(fs_names))
    u_pg  = list(set(pg_names))

    print(f"\n   ProductGuide  : {u_pg}")
    print(f"   Submissions   : {u_sub}")
    print(f"   Remains       : {u_rem}")
    print(f"   free_stock    : {u_fs}")
    print(f"   valid_free_stock: {u_vfs}")

    print(f"\n   Submissions == Remains?          {set(u_sub) == set(u_rem)}")
    print(f"   Submissions == valid_free_stock? {set(u_sub) == set(u_vfs)}")
    print(f"   Remains == valid_free_stock?     {set(u_rem) == set(u_vfs)}")

    # Знаходимо перше відхилення між рядками
    if u_sub and u_vfs and u_sub[0] != u_vfs[0]:
        s, v = u_sub[0], u_vfs[0]
        print(f"\n   ⚠️  РЯДКИ РІЗНІ!")
        print(f"      Submissions   : {repr(s)}")
        print(f"      valid_free_stock: {repr(v)}")
        for i, (cs, cv) in enumerate(zip(s, v)):
            if cs != cv:
                ctx_s = s[max(0,i-3):i+5]
                ctx_v = v[max(0,i-3):i+5]
                print(f"      Перша відмінність на позиції {i}:")
                print(f"        Submissions   : ...{repr(ctx_s)}...")
                print(f"        valid_free_stock: ...{repr(ctx_v)}...")
                break
        if len(s) != len(v):
            print(f"      Довжини різні: Submissions={len(s)}, valid_free_stock={len(v)}")

    await conn.close()
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
