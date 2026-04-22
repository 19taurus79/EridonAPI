import sqlite3
import pandas as pd

conn = sqlite3.connect('piccolo.sqlite')

# Список таблиц
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", [t[0] for t in tables])

# Ищем таблицу с moved
for t in tables:
    name = t[0]
    if 'moved' in name.lower() or 'submission' in name.lower() or 'product' in name.lower():
        print(f"\n--- Таблица: {name} ---")
        try:
            sample = pd.read_sql(f'SELECT * FROM "{name}" LIMIT 3', conn)
            print(sample.columns.tolist())
        except Exception as e:
            print(f"  Ошибка: {e}")

conn.close()
