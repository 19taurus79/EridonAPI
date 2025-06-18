# data_loader_api/app/database.py
from piccolo.engine.postgres import PostgresEngine
from config import DATABASE_URL
import urllib.parse

# Парсим DATABASE_URL
# urllib.parse.urlparse помогает разобрать URL на компоненты
parsed_url = urllib.parse.urlparse(DATABASE_URL)

DB_CONFIG = {
    "host": parsed_url.hostname,
    "database": parsed_url.path.lstrip("/"), # Убираем ведущий слэш
    "user": parsed_url.username,
    "password": parsed_url.password,
    "port": parsed_url.port if parsed_url.port else 5432 # Если порт не указан, используем 5432 по умолчанию
}

# Создаем экземпляр движка Piccolo для PostgreSQL
DB = PostgresEngine(config=DB_CONFIG)