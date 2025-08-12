# app/config.py
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# --- Настройки Telegram Бота ---
# Ваш токен Telegram бота. Получается у BotFather.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ID менеджеров Telegram, которым будут отправляться уведомления.
# Это должен быть словарь, где ключ - это имя (для удобства), а значение - Telegram ID (число).
# Пример: {"manager_name_1": 123456789, "manager_name_2": 987654321}
MANAGERS_ID = {
    "YourNameHere": int(
        os.getenv("MANAGER_TELEGRAM_ID_1", "0")
    ),  # Замените на реальные ID
    # Добавьте больше менеджеров по необходимости
}

# --- Валидация данных для обработки Excel ---
# Эти списки используются для фильтрации данных в Pandas.
# Замените на реальные значения, которые ожидаются в ваших данных Excel.
valid_line_of_business = [
    "Власне виробництво насіння",
    "Демо-продукція",
    "ЗЗР",
    "Міндобрива (основні)",
    "Насіння",
    "Позакореневi добрива",
]

valid_warehouse = [
    'Харківський підрозділ  ТОВ "Фірма Ерідон" с.Коротич',
    'Харківський підрозділ  ТОВ "Фірма Ерідон" м.Балаклія',
]

# --- Общие настройки приложения ---
# SECRET_KEY для JWT (хотя мы убрали JWT, может пригодиться для других целей)
# Его также следует загружать из .env для безопасности
SECRET_KEY = os.getenv("SECRET_KEY", "your_super_secret_fallback_key")

# Время жизни токена (если JWT снова будет использоваться)
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

# Учетные данные для админа (если нужны для внутренних инструментов, а не для публичного API)
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
