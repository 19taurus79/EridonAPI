# app/config.py
import os
import logging
import sys
import json
from aiogram import Bot
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# --- Настройки Telegram Бота ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_WIDGET_BOT_TOKEN = os.getenv("TELEGRAM_WIDGET_BOT_TOKEN", TELEGRAM_BOT_TOKEN)

# ID менеджеров Telegram, которым будут отправляться уведомления.
MANAGERS_ID = {
    "YourNameHere": int(os.getenv("MANAGER_TELEGRAM_ID_1", "0")),
}

# Список ID администраторов (из JSON строки в .env)
ADMINS_JSON = os.getenv("ADMINS", "[]")
try:
    ADMINS_ID = json.loads(ADMINS_JSON)
except Exception:
    ADMINS_ID = []

# ID логістів (з .env або за замовчуванням)
LOGISTICS_IDS_JSON = os.getenv("LOGISTICS_TELEGRAM_IDS", "[548019148, 7953178333, 1060393824]")
try:
    LOGISTICS_TELEGRAM_IDS = json.loads(LOGISTICS_IDS_JSON)
except Exception:
    LOGISTICS_TELEGRAM_IDS = [548019148, 7953178333, 1060393824]

# --- Валидация данных для обработки Excel ---
valid_line_of_business = [
    "Власне виробництво насіння",
    "Демо-продукція",
    "ЗЗР",
    "Міндобрива (основні)",
    "Насіння",
    "Позакореневi добрива",
]

valid_warehouse = [
    'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич',
    'Харківський підрозділ  ТОВ "Фірма Ерідон" м.Балаклія',
    'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (Транзитний склад)',
]

# --- Общие настройки приложения ---
SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    logger.warning("SECRET_KEY is not set. Security will be compromised!")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Nova Poshta API Key
NP_API_KEY = os.getenv("NP_API_KEY")

SEND_NOTIFICATIONS = os.getenv("SEND_NOTIFICATIONS", "true").lower() == "true"
BACKEND_URL = os.getenv("BACKEND_URL", "")

# Инициализация бота
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("app.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("agri_bot")

