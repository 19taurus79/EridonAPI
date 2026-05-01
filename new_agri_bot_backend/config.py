# app/config.py
import os
import logging
import sys
import json
from aiogram import Bot
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# --- Настройка логирования ---
# Сначала настраиваем логирование, чтобы logger был доступен во всем файле
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("app.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("agri_bot")
# Приглушуємо логування APScheduler, щоб він не спамив у консоль щохвилини
logging.getLogger("apscheduler").setLevel(logging.WARNING)

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
    # Декодируем JSON и приводим каждый ID к типу int для надежности
    ADMINS_ID = [int(admin_id) for admin_id in json.loads(ADMINS_JSON)]
except Exception as e:
    logger.error(f"Error parsing ADMINS_ID: {e}")
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

# --- Настройки CORS ---
CORS_ORIGINS = [
    "https://taurus.pp.ua",
    "https://eridon-react.vercel.app",
    "https://eridon-bot-next-js.vercel.app",
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8000",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://telegram-mini-app-six-inky.vercel.app",
    "https://geocode-six.vercel.app",
    "https://paravail-aubrianna-noncrystalline.ngrok-free.dev",
    "http://eridon-dev.local",
]

# Инициализация бота
if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN not found in environment variables")
else:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
