# app/utils.py
from datetime import datetime, timedelta
from aiogram import Bot
from dotenv import load_dotenv
import os

from .config import MANAGERS_ID, TELEGRAM_BOT_TOKEN  # Импорт из config.py
from .tables import Users

load_dotenv()

# Определяем режим работы: 'production' или 'development'. По умолчанию 'development'.
APP_ENV = os.getenv("APP_ENV", "development")

bot = Bot(TELEGRAM_BOT_TOKEN)


async def send_message_to_managers():
    """Отправляет сообщение всем менеджерам в Telegram или выводит в консоль в зависимости от режима."""
    user_tg_id_list = await Users.select().where(Users.is_allowed == True).run()
    now = datetime.now() + timedelta(hours=3)
    time_format = "%d-%m-%Y %H:%M:%S"
    message_text = f"Дані в боті оновлені.\nІ вони актуальні станом на… {now.strftime(time_format)}"

    print(f"--- Running in {APP_ENV.upper()} mode ---")

    for user in user_tg_id_list:
        telegram_id = user.get("telegram_id")
        if not telegram_id:
            print(f"Skipping user due to missing telegram_id: {user}")
            continue

        try:
            if APP_ENV == "production":
                await bot.send_message(chat_id=telegram_id, text=message_text)
                print(f"Successfully sent message to manager ID: {telegram_id}")
            else:
                # В режиме разработки просто выводим в консоль
                print(f"DEV MODE: Would send to {telegram_id}: '{message_text}'")
        except Exception as e:
            print(f"Failed to send message to manager ID {telegram_id}: {e}")


def create_composite_key_from_dict(item: dict, keys: list) -> str:
    """
    Создает стандартизированный композитный ключ из словаря.
    """
    key_parts = []
    for key in keys:
        value = item.get(key)
        key_parts.append(str(value).strip() if value is not None else "")
    return "_".join(key_parts)
