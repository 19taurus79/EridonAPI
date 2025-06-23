# app/utils.py
from datetime import datetime, timedelta
from aiogram import Bot
from dotenv import load_dotenv
import os

from .config import MANAGERS_ID, TELEGRAM_BOT_TOKEN  # Импорт из config.py

load_dotenv()

bot = Bot(TELEGRAM_BOT_TOKEN)


async def send_message_to_managers():
    """Отправляет сообщение всем менеджерам в Telegram."""
    user_tg_id = MANAGERS_ID.values()
    now = datetime.now() + timedelta(
        hours=3
    )  # Убедитесь, что это правильное смещение часового пояса
    time_format = "%d-%m-%Y %H:%M:%S"
    message_text = (
        f"Дані в боті оновлені.{chr(10)}І вони актуальні станом на… {now:{time_format}}"
    )

    for i in user_tg_id:
        try:
            await bot.send_message(chat_id=i, text=message_text)
            print(f"Sent message to manager ID: {i}")
        except Exception as e:
            print(f"Failed to send message to manager ID {i}: {e}")
