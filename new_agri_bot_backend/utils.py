# app/utils.py
from datetime import datetime, timedelta
from aiogram import Bot
from dotenv import load_dotenv
import os

from .config import MANAGERS_ID, TELEGRAM_BOT_TOKEN, logger, SEND_NOTIFICATIONS  # Импорт из config.py
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

    logger.info(f"--- Running in {APP_ENV.upper()} mode ---")

    for user in user_tg_id_list:
        telegram_id = user.get("telegram_id")
        if not telegram_id:
            logger.warning(f"Skipping user due to missing telegram_id: {user}")
            continue

        old_status_msg_id = user.get("status_message_id")

        try:
            if SEND_NOTIFICATIONS:
                # 1. Спробуємо видалити старе закріплене повідомлення, щоб не засмічувати чат
                if old_status_msg_id:
                    try:
                        await bot.delete_message(chat_id=telegram_id, message_id=old_status_msg_id)
                    except Exception:
                        pass
                
                # 2. Надсилаємо нове (воно прийде з повідомленням, що приверне увагу)
                msg = await bot.send_message(chat_id=telegram_id, text=message_text)
                
                # 3. Закріплюємо його в шапці
                try:
                    await bot.pin_chat_message(chat_id=telegram_id, message_id=msg.message_id, disable_notification=True)
                except Exception as pin_err:
                    logger.warning(f"Could not pin message in {telegram_id}: {pin_err}")
                
                # 4. Оновлюємо ID в базі, щоб наступного разу його видалити
                await Users.update({Users.status_message_id: msg.message_id}).where(Users.telegram_id == telegram_id).run()
                
                logger.info(f"Successfully updated status message for manager ID: {telegram_id}")
            else:
                # В режиме разработки просто выводим в консоль
                logger.info(f"NOTIFICATIONS DISABLED: Would send to {telegram_id}: '{message_text}'")
        except Exception as e:
            logger.error(f"Failed to handle notification for manager ID {telegram_id}: {e}")


def create_composite_key_from_dict(item: dict, keys: list) -> str:
    """
    Создает стандартизированный композитный ключ из словаря.
    """
    key_parts = []
    for key in keys:
        value = item.get(key)
        key_parts.append(str(value).strip() if value is not None else "")
    return "_".join(key_parts)

async def schedule_message_deletion(chat_id: int, message_id: int, delay_minutes: int = 30):
    """Реєструє повідомлення для видалення через вказаний час"""
    from .tables import ScheduledDeletions
    from datetime import datetime, timedelta
    
    delete_at = datetime.now() + timedelta(minutes=delay_minutes)
    await ScheduledDeletions.insert(
        ScheduledDeletions(
            chat_id=chat_id,
            message_id=message_id,
            delete_at=delete_at
        )
    ).run()
    logger.info(f"🕒 Повідомлення {message_id} у чаті {chat_id} заплановано до видалення на {delete_at}")
