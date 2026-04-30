import logging
from typing import List

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

# Настраиваем логирование для отслеживания процесса отправки
logger = logging.getLogger(__name__)


async def send_notification(
    bot: Bot, chat_ids: List[int], text: str, parse_mode: str = "Markdown", reply_markup = None
):
    """
    Асинхронно отправляет текстовое сообщение списку пользователей.
    Повертає список відправлених об'єктів Message.
    """
    if not text:
        logger.warning("Попытка отправить пустое сообщение. Отправка отменена.")
        return []

    sent_messages = []
    for chat_id in chat_ids:
        try:
            msg = await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            sent_messages.append(msg)
            logger.info(f"Сообщение успешно отправлено в чат {chat_id}")
        except TelegramAPIError as e:
            logger.error(f"Ошибка API Telegram при отправке в чат {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при отправке в чат {chat_id}: {e}")
    
    return sent_messages
