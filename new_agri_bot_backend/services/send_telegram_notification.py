import logging
from typing import List

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

# Настраиваем логирование для отслеживания процесса отправки
logger = logging.getLogger(__name__)


async def send_notification(
    bot: Bot, chat_ids: List[int], text: str, parse_mode: str = "Markdown"
):
    """
    Асинхронно отправляет текстовое сообщение списку пользователей.

    :param bot: Экземпляр aiogram.Bot для отправки сообщений.
    :param chat_ids: Список ID чатов (пользователей), которым нужно отправить сообщение.
    :param text: Текст сообщения.
    :param parse_mode: Режим форматирования текста ('Markdown' или 'HTML').
    """
    if not text:
        logger.warning("Попытка отправить пустое сообщение. Отправка отменена.")
        return

    for chat_id in chat_ids:
        try:
            await bot.send_message(
                chat_id=chat_id[0]["telegram_id"], text=text, parse_mode=parse_mode
            )
            logger.info(f"Сообщение успешно отправлено в чат {chat_id}")
        except TelegramAPIError as e:
            # Обрабатываем ошибки, если пользователь заблокировал бота, чат не найден и т.д.
            logger.error(f"Ошибка API Telegram при отправке в чат {chat_id}: {e}")
        except Exception as e:
            # Обрабатываем другие возможные ошибки (например, сетевые)
            logger.error(f"Непредвиденная ошибка при отправке в чат {chat_id}: {e}")
