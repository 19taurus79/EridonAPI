"""
error_notifier.py — Модуль уведомлений об ошибках в Telegram.

При любом необработанном исключении (500) или критической ошибке
отправляет сообщение всем администраторам из ADMINS_ID.
"""
from __future__ import annotations

import traceback
import asyncio
from datetime import datetime, timezone
from typing import Optional

from aiogram.exceptions import TelegramAPIError

from .config import logger, ADMINS_ID, TELEGRAM_BOT_TOKEN

# Ленивый импорт bot, чтобы не создавать циклических зависимостей
_bot = None


def _get_bot():
    """Возвращает экземпляр бота из config (ленивый импорт)."""
    global _bot
    if _bot is None:
        try:
            from .config import bot
            _bot = bot
        except ImportError:
            pass
    return _bot


def _format_error_message(
    error: Exception,
    path: Optional[str] = None,
    method: Optional[str] = None,
    user_id: Optional[int] = None,
    extra: Optional[str] = None,
) -> str:
    """Форматирует сообщение об ошибке для Telegram."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    tb = traceback.format_exc()
    # Обрезаем трейсбек до 1500 символов чтобы не превышать лимит Telegram
    if len(tb) > 1500:
        tb = "..." + tb[-1500:]

    parts = [
        "🚨 <b>ОШИБКА НА СЕРВЕРЕ</b>",
        f"🕐 <code>{now}</code>",
    ]

    if method and path:
        parts.append(f"📡 <code>{method} {path}</code>")
    elif path:
        parts.append(f"📡 <code>{path}</code>")

    if user_id:
        parts.append(f"👤 user_id: <code>{user_id}</code>")

    parts.append(f"❌ <b>{type(error).__name__}</b>: {str(error)[:200]}")

    if extra:
        parts.append(f"ℹ️ {extra}")

    parts.append(f"<pre>{tb}</pre>")

    return "\n".join(parts)


async def notify_admins_error(
    error: Exception,
    path: Optional[str] = None,
    method: Optional[str] = None,
    user_id: Optional[int] = None,
    extra: Optional[str] = None,
) -> None:
    """
    Асинхронно отправляет уведомление об ошибке всем администраторам.
    Никогда не поднимает исключения — ошибки нотификации логируются тихо.
    """
    if not ADMINS_ID:
        logger.warning("error_notifier: ADMINS_ID пустой, некому отправлять уведомления")
        return

    bot = _get_bot()
    if not bot or not TELEGRAM_BOT_TOKEN:
        logger.warning("error_notifier: бот не инициализирован, не могу отправить уведомление")
        return

    message = _format_error_message(error, path=path, method=method, user_id=user_id, extra=extra)

    for admin_id in ADMINS_ID:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=message,
                parse_mode="HTML",
            )
        except TelegramAPIError as tg_err:
            logger.error(f"error_notifier: не удалось отправить в Telegram admin {admin_id}: {tg_err}")
        except Exception as e:
            logger.error(f"error_notifier: неожиданная ошибка при отправке admin {admin_id}: {e}")


def notify_admins_error_sync(
    error: Exception,
    path: Optional[str] = None,
    method: Optional[str] = None,
    user_id: Optional[int] = None,
    extra: Optional[str] = None,
) -> None:
    """
    Синхронная обёртка для вызова из синхронного кода.
    Запускает уведомление как fire-and-forget задачу в текущем event loop.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Если event loop уже запущен (внутри FastAPI/uvicorn) — создаём task
            loop.create_task(
                notify_admins_error(error, path=path, method=method, user_id=user_id, extra=extra)
            )
        else:
            loop.run_until_complete(
                notify_admins_error(error, path=path, method=method, user_id=user_id, extra=extra)
            )
    except Exception as e:
        logger.error(f"error_notifier: ошибка в notify_admins_error_sync: {e}")
