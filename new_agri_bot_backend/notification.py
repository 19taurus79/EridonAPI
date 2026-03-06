from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
import os
import html
from typing import Dict, Any, Optional
from urllib.parse import quote

from new_agri_bot_backend.config import bot, LOGISTICS_TELEGRAM_IDS
from new_agri_bot_backend.tables import Users, Submissions, OrderChatMessage
from new_agri_bot_backend.telegram_auth import get_current_telegram_user, check_not_guest

WEBAPP_URL = os.getenv("WEBAPP_URL")


# services/telegram_service.py


async def send_chat_notification(
    telegram_id: int, order_ref: str, message_text: str, sender_name: str, client_name: str = ""
) -> Dict[str, Any]:
    """
    Відправити сповіщення про нове повідомлення в чаті через aiogram

    Args:
        telegram_id: Telegram ID отримувача
        order_ref: Номер заявки
        message_text: Текст повідомлення
        sender_name: Ім'я відправника
        client_name: Назва клієнта

    Returns:
        Dict з результатом відправки
    """

    # 1. Формування URL для відкриття чату
    # Telegram API не приймає 'localhost', замінюємо на '127.0.0.1' для локальної розробки
    base_url = WEBAPP_URL.replace("localhost", "127.0.0.1") if WEBAPP_URL else ""
    chat_link = f"{base_url}/detail/{quote(order_ref)}?openChat=true"

    # 2. Обрізання довгого тексту
    max_length = 100
    preview_text = message_text[:max_length]
    if len(message_text) > max_length:
        preview_text += "..."

    # 3. Екранування HTML
    safe_order_ref = html.escape(order_ref)
    safe_sender = html.escape(sender_name)
    safe_preview = html.escape(preview_text)
    safe_client = html.escape(client_name)

    # 4. Формування тексту повідомлення
    notification_text = (
        f"💬 <b>Нове повідомлення в чаті заявки {safe_order_ref}</b>\n\n"
        f"<b>Клієнт:</b> {safe_client}\n"
        f"<b>Від:</b> {safe_sender}\n"
        f"<b>Повідомлення:</b> {safe_preview}"
    )

    # 5. Створення inline клавіатури (aiogram стиль)
    if chat_link.startswith("https"):
        button = InlineKeyboardButton(text="📱 Відкрити чат", web_app=WebAppInfo(url=chat_link))
    else:
        # Fallback для локальної розробки (HTTP) - відкриває у браузері
        button = InlineKeyboardButton(text="📱 Відкрити чат", url=chat_link)

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[button]]
    )

    # 6. Відправка через aiogram
    try:
        message = await bot.send_message(
            chat_id=telegram_id,
            text=notification_text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=False,
        )

        return {
            "telegram_id": telegram_id,
            "status": "sent",
            "message_id": message.message_id,
        }

    except TelegramForbiddenError:
        # Користувач заблокував бота
        return {
            "telegram_id": telegram_id,
            "status": "blocked",
            "error": "User blocked the bot",
        }

    except TelegramBadRequest as e:
        # Невірний chat_id або інші помилки запиту
        return {
            "telegram_id": telegram_id,
            "status": "failed",
            "error": f"Bad request: {str(e)}",
        }

    except Exception as e:
        # Інші помилки
        return {"telegram_id": telegram_id, "status": "failed", "error": str(e)}


# routers/chat.py
from fastapi import APIRouter, HTTPException, Depends


# from services.telegram_service import send_chat_notification
# from services.notification_service import determine_recipients

router = APIRouter(prefix="/orders", tags=["chat"])


@router.post("/{order_ref}/chat/messages/{message_id}/notify", dependencies=[Depends(check_not_guest)])
async def notify_chat_message(
    order_ref: str, message_id: str, current_user=Depends(get_current_telegram_user)
):
    """Відправити Telegram сповіщення про нове повідомлення"""

    # 1. Отримати повідомлення
    message = await OrderChatMessage.objects().where(
        (OrderChatMessage.id == message_id) & (OrderChatMessage.order_ref == order_ref)
    ).first()

    if not message:
        raise HTTPException(status_code=404, detail="Message not found")

    # Отримати дані про заявку (клієнта)
    order_info = await Submissions.objects().where(
        Submissions.contract_supplement == order_ref
    ).first()
    client_name = order_info.client if order_info and order_info.client else "Невідомий клієнт"

    # 2. Визначити отримувачів
    recipients = await determine_recipients(order_ref, current_user)

    # 3. Відправити сповіщення через aiogram
    notification_results = []
    for recipient_id in recipients:
        result = await send_chat_notification(
            telegram_id=recipient_id,
            order_ref=order_ref,
            message_text=message.message_text,
            sender_name=current_user.full_name_for_orders,
            client_name=client_name,
        )
        notification_results.append(result)

    return {
        "status": "success",
        "recipients_count": len(recipients),
        "results": notification_results,
    }


# services/notification_service.py
from typing import List


async def determine_recipients(order_ref: str, sender: Users) -> List[int]:
    """
    Визначити Telegram ID отримувачів сповіщення

    Логіка:
    - Якщо відправник = менеджер → отримувачі = логісти
    - Якщо відправник = логіст → отримувач = менеджер заявки

    Returns:
        List[int]: Список Telegram ID отримувачів
    """

    # Перевірити чи відправник є логістом
    is_logistics = sender.telegram_id in LOGISTICS_TELEGRAM_IDS

    if is_logistics:
        # Відправник - логіст, знайти менеджера заявки
        manager = await get_order_manager(order_ref)
        return [manager.telegram_id] if manager else []
    else:
        # Відправник - менеджер, повернути всіх логістів
        return LOGISTICS_TELEGRAM_IDS


async def get_order_manager(order_ref: str) -> Optional[Users]:
    """Отримати менеджера заявки"""
    # Припускаємо, що є таблиця Orders з полем manager
    # Адаптуйте під вашу структуру БД

    # Варіант 1: Якщо є таблиця Orders
    order = await Submissions.objects().where(
        Submissions.contract_supplement == order_ref
    ).first()

    if not order or not order.manager:
        return None

    # Знайти користувача за ім'ям менеджера
    user = await Users.objects().where(Users.full_name_for_orders == order.manager).first()

    return user
