from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from .config import bot, LOGISTICS_TELEGRAM_IDS
from .tables import DeliveryNotifications, Deliveries
import html
import logging

logger = logging.getLogger("agri_bot")

async def delete_delivery_notifications(delivery_id: int):
    """Видалити всі існуючі повідомлення в Telegram для даної доставки"""
    notifications = await DeliveryNotifications.objects().where(
        DeliveryNotifications.delivery_id == delivery_id
    ).run()
    
    for note in notifications:
        try:
            await bot.delete_message(chat_id=note.telegram_id, message_id=note.message_id)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"Не вдалося видалити повідомлення {note.message_id} для {note.telegram_id}: {e}")
        except Exception as e:
            logger.error(f"Помилка при видаленні повідомлення: {e}")
            
    await DeliveryNotifications.delete().where(
        DeliveryNotifications.delivery_id == delivery_id
    ).run()

async def notify_new_delivery(delivery: Deliveries):
    """Сповістити всіх логістів про нову доставку"""
    # Спочатку видаляємо старі, якщо вони були (наприклад, при перестворенні)
    await delete_delivery_notifications(delivery.id)
    
    safe_client = html.escape(delivery.client)
    safe_manager = html.escape(delivery.manager or "Невідомий")
    safe_address = html.escape(delivery.address or "Не вказано")
    
    text = (
        f"🆕 <b>Нова заявка на доставку!</b>\n\n"
        f"👤 Клієнт: <b>{safe_client}</b>\n"
        f"👨‍💼 Менеджер: {safe_manager}\n"
        f"📍 Адреса: {safe_address}\n"
        f"📅 Дата: {delivery.delivery_date}\n"
        f"⚖️ Вага: {delivery.total_weight} кг\n"
        f"📝 Коментар: {html.escape(delivery.comment or '')}"
    )
    
    for admin_id in LOGISTICS_TELEGRAM_IDS:
        try:
            msg = await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            await DeliveryNotifications.insert(
                DeliveryNotifications(
                    delivery_id=delivery.id,
                    telegram_id=admin_id,
                    message_id=msg.message_id,
                    event_type="created"
                )
            ).run()
        except Exception as e:
            logger.error(f"Помилка відправки повідомлення адміну {admin_id}: {e}")

async def notify_delivery_status_change(delivery: Deliveries, status: str, actor_name: str = None):
    """Сповістити про зміну статусу та видалити старі повідомлення"""
    # 1. Видаляємо старі повідомлення ("Нова заявка")
    await delete_delivery_notifications(delivery.id)
    
    # 2. Формуємо текст нового повідомлення
    safe_client = html.escape(delivery.client)
    status_text = ""
    
    if status == "В роботі":
        status_text = "✅ <b>Взято в роботу</b>"
    elif status == "Доставка з ЦО на клієнта":
        status_text = "🏢 <b>Доставка з ЦО</b>"
    elif status == "Виконано":
        status_text = "🎉 <b>Виконано</b>"
    else:
        status_text = f"🔄 Статус змінено на: <b>{status}</b>"

    actor_info = f"\n👤 Хто: <b>{html.escape(actor_name)}</b>" if actor_name else ""
    
    text = (
        f"{status_text}\n\n"
        f"👤 Клієнт: <b>{safe_client}</b>"
        f"{actor_info}\n"
        f"📅 Дата: {delivery.delivery_date}"
    )
    
    # 3. Надсилаємо нове повідомлення всім логістам
    for admin_id in LOGISTICS_TELEGRAM_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            # Тут ми НЕ зберігаємо message_id, бо це повідомлення фінальне/інформаційне
            # і зазвичай не потребує видалення при наступних діях, 
            # або можна додати логіку збереження якщо потрібно видаляти і їх.
        except Exception as e:
            logger.error(f"Помилка відправки оновлення адміну {admin_id}: {e}")
