from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from .config import bot, LOGISTICS_TELEGRAM_IDS, SEND_NOTIFICATIONS
from .tables import DeliveryNotifications, Deliveries
import html
import logging

logger = logging.getLogger("agri_bot")

async def delete_delivery_notifications(delivery_id: int):
    logger.info(f"🔍 Спроба видалення повідомлень для доставки ID: {delivery_id}")
    notifications = await DeliveryNotifications.objects().where(
        DeliveryNotifications.delivery_id == delivery_id
    ).run()
    
    logger.info(f"🔍 Знайдено повідомлень для видалення: {len(notifications)}")
    
    for note in notifications:
        try:
            logger.info(f"🗑 Видалення повідомлення {note.message_id} у чаті {note.telegram_id}")
            await bot.delete_message(chat_id=note.telegram_id, message_id=note.message_id)
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"⚠️ Не вдалося видалити повідомлення {note.message_id} для {note.telegram_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні повідомлення: {e}")
            
    await DeliveryNotifications.delete().where(
        DeliveryNotifications.delivery_id == delivery_id
    ).run()

async def notify_new_delivery(delivery: Deliveries, actor_name: str = None):
    """Сповістити всіх логістів про нову доставку"""
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо notify_new_delivery.")
        return
    # Якщо доставка створюється зі статусом відмінним від "Створено" або "Самовивіз",
    # то це фактично створення + зміна статусу.
    if delivery.status not in ["Створено", "Самовивіз", ""]:
        # Видаляємо старі, якщо були
        await delete_delivery_notifications(delivery.id)
        # Надсилаємо сповіщення про статус
        await notify_delivery_status_change(delivery, delivery.status, actor_name)
        return

    # Стандартна логіка для нової заявки
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
    
    logger.info(f"🆕 Спроба сповіщення про нову доставку ID: {delivery.id}")
    logger.info(f"👥 Список отримувачів (LOGISTICS_TELEGRAM_IDS): {LOGISTICS_TELEGRAM_IDS}")
    for admin_id in LOGISTICS_TELEGRAM_IDS:
        try:
            logger.info(f"📤 Надсилання повідомлення для {admin_id}")
            msg = await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            
            # Використовуємо .save() для більшої надійності
            new_note = DeliveryNotifications(
                delivery_id=delivery.id,
                telegram_id=admin_id,
                message_id=msg.message_id,
                event_type="created"
            )
            await new_note.save().run()
            logger.info(f"✅ Повідомлення надіслано та збережено в БД. ID запису: {new_note.id}, Message ID: {msg.message_id}")
        except Exception as e:
            logger.error(f"❌ Помилка відправки або збереження повідомлення адміну {admin_id}: {e}")

async def notify_delivery_status_change(delivery: Deliveries, status: str, actor_name: str = None):
    """Сповістити про зміну статусу та видалити старі повідомлення"""
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо notify_delivery_status_change.")
        return
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
            msg = await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            
            # Зберігаємо повідомлення, щоб його можна було видалити при наступних змінах або видаленні
            new_note = DeliveryNotifications(
                delivery_id=delivery.id,
                telegram_id=admin_id,
                message_id=msg.message_id,
                event_type="status_change"
            )
            await new_note.save().run()
            logger.info(f"✅ Статус оновлено та збережено в БД. ID запису: {new_note.id}")

            # Запланувати видалення через 30 хвилин
            from .utils import schedule_message_deletion
            await schedule_message_deletion(chat_id=admin_id, message_id=msg.message_id, delay_minutes=30)
        except Exception as e:
            logger.error(f"❌ Помилка відправки оновлення адміну {admin_id}: {e}")
