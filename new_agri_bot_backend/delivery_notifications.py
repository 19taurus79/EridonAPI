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

async def notify_new_delivery(delivery: Deliveries, actor_name: str = None, custom_text: str = None):
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
    
    if custom_text:
        text = custom_text
    else:
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

async def _send_and_save_notification(delivery_id: int, text: str, actor_id: int = None, event_type: str = "notification"):
    """Внутрішня функція для розсилки повідомлень всім логістам (крім актора)"""
    for admin_id in LOGISTICS_TELEGRAM_IDS:
        if actor_id and admin_id == actor_id:
            continue
            
        try:
            msg = await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            
            new_note = DeliveryNotifications(
                delivery_id=delivery_id,
                telegram_id=admin_id,
                message_id=msg.message_id,
                event_type=event_type
            )
            await new_note.save().run()
            
            # Запланувати видалення через 30 хвилин
            try:
                from .utils import schedule_message_deletion
                await schedule_message_deletion(chat_id=admin_id, message_id=msg.message_id, delay_minutes=30)
            except Exception as e:
                logger.warning(f"⚠️ Не вдалося запланувати видалення повідомлення {msg.message_id}: {e}")
                
        except Exception as e:
            logger.error(f"❌ Помилка відправки повідомлення логісту {admin_id}: {e}")

async def notify_delivery_status_change(delivery: Deliveries, status: str, actor_name: str = None, actor_id: int = None):
    """Повідомити всіх логістів про зміну статусу доставки (крім автора зміни)"""
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо notify_delivery_status_change.")
        return
    
    # 1. Видаляємо всі попередні повідомлення по цій доставці
    await delete_delivery_notifications(delivery.id)
    
    # 2. Формуємо текст
    safe_client = html.escape(delivery.client)
    
    if status == "Виконано":
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
    
    # 3. Надсилаємо
    await _send_and_save_notification(
        delivery_id=delivery.id, 
        text=text, 
        actor_id=actor_id, 
        event_type="status_change"
    )

async def notify_delivery_date_change(delivery: Deliveries, new_date, actor_name: str = None, actor_id: int = None):
    """Повідомити всіх логістів про зміну дати доставки (крім автора зміни)"""
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено. Пропускаємо notify_delivery_date_change.")
        return
        
    # 1. Видаляємо старі
    await delete_delivery_notifications(delivery.id)
    
    # 2. Формуємо текст
    safe_client = html.escape(delivery.client)
    actor_info = f"\n👤 Хто: <b>{html.escape(actor_name)}</b>" if actor_name else ""
    
    text = (
        f"📅 <b>Змінено дату доставки</b>\n\n"
        f"👤 Клієнт: <b>{safe_client}</b>"
        f"{actor_info}\n"
        f"🆕 <b>Дата: {new_date}</b>"
    )
    
    # 3. Надсилаємо
    await _send_and_save_notification(
        delivery_id=delivery.id, 
        text=text, 
        actor_id=actor_id, 
        event_type="date_change"
    )
