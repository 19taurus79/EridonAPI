from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from .config import bot, LOGISTICS_TELEGRAM_IDS, ADMINS_ID, SEND_NOTIFICATIONS
from .tables import DeliveryNotifications, Deliveries
import html
import logging

logger = logging.getLogger("agri_bot")

# Об'єднуємо всіх отримувачів (адмінів та логістів) в один список унікальних ID
# Використовуємо set для унікальності, щоб уникнути подвійних повідомлень
ALL_RECIPIENTS = list(set(LOGISTICS_TELEGRAM_IDS + ADMINS_ID))

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
    """Сповістити всіх логістів та адмінів про нову доставку"""
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо notify_new_delivery.")
        return
    # Якщо доставка створюється зі статусом відмінним від "Створено", "Самовивіз" або "Нова Пошта",
    # то це фактично створення + зміна статусу.
    if delivery.status not in ["Створено", "Самовивіз", "Нова Пошта", ""]:
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
        
        if delivery.status == "Самовивіз":
            header = "🚗 <b>Нова заявка на Самовивіз!</b>"
        elif delivery.status == "Нова Пошта":
            header = "📦 <b>Нова заявка на Нову Пошту!</b>"
        else:
            header = "🆕 <b>Нова заявка на доставку!</b>"

        text = (
            f"{header}\n\n"
            f"👤 Клієнт: <b>{safe_client}</b>\n"
            f"👨‍💼 Менеджер: {safe_manager}\n"
            f"📍 Адреса: {safe_address}\n"
            f"📅 Дата: {delivery.delivery_date}\n"
            f"⚖️ Вага: {delivery.total_weight} кг\n"
            f"📝 Коментар: {html.escape(delivery.comment or '')}"
        )


    
    logger.info(f"🆕 Спроба сповіщення про нову доставку ID: {delivery.id}")
    logger.info(f"👥 Список отримувачів: {ALL_RECIPIENTS}")
    for admin_id in ALL_RECIPIENTS:
        try:
            logger.info(f"📤 Надсилання повідомлення для {admin_id}")
            msg = await bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
            
            # Зберігаємо ID повідомлення для видалення в майбутньому
            new_note = DeliveryNotifications(
                delivery_id=delivery.id,
                telegram_id=admin_id,
                message_id=msg.message_id,
                event_type="created"
            )
            await new_note.save().run()
            logger.info(f"✅ Повідомлення надіслано та збережено в БД. ID запису: {new_note.id}, Message ID: {msg.message_id}")
        except Exception as e:
            logger.error(f"❌ Помилка відправки або збереження повідомлення отримувачу {admin_id}: {e}")

async def _send_and_save_notification(delivery_id: int, text: str, actor_id: int = None, event_type: str = "notification"):
    """Внутрішня функція для розсилки повідомлень всім отримувачам (крім актора)"""
    for admin_id in ALL_RECIPIENTS:
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
            logger.error(f"❌ Помилка відправки повідомлення отримувачу {admin_id}: {e}")


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

async def check_unresolved_deliveries_and_notify(target_day: str):
    """
    Перевіряє заявки зі статусом 'Створено' на сьогодні або завтра
    та надсилає сповіщення менеджеру та адміністраторам/логістам.
    """
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо перевірку завислих заявок.")
        return

    import pytz
    from datetime import datetime, timedelta

    KIEV_TZ = pytz.timezone("Europe/Kyiv")
    now = datetime.now(KIEV_TZ)

    if target_day == "today":
        target_date = now.date()
    elif target_day == "tomorrow":
        target_date = (now + timedelta(days=1)).date()
    else:
        logger.error(f"❌ Невідомий тип дня для перевірки: {target_day}")
        return

    logger.info(f"🔍 Запуск перевірки завислих заявок на дату: {target_date} ({target_day})")

    try:
        # Шукаємо заявки в статусі "Створено" на вказану дату
        deliveries = await Deliveries.objects().where(
            (Deliveries.status == "Створено") &
            (Deliveries.delivery_date == target_date)
        ).run()
    except Exception as e:
        logger.error(f"❌ Помилка отримання заявок з БД: {e}")
        return

    if not deliveries:
        logger.info(f"✅ Завислих заявок на {target_date} не знайдено.")
        return

    logger.info(f"⚠️ Знайдено завислих заявок: {len(deliveries)} на {target_date}")

    for delivery in deliveries:
        safe_client = html.escape(delivery.client)
        safe_manager = html.escape(delivery.manager or "Невідомий")
        
        # Текст для менеджера
        manager_text = (
            f"⚠️ <b>Заявка не прийнята в роботу!</b>\n\n"
            f"👤 Клієнт: <b>{safe_client}</b>\n"
            f"👨‍💼 Менеджер: {safe_manager}\n"
            f"📅 Дата доставки: <b>{delivery.delivery_date}</b>\n"
            f"⚖️ Вага: {delivery.total_weight or 0} кг\n"
            f"📝 Статус: <b>Створено</b>\n\n"
            f"Будь ласка, зв'яжіться з логістом для уточнення деталей доставки."
        )

        # Текст для адміністраторів та логістів
        admin_text = (
            f"⚠️ <b>Заявка не прийнята в роботу!</b>\n\n"
            f"👤 Клієнт: <b>{safe_client}</b>\n"
            f"👨‍💼 Менеджер: {safe_manager}\n"
            f"📅 Дата доставки: <b>{delivery.delivery_date}</b>\n"
            f"⚖️ Вага: {delivery.total_weight or 0} кг\n"
            f"📝 Статус: <b>Створено</b>\n\n"
            f"Будь ласка, перевірте заявку та візьміть її в роботу або зв'яжіться з менеджером."
        )

        # Відправляємо менеджеру
        if delivery.created_by:
            try:
                await bot.send_message(chat_id=delivery.created_by, text=manager_text, parse_mode="HTML")
                logger.info(f"✅ Надіслано сповіщення менеджеру {delivery.created_by} для доставки {delivery.id}")
            except Exception as e:
                logger.error(f"❌ Помилка надсилання сповіщення менеджеру {delivery.created_by} для доставки {delivery.id}: {e}")

        # Відправляємо логістам та адмінам (виключаємо менеджера, якщо він є серед них, щоб не дублювати)
        admins_to_notify = [adm_id for adm_id in ALL_RECIPIENTS if adm_id != delivery.created_by]
        for admin_id in admins_to_notify:
            try:
                await bot.send_message(chat_id=admin_id, text=admin_text, parse_mode="HTML")
                logger.info(f"✅ Надіслано сповіщення адміну/логісту {admin_id} для доставки {delivery.id}")
            except Exception as e:
                logger.error(f"❌ Помилка надсилання сповіщення адміну/логісту {admin_id} для доставки {delivery.id}: {e}")

def get_working_minutes_elapsed(start_dt, end_dt, tz) -> float:
    """
    Рахує кількість робочих хвилин (Пн-Пт, з 09:00 до 18:00 за київським часом)
    між start_dt та end_dt. Обидві дати мають бути aware (в таймзоні tz).
    """
    if start_dt >= end_dt:
        return 0.0

    import datetime as dt_mod
    
    total_seconds = 0.0
    current_date = start_dt.date()
    end_date = end_dt.date()

    while current_date <= end_date:
        if current_date.weekday() < 5:  # Пн-Пт
            work_start = tz.localize(dt_mod.datetime.combine(current_date, dt_mod.time(9, 0, 0)))
            work_end = tz.localize(dt_mod.datetime.combine(current_date, dt_mod.time(18, 0, 0)))

            overlap_start = max(start_dt, work_start)
            overlap_end = min(end_dt, work_end)

            if overlap_start < overlap_end:
                total_seconds += (overlap_end - overlap_start).total_seconds()

        current_date += dt_mod.timedelta(days=1)

    return total_seconds / 60.0

async def check_urgent_pickups_and_notify():
    """
    Перевіряє термінові заявки зі статусом 'Самовивіз' на сьогодні,
    які не були взяті в роботу протягом 30 робочих хвилин (з 09:00 до 18:00 по буднях).
    Надсилає сповіщення менеджеру та адміністраторам.
    """
    if not SEND_NOTIFICATIONS:
        return

    import pytz
    from datetime import datetime, timezone

    KIEV_TZ = pytz.timezone("Europe/Kyiv")
    now_kiev = datetime.now(KIEV_TZ)
    
    # Сповіщення надсилаємо тільки в будні дні
    if now_kiev.weekday() >= 5:
        return

    today_date = now_kiev.date()

    try:
        deliveries = await Deliveries.objects().where(
            (Deliveries.status == "Самовивіз") &
            (Deliveries.delivery_date == today_date)
        ).run()
    except Exception as e:
        logger.error(f"❌ Помилка отримання термінових самовивозів з БД: {e}")
        return

    for delivery in deliveries:
        if not delivery.created_at:
            continue

        # Переводимо naive UTC datetime з БД в aware datetime в KIEV_TZ
        created_at_utc = delivery.created_at.replace(tzinfo=timezone.utc)
        created_at_kiev = created_at_utc.astimezone(KIEV_TZ)

        # Рахуємо робочі хвилини
        working_minutes = get_working_minutes_elapsed(created_at_kiev, now_kiev, KIEV_TZ)

        if working_minutes < 30.0:
            continue

        # Перевіряємо, чи вже надсилалося сповіщення про перевищення 30 хвилин
        already_notified = await DeliveryNotifications.objects().where(
            (DeliveryNotifications.delivery_id == delivery.id) &
            (DeliveryNotifications.event_type == "pickup_warning")
        ).first().run()

        if already_notified:
            continue

        logger.info(f"⚠️ Знайдено завислу термінову заявку 'Самовивіз' ID: {delivery.id} (створена: {delivery.created_at}, робочих хвилин: {working_minutes:.1f})")

        safe_client = html.escape(delivery.client)
        safe_manager = html.escape(delivery.manager or "Невідомий")

        # Текст для менеджера
        manager_text = (
            f"⚠️ <b>Терміновий самовивіз не прийнято в роботу!</b>\n\n"
            f"👤 Клієнт: <b>{safe_client}</b>\n"
            f"👨‍💼 Менеджер: {safe_manager}\n"
            f"📅 Дата доставки: <b>{delivery.delivery_date} (Сьогодні)</b>\n"
            f"⚖️ Вага: {delivery.total_weight or 0} кг\n"
            f"📝 Статус: <b>Самовивіз</b>\n\n"
            f"Заявка була створена більше 30 робочих хвилин тому. Будь ласка, зв'яжіться з логістом."
        )

        # Текст для адміністраторів та логістів
        admin_text = (
            f"⚠️ <b>Терміновий самовивіз не прийнято в роботу!</b>\n\n"
            f"👤 Клієнт: <b>{safe_client}</b>\n"
            f"👨‍💼 Менеджер: {safe_manager}\n"
            f"📅 Дата доставки: <b>{delivery.delivery_date} (Сьогодні)</b>\n"
            f"⚖️ Вага: {delivery.total_weight or 0} кг\n"
            f"📝 Статус: <b>Самовивіз</b>\n\n"
            f"Заявка була створена більше 30 робочих хвилин тому. Будь ласка, обробіть її або зв'яжіться з менеджером."
        )

        # Відправляємо менеджеру та зберігаємо в БД
        if delivery.created_by:
            try:
                msg = await bot.send_message(chat_id=delivery.created_by, text=manager_text, parse_mode="HTML")
                new_note = DeliveryNotifications(
                    delivery_id=delivery.id,
                    telegram_id=delivery.created_by,
                    message_id=msg.message_id,
                    event_type="pickup_warning"
                )
                await new_note.save().run()
                logger.info(f"✅ Надіслано та збережено сповіщення про самовивіз менеджеру {delivery.created_by}")
            except Exception as e:
                logger.error(f"❌ Помилка надсилання сповіщення про самовивіз менеджеру {delivery.created_by}: {e}")

        # Відправляємо адмінам та логістам та зберігаємо в БД (виключаючи менеджера)
        admins_to_notify = [adm_id for adm_id in ALL_RECIPIENTS if adm_id != delivery.created_by]
        for admin_id in admins_to_notify:
            try:
                msg = await bot.send_message(chat_id=admin_id, text=admin_text, parse_mode="HTML")
                new_note = DeliveryNotifications(
                    delivery_id=delivery.id,
                    telegram_id=admin_id,
                    message_id=msg.message_id,
                    event_type="pickup_warning"
                )
                await new_note.save().run()
                logger.info(f"✅ Надіслано та збережено сповіщення про самовивіз адміну/логісту {admin_id}")
            except Exception as e:
                logger.error(f"❌ Помилка надсилання сповіщення про самовивіз адміну/логісту {admin_id}: {e}")



