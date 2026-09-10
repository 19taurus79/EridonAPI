import html
import logging
from datetime import datetime, timedelta
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from ..config import bot, logger, SEND_NOTIFICATIONS
from ..tables import Deliveries, DeliveryItems, DeliveryReminders


def is_np_delivery(delivery, old_status: str = None) -> bool:
    """
    Визначає, чи є доставка відправкою Нової Пошти:
    1. Попередній або поточний статус містить 'Нова Пошта' або 'Потрібні дані НП'
    2. В адресі доставки міститься маркер 'нова пошт' або 'нп'
    3. Заповнено ТТН
    """
    if not delivery:
        return False

    status = getattr(delivery, "status", None) or ""
    old_status = old_status or ""
    address = getattr(delivery, "address", None) or ""
    ttn = getattr(delivery, "ttn", None) or ""

    status_lower = status.lower()
    old_status_lower = old_status.lower()
    address_lower = address.lower()

    if any(s in status_lower for s in ["нова пошт", "нп", "потрібні дані нп"]):
        return True
    if any(s in old_status_lower for s in ["нова пошт", "нп", "потрібні дані нп"]):
        return True
    if "нова пошт" in address_lower:
        return True
    if "нп " in address_lower or "нп:" in address_lower or "нп," in address_lower:
        return True
    if str(ttn).strip() and str(ttn).strip() not in ["", "Не вказано", "None"]:
        return True

    return False


async def schedule_np_reminder(delivery_id: int, telegram_id: int, delay_minutes: int = 15):
    """
    Планує нагадування через delay_minutes хвилин для співробітника, який взяв доставку НП в роботу.
    Якщо для цієї заявки вже існували незавершені нагадування (pending або sent),
    вони скасовуються (а якщо повідомлення вже висіло в чаті, воно видаляється),
    після чого створюється нове актуальне нагадування.
    """
    try:
        now = datetime.now()
        remind_at = now + timedelta(minutes=delay_minutes)

        # Знаходимо всі активні нагадування по цій доставці
        existing_reminders = await DeliveryReminders.objects().where(
            (DeliveryReminders.delivery_id == delivery_id) &
            (DeliveryReminders.status.is_in(["pending", "sent"]))
        ).run()

        for rem in existing_reminders:
            if rem.status == "sent" and rem.message_id and rem.telegram_id:
                try:
                    await bot.delete_message(chat_id=rem.telegram_id, message_id=rem.message_id)
                except Exception as e:
                    logger.debug(f"Не вдалося видалити попереднє нагадування {rem.message_id}: {e}")
            rem.status = "cancelled"
            await rem.save().run()

        # Створюємо нове нагадування
        new_reminder = DeliveryReminders(
            delivery_id=delivery_id,
            telegram_id=telegram_id,
            reminder_type="np_movement_check",
            remind_at=remind_at,
            status="pending",
        )
        await new_reminder.save().run()
        logger.info(f"⏰ Заплановано нагадування по доставці НП ID {delivery_id} для {telegram_id} на {remind_at}.")
    except Exception as e:
        logger.error(f"❌ Помилка при плануванні нагадування НП для доставки {delivery_id}: {e}")


async def cancel_np_reminder(delivery_id: int):
    """
    Скасовує всі активні нагадування для доставки (наприклад, якщо доставку переведено у 'Виконано').
    Якщо повідомлення вже було надіслано в Telegram, видаляє його з чату.
    """
    try:
        active_reminders = await DeliveryReminders.objects().where(
            (DeliveryReminders.delivery_id == delivery_id) &
            (DeliveryReminders.status.is_in(["pending", "sent"]))
        ).run()

        for rem in active_reminders:
            if rem.status == "sent" and rem.message_id and rem.telegram_id:
                try:
                    await bot.delete_message(chat_id=rem.telegram_id, message_id=rem.message_id)
                except Exception as e:
                    logger.debug(f"Не вдалося видалити нагадування {rem.message_id} при скасуванні: {e}")
            rem.status = "cancelled"
            await rem.save().run()

        if active_reminders:
            logger.info(f"🚫 Скасовано {len(active_reminders)} нагадувань по доставці ID {delivery_id}.")
    except Exception as e:
        logger.error(f"❌ Помилка при скасуванні нагадувань НП для доставки {delivery_id}: {e}")


async def process_due_reminders():
    """
    Періодична задача (запускається щохвилини через APScheduler).
    Знаходить усі нагадування зі статусом 'pending', час яких настав (remind_at <= now),
    перевіряє статус заявки та надсилає повідомлення користувачу з інлайн-кнопками.
    """
    if not SEND_NOTIFICATIONS:
        return

    now = datetime.now()
    try:
        due_reminders = await DeliveryReminders.objects().where(
            (DeliveryReminders.status == "pending") &
            (DeliveryReminders.remind_at <= now)
        ).run()
    except Exception as e:
        logger.error(f"❌ Помилка вибірки нагадувань з БД: {e}")
        return

    if not due_reminders:
        return

    logger.info(f"🔔 Знайдено {len(due_reminders)} нагадувань по НП для відправки.")

    for rem in due_reminders:
        try:
            # Перевіряємо доставку в БД
            delivery = await Deliveries.objects().where(Deliveries.id == rem.delivery_id).first().run()
            if not delivery or delivery.status == "Виконано":
                rem.status = "cancelled"
                await rem.save().run()
                logger.info(f"Доставка ID {rem.delivery_id} виконана або відсутня. Нагадування скасовано.")
                continue

            # Отримуємо товари по доставці
            items = await DeliveryItems.objects().where(DeliveryItems.delivery == delivery.id).run()

            # Формуємо текст повідомлення
            safe_client = html.escape(delivery.client or "Невідомий")
            safe_manager = html.escape(delivery.manager or "Невідомий")

            items_lines = []
            if items:
                for it in items:
                    qty = it.quantity
                    qty_str = f"{int(qty)}" if qty is not None and qty == int(qty) else f"{qty}"
                    items_lines.append(f"• {html.escape(it.product or 'Товар')} — {qty_str} шт.")
                items_text = "\n".join(items_lines)
            else:
                items_text = "• (товари не вказані)"

            text = (
                f"📦 <b>Нагадування по доставці Нова Пошта №{delivery.id}</b>\n\n"
                f"👤 <b>Клієнт:</b> {safe_client}\n"
                f"👨‍💼 <b>Менеджер:</b> {safe_manager}\n"
                f"🛒 <b>Товари:</b>\n"
                f"{items_text}\n\n"
                f"ℹ️ <i>Робили заявку на доставку Новою Поштою. Перевірте: якщо є переміщення і воно не з ЦО, зробіть рейс. Якщо з ЦО — потрібно смикнути логіста.</i>"
            )

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Готово", callback_data=f"np_remind:done:{rem.id}"),
                        InlineKeyboardButton(text="⏰ Перенести", callback_data=f"np_remind:delay:{rem.id}"),
                    ]
                ]
            )

            msg = await bot.send_message(
                chat_id=rem.telegram_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard,
            )

            rem.message_id = msg.message_id
            rem.status = "sent"
            await rem.save().run()
            logger.info(f"✅ Надіслано нагадування по НП ID {rem.id} для чату {rem.telegram_id} (msg_id: {msg.message_id}).")

        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"⚠️ Помилка Telegram при відправці нагадування {rem.id} до {rem.telegram_id}: {e}")
            rem.status = "cancelled"
            await rem.save().run()
        except Exception as e:
            logger.error(f"❌ Помилка обробки нагадування {rem.id}: {e}")
