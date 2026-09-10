import html
import logging
from typing import Optional, List, Set
from datetime import datetime, timedelta
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from ..config import bot, logger, LOGISTICS_TELEGRAM_IDS, SEND_NOTIFICATIONS
from ..tables import Deliveries, DeliveryItems, Users, Accountants, ManagerAccountantGuide
from ..nova_poshta import call_np_api

# Коди статусів Нової Пошти, що означають отримання посилки клієнтом:
# 9  - Відправлення отримано
# 10 - Відправлення отримано. Грошовий переказ видано одержувачу
# 11 - Відправлення отримано. Грошовий переказ очікує видачі одержувачу
# 106 - Одержано (переадресація)
RECEIVED_STATUS_CODES = {"9", "10", "11", "106"}


async def get_accountant_telegram_id_for_delivery(delivery: Deliveries) -> Optional[int]:
    """
    Знаходить Telegram ID закріпленого бухгалтера для менеджера доставки.
    1. Перевіряє зв'язку ManagerAccountantGuide за created_by менеджера.
    2. Якщо не знайдено, шукає користувача за ім'ям менеджера delivery.manager.
    3. Якщо зв'язки немає, повертає дефолтного активного бухгалтера (is_default=True).
    4. Якщо дефолтного немає, повертає першого активного бухгалтера.
    """
    accountant = None
    manager_tg_id = delivery.created_by

    # 1. Пошук через telegram_id менеджера
    if manager_tg_id:
        link = await ManagerAccountantGuide.objects().where(
            ManagerAccountantGuide.manager == manager_tg_id
        ).first().run()
        if link and link.accountant:
            accountant = await Accountants.objects().where(
                (Accountants.id == link.accountant) &
                (Accountants.is_active == True)
            ).first().run()

    # 2. Якщо не знайдено, шукаємо по ПІБ або username менеджера
    if not accountant and delivery.manager:
        mgr_str = delivery.manager.strip()
        user = await Users.objects().where(
            (Users.full_name_for_orders.ilike(mgr_str)) |
            (Users.first_name.ilike(mgr_str)) |
            (Users.username.ilike(mgr_str.lstrip("@")))
        ).first().run()
        if user:
            link = await ManagerAccountantGuide.objects().where(
                ManagerAccountantGuide.manager == user.telegram_id
            ).first().run()
            if link and link.accountant:
                accountant = await Accountants.objects().where(
                    (Accountants.id == link.accountant) &
                    (Accountants.is_active == True)
                ).first().run()

    # 3. Дефолтний бухгалтер
    if not accountant:
        accountant = await Accountants.objects().where(
            (Accountants.is_default == True) &
            (Accountants.is_active == True)
        ).first().run()

    # 4. Будь-який активний бухгалтер
    if not accountant:
        accountant = await Accountants.objects().where(
            Accountants.is_active == True
        ).first().run()

    if accountant and accountant.telegram_id:
        return accountant.telegram_id
    return None


async def check_np_deliveries_status():
    """
    Планова перевірка вручення посилок Нової Пошти за номерами ТТН.
    Запускається за розкладом (Пн-Пт о 10:00, 13:00, 16:00).
    Знаходить усі доставки, де ttn непорожній, а is_received == False.
    При статусі 'Отримано' фіксує отримання в БД та сповіщає:
    - Логістів (LOGISTICS_TELEGRAM_IDS)
    - Менеджера (created_by)
    - Бухгалтера (закріпленого або дефолтного)
    """
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо check_np_deliveries_status.")
        return

    logger.info("🔍 Початок перевірки статусів ТТН Нової Пошти...")

    try:
        min_date = datetime.now().date() - timedelta(days=3)
        min_dt = datetime.now() - timedelta(days=3)
        # Вибираємо доставки з ТТН за останні 3 дні, які ще не відмічені як отримані
        deliveries = await Deliveries.objects().where(
            (Deliveries.ttn.is_not_null()) &
            (Deliveries.ttn != "") &
            (Deliveries.ttn != "Не вказано") &
            (Deliveries.is_received == False) &
            (Deliveries.status != "Видалено") &
            (
                (Deliveries.delivery_date >= min_date) |
                ((Deliveries.delivery_date.is_null()) & (Deliveries.created_at >= min_dt))
            )
        ).run()
    except Exception as e:
        logger.error(f"❌ Помилка вибірки доставок для перевірки ТТН: {e}")
        return

    if not deliveries:
        logger.info("✅ Немає активних доставок НП для перевірки статусу ТТН.")
        return

    logger.info(f"📦 Знайдено {len(deliveries)} доставок з ТТН для перевірки статусу.")

    # Групуємо пачками до 50 штук для одного запиту до API Нової Пошти
    chunk_size = 50
    for i in range(0, len(deliveries), chunk_size):
        chunk = deliveries[i:i + chunk_size]
        ttn_map = {str(d.ttn).strip(): d for d in chunk if d.ttn and str(d.ttn).strip()}

        if not ttn_map:
            continue

        doc_payload = [{"DocumentNumber": ttn, "Phone": ""} for ttn in ttn_map.keys()]

        try:
            np_resp = await call_np_api("TrackingDocument", "getStatusDocuments", {
                "Documents": doc_payload
            })
        except Exception as api_err:
            logger.error(f"❌ Помилка виклику API Нової Пошти при пакетному трекінгу: {api_err}")
            continue

        if not np_resp.get("success"):
            logger.warning(f"⚠️ Відповідь API НП містить помилки: {np_resp.get('errors')}")
            continue

        results = np_resp.get("data", [])
        for track_item in results:
            ttn_num = str(track_item.get("Number") or "").strip()
            delivery = ttn_map.get(ttn_num)
            if not delivery:
                continue

            status_code = str(track_item.get("StatusCode") or "").strip()
            status_text = track_item.get("Status", "")
            status_lower = status_text.lower()

            is_received = (
                status_code in RECEIVED_STATUS_CODES
                or "відправлення отримано" in status_lower
                or "одержано" in status_lower
            )

            # Оновлюємо статус в будь-якому випадку для інформативності
            delivery.np_status = status_text
            delivery.np_status_code = status_code

            if not is_received:
                # Зберігаємо проміжний статус, щоб бачити актуальний стан трекінгу
                try:
                    await delivery.save().run()
                except Exception as save_err:
                    logger.debug(f"Не вдалося оновити проміжний статус ТТН {ttn_num}: {save_err}")
                continue

            # Посилку отримано!
            recipient_datetime_str = track_item.get("RecipientDateTime") or ""
            rec_dt = datetime.now()
            if recipient_datetime_str:
                for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M"):
                    try:
                        rec_dt = datetime.strptime(recipient_datetime_str, fmt)
                        break
                    except ValueError:
                        pass

            delivery.is_received = True
            delivery.received_at = rec_dt

            try:
                await delivery.save().run()
                logger.info(f"🎉 Доставка ID {delivery.id} (ТТН {ttn_num}) позначена як отримана ({rec_dt}).")
            except Exception as e:
                logger.error(f"❌ Помилка збереження статусу отримання для доставки {delivery.id}: {e}")
                continue

            # Формуємо та надсилаємо сповіщення
            await send_delivery_received_notification(delivery, track_item, rec_dt)


async def send_delivery_received_notification(delivery: Deliveries, track_item: dict, received_dt: datetime):
    """
    Надсилає сповіщення про вручення посилки менеджеру, бухгалтеру та логістам.
    """
    try:
        items = await DeliveryItems.objects().where(DeliveryItems.delivery == delivery.id).run()
    except Exception as e:
        logger.warning(f"Не вдалося отримати товари для доставки {delivery.id}: {e}")
        items = []

    safe_client = html.escape(delivery.client or "Невідомий")
    safe_manager = html.escape(delivery.manager or "Невідомий")
    safe_ttn = html.escape(str(delivery.ttn or "").strip())
    warehouse_recipient = track_item.get("WarehouseRecipient") or ""
    warehouse_str = f"\n🏢 <b>Отримано:</b> {html.escape(warehouse_recipient)}" if warehouse_recipient else ""
    date_str = received_dt.strftime("%d.%m.%Y %H:%M")

    items_lines = []
    if items:
        for it in items:
            qty = it.quantity
            qty_str = f"{int(qty)}" if qty is not None and qty == int(qty) else f"{qty}"
            items_lines.append(f"• {html.escape(it.product or 'Товар')} — {qty_str} шт.")
        items_text = "\n".join(items_lines)
    else:
        items_text = "• (товари не вказані)"

    message_text = (
        f"🎉 <b>Посилку Нової Пошти отримано клієнтом!</b>\n\n"
        f"🏷 <b>ТТН:</b> <code>{safe_ttn}</code>\n"
        f"👤 <b>Клієнт:</b> {safe_client}\n"
        f"👨‍💼 <b>Менеджер:</b> {safe_manager}\n"
        f"📅 <b>Час вручення:</b> {date_str}"
        f"{warehouse_str}\n\n"
        f"🛒 <b>Товари:</b>\n"
        f"{items_text}\n\n"
        f"🔗 <a href=\"https://novaposhta.ua/tracking/{safe_ttn}\">Відстежити на сайті Нової Пошти</a>"
    )

    # Збираємо всіх отримувачів (без дублів)
    recipients: Set[int] = set()

    # 1. Логісти
    for log_id in LOGISTICS_TELEGRAM_IDS:
        if log_id:
            recipients.add(int(log_id))

    # 2. Менеджер (автор заявки)
    if delivery.created_by:
        recipients.add(int(delivery.created_by))

    # 3. Закріплений або дефолтний бухгалтер
    accountant_tg_id = await get_accountant_telegram_id_for_delivery(delivery)
    if accountant_tg_id:
        recipients.add(int(accountant_tg_id))

    logger.info(f"📤 Відправка сповіщення про вручення ТТН {safe_ttn} отримувачам: {recipients}")

    for r_id in recipients:
        try:
            await bot.send_message(
                chat_id=r_id,
                text=message_text,
                parse_mode="HTML"
            )
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"⚠️ Помилка надсилання сповіщення про отримання ТТН користувачу {r_id}: {e}")
        except Exception as e:
            logger.error(f"❌ Невідома помилка надсилання сповіщення користувачу {r_id}: {e}")
