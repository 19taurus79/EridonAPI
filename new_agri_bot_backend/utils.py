# app/utils.py
from datetime import datetime, timedelta
from aiogram import Bot
from dotenv import load_dotenv
import os

from .config import MANAGERS_ID, TELEGRAM_BOT_TOKEN, logger, SEND_NOTIFICATIONS  # Импорт из config.py
from .tables import Users

load_dotenv()

# Определяем режим работы: 'production' или 'development'. По умолчанию 'development'.
APP_ENV = os.getenv("APP_ENV", "development")

bot = Bot(TELEGRAM_BOT_TOKEN)


async def send_message_to_managers():
    """Отправляет сообщение всем менеджерам в Telegram или выводит в консоль в зависимости от режима."""
    user_tg_id_list = await Users.select().where(Users.is_allowed == True).run()
    now = datetime.now() + timedelta(hours=3)
    time_format = "%d-%m-%Y %H:%M:%S"
    message_text = f"Дані в боті оновлені.\nІ вони актуальні станом на… {now.strftime(time_format)}"

    logger.info(f"--- Running in {APP_ENV.upper()} mode ---")

    for user in user_tg_id_list:
        telegram_id = user.get("telegram_id")
        if not telegram_id:
            logger.warning(f"Skipping user due to missing telegram_id: {user}")
            continue

        old_status_msg_id = user.get("status_message_id")

        try:
            if SEND_NOTIFICATIONS:
                # 1. Спробуємо видалити старе закріплене повідомлення, щоб не засмічувати чат
                if old_status_msg_id:
                    try:
                        await bot.delete_message(chat_id=telegram_id, message_id=old_status_msg_id)
                    except Exception:
                        pass
                
                # 2. Надсилаємо нове (воно прийде з повідомленням, що приверне увагу)
                msg = await bot.send_message(chat_id=telegram_id, text=message_text)
                
                # 3. Закріплюємо його в шапці
                try:
                    await bot.pin_chat_message(chat_id=telegram_id, message_id=msg.message_id, disable_notification=True)
                except Exception as pin_err:
                    logger.warning(f"Could not pin message in {telegram_id}: {pin_err}")
                
                # 4. Оновлюємо ID в базі, щоб наступного разу його видалити
                await Users.update({Users.status_message_id: msg.message_id}).where(Users.telegram_id == telegram_id).run()
                
                logger.info(f"Successfully updated status message for manager ID: {telegram_id}")
            else:
                # В режиме разработки просто выводим в консоль
                logger.info(f"NOTIFICATIONS DISABLED: Would send to {telegram_id}: '{message_text}'")
        except Exception as e:
            logger.error(f"Failed to handle notification for manager ID {telegram_id}: {e}")


def create_composite_key_from_dict(item: dict, keys: list) -> str:
    """
    Создает стандартизированный композитный ключ из словаря.
    """
    key_parts = []
    for key in keys:
        value = item.get(key)
        key_parts.append(str(value).strip() if value is not None else "")
    return "_".join(key_parts)

async def schedule_message_deletion(chat_id: int, message_id: int, delay_minutes: int = 30):
    """Реєструє повідомлення для видалення через вказаний час"""
    from .tables import ScheduledDeletions
    from datetime import datetime, timedelta
    
    delete_at = datetime.now() + timedelta(minutes=delay_minutes)
    await ScheduledDeletions.insert(
        ScheduledDeletions(
            chat_id=chat_id,
            message_id=message_id,
            delete_at=delete_at
        )
    ).run()
    logger.info(f"🕒 Повідомлення {message_id} у чаті {chat_id} заплановано до видалення на {delete_at}")


import re
from typing import Optional

def extract_order_ref(text: str) -> Optional[str]:
    if not text:
        return None
    # Match TE-XXXXXXXX format (Cyrillic or Latin)
    match = re.search(r'([ТтTt][ЕеEe]-\d+)', text)
    if match:
        ref = match.group(1).upper()
        # Translate Latin T -> Cyrillic Т, Latin E -> Cyrillic Е
        ref = ref.replace('T', 'Т').replace('E', 'Е')
        return ref
    # General fallback for any prefix-digits pattern
    match = re.search(r'([A-Za-zА-Яа-яЁё]+-\d+)', text)
    if match:
        ref = match.group(1).upper()
        ref = ref.replace('T', 'Т').replace('E', 'Е')
        return ref
    return None

async def format_delivery_final_data(delivery_id: int) -> str:
    from .tables import Deliveries, DeliveryItems
    
    # 1. Fetch delivery
    delivery = await Deliveries.objects().where(Deliveries.id == delivery_id).first().run()
    if not delivery:
        return ""
    
    # 2. Fetch items
    items = await DeliveryItems.select().where(DeliveryItems.delivery == delivery_id).run()
    
    # Group items by product to show aggregated info and list of batches
    products_map = {}
    for item in items:
        prod_name = item.get("product") if isinstance(item, dict) else item.product
        qty = item.get("quantity") if isinstance(item, dict) else item.quantity
        party = item.get("party") if isinstance(item, dict) else item.party
        party_qty = item.get("party_quantity") if isinstance(item, dict) else item.party_quantity
        
        if prod_name not in products_map:
            products_map[prod_name] = {
                "total_qty": qty,
                "batches": []
            }
        if party:
            products_map[prod_name]["batches"].append({
                "party": party,
                "qty": party_qty
            })
            
    # Format the message
    lines = []
    lines.append(f"📋 <b>Фактичні дані доставки:</b>")
    lines.append(f"📍 Адреса: <b>{delivery.address or 'Не вказано'}</b>")
    lines.append(f"📅 Дата: <b>{delivery.delivery_date or 'Не вказано'}</b>")
    if delivery.comment:
        lines.append(f"📝 Коментар: <i>{delivery.comment}</i>")
    if delivery.total_weight:
        lines.append(f"⚖️ Вага: <b>{delivery.total_weight} кг</b>")
        
    if products_map:
        lines.append("\n📦 <b>Товари та партії:</b>")
        for prod_name, info in products_map.items():
            lines.append(f"🔹 {prod_name}: <b>{info['total_qty']}</b>")
            for batch in info["batches"]:
                lines.append(f"   └ Партія: <code>{batch['party']}</code> — <b>{batch['qty']}</b>")
            
    return "\n".join(lines)
