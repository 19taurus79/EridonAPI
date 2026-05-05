import asyncio
import pandas as pd
from typing import List, Dict
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from ..config import bot, logger, ADMINS_ID, SEND_NOTIFICATIONS
from ..tables import Submissions, Users
from .send_telegram_notification import send_notification
from .ordered_moved_notifications import split_message_into_chunks

async def check_supplements_and_notify():
    """
    Проверяет все дополнения на статусы и отправляет отчет менеджерам.
    Условия:
    1. different > 0
    2. delivery_status (статус до постачання) содержит "ні" (case-insensitive)
    3. document_status (статус заявки) != "затверджено" (case-insensitive)
    """
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Уведомления отключены (SEND_NOTIFICATIONS=false). Пропускаем проверку дополнений.")
        return

    logger.info("🔍 Начало плановой проверки статусов дополнений...")

    try:
        # Получаем данные из БД
        all_data = await Submissions.select(
            Submissions.manager,
            Submissions.client,
            Submissions.contract_supplement,
            Submissions.nomenclature,
            Submissions.different,
            Submissions.document_status,
            Submissions.delivery_status
        ).where(
            (Submissions.different > 0) &
            (Submissions.delivery_status.ilike("%ні%"))
        ).run()

        if not all_data:
            logger.info("✅ Доповнень з проблемними статусами не знайдено.")
            return

        # Фильтруем в Python для надежной регистронезависимой проверки "не равно затверджено"
        data = [
            row for row in all_data 
            if row['document_status'].strip().lower() != 'затверджено'
        ]

        if not data:
            logger.info("✅ Після фільтрації (документ_статус != затверджено) позицій не залишилося.")
            return

        df = pd.DataFrame(data)
        
        # Видаляємо дублікати, оскільки ми більше не показуємо товар та кількість
        df_unique = df.drop_duplicates(subset=['manager', 'client', 'contract_supplement', 'document_status', 'delivery_status'])
        
        # Группируем по менеджеру
        grouped = df_unique.groupby("manager")
        
        # Дисклеймер на українській мові
        disclaimer = "\n\n⚠️ <i>Можливо, статус вже інший, але на момент оновлення даних він був такий. Потрібно перевірити і, якщо потрібно, змінити.</i>"
        
        admin_report_parts = ["📊 <b>Звіт по доповненнях з некоректними статусами</b>\n" + "="*30 + "\n"]

        for manager_name, group in grouped:
            if not manager_name:
                manager_name = "Невідомий менеджер"

            # Формуємо заголовок та список позицій
            manager_header = f"👤 <b>Менеджер: {manager_name}</b>\n"
            items_list = []
            
            for _, row in group.iterrows():
                item_msg = (
                    f"  👤 <b>Клієнт:</b> {row['client']}\n"
                    f"  📄 <b>Доповнення:</b> <code>{row['contract_supplement']}</code>\n"
                    f"  📝 <b>Статус заявки:</b> {row['document_status']}\n"
                    f"  🚚 <b>Статус до пост.:</b> {row['delivery_status']}\n"
                    + "  " + "-"*20 + "\n"
                )
                items_list.append(item_msg)
            
            # Повне повідомлення для менеджера
            full_msg = f"👋 <b>Доброго дня, {manager_name}!</b>\n\n"
            full_msg += "Знайдено доповнення, які потребують вашої уваги (статус до постачання містить 'ні', статус заявки не 'затверджено'):\n\n"
            full_msg += "".join(items_list)
            full_msg += disclaimer
            
            # Шукаємо telegram_id менеджера
            user = await Users.select(Users.telegram_id).where(Users.full_name_for_orders == manager_name).first().run()
            
            if user and user['telegram_id']:
                chunks = await split_message_into_chunks(full_msg)
                for chunk in chunks:
                    try:
                        await send_notification(bot=bot, chat_ids=[user['telegram_id']], text=chunk, parse_mode="HTML")
                    except Exception as e:
                        logger.error(f"❌ Помилка відправки менеджеру {manager_name} ({user['telegram_id']}): {e}")
            else:
                logger.warning(f"⚠️ Менеджер {manager_name} не знайдений у базі Users або не має telegram_id.")

            # Додаємо детальну інформацію в адмінський звіт
            admin_report_parts.append(manager_header)
            admin_report_parts.append("".join(items_list))
            admin_report_parts.append("\n" + "="*30 + "\n")

        # Відправляємо зведений звіт адмінам
        if ADMINS_ID:
            delete_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑 Видалити", callback_data="delete_msg")]
            ])
            admin_full_msg = "".join(admin_report_parts)
            chunks = await split_message_into_chunks(admin_full_msg)
            for chunk in chunks:
                try:
                    await send_notification(
                        bot=bot, 
                        chat_ids=ADMINS_ID, 
                        text=chunk, 
                        parse_mode="HTML",
                        reply_markup=delete_kb
                    )
                except Exception as e:
                    logger.error(f"❌ Помилка відправки зведеного звіту адмінам: {e}")

    except Exception as e:
        logger.error(f"❌ Ошибка в check_supplements_and_notify: {e}")
