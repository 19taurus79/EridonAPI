import os
import json
import pytz
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .config import bot, logger, SEND_NOTIFICATIONS
from .tables import Events

# Таймзона Київ
KIEV_TZ = pytz.timezone("Europe/Kyiv")

async def send_event_summary(subset="all", day="today"):
    """
    Відправляє звіт про події адміністраторам.
    subset: "all" - всі, "unclosed" - лише зі статусом != 2
    day: "today" - на сьогодні, "tomorrow" - на завтра
    """
    if not SEND_NOTIFICATIONS:
        logger.info("🔇 Сповіщення вимкнено (SEND_NOTIFICATIONS=false). Пропускаємо send_event_summary.")
        return

    now = datetime.now(KIEV_TZ)
    if day == "today":
        target_date = now.date()
        day_str = "на сьогодні"
    else:
        target_date = (now + timedelta(days=1)).date()
        day_str = "на завтра"

    query = Events.select().where(Events.start_event == target_date)
    if subset == "unclosed":
        query = query.where(Events.event_status != 2)
        type_str = "📌 Не закриті події"
    else:
        type_str = "📅 Події"

    events = await query.run()
    
    if not events:
        return # Не спамити взагалі, якщо подій немає
    else:
        msg_lines = [f"<b>{type_str} {day_str}:</b>", ""]
        for ev in events:
            status_emoji = "✅" if ev['event_status'] == 2 else ("⏳" if ev['event_status'] == 1 else "🆕")
            msg_lines.append(f"{status_emoji} {ev['event']} (Автор: {ev['event_creator_name']})")
        msg = "\n".join(msg_lines)

    admins_json = os.getenv("ADMINS", "[]")
    try:
        admins = json.loads(admins_json)
    except Exception as e:
        logger.error(f"Error parsing ADMINS env: {e}")
        return

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Видалити", callback_data="delete_msg")]
        ]
    )

    for admin_id in admins:
        try:
            await bot.send_message(chat_id=admin_id, text=msg, parse_mode="HTML", reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error sending scheduled message to {admin_id}: {e}")

async def check_and_delete_messages():
    """
    Перевіряє таблицю ScheduledDeletions та видаляє повідомлення, час яких вийшов.
    """
    from .tables import ScheduledDeletions
    from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

    now = datetime.now()
    expired_messages = await ScheduledDeletions.select().where(
        ScheduledDeletions.delete_at <= now
    ).run()

    if not expired_messages:
        return

    logger.info(f"🧹 Початок очищення застарілих повідомлень ({len(expired_messages)} шт.)")
    
    for msg in expired_messages:
        try:
            await bot.delete_message(chat_id=msg['chat_id'], message_id=msg['message_id'])
            logger.info(f"🗑 Видалено повідомлення {msg['message_id']} у чаті {msg['chat_id']}")
        except (TelegramForbiddenError, TelegramBadRequest) as e:
            logger.warning(f"⚠️ Не вдалося видалити повідомлення {msg['message_id']} у {msg['chat_id']}: {e}")
        except Exception as e:
            logger.error(f"❌ Помилка при видаленні запланованого повідомлення: {e}")
        
        # Видаляємо запис з БД у будь-якому випадку (щоб не пробувати вічно)
        await ScheduledDeletions.delete().where(
            ScheduledDeletions.id == msg['id']
        ).run()

scheduler = AsyncIOScheduler(timezone=KIEV_TZ)

def setup_scheduler():
    # Очищення повідомлень за розкладом (кожної хвилини)
    scheduler.add_job(check_and_delete_messages, 'interval', minutes=1)

    # 9:30 - Всі події на сьогодні (Пн-Пт)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=9, minute=30, args=["all", "today"], misfire_grace_time=60, coalesce=True)
    # 15:00 - Незакриті події на сегодня (Пн-Пт)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=15, minute=0, args=["unclosed", "today"], misfire_grace_time=60, coalesce=True)
    # 17:00 - Події на завтра (Пн-Чт для наступного робочого дня)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=17, minute=0, args=["all", "tomorrow"], misfire_grace_time=60, coalesce=True)
    
    # Перевірка статусів доповнень (9:30, 14:00, 17:00)
    from .services.supplement_check import check_supplements_and_notify
    scheduler.add_job(check_supplements_and_notify, 'cron', day_of_week='mon-fri', hour=9, minute=30, misfire_grace_time=60, coalesce=True)
    scheduler.add_job(check_supplements_and_notify, 'cron', day_of_week='mon-fri', hour=14, minute=0, misfire_grace_time=60, coalesce=True)
    scheduler.add_job(check_supplements_and_notify, 'cron', day_of_week='mon-fri', hour=17, minute=0, misfire_grace_time=60, coalesce=True)

    scheduler.start()
    logger.info("Scheduler started with cleanup, summary, and supplement check jobs.")
