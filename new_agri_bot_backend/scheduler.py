import os
import json
import pytz
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .config import bot, logger
from .tables import Events

# Таймзона Київ
KIEV_TZ = pytz.timezone("Europe/Kyiv")

async def send_event_summary(subset="all", day="today"):
    """
    Відправляє звіт про події адміністраторам.
    subset: "all" - всі, "unclosed" - лише зі статусом != 2
    day: "today" - на сьогодні, "tomorrow" - на завтра
    """
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
        if subset == "unclosed":
            return # Не спамити, якщо все закрито
        msg = f"<b>{type_str} {day_str} відсутні.</b>"
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

    for admin_id in admins:
        try:
            await bot.send_message(chat_id=admin_id, text=msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error sending scheduled message to {admin_id}: {e}")

scheduler = AsyncIOScheduler(timezone=KIEV_TZ)

def setup_scheduler():
    # 9:30 - Всі події на сьогодні (Пн-Пт)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=9, minute=30, args=["all", "today"])
    # 15:00 - Незакриті події на сьогодні (Пн-Пт)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=15, minute=0, args=["unclosed", "today"])
    # 17:00 - Події на завтра (Пн-Чт для наступного робочого дня)
    scheduler.add_job(send_event_summary, 'cron', day_of_week='mon-fri', hour=17, minute=0, args=["all", "tomorrow"])
    
    scheduler.start()
    logger.info("Scheduler started with jobs at 9:30, 15:00, 17:00 (Kiev time)")
