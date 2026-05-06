import os
import re
import pytz
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from .config import logger
from .models import DeliveryRequest

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials.json")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
# ID календаря
CALENDAR_ID = "dca9aa4129540be8ec133f20092e7f0a500897595fc1736cd295a739d9dc9466@group.calendar.google.com"

def get_calendar_service():
    """Ініціалізує та повертає сервіс Google Calendar."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logger.warning(f"Google Calendar credentials file not found at {SERVICE_ACCOUNT_FILE}")
        return None
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        return build("calendar", "v3", credentials=credentials)
    except Exception as e:
        logger.error(f"❌ Помилка ініціалізації сервісу Google Calendar: {e}")
        return None

async def create_calendar_event(data: DeliveryRequest) -> Optional[Dict]:
    """Створює подію в Google Calendar для нової доставки."""
    service = get_calendar_service()
    if not service: return None

    try:
        # Отримуємо існуючі події на цей день, щоб розрахувати зміщення
        existing_events = get_calendar_events(data.date, data.date)
        offset = len(existing_events)

        # Базовий час — 09:00 ранку + зміщення по 1 хвилині
        delivery_dt = datetime.strptime(data.date, "%Y-%m-%d")
        start_time = delivery_dt.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(minutes=offset)
        end_time = start_time + timedelta(minutes=15)

        lines = [
            f"Контрагент: {data.client}",
            f"Менеджер: {data.manager}",
            f"Адреса: {data.address}",
            f"Контакт: {data.contact}",
            f"Телефон: {data.phone}",
            f"Дата доставки: {data.date}",
            f"Коментар : {data.comment}",
            "",
        ]

        for order in data.orders:
            lines.append(f"📦 Доповнення: {order.order}")
            for item in order.items:
                lines.append(f" • {item.product} — {item.quantity}")
            lines.append("")

        event = {
            "summary": f"🚚 Доставка: {data.client}",
            "location": data.address,
            "description": "\n".join(lines),
            "start": {
                "dateTime": start_time.isoformat(),
                "timeZone": "Europe/Kiev",
            },
            "end": {
                "dateTime": end_time.isoformat(),
                "timeZone": "Europe/Kiev",
            },
            "colorId": "11",
        }

        return service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
    except Exception as e:
        logger.error(f"❌ Помилка при додаванні в календар Google: {e}")
        return None

def get_calendar_events(start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
    """Отримує список подій з календаря в заданому діапазоні дат."""
    service = get_calendar_service()
    if not service: return []

    try:
        now = datetime.utcnow()
        time_min = (now - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"
        time_max = (now + timedelta(days=3)).replace(hour=23, minute=59, second=0, microsecond=0).isoformat() + "Z"

        if start_date:
            time_min = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0).isoformat() + "Z"
        if end_date:
            time_max = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59).isoformat() + "Z"

        result = service.events().list(calendarId=CALENDAR_ID, timeMin=time_min, timeMax=time_max, singleEvents=True, orderBy="startTime").execute()
        return result.get("items", [])
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні подій з календаря Google: {e}")
        return []

def get_calendar_event_by_id(event_id: str) -> Optional[Dict]:
    """Отримує одну подію за її ID."""
    service = get_calendar_service()
    if not service: return None
    try:
        return service.events().get(calendarId=CALENDAR_ID, eventId=event_id).execute()
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні події {event_id} з календаря Google: {e}")
        return None

def changed_color_calendar_events_by_id(event_id: str, status_code: int):
    """Змінює колір події в календарі залежно від статусу."""
    service = get_calendar_service()
    if not service: return None
    
    color = "5" if status_code == 1 else "10" if status_code == 2 else None
    if not color: return None

    try:
        return service.events().patch(calendarId=CALENDAR_ID, eventId=event_id, body={"colorId": color}).execute()
    except Exception as e:
        logger.error(f"❌ Помилка зміни кольору події {event_id}: {e}")
        return None

def changed_date_calendar_events_by_id(event_id: str, new_delivery_date: date):
    """Змінює дату та опис події в календарі."""
    service = get_calendar_service()
    if not service: return None

    try:
        # Отримуємо існуючі події на НОВУ дату, щоб розрахувати зміщення
        new_date_str = new_delivery_date.strftime("%Y-%m-%d")
        existing_events = get_calendar_events(new_date_str, new_date_str)
        offset = len(existing_events)

        # Розраховуємо новий час
        start_time = datetime.combine(new_delivery_date, datetime.min.time()).replace(hour=9, minute=0) + timedelta(minutes=offset)
        end_time = start_time + timedelta(minutes=15)

        event_data = service.events().get(calendarId=CALENDAR_ID, eventId=event_id).execute()
        description = event_data.get("description", "")
        
        new_description = re.sub(
            r"(Дата доставки:\s*)(\d{4}-\d{2}-\d{2})",
            lambda m: m.group(1) + new_date_str,
            description,
        )

        return service.events().patch(
            calendarId=CALENDAR_ID,
            eventId=event_id,
            body={
                "start": {
                    "dateTime": start_time.isoformat(),
                    "timeZone": "Europe/Kiev",
                },
                "end": {
                    "dateTime": end_time.isoformat(),
                    "timeZone": "Europe/Kiev",
                },
                "description": new_description,
            },
        ).execute()
    except Exception as e:
        logger.error(f"❌ Помилка зміни дати події {event_id}: {e}")
        return None

def delete_calendar_event_by_id(event_id: str) -> bool:
    """Видаляє подію з Google Calendar по її ID."""
    service = get_calendar_service()
    if not service: return False

    try:
        service.events().delete(calendarId=CALENDAR_ID, eventId=event_id).execute()
        logger.info(f"✅ Подія {event_id} успішно видалена")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка при видаленні події {event_id}: {e}")
        return False
