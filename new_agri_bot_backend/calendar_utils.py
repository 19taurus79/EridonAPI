import os
from datetime import date, datetime, time
import re
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials.json")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
CALENDAR_ID = "dca9aa4129540be8ec133f20092e7f0a500897595fc1736cd295a739d9dc9466@group.calendar.google.com"  # или укажи явный ID календаря


class ChangeDateRequest(BaseModel):
    new_date: date


def changed_color_calendar_events_by_id(id: str, status: int):
    if status == 1:
        color = "5"
    elif status == 2:
        color = "10"
    try:
        # 1. Подключение к API
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("calendar", "v3", credentials=credentials)

        # 3. Выполнение запроса к API
        events_result = (
            service.events()
            .patch(
                calendarId=CALENDAR_ID,
                eventId=id,
                body={"colorId": color},
            )
            .execute()
        )
        # print(events_result)
        # events = events_result.get("items", [])
        return events_result

    except Exception as e:
        print("Ошибка при получении событий из календаря:", e)
        return None


def changed_date_calendar_events_by_id(id: str, new_date):
    kiev_tz = pytz.timezone("Europe/Kiev")
    # Парсим новую дату из строки в формате ГГГГ-ММ-ДД
    # new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()

    # Формируем datetime для начала (9:00) и конца (10:00) с тайм-зоной
    start_dt = kiev_tz.localize(datetime.combine(new_date, time(9, 0)))
    end_dt = kiev_tz.localize(datetime.combine(new_date, time(10, 0)))
    try:
        # 1. Подключение к API
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("calendar", "v3", credentials=credentials)
        event_data = (
            service.events()
            .get(
                calendarId=CALENDAR_ID,
                eventId=id,
            )
            .execute()
        )
        description = event_data["description"]
        new_description = re.sub(
            r"(Дата доставки:\s*)(\d{4}-\d{2}-\d{2})",
            lambda m: m.group(1) + new_date.strftime("%Y-%m-%d"),
            description,
        )
        # 3. Выполнение запроса к API
        events_result = (
            service.events()
            .patch(
                calendarId=CALENDAR_ID,
                eventId=id,
                body={
                    "start": {
                        "dateTime": start_dt.isoformat(),
                        "timeZone": "Europe/Kiev",
                    },
                    "end": {"dateTime": end_dt.isoformat(), "timeZone": "Europe/Kiev"},
                    "description": new_description,
                },
            )
            .execute()
        )
        # print(events_result)
        # events = events_result.get("items", [])
        return events_result

    except Exception as e:
        print("Ошибка при получении событий из календаря:", e)
        return None
