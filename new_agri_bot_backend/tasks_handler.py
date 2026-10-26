import json
import os.path
from datetime import date, timedelta, datetime, timezone
from pprint import pprint

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from .config import TELEGRAM_BOT_TOKEN, SEND_NOTIFICATIONS, logger
from new_agri_bot_backend.tables import Tasks

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/tasks"]

bot = Bot(TELEGRAM_BOT_TOKEN)


from .cache import db_cache
import uuid


async def get_all_tasks(user):
    """Retrieves tasks from PostgreSQL Piccolo Tasks table.

    Guarantees that local database tasks are always returned regardless of
    Google Tasks API availability, network errors, or expired credentials.
    """
    def date_minus_3_days():
        return datetime.now(timezone.utc) - timedelta(days=3)

    filter_date = date_minus_3_days()

    if user.is_admin:
        tasks = (
            await Tasks.select()
            .where(
                (Tasks.task_status != 2)
                | ((Tasks.created_at > filter_date) & (Tasks.task_status == 2))
            )
            .order_by(Tasks.task_status)
            .order_by(Tasks.created_at, ascending=False)
            .run()
        )
    else:
        tasks = (
            await Tasks.select()
            .where(
                (Tasks.task_creator == user.telegram_id)
                & (
                    (Tasks.task_status != 2)
                    | ((Tasks.created_at > filter_date) | (Tasks.task_status == 2))
                )
            )
            .order_by(Tasks.task_status)
            .order_by(Tasks.created_at, ascending=False)
            .run()
        )
    return tasks


def complete_task(task_id, user):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    service = build("tasks", "v1", credentials=creds)

    task_id = task_id
    tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'

    # Получаем задачу
    task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()
    previous_title = task["title"]
    split_title = previous_title.split("_")
    new_title = f"{split_title[0]} виконав {user.full_name_for_orders}"
    task["title"] = new_title
    # Обновляем статус
    task["status"] = "completed"
    # Можно также установить дату выполнения (RFC3339 формат)
    import datetime

    task["completed"] = datetime.datetime.utcnow().isoformat() + "Z"

    # Отправляем обновления
    updated_task = (
        service.tasks().update(tasklist=tasklist_id, task=task_id, body=task).execute()
    )
    print(
        f"Обновленная задача: {updated_task['title']} со статусом {updated_task['status']}"
    )


def in_progress_task(task_id, user):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    service = build("tasks", "v1", credentials=creds)

    task_id = task_id
    tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'

    # Получаем задачу
    task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()

    # Обновляем статус
    previous_title = task["title"]
    new_title = f"{previous_title}_зараз виконує {user.full_name_for_orders}"
    task["title"] = new_title
    # Можно также установить дату выполнения (RFC3339 формат)
    import datetime

    task["updated"] = datetime.datetime.utcnow().isoformat() + "Z"

    # Отправляем обновления
    updated_task = (
        service.tasks().update(tasklist=tasklist_id, task=task_id, body=task).execute()
    )
    print(
        f"Обновленная задача: {updated_task['title']} со статусом {updated_task['status']}"
    )


def get_task_by_id(task_id):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                SERVICE_ACCOUNT_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_ACCOUNT_FILE, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("tasks", "v1", credentials=creds)
        tasklist_id = "RnFScjhXZHRvVHhhZWN0Sg"  # Например, '@default'
        task = service.tasks().get(tasklist=tasklist_id, task=task_id).execute()
        return task
    except HttpError as err:
        print(err)

    # service = build("tasks", "v1", credentials=creds)

    # task_id = "TVZMWXNPOFAycExNbE4tag"

    # Получаем задачу


async def create_task(date, note, title, user, idempotency_key=None):
    if idempotency_key:
        cache_k = f"task_idemp:{user.telegram_id}:{idempotency_key}"
        cached = db_cache.get(cache_k)
        if cached:
            logger.info(f"Returning cached task for idempotency_key: {idempotency_key}")
            return cached

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    TOKEN_ACCOUNT_FILE = os.path.join(BASE_DIR, "token.json")
    SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials_task.json")
    creds = None
    if os.path.exists(TOKEN_ACCOUNT_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_ACCOUNT_FILE, SCOPES)
        except Exception as e:
            logger.warning(f"Failed to read token.json: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(TOKEN_ACCOUNT_FILE, "w") as token:
                    token.write(creds.to_json())
            except Exception as e:
                logger.warning(f"Could not refresh Google Tasks token: {e}")
                creds = None
        else:
            logger.warning("No valid Google Tasks token and cannot refresh non-interactively")
            creds = None

    actual_task_id = None
    if creds:
        try:
            service = build("tasks", "v1", credentials=creds)
            task = {
                "due": date,
                "notes": note,
                "title": title,
            }
            results = (
                service.tasks()
                .insert(tasklist="RnFScjhXZHRvVHhhZWN0Sg", body=task)
                .execute()
            )
            actual_task_id = results.get("id")
        except Exception as err:
            logger.warning(f"Google Tasks insert failed: {err}")
            actual_task_id = None

    if not actual_task_id:
        actual_task_id = f"local-{uuid.uuid4().hex[:12]}"

    admins_json = os.getenv("ADMINS", "[]")
    try:
        admins = json.loads(admins_json)
    except Exception:
        admins = []

    web_app_url = f"https://telegram-mini-app-six-inky.vercel.app/tasks/{actual_task_id}?from_link=1"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Відкрити", web_app=WebAppInfo(url=web_app_url)
                )
            ]
        ]
    )
    for admin in admins:
        if SEND_NOTIFICATIONS:
            try:
                await bot.send_message(
                    chat_id=admin,
                    text="З'явилось нове завдання",
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as e:
                logger.warning(f"Notification to admin {admin} failed: {e}")
        else:
            logger.info(f"🔇 Сповіщення вимкнено. Не надсилаємо задачу адміну {admin}")

    if SEND_NOTIFICATIONS:
        try:
            user_msg = await bot.send_message(
                chat_id=user.telegram_id, text="Ви надіслали завдання логісту"
            )
        except Exception:
            class FakeMsg:
                def __init__(self):
                    self.chat = type('obj', (object,), {'id': 0})()
                    self.message_id = 0
            user_msg = FakeMsg()
    else:
        logger.info(f"🔇 Сповіщення вимкнено. Користувач {user.telegram_id} надіслав задачу.")
        class FakeMsg:
            def __init__(self):
                self.chat = type('obj', (object,), {'id': 0})()
                self.message_id = 0
        user_msg = FakeMsg()

    await Tasks.insert(
        Tasks(
            task_id=actual_task_id,
            task_creator=user.telegram_id,
            task_creator_name=user.full_name_for_orders,
            task_status=0,
            task=title,
            chat_id=user_msg.chat.id,
            message_id=user_msg.message_id,
        )
    )

    created_result = {
        "id": actual_task_id,
        "task_id": actual_task_id,
        "title": title,
        "notes": note,
        "status": "needsAction",
        "due": date,
    }
    if idempotency_key:
        db_cache.set(cache_k, created_result, ttl=86400)

    return created_result


if __name__ == "__main__":
    # pprint(get_all_tasks())
    complete_task("b2R2X3N3VVNnVUk4RkFBVg")
